#!/bin/bash
# Entry point for the macOS E2E CodeBuild project.
#
# Separate from e2e.sh rather than a branch inside it. e2e.sh runs in a Linux container for
# both the Linux and Windows projects -- the target OS there is a separate EC2 instance the
# fixtures drive over SSM. This script runs *natively* on the CodeBuild MAC_ARM host, so the
# bootstrap has nothing in common with the container's. Keeping them apart means a macOS change
# cannot break the two suites that already work.
#
# WIRING: nothing in this repository invokes this file. The macOS CodeBuild project's inline
# buildspec must run `./pipeline/e2e-macos.sh` -- if it was cloned from the Linux/Windows projects
# it runs `./pipeline/e2e.sh` instead, which on a native Mac goes red for unrelated reasons and is
# indistinguishable from this script's own non-zero exits. The banner below names the file, so
# "the wrong script ran" is visible in the first line of the log rather than inferred.
#
# WHERE TO READ THE RESULT: not the GitHub Actions log. The reusable workflow passes
# hide-cloudwatch-logs: true (aws-deadline/.github reusable_e2e_test.yml:54) and takes no input to
# override it, so GitHub shows only pass or fail. Test output, including which tests failed and the
# agent's own logs, is in CloudWatch for CodeBuild project
# deadline-cloud-worker-agent-mainline-macos-e2e.
#
# The agent installs onto this same host via LocalMacWorker, from deadline-cloud-test-fixtures --
# it configures the agent on the host running the tests instead of provisioning one, which is why
# this needs no SSM and no second instance. A capability probe on this fleet confirmed everything
# that worker requires is permitted here: passwordless sudo, a hidden sub-500 account via dscl,
# dseditgroup GID allocation, an /etc/sudoers.d rule, and a LaunchDaemon bootstrapped and running
# as that account.
set -euo pipefail

# This script creates a local account and group, writes a file in /etc/sudoers.d, and
# bootstraps a system LaunchDaemon. On a developer's Mac that mutates their machine, and an
# interrupted run can leave the artefacts behind, so refuse to run anywhere but a CodeBuild
# host. Override deliberately if you really want it locally.
if [ -z "${CODEBUILD_BUILD_ID:-}" ] && [ -z "${E2E_MACOS_ALLOW_LOCAL:-}" ]; then
    echo "Refusing to run outside CodeBuild: this creates a local user and group, a file in" >&2
    echo "/etc/sudoers.d, and a system LaunchDaemon. Set E2E_MACOS_ALLOW_LOCAL=1 to override." >&2
    exit 1
fi

# The real agent's state, not a probe's. LocalMacWorker installs onto this host and its
# _reset_host_state only removes worker.toml and worker.json inside a run, so on reserved capacity
# every other artefact outlives the build.
AGENT_LAUNCHD_LABEL=com.amazon.deadline.worker-agent
AGENT_WORKER_JSON=/var/lib/deadline/worker.json
AGENT_WORKER_TOML=/etc/amazon/deadline/worker.toml
AGENT_VENV=/opt/deadline/worker

# Per-build accumulations. Each build creates a fresh set of these for worker and queue ids that
# no longer exist afterwards, and on reserved capacity nothing else removes them, so the host
# grows without bound until the volume fills -- which would surface as an unrelated test failing
# in some later build rather than as a disk problem.
#
# /opt/mysessionroot is the session root test_worker_config.py configures on macOS; the other two
# are the agent's own per-queue state.
#
# /var/log/amazon/deadline is deliberately absent, though it grows per queue per build the same
# way. _grab_bootstrap_log reads the agent and bootstrap logs from there after a start failure, so
# a build that wiped them first would destroy the only diagnostic for the failure it is about to
# hit. Log text also grows far more slowly than job working files, so it is the less pressing of
# the two. The LaunchDaemon plist and the /etc/sudoers.d rule are absent for a different reason:
# every start() rewrites both wholesale, so they are self-healing rather than accumulating.
BUILD_RESIDUE=(
    /opt/mysessionroot
    /var/lib/deadline/credentials
    /var/lib/deadline/queues
    /var/root/.aws
)

# Suffix used by test_session_runtime.py's rust_unavailable_worker fixture when it moves
# openjd/model/_v1 aside to make the Rust adapter unloadable. Kept in sync with
# _V1_MOVED_ASIDE_SUFFIX there.
V1_MOVED_ASIDE_SUFFIX=.e2e-moved-aside

# Move back an openjd/model/_v1 that a previous build moved aside and never restored.
#
# The fixture restores it in its own teardown, so this only matters when a build died in between --
# CodeBuild timing it out, or the host being reclaimed mid-test. On EC2 that cannot bite, because the
# instance goes away with the class; here the venv is deliberately preserved across builds, so a tree
# left aside would fail every later rust assertion on this machine, starting with
# TestExplicitModeRouting[rust], which runs before the class that moved it.
#
# find rather than asking python for the package location: with _v1 missing, `import openjd.model`
# is exactly the import that no longer works.
restore_moved_aside_openjd_v1() {
    [ -d "${AGENT_VENV}" ] || return 0
    local moved target
    # -print -quit rather than `| head -1`, and `|| true` on the whole thing. Under the script's
    # `set -euo pipefail` a pipeline here can abort the build before the suite starts, with nothing
    # on stderr: `head` exits at the first line, so `find` can take SIGPIPE and return 141, which
    # pipefail propagates. That only happens when a moved-aside tree is present, which is the one
    # case this function exists for. -print -quit also stops find at the first hit.
    moved="$(sudo -n find "${AGENT_VENV}" -type d -name "_v1${V1_MOVED_ASIDE_SUFFIX}" -print -quit 2>/dev/null || true)"
    [ -n "${moved}" ] || return 0
    target="${moved%${V1_MOVED_ASIDE_SUFFIX}}"
    # A present target means the fixture already restored it and this copy is stale, so keep the
    # good tree and drop the leftover. Removing the target first would delete the working tree and
    # move the stale one into its place.
    if [ -d "${target}" ]; then
        echo "NOTE: ${target} is present, so ${moved} is a stale leftover; removing it." >&2
        sudo -n rm -rf "${moved}" >/dev/null 2>&1 || true
        return 0
    fi
    echo "WARNING: a previous build left ${target} moved aside; restoring it." >&2
    sudo -n mv "${moved}" "${target}" >/dev/null 2>&1 || true
    if [ ! -d "${target}" ]; then
        echo "WARNING: failed to restore ${target}. Tests asserting the rust session runtime will" >&2
        echo "         fail on this host until it is put back." >&2
    fi
}

# Reset what poisons the next build -- a daemon still loaded from a previous run and the config
# and worker id it registered with -- and the per-build residue that would otherwise accumulate
# forever on a host that outlives every build. The account, group and /opt/deadline venv are
# deliberately left: the installer is idempotent over them and reusing them saves minutes.
#
# Best effort throughout. A host with none of this present is the normal case, and a refusal here
# must not fail a build before the suite has had a chance to report anything.
reset_agent_state() {
    sudo -n launchctl bootout "system/${AGENT_LAUNCHD_LABEL}" >/dev/null 2>&1 || true
    sudo -n rm -f "${AGENT_WORKER_JSON}" >/dev/null 2>&1 || true
    sudo -n rm -f "${AGENT_WORKER_TOML}" >/dev/null 2>&1 || true
    for path in "${BUILD_RESIDUE[@]}"; do
        sudo -n rm -rf "${path}" >/dev/null 2>&1 || true
    done
    restore_moved_aside_openjd_v1
}

echo "=== pipeline/e2e-macos.sh: macOS E2E suite ==="
echo "=== host ==="
sw_vers
uname -m
id

# The interpreter LocalMacWorker builds the agent venv from. Logged because the
# TestServiceSelectedFollowsServiceHint xfail is justified by this being 3.9, which caps botocore
# below the release that models AssignedSession.metadata: if a future macOS ships something newer,
# the xfail should be removed rather than left to pass silently as an xpass.
echo "=== agent venv interpreter ==="
/usr/bin/python3 --version 2>&1 || echo "no /usr/bin/python3"

# Is IMDS reachable here? Nothing has established this, and one skip depends on the answer.
# test_worker_requires_no_instance_profile asserts a job is never picked up, which holds on EC2
# because the agent finds an instance profile and refuses to run. It is skipped on macOS on the
# grounds that a Mac has no IMDS so the agent exits instead -- but CodeBuild's reserved capacity is
# EC2 Mac underneath, so IMDS may well answer here and that skip may be hiding a test that works.
#
# Diagnostic only: it prints and never fails the build. --max-time keeps a silent drop from costing
# the build two minutes, and 169.254.169.254 is link-local so this reaches nothing outside the host.
echo "=== IMDS reachability (decides whether the no-instance-profile skip is correct) ==="
IMDS_TOKEN="$(curl -s --max-time 3 -X PUT "http://169.254.169.254/latest/api/token" \
    -H "X-aws-ec2-metadata-token-ttl-seconds: 60" 2>/dev/null || true)"
if [ -n "${IMDS_TOKEN}" ]; then
    echo "IMDS: reachable (IMDSv2 token obtained)"
    echo "IMDS iam/info: $(curl -s --max-time 3 -H "X-aws-ec2-metadata-token: ${IMDS_TOKEN}" \
        -o /dev/null -w '%{http_code}' http://169.254.169.254/latest/meta-data/iam/info 2>/dev/null || echo unknown)"
    echo "  -> a 200 means an instance profile is attached, so the skip on"
    echo "     test_worker_requires_no_instance_profile is wrong and that test may run here."
    echo "  -> a 404 means IMDS answers but no profile is attached, so the agent would not refuse"
    echo "     work and the test would fail rather than being inapplicable. The skip stays."
else
    echo "IMDS: unreachable, so _enforce_no_instance_profile would raise IMDSUnreachableError"
    echo "  -> confirms the reason given on the test_worker_requires_no_instance_profile skip."
fi

# Before the suite, not after: a build killed mid-run cannot clean up after itself, so the next
# build has to assume it inherited a loaded daemon and a stale worker id.
echo "=== resetting agent state left by any previous build ==="
reset_agent_state

# bootout is asynchronous, so give it time to land rather than racing the install below. Only a job
# still loaded after that is wedged, and the installer's own bootstrap retry handles the rest.
for _ in $(seq 1 10); do
    sudo -n launchctl print "system/${AGENT_LAUNCHD_LABEL}" >/dev/null 2>&1 || break
    sleep 1
done
if sudo -n launchctl print "system/${AGENT_LAUNCHD_LABEL}" >/dev/null 2>&1; then
    echo "WARNING: ${AGENT_LAUNCHD_LABEL} is still loaded after bootout. The install will try to" >&2
    echo "         reload it and may fail; if it does, that is host state, not this commit." >&2
fi

# The suite, replacing the capability probe that used to live here. A build on this fleet
# confirmed everything LocalMacWorker's docstring requires is permitted: passwordless sudo, a
# hidden sub-500 account via dscl, dseditgroup GID allocation, an /etc/sudoers.d rule, and a
# LaunchDaemon bootstrapped and running as that account.
#
# The cleanup and stale-state machinery above is kept, not probe leftovers. LocalMacWorker installs
# onto this host and its _reset_host_state only removes worker.toml and worker.json, so on reserved
# capacity every artefact it leaves outlives the build.
cd "$(dirname "$0")/.."

# A venv for the build tooling, not a system-wide pip install. The host's pip3 is Homebrew's and
# enforces PEP 668, so `pip3 install hatch` fails with externally-managed-environment. The obvious
# escape, --break-system-packages, is the wrong one here: this is reserved capacity, so mutating the
# host's Homebrew python is a change every later build inherits, which is the class of problem the
# rest of this script exists to avoid. The venv lives under the build directory, which CodeBuild
# already cleans between builds.
TOOLS_VENV="${CODEBUILD_SRC_DIR:-/tmp}/.e2e-tools"
rm -rf "${TOOLS_VENV}"
python3 -m venv "${TOOLS_VENV}"
"${TOOLS_VENV}/bin/pip" install --upgrade pip
"${TOOLS_VENV}/bin/pip" install --upgrade hatch "virtualenv<21"
# Ahead of the existing PATH so `hatch` below resolves to this venv's copy rather than anything
# preinstalled on the image.
export PATH="${TOOLS_VENV}/bin:${PATH}"
echo "hatch: $(command -v hatch) ($(hatch --version 2>&1))"

# Mirrors e2e.sh rather than shortening to `hatch run e2e:test`. The reusable workflow sets
# TEST_TYPE=WHEEL, and with WORKER_AGENT_WHL_PATH unset conftest installs the PUBLISHED agent
# instead of this commit -- a green run that exercised nothing.
if [ "${TEST_TYPE:-}" = WHEEL ]; then
    hatch build
    hatch env create
    WHL_NAME="$(hatch run metadata name | sed 's/-/_/g')"
    WHL_VERSION="$(hatch run version)"
    export WORKER_AGENT_WHL_PATH="dist/${WHL_NAME}-${WHL_VERSION}-py3-none-any.whl"
    echo "WORKER_AGENT_WHL_PATH=${WORKER_AGENT_WHL_PATH}"
    # Asserted, because the fallback is silent: a glob that resolves to nothing leaves conftest
    # installing the release and the run proves nothing about this commit.
    if [ ! -f "${WORKER_AGENT_WHL_PATH}" ]; then
        echo "built wheel not found at ${WORKER_AGENT_WHL_PATH}; refusing to test the published" >&2
        echo "agent instead of this commit. dist/ holds:" >&2
        ls -1 dist/ >&2 || true
        exit 1
    fi
fi

# Let sudo keep the AWS credential environment, rather than copying credentials to a file.
#
# send_command runs every fixture command through sudo, and sudo's env_reset strips AWS_*. The CLI
# then falls back to IMDS -- this host has it, being EC2 Mac underneath -- and picks up the
# CodeBuild host's own instance profile: an identity in an AWS-owned account with no access to our
# CodeArtifact domain, so the agent install fails AccessDenied on GetAuthorizationToken.
#
# An earlier revision copied the credentials to a file instead. That worked for about 25 minutes
# and then every later install failed the same way, because CodeBuild's build credentials arrive
# through the container endpoint and rotate, while a file is a snapshot that expires. Preserving
# the environment means root resolves credentials exactly as the build user does, refresh included,
# and nothing secret is written to a disk that outlives the build.
#
# Validated with visudo -cf before installing: a malformed file in /etc/sudoers.d breaks sudo for
# every user on the host, which on reserved capacity would outlast this build.
echo "=== allowing sudo to keep the AWS credential environment ==="
SUDOERS_AWS_ENV=/etc/sudoers.d/deadline-e2e-aws-env
cleanup_sudoers_aws_env() {
    sudo -n rm -f "${SUDOERS_AWS_ENV}" >/dev/null 2>&1 || true
}
trap cleanup_sudoers_aws_env EXIT

SUDOERS_TMP="$(mktemp)"
cat > "${SUDOERS_TMP}" <<'SUDOERS'
Defaults env_keep += "AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN"
Defaults env_keep += "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI AWS_CONTAINER_CREDENTIALS_FULL_URI"
Defaults env_keep += "AWS_CONTAINER_AUTHORIZATION_TOKEN AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE"
Defaults env_keep += "AWS_REGION AWS_DEFAULT_REGION"
SUDOERS
if sudo -n visudo -cf "${SUDOERS_TMP}" >/dev/null 2>&1; then
    sudo -n install -m 440 -o root -g wheel "${SUDOERS_TMP}" "${SUDOERS_AWS_ENV}"
    rm -f "${SUDOERS_TMP}"
    echo "identity as root is now:"
    sudo -n aws sts get-caller-identity --query Arn --output text 2>&1 || true
else
    rm -f "${SUDOERS_TMP}"
    echo "WARNING: the sudoers drop-in did not validate and was not installed. The agent install" >&2
    echo "         runs 'aws codeartifact login' under sudo and will fail AccessDenied." >&2
fi

hatch run e2e:test
