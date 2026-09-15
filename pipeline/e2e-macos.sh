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

# Reset only what poisons the next build: a daemon still loaded from a previous run, and the
# config and worker id it registered with. The account, group and /opt/deadline venv are
# deliberately left -- the installer is idempotent over them and reusing them saves minutes.
#
# Best effort throughout. A host with none of this present is the normal case, and a refusal here
# must not fail a build before the suite has had a chance to report anything.
reset_agent_state() {
    sudo -n launchctl bootout "system/${AGENT_LAUNCHD_LABEL}" >/dev/null 2>&1 || true
    sudo -n rm -f "${AGENT_WORKER_JSON}" >/dev/null 2>&1 || true
    sudo -n rm -f "${AGENT_WORKER_TOML}" >/dev/null 2>&1 || true
}

echo "=== pipeline/e2e-macos.sh: macOS E2E suite ==="
echo "=== host ==="
sw_vers
uname -m
id

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

pip3 install --upgrade pip
pip3 install --upgrade hatch "virtualenv<21"

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

hatch run e2e:test
