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
# hide-cloudwatch-logs: true (aws-deadline/.github reusable_e2e_test.yml:54), and it takes no input
# to override it, so GitHub shows only a red X for both outcomes. The OK/DENIED lines and the stderr
# that names the reason are in CloudWatch for CodeBuild project
# deadline-cloud-worker-agent-mainline-macos-e2e. Exit 1 = denied or unproven, exit 2 = all present
# but no tests ran, exit 3 = stale records from an earlier run so nothing was probed; the exit status
# is in the failed phase's context there too.
#
# The worker fixture that would install the agent onto this host does not exist yet: per
# test/e2e/conftest.py:617 deadline-cloud-test-fixtures types the OS as
# Literal["AL2023", "WIN2022"] and still needs a MacInstanceWorker, because the posix worker
# hardcodes an AL2023 AMI and provisions with useradd/groupadd. That external package change is
# the larger of the two blockers to running the suite here; this probe answers the smaller one.
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

PROBE_USER=e2eprobeuser
PROBE_GROUP=e2eprobegroup
# Second group: the installer's job group is created by `dseditgroup -o create`, and `-o create`
# on a record that already exists is a different result, so it cannot reuse PROBE_GROUP.
PROBE_JOB_GROUP=e2eprobejobgroup
PROBE_LABEL=com.amazon.deadline.e2eprobe
# Probe-named directories under the system parents the installer provisions into. NOT the
# installer's own paths: creating /var/lib/deadline here would plant state the leftovers survey
# reports as residue, and those paths belong to the real install.
PROBE_DIR_NAME=e2eprobe.d
PROBE_SYSTEM_PARENTS="/var/log /var/lib /etc"
PROBE_SUDOERS=/etc/sudoers.d/deadline-e2eprobe

# Removes everything this script creates. Idempotent -- every step tolerates absence -- so it
# is safe to run at startup as well as on exit.
#
# The fleet is reserved capacity, so one Mac host serves every build and state survives between
# runs. A leaked account, group or sudoers file is permanent rather than a per-build annoyance,
# and a stray file in /etc/sudoers.d can break sudo host-wide, taking the fleet out for every
# later build.
cleanup() {
    sudo -n rm -f "${PROBE_SUDOERS}" >/dev/null 2>&1 || true
    sudo -n launchctl bootout "system/${PROBE_LABEL}" >/dev/null 2>&1 || true
    # No `launchctl disable` here. enable/disable write opposite entries into the same persistent
    # override database and there is no verb that removes an entry, so disabling would leave a
    # permanent "disabled" record -- more residue than the "enabled" one the probe's own `enable`
    # leaves, which is already equivalent to the no-entry default. It would also be actively
    # harmful if the probe order ever changed: a startup `disable` ahead of a `bootstrap` with no
    # intervening `enable` would make bootstrap fail and be reported as blocking the suite.
    sudo -n rm -f "/Library/LaunchDaemons/${PROBE_LABEL}.plist" >/dev/null 2>&1 || true
    # dscl, not sysadminctl -deleteUser: the records below are created with dscl, and
    # sysadminctl will not reliably remove a dscl-created record.
    sudo -n dscl . -delete "/Users/${PROBE_USER}" >/dev/null 2>&1 || true
    sudo -n dscl . -delete "/Groups/${PROBE_GROUP}" >/dev/null 2>&1 || true
    # dscl -delete rather than `dseditgroup -o delete` for the same reason as above, even though
    # this record is created by dseditgroup.
    sudo -n dscl . -delete "/Groups/${PROBE_JOB_GROUP}" >/dev/null 2>&1 || true
    for parent in ${PROBE_SYSTEM_PARENTS}; do
        sudo -n rm -rf "${parent}/${PROBE_DIR_NAME}" >/dev/null 2>&1 || true
    done
    # :- because cleanup runs once before PROBE_DIR is assigned, and set -u would abort there.
    rm -rf "${PROBE_DIR:-}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Run once up front so each build is self-healing. bash does run the EXIT trap when it takes a
# SIGTERM, so an ordinary cancel or timeout is covered, but a SIGKILL is not -- and on a host
# that persists, one killed build would otherwise leave its artefacts for every later run.
cleanup

# After the startup cleanup, not before: cleanup removes PROBE_DIR, so creating it first left
# every run with no directory to write the plist into, aborting the script before the launchctl
# probes -- with the same exit 1 a completed run gives, so it looked like it had tested them.
PROBE_DIR="$(mktemp -d)"

# Every step in cleanup is `|| true` and a `sudo -n` refusal there is silent, so a predecessor's
# records can survive it. The probes then run against pre-existing state -- `dseditgroup -o create`
# on an existing group in particular reports a different result -- so report it rather than assume
# a clean slate. dscl records only: /etc/sudoers.d is not readable unprivileged, and the launchd
# plist is covered by the survey below.
STALE=""
for record in "/Users/${PROBE_USER}" "/Groups/${PROBE_GROUP}" "/Groups/${PROBE_JOB_GROUP}"; do
    if dscl . -read "${record}" >/dev/null 2>&1; then
        STALE="${STALE} ${record}"
    fi
done
# Terminal, not a warning. A suspect run would otherwise exit 2 ("all capabilities available") on
# stderr nobody sees, and stale state actively inverts a required probe: `dseditgroup -o create`
# against a surviving PROBE_JOB_GROUP fails on the existing record, so the build would exit 1
# blaming host policy for a capability that was never refused. Its own code keeps the three
# outcomes distinguishable by status alone.
if [ -n "${STALE}" ]; then
    echo "exit 3: startup cleanup left:${STALE}" >&2
    echo "        Probes would run against pre-existing records and could report a capability as" >&2
    echo "        denied that was never refused, so nothing is probed. Remove these records, or" >&2
    echo "        find out why sudo is refused on this host, and dispatch again." >&2
    exit 3
fi

echo "=== pipeline/e2e-macos.sh: capability probe, NO TESTS ==="
echo "=== host ==="
sw_vers
uname -m
id

# ---------------------------------------------------------------------------------------
# Capability probe.
#
# This is the whole script for now, and it runs no tests. The installer creates a hidden
# system account and group, writes a sudoers rule, and enables and bootstraps a LaunchDaemon;
# whether a CodeBuild macOS build is permitted to do those is unverified, and it decides
# whether the suite can run here at all. Answering it in two minutes beats inferring it from a
# 180-minute failure whose cause is ambiguous.
#
# Every check mirrors what install_macos.sh actually does, not an equivalent-looking
# alternative -- a probe that passes on a mechanism the installer does not use has cost a build
# and answered nothing.
#
# Scope of an exit 2: the installer's privileged operations -- the account and groups, the sudoers
# rule, the LaunchDaemon, and directory provisioning. Deliberately NOT covered are the things the
# installer's own OPERATOR NOTES (install_macos.sh:910) call environment-dependent and not verifiable
# from a build: TCC/Full Disk Access and Gatekeeper quarantine. Those can still block a real agent on
# this fleet, so exit 2 means "the install's privileged steps are permitted", not "the suite will
# pass".
#
# TO RUN THE REAL SUITE: delete from BEGIN PROBE to END PROBE below -- both markers inclusive,
# and END PROBE is past the exit-status block, not above it -- then replace with the bootstrap
# e2e.sh uses, adapted for a native macOS host. Mirror its TEST_TYPE block, do not shorten it to
# the pip/hatch lines: the reusable workflow sets TEST_TYPE=WHEEL, and with WORKER_AGENT_WHL_PATH
# unset the harness installs the *published* worker agent instead of this commit -- a green run
# that exercised nothing, and one a probe-only build cannot detect:
#
#   cd "$(dirname "$0")/.."     # hatch build and the relative dist/ path both need the checkout
#   pip3 install --upgrade pip
#   pip3 install --upgrade hatch "virtualenv<21"
#   if [ "${TEST_TYPE:-}" = WHEEL ]; then
#       hatch build
#       hatch env create
#       export WORKER_AGENT_WHL_PATH=dist/$(hatch run metadata name | sed 's/-/_/g')-$(hatch run version)-py3-none-any.whl
#   fi
#   hatch run e2e:test
#
# Cutting at the "PROBE COMPLETE" banner instead would keep the trailing `if` while deleting the
# REQUIRED_FAILURES it reads: under set -u that aborts *after* the suite has run, turning a green
# 180-minute suite into a failed build.
# ------------------------------- BEGIN PROBE -------------------------------------------

REQUIRED_FAILURES=0
# Counted separately from denials: a section that never ran is not evidence the host refuses it, but
# it is equally not the "all required capabilities available" that exit 2 asserts. Reporting them
# apart keeps "denied" and "unproven" from being conflated in the one number a maintainer reads.
UNPROVEN=0
skip_required() {
    echo "SKIPPED: ${1} -- not probed because ${2}."
    UNPROVEN=$((UNPROVEN + 1))
}

# Reports a check and records whether a required one failed, so the build's exit status carries
# the answer rather than leaving it to a human reading the log. Prints stderr on failure: for
# the sudo checks the reason *is* the answer -- "no tty present and no askpass program", "a
# password is required" and an SIP/TCC denial are different problems with different fixes, and
# on a single reserved host re-running to get the detail costs another build.
#
# Always returns 0. As an `if`/`&&` tail its status would become the function's, and under
# `set -e` a check failing with no stderr would abort before the later probes -- which are the
# ones that answer whether the suite can work at all.
# PROBE_LAST_OK carries the most recent check's outcome, so a later decision can be driven by the
# recorded verdict instead of re-running its own lookup and drifting from it.
PROBE_LAST_OK=0
probe() {
    local required="$1" label="$2" out
    shift 2
    printf '%-48s' "${label}"
    if out="$("$@" 2>&1 >/dev/null)"; then
        PROBE_LAST_OK=1
        echo "OK"
    else
        PROBE_LAST_OK=0
        if [ "${required}" = required ]; then
            echo "DENIED  <-- blocks the suite"
            REQUIRED_FAILURES=$((REQUIRED_FAILURES + 1))
        else
            echo "denied (informational)"
        fi
        if [ -n "${out}" ]; then
            printf '%s\n' "${out}" | head -3 | sed 's/^/    /'
        fi
    fi
    return 0
}

echo "=== sudo and the tools the installer calls ==="
probe required "sudo, non-interactive"          sudo -n true
probe required "dscl present"                   command -v dscl
probe required "dseditgroup present"            command -v dseditgroup
probe required "visudo present"                 command -v visudo
probe required "read /Library/LaunchDaemons"    sudo -n ls /Library/LaunchDaemons
probe required "launchctl print system"         sudo -n launchctl print system

# install_macos.sh:480 records a DESIGN CHOICE to use dscl rather than `sysadminctl -addUser`,
# because sysadminctl auto-assigns a UID >= 501 and cannot force a hidden sub-500 system UID.
# So the permission surface that matters is writing attributes into the local Directory
# Services node -- especially UniqueID below 500 -- and not sysadminctl at all.
#
# Mirrors find_unused_system_id (install_macos.sh:192): the union of both namespaces, searched
# downward from 499, once per principal so the group and the user get distinct ids. A hardcoded
# number would be a shared id the installer goes out of its way not to create, and could already
# be taken on the CodeBuild image -- dscl does not enforce uniqueness, so `id` could then resolve
# to the other record and report DENIED on a host that is actually fine. On a one-host reserved
# fleet each answer costs a build, so a false negative is expensive. Exercising the search also
# means a host with [200,500) exhausted shows up now rather than mid-install.
find_unused_system_id() {
    local used candidate exclude="${1:-}"
    used=$( { dscl . -list /Users UniqueID; dscl . -list /Groups PrimaryGroupID; } 2>/dev/null \
        | awk 'NF>1 {print $NF}' | sort -n -u)
    for candidate in $(seq 499 -1 200); do
        [ "${candidate}" = "${exclude}" ] && continue
        if ! grep -qx "${candidate}" <<< "${used}"; then
            echo "${candidate}"
            return 0
        fi
    done
    return 1
}

echo "=== create the hidden system account the way the installer does (dscl) ==="
# Explicit exclude rather than relying on the group write landing first: if that probe is denied
# the second search would otherwise hand back the same id.
PROBE_GID=""
PROBE_UID=""
# Initialised here, not only inside the block below: on the skip path it is never assigned there, and
# `set -u` would abort at the launchd guard rather than reporting the section as unproven.
ACCOUNT_READY=0
printf '%-48s' "find two unused system ids < 500"
if PROBE_GID="$(find_unused_system_id)" && PROBE_UID="$(find_unused_system_id "${PROBE_GID}")"; then
    echo "OK (gid=${PROBE_GID} uid=${PROBE_UID})"
else
    echo "DENIED  <-- blocks the suite"
    echo "    no free id in [200,500); the installer's own search would fail here too"
    REQUIRED_FAILURES=$((REQUIRED_FAILURES + 1))
fi

if [ -n "${PROBE_GID}" ] && [ -n "${PROBE_UID}" ]; then
probe required "dscl create group"              sudo -n dscl . -create "/Groups/${PROBE_GROUP}"
probe required "dscl set group PrimaryGroupID"  sudo -n dscl . -create "/Groups/${PROBE_GROUP}" PrimaryGroupID "${PROBE_GID}"
probe required "dscl create user"               sudo -n dscl . -create "/Users/${PROBE_USER}"
probe required "dscl set UniqueID below 500"    sudo -n dscl . -create "/Users/${PROBE_USER}" UniqueID "${PROBE_UID}"
probe required "dscl set PrimaryGroupID"        sudo -n dscl . -create "/Users/${PROBE_USER}" PrimaryGroupID "${PROBE_GID}"
probe required "dscl set NFSHomeDirectory"      sudo -n dscl . -create "/Users/${PROBE_USER}" NFSHomeDirectory /var/empty
probe required "dscl set UserShell"             sudo -n dscl . -create "/Users/${PROBE_USER}" UserShell /usr/bin/false
probe required "dscl set IsHidden"              sudo -n dscl . -create "/Users/${PROBE_USER}" IsHidden 1
probe required "dscl set Password '*'"          sudo -n dscl . -create "/Users/${PROBE_USER}" Password '*'
probe required "user resolves"                  id "${PROBE_USER}"
# From the recorded verdict, not a fresh `id`: the two agree today only because they are the same
# command, and would silently diverge if this check ever became narrower (asserting the UID is below
# 500, say, which is the property the installer actually depends on).
ACCOUNT_READY="${PROBE_LAST_OK}"
# Informational, not required: the installer never runs `dseditgroup -o edit -a` against the user's
# dedicated primary group -- membership there is implicit via PrimaryGroupID (install_macos.sh:510).
# Kept because this record is a bare `dscl . -create` and so lacks the GeneratedUID that
# dseditgroup writes on create, making it the harder case; but a refusal here must not block the
# suite, since nothing in the install depends on it.
probe optional "dseditgroup add to primary group" sudo -n dseditgroup -o edit -a "${PROBE_USER}" -t user "${PROBE_GROUP}"
else
    # Counted, not silent: skipping the block would otherwise leave the id-search failure as the
    # only increment, so the log would read "1 denied" for a host where a dozen capabilities were
    # never exercised, and the skip messages below would point at a DENIED line never printed.
    skip_required "the dscl account sequence" "no free system id was available"
fi

# The job group is the one group the installer does NOT create with dscl: install_macos.sh:577
# uses `dseditgroup -o create`, and line 574 notes it "allocates a system GID and creates the
# group record". Allocating a GID goes through the Directory Services allocation path rather than
# writing a caller-chosen attribute, so it can be permitted or refused independently of the dscl
# writes above -- and `-o edit -a` only proves membership can be added to a group that exists.
# A false OK here fails the install at job-group creation 100+ minutes into a build.
probe required "dseditgroup -o create group"    sudo -n dseditgroup -o create "${PROBE_JOB_GROUP}"

# The installer's only `-o edit -a` is at install_macos.sh:596 and targets the JOB group, guarded by
# the `-o checkmember` at :594. That is the pairing to mirror: adding a member to a GID-allocated
# dseditgroup record is not the same operation as adding one to a hand-written dscl record, and it
# is the step immediately after the group creation above -- so an OK there followed by a membership
# failure is exactly the 100+ minute install failure this section exists to rule out. checkmember
# is probed too because it is the installer's idempotence guard, and because it is the only thing
# that confirms the add actually took rather than merely exiting zero.
if [ "${ACCOUNT_READY}" -eq 1 ]; then
probe required "dseditgroup add user to job group" \
    sudo -n dseditgroup -o edit -a "${PROBE_USER}" -t user "${PROBE_JOB_GROUP}"
probe required "dseditgroup -o checkmember" \
    sudo -n dseditgroup -o checkmember -m "${PROBE_USER}" "${PROBE_JOB_GROUP}"
else
    skip_required "job group membership" "the probe account does not exist"
fi

# install_macos.sh:630-679 provisions /var/log/amazon/deadline, /var/lib/deadline/{queues,
# credentials}, the session root and /etc/amazon/deadline. Each is a `mkdir -p` plus a `chown` to the
# sub-500 account under a top-level system directory, and none of it was probed -- so a host that
# permits the Directory Services and launchd writes but refuses a chown under /var/log would exit 2
# as "all required capabilities available" and then fail the real install at log provisioning, which
# is the ambiguous 100+ minute failure this file exists to pre-empt.
#
# Gated on ACCOUNT_READY because the chown target is the probe account: without it this would report
# a DENIED whose cause is the probe's own missing user.
echo "=== provision directories under the parents the installer writes to ==="
if [ "${ACCOUNT_READY}" -eq 1 ]; then
    for parent in ${PROBE_SYSTEM_PARENTS}; do
        probe required "mkdir+chown+chmod under ${parent}" sudo -n bash -c '
            set -e
            d="'"${parent}"'/'"${PROBE_DIR_NAME}"'"
            mkdir -p "${d}/nested"
            chown -R "'"${PROBE_USER}"':'"${PROBE_GROUP}"'" "${d}"
            chmod -R 750 "${d}"
            chmod 700 "${d}/nested"'
    done
else
    skip_required "directory provisioning" "the probe account does not exist, so the chown cannot be exercised"
fi

# install_macos.sh writes /etc/sudoers.d/deadline-worker-shutdown through install_sudoers_file
# (:228): mktemp, chmod 440, `visudo -cf` to validate, then an atomic mv, chown root:wheel, chmod
# 440. Mirrored here so a refusal shows up now rather than mid-install. Named for the probe, and
# removed by cleanup, so it can never be confused with the real rule.
#
# The rule names PROBE_USER, not root, because the installer's rule names the agent account
# (install_macos.sh:614). root is the one user guaranteed to resolve, so validating it would skip the
# property that can actually differ here: whether `visudo -cf` accepts a rule naming a freshly
# created hidden sub-500 account, which depends on the sudo build and on how quickly Directory
# Services publishes a dscl-created record. That makes the check depend on the account, hence the
# ACCOUNT_READY gate.
#
# `mkdir -p /etc/sudoers.d` first, as the installer does at :612: without it a host lacking the
# directory would report DENIED for a reason that is the probe's own rather than a host refusal.
echo "=== write and validate a file in /etc/sudoers.d ==="
if [ "${ACCOUNT_READY}" -eq 1 ]; then
probe required "mkdir + visudo -cf + atomic mv" sudo -n bash -c '
    set -e
    mkdir -p /etc/sudoers.d
    t="$(mktemp)"
    printf "%s\n%s\n" "# Allow '"${PROBE_USER}"' user to shutdown the system" \
        "'"${PROBE_USER}"' ALL=(root) NOPASSWD: /sbin/shutdown -h now" > "$t"
    chmod 440 "$t"
    visudo -cf "$t" >/dev/null
    mv "$t" '"${PROBE_SUDOERS}"'
    chown root:wheel '"${PROBE_SUDOERS}"'
    chmod 440 '"${PROBE_SUDOERS}"
else
    skip_required "the sudoers rule" "the probe account does not exist, and the rule names it"
fi

# The plist is written inside mktemp -d: 0700 and unpredictable. A fixed path under /tmp would
# be a root-escalation primitive here, because /tmp is world-writable and the host persists
# between builds -- any local process could pre-plant a symlink or rewrite the file between the
# write and the `sudo cp`, and the content would then be bootstrapped as root.
#
# `launchctl enable` is probed separately: install_macos.sh:848 runs it unconditionally on every
# install, ahead of the bootstrap that is gated on --start, and it mutates the persistent
# launchd override database, so it can be refused independently.
#
# UserName and WorkingDirectory are set because the installer sets both (install_macos.sh:790,792)
# and their absence makes this a strictly easier check than the real one. With UserName, launchd
# must resolve and adopt a hidden sub-500 uid; without it the job just runs as root, and a host
# policy can permit the second while refusing the first. WorkingDirectory covers the chdir the
# installer's own comment (line 742) calls fatal rather than cosmetic when the value is unusable.
# RunAtLoad means bootstrap actually spawns the job, so a uid launchd will not adopt surfaces here
# rather than 100+ minutes into an install.
echo "=== enable and bootstrap a LaunchDaemon ==="
# Explicit failure: a bare redirection aborts under set -e with the same exit 1 a completed run
# gives, so a breakage here would masquerade as a finished probe.
if ! cat >"${PROBE_DIR}/probe.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>${PROBE_LABEL}</string>
  <key>UserName</key><string>${PROBE_USER}</string>
  <key>WorkingDirectory</key><string>/var/empty</string>
  <key>ProgramArguments</key><array><string>/bin/sleep</string><string>10</string></array>
  <key>RunAtLoad</key><true/>
</dict></plist>
PLIST
then
    echo "could not write the probe plist into ${PROBE_DIR}" >&2
    exit 1
fi
# install, not cp: launchd REJECTS a group- or other-writable plist (install_macos.sh:819), and
# `cat >` gives 0666 & ~umask -- 666 under a umask of 000, which CI environments do set. cp
# without -p derives the destination mode from that source, so bootstrap would refuse the file and
# the probe would report DENIED for a reason that is the probe's own, not the host's. install also
# does the copy, chown and chmod in one privileged step, leaving no window at the real path.
if [ "${ACCOUNT_READY}" -eq 1 ]; then
probe required "install plist root:wheel 0644" \
    sudo -n install -o root -g wheel -m 644 "${PROBE_DIR}/probe.plist" "/Library/LaunchDaemons/${PROBE_LABEL}.plist"
probe required "launchctl enable" \
    sudo -n launchctl enable "system/${PROBE_LABEL}"
# Ten attempts, then one unguarded call, mirroring install_macos.sh:860-885. The startup cleanup
# runs `launchctl bootout`, which is asynchronous, and the installer's own comment says bootstrapping
# while the old instance is still unloading fails transiently ("service already loaded" / EIO). A
# one-shot probe would report that race as DENIED and read as a clean "launchd refuses this" -- and
# a host where bootstrap succeeds on the third attempt is a host where the install works. The final
# unguarded call is what leaves the real launchd error on stderr for probe to print, which is why
# the installer re-runs it unguarded too.
probe required "launchctl bootstrap system" sudo -n bash -c '
    plist="/Library/LaunchDaemons/'"${PROBE_LABEL}"'.plist"
    for _ in $(seq 1 10); do
        launchctl bootstrap system "${plist}" 2>/dev/null && exit 0
        sleep 1
    done
    launchctl bootstrap system "${plist}"'

# bootstrap returning 0 only means the job is LOADED; RunAtLoad spawns it asynchronously, so a uid
# launchd will not adopt surfaces a moment later as a spawn failure. That is the capability the
# plist's UserName exists to test, so it feeds REQUIRED_FAILURES rather than being printed as free
# text -- with hide-cloudwatch-logs: true the exit status is the only signal a maintainer sees, and
# an uncounted spawn failure would exit 2 ("all required capabilities available").
#
# Field names are launchctl's own: `state = running`, `last exit code = N`, and `(never exited)`
# before the first run. /bin/sleep 10 gives a 10s window where a healthy job reads running; polling
# rather than reading once avoids calling a healthy host DENIED because print raced the spawn.
probe required "daemon spawned as the probe user" sudo -n bash -c '
    label="system/'"${PROBE_LABEL}"'"
    for _ in $(seq 1 15); do
        if ! out="$(launchctl print "${label}" 2>&1)"; then
            # print failing outright is itself the answer: a hard spawn failure can leave no job.
            printf "%s\n" "${out}" >&2
            exit 1
        fi
        if printf "%s" "${out}" | grep -q "state = running"; then
            exit 0
        fi
        code="$(printf "%s" "${out}" | sed -n "s/.*last exit code = \([0-9][0-9]*\).*/\1/p" | head -1)"
        if [ -n "${code}" ]; then
            [ "${code}" = 0 ] && exit 0
            echo "job ran but exited ${code} -- launchd did not run it as the probe user" >&2
            printf "%s" "${out}" | grep -E "state|last exit code|runs" >&2
            exit 1
        fi
        sleep 1
    done
    echo "job never left waiting after 15s" >&2
    printf "%s" "${out}" | grep -E "state|last exit code|runs" >&2
    exit 1'
# Informational dump kept alongside the verdict: on a DENIED the full state is what a human needs.
sudo -n launchctl print "system/${PROBE_LABEL}" 2>&1 \
    | grep -E 'state = |last exit code|runs = |pid = ' | head -5 || true
else
    skip_required "the LaunchDaemon sequence" "the probe account does not exist, so bootstrapping as it would prove nothing"
fi

# Anything here that is not this build's own probe artefacts came from an earlier run, and is a
# leftover the suite would have to reset. A host-state reset on the MacInstanceWorker fixture
# (not yet written -- see the header) would need to cover each of these. Includes the probe's own
# names so a leak that survived a refused startup cleanup is still visible -- but tags them, since
# the startup cleanup means a matching name is otherwise always this build's own and an untagged
# list would make every clean run look like it had leftovers.
echo "=== leftovers on this host ==="
PATTERN='deadline|job|worker|e2eprobe'
# The names this run creates itself. Tagged in the output so "is this host clean?" does not require
# knowing them by heart, and so a real leftover from a previous installer run stands out.
SELF_NAMES="${PROBE_USER}|${PROBE_GROUP}|${PROBE_JOB_GROUP}|${PROBE_LABEL}|${PROBE_SUDOERS##*/}"

# Separates the producer's status from the match. `cmd | grep || echo none` cannot tell them
# apart -- grep exits 1 both when the host is clean and when the command was refused, so a
# denied `sudo -n ls /etc/sudoers.d` would print the all-clear for the one artefact that can
# break sudo host-wide. A false all-clear also propagates: this survey is the input to what that
# fixture's host-state reset will have to cover.
survey() {
    local label="$1" out matches
    shift
    if ! out="$("$@" 2>&1)"; then
        printf '    %-22s COULD NOT CHECK -- %s\n' "${label}:" "$(printf '%s' "${out}" | head -1)"
    elif matches="$(printf '%s\n' "${out}" | grep -iE "${PATTERN}")"; then
        printf '    %s\n' "${label}:"
        printf '%s\n' "${matches}" \
            | sed -E "s|^|        |; /${SELF_NAMES}/ s|\$|   <- this build, not a leftover|"
    else
        printf '    %-22s none\n' "${label}:"
    fi
}
survey "LaunchDaemons" ls /Library/LaunchDaemons/
survey "sudoers files" sudo -n ls /etc/sudoers.d/
survey "users"         dscl . -list /Users
survey "groups"        dscl . -list /Groups

# Per path, not `ls -d a b c`: that exits non-zero when any one is absent, so the survey helper
# would report COULD NOT CHECK on the normal case of a clean host.
STATE_DIRS_FOUND=0
for d in /var/lib/deadline /var/lib/deadline-worker /etc/amazon/deadline /var/log/amazon/deadline \
         /var/log/"${PROBE_DIR_NAME}" /var/lib/"${PROBE_DIR_NAME}" /etc/"${PROBE_DIR_NAME}"; do
    if [ -e "${d}" ]; then
        if [ "${STATE_DIRS_FOUND}" -eq 0 ]; then
            printf '    %s\n' "state dirs:"
        fi
        echo "        ${d}"
        STATE_DIRS_FOUND=1
    fi
done
if [ "${STATE_DIRS_FOUND}" -eq 0 ]; then
    printf '    %-22s none\n' "state dirs:"
fi

echo "=== toolchain baseline ==="
probe optional "python3 present"                command -v python3
python3 --version 2>&1 || true
probe optional "pip3 present"                   command -v pip3
probe optional "brew present"                   command -v brew

echo ""
echo "=== PROBE COMPLETE ==="
# Every outcome is non-zero -- a green "macOS E2E Test" that ran no tests asserts nothing, and would
# be actively misleading once the push trigger is added -- but the codes are distinct so the exit
# status carries the answer without anyone reading the log: 1 denied or unproven, 2 all present and
# no tests run, 3 stale host state so nothing was probed.
if [ "${REQUIRED_FAILURES}" -gt 0 ] || [ "${UNPROVEN}" -gt 0 ]; then
    echo "exit 1: ${REQUIRED_FAILURES} required check(s) DENIED, ${UNPROVEN} section(s) unproven." >&2
    echo "        Denied means the host refused it; unproven means a prerequisite failed so it was" >&2
    echo "        never exercised. Either way the suite cannot run here as written." >&2
    exit 1
fi
echo "exit 2: all required capabilities available, and this build ran NO TESTS -- it is a probe."
echo "Replace the probe block in pipeline/e2e-macos.sh with the suite; see the comment there."
exit 2
# -------------------------------- END PROBE --------------------------------------------
