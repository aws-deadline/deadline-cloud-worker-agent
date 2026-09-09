#!/usr/bin/env bash

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

#
# AWS Deadline Cloud Worker Agent Installer (macOS)
#
# This is the macOS port of install.sh. It mirrors the Linux installer's flag surface and
# overall structure so the deadline_worker_agent.installer dispatcher can invoke it identically
# (same getopt arguments, including --python-interpreter-path and --scripts-path).
#
# The installer:
#     1.  Creates a hidden system OS user for the worker agent if required (via Directory Services)
#     2.  Creates an OS group for all job users if required (via Directory Services)
#     3.  Provisions directories used by the worker agent at runtime (identical to Linux).
#     4.  Creates an agent configuration file if required and installs an example configuration file.
#     5.  Updates the configuration file with arguments passed to the installer.
#     6.  Creates, enables, and (optionally) starts a launchd LaunchDaemon that runs the worker
#         agent and restarts it upon failure.
#
# PORTING NOTES (macOS differs from Linux):
#   * Users/groups: dscl/dseditgroup instead of useradd/groupadd/getent/usermod.
#   * Service:      launchd LaunchDaemon instead of a systemd unit.
#   * Shutdown:     /sbin/shutdown -h now instead of /usr/sbin/shutdown now.
#   * VFS:          NOT supported on macOS -- hard error if requested.
#   * Directories, permission modes, worker.toml handling, and the python config call are
#     kept IDENTICAL to the Linux installer (they are portable to macOS/BSD).

set -euo pipefail

SCRIPT_DIR=$(cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# Defaults
default_wa_user=deadline-worker
default_job_group=deadline-job-users
farm_id="unset"
fleet_id="unset"
wa_user=$default_wa_user
confirm=""
region="unset"
scripts_path="unset"
worker_agent_program="deadline-worker-agent"
allow_shutdown="no"
disallow_instance_profile="no"
no_install_service="no"
start_service="no"
telemetry_opt_out="no"
warning_lines=()
vfs_install_path="unset"
python_interpreter_path="unset"
# macOS seals the root volume read-only (macOS 10.15+), so /sessions (a top-level directory)
# cannot be created. Default to a writable path under /var. The dispatcher normally passes
# --session-root-dir explicitly; this is the fallback for a direct script invocation.
session_root_dir="/var/lib/deadline/sessions"

# macOS-specific constants
# NOTE: launchd label + plist path. Uses the reverse-DNS label convention that launchd expects;
# the vendor-namespaced label makes collision with another daemon on a fleet image as unlikely
# as the systemd unit name collision on Linux. The installer's bootout-before-bootstrap reload
# path treats an existing service with this label as a prior install of this agent.
launchd_label="com.amazon.deadline.worker-agent"
launchd_plist="/Library/LaunchDaemons/${launchd_label}.plist"
# NOTE: macOS has no `useradd -m`; we must create and own a home directory ourselves.
worker_agent_homedir="/var/lib/deadline-worker"

usage()
{
    echo "Usage: install_macos.sh --farm-id FARM_ID"
    echo "                  --fleet-id FLEET_ID"
    echo "                  --scripts-path SCRIPTS_PATH"
    echo "                  --python-interpreter-path PYTHON_INTERPRETER_PATH"
    echo "                  --region REGION"
    echo "                  [--user USER]"
    echo "                  [--group GROUP]"
    echo "                  [-y]"
    echo "                  [--disallow-instance-profile]"
    echo "                  [--no-install-service]"
    echo "                  [--allow-shutdown]"
    echo "                  [--session-root-dir SESSION_ROOT_DIR]"
    echo ""
    echo "Arguments"
    echo "---------"
    echo "    --farm-id FARM_ID"
    echo "        The AWS Deadline Cloud Farm ID that the Worker belongs to."
    echo "    --fleet-id FLEET_ID"
    echo "        The AWS Deadline Cloud Fleet ID that the Worker belongs to."
    echo "    --region REGION"
    echo "        The AWS region of the AWS Deadline Cloud farm."
    echo "    --user USER"
    echo "        A user name that the AWS Deadline Cloud Worker Agent will run as. Defaults to $default_wa_user."
    echo "    --group GROUP"
    echo "        A group name that the Worker Agent shares with the user(s) that Jobs will be running as."
    echo "        Do not use the primary/effective group of the Worker Agent user specified in --user as"
    echo "        this is not a secure configuration. Defaults to $default_job_group."
    echo "    --scripts-path SCRIPTS_PATH"
    echo "        An optional path to the directory that the Worker Agent is installed. This is used as the"
    echo "        program path when creating the launchd service for the Worker Agent."
    echo "    --python-interpreter-path"
    echo "        Path to the Python interpreter for the worker agent."
    echo "    --allow-shutdown"
    echo "        Dictates whether a sudoers rule is created/deleted allowing the worker agent the"
    echo "        ability to shutdown the host system."
    echo "    --no-install-service"
    echo "        Skips the worker agent launchd service installation."
    echo "    --telemetry-opt-out"
    echo "        Opts out of telemetry collection for the worker agent."
    echo "    --start"
    echo "        Starts the launchd service as part of the installation."
    echo "    -y"
    echo "        Skips a confirmation prompt before performing the installation."
    echo "    --vfs-install-path VFS_INSTALL_PATH"
    echo "        NOT SUPPORTED on macOS. Providing this option is an error."
    echo "    --disallow-instance-profile"
    echo "        Disallow running the worker agent with an EC2 instance profile."
    echo "    --session-root-dir SESSION_ROOT_DIR"
    echo "        The root directory under which the worker agent will create session directories."

    exit 2
}

banner() {
    echo "==========================================================="
    echo "|        AWS Deadline Cloud Worker Agent Installer        |"
    echo "|                         (macOS)                         |"
    echo "==========================================================="
}

# --- macOS Directory Services helpers -------------------------------------------------
# macOS has no /etc/passwd-backed `id`/`getent` semantics for lookups the way Linux does;
# we query the local Directory Services node ("." = /Local/Default) with dscl.

user_exists() {
    # `dscl . -read /Users/<name>` exits non-zero if the record does not exist.
    dscl . -read /Users/"$1" &> /dev/null
}

group_exists() {
    dscl . -read /Groups/"$1" &> /dev/null
}

# Read a single-valued attribute from a Directory Services record.
#
# `dscl -read` has two output shapes and the value's content decides which: a value with no
# whitespace is printed inline ("NFSHomeDirectory: /var/empty"), while one containing spaces
# is printed on an indented CONTINUATION line under a bare "NFSHomeDirectory:" header. Any
# `awk '{print $2}'` or `sed 's/^Key: //'` parse therefore handles one shape and silently
# returns the wrong thing (or nothing) for the other. Ask for a plist and extract the value
# structurally instead, which is unambiguous for both.
dscl_read_value() {
    local record="$1" attr="$2"
    dscl -plist . -read "${record}" "${attr}" 2>/dev/null \
        | plutil -extract "dsAttrTypeStandard:${attr}.0" raw - 2>/dev/null || true
}

# Primary group NAME of a user (Linux `id -gn` equivalent). Resolves PrimaryGroupID -> group name.
user_primary_group_name() {
    local u="$1" gid
    gid=$(dscl_read_value /Users/"$u" PrimaryGroupID)
    if [[ -n "${gid}" ]]; then
        # `dscl . -list` plus an exact field comparison, NOT `dscl . -search`. -search is
        # documented as a substring match on the attribute value, so searching for gid 20 can
        # also match 120/200/2000, and the previous `awk 'NR==1{print $1}'` then took whichever
        # record dscl emitted first. A wrong answer here is not cosmetic: the result becomes the
        # group owner of /etc/amazon/deadline (0750), worker.toml (0640) and the agent's logs,
        # and it also gates the job-group-is-not-the-primary-group invariant. is_broad_group
        # cannot catch a misresolution, because it checks the resolved NAME and a wrong name is
        # not in broad_groups.
        #
        # $NF rather than $2 for the same reason find_unused_system_id uses it: `-list` prints
        # "name<padding>id", so a group name containing a space shifts the fields.
        dscl . -list /Groups PrimaryGroupID 2>/dev/null \
            | awk -v want="${gid}" 'NF>1 && $NF == want {print $1; exit}' || true
    fi
    # Always succeed: callers decide what an empty result means. Without this the function's
    # status is whatever the last command happened to return.
    return 0
}

# Returns the highest unused ID in [200,500) from a dscl attribute listing.
#
# This only FINDS an unused id; it does not reserve one. The caller creates the record, so the
# id is unclaimed between this returning and that write. That is not race-safe against
# concurrent account creation, which matches the Linux installer's useradd behavior under the
# same (root, single-installer) assumption.
#
# NOTE: macOS reserves IDs < 500 for hidden/system accounts. Combined with IsHidden=1 this keeps
#       the agent account out of the login window. The search scans the live directory
#       (dscl -list) at install time, so IDs already taken on the image -- including by
#       MDM-provisioned accounts -- are skipped; searching downward from 499 also stays clear of
#       Apple's own low-numbered system accounts. The install must fail if the range is
#       exhausted rather than pick a UID >= 500 (which would appear in the login window).
find_unused_system_id() {
    local used candidate
    # Union of BOTH namespaces. Searching only one lets the group and the user land on the
    # same number: on a stock image 499 is free as a UID and as a GID, so the first install
    # would take gid=499 and then uid=499. A shared number between two unrelated principals
    # is ambiguous for getpwuid/getgrgid round-trips and makes the account harder to audit.
    # Callers pass no arguments: both want "an id unused by any principal".
    # $NF, not $2: `dscl . -list` prints "name<padding>id", so a record name containing a
    # space makes $2 a fragment of the NAME. That id would then be missing from the used set
    # and could be handed out again, producing a duplicate-UID install rather than a clean
    # failure. The id is always the last field.
    used=$( { dscl . -list /Users UniqueID; dscl . -list /Groups PrimaryGroupID; } 2>/dev/null \
        | awk 'NF>1 {print $NF}' | sort -n -u)
    for candidate in $(seq 499 -1 200); do
        if ! grep -qx "${candidate}" <<< "${used}"; then
            echo "${candidate}"
            return 0
        fi
    done
    echo "ERROR: Could not find an unused system UID/GID in range [200,500)." >&2
    return 1
}

validate_deadline_id() {
    prefix="$1"
    input="$2"
    [[ "${input}" =~ ^$prefix-[a-f0-9]{32}$ ]]
}

# Install a file into /etc/sudoers.d only if sudo can parse it.
# A malformed file in /etc/sudoers.d breaks sudo HOST-WIDE, not just for this agent, so every
# file this installer writes there goes through here: write to a temporary location, validate,
# and only then move it into place. Validating before the file is ever visible to sudo (rather
# than writing it and removing it on failure) means a rejected file never exists at the real
# path, so a concurrent sudo cannot observe the broken state.
# Args: $1 = destination path under /etc/sudoers.d, $2 = file content.
install_sudoers_file() {
    local dest="$1" content="$2" tmp
    tmp="$(mktemp)"
    printf '%s' "${content}" > "${tmp}"
    chmod 440 "${tmp}"
    if ! visudo -cf "${tmp}" > /dev/null; then
        rm -f "${tmp}"
        echo "ERROR: generated an invalid sudoers file for ${dest}; not installing it." >&2
        return 1
    fi
    # mv within the same filesystem is atomic; /etc/sudoers.d and mktemp's /var/folders are both
    # on the root volume. Re-assert mode/owner after the move: mktemp created the file as root
    # (the installer requires root) but be explicit rather than relying on it.
    mv "${tmp}" "${dest}"
    chown root:wheel "${dest}"
    chmod 440 "${dest}"
}

# Validate arguments
# macOS ships only BSD getopt, which does NOT support `--longoptions` -- it silently treats the
# long option spec as positional args and drops every real flag, leaving values "unset". Rather
# than depend on GNU getopt being on PATH, we parse the long options directly with a portable
# while-loop.

while [[ $# -gt 0 ]]
do
    case "${1}" in
    --farm-id)                      farm_id="$2"                    ; shift 2 ;;
    --fleet-id)                     fleet_id="$2"                   ; shift 2 ;;
    --region)                       region="$2"                     ; shift 2 ;;
    --user)                         wa_user="$2"                    ; shift 2 ;;
    --group)                        job_group="$2"                  ; shift 2 ;;
    --scripts-path)                 scripts_path="$2"               ; shift 2 ;;
    --python-interpreter-path)      python_interpreter_path="$2"    ; shift 2 ;;
    --vfs-install-path)             vfs_install_path="$2"           ; shift 2 ;;
    --session-root-dir)             session_root_dir="$2"           ; shift 2 ;;
    --allow-shutdown)               allow_shutdown="yes"            ; shift   ;;
    --disallow-instance-profile)    disallow_instance_profile="yes" ; shift   ;;
    --no-install-service)           no_install_service="yes"        ; shift   ;;
    --telemetry-opt-out)            telemetry_opt_out="yes"         ; shift   ;;
    --start)                        start_service="yes"             ; shift   ;;
    -y)                             confirm="-y"                    ; shift   ;;
    --) shift; break ;;
    *) echo "ERROR: Unexpected option: $1"
       usage ;;
  esac
done

# Require root (sudo), like the Linux installer. macOS account/plist operations need root.
if [[ "$(id -u)" -ne 0 ]]; then
    echo "ERROR: This installer must be run as root (via sudo)."
    exit 1
fi

# Validate required command-line arguments
if [[ "${farm_id}" == "unset" ]]; then
    echo "ERROR: --farm-id not specified"
    usage
elif ! validate_deadline_id farm "${farm_id}"; then
    echo "ERROR: Not a valid value for --farm-id: ${farm_id}"
    usage
fi

if [[ "${fleet_id}" == "unset" ]]; then
    echo "ERROR: --fleet-id not specified"
    usage
elif ! validate_deadline_id fleet "${fleet_id}"; then
    echo "ERROR: Not a valid value for --fleet-id: ${fleet_id}"
    usage
fi

if [[ "${scripts_path}" == "unset" ]]; then
    echo "ERROR: --scripts-path is not specified"
    usage
elif [[ ! -d "${scripts_path}" ]]; then
    echo "ERROR: The specified scripts path is not found: \"${scripts_path}\""
    usage
else
    set +e
    worker_agent_program="${scripts_path}"/deadline-worker-agent
    if [[ ! -f "${worker_agent_program}" ]]; then
        echo "ERROR: Could not find deadline-worker-agent in scripts path: \"${worker_agent_program}\""
        exit 1
    fi
    set -e
fi

if [[ "${python_interpreter_path}" == "unset" ]]; then
    echo "ERROR: --python-interpreter-path is not specified"
    usage
elif [[ ! -f "${python_interpreter_path}" ]]; then
    echo "ERROR: The Python interpreter path is not found: \"${python_interpreter_path}\""
    usage
fi

if [[ "${region}" == "unset" ]]; then
    echo "ERROR: --region not specified"
    usage
fi
if [[ ! "${region}" =~ ^[a-z]+-[a-z]+-([a-z]+-)?[0-9]+$ ]]; then
    echo "ERROR: Not a valid value for --region: ${region}"
    usage
fi
if [[ ! -z "${wa_user}" ]] && [[ ! "${wa_user}" =~ ^[a-z_]([a-z0-9_-]{0,31}|[a-z0-9_-]{0,30}\$)$ ]]; then
    echo "ERROR: Not a valid value for --user: ${wa_user}"
    usage
fi

# --- VFS is not supported on macOS: fail loudly -------------------------------------
# DESIGN CHOICE: hard error (exit non-zero) rather than a silent warning, so that a mistaken
# VFS request surfaces immediately instead of producing a silently-degraded install. The
# dispatcher (__init__.py) also rejects --vfs-install-path on macOS before we ever get here;
# this is defense-in-depth for direct script invocation.
if [[ "${vfs_install_path}" != "unset" ]]; then
    echo "ERROR: The Deadline Virtual File System (VFS) is not supported on macOS."
    echo "       Remove the --vfs-install-path option and re-run the installer."
    exit 1
fi

# Determine the worker agent's PRIMARY group.
# CRITICAL SECURITY INVARIANT: the agent user's PRIMARY group must NOT be the job group. The job
# group is a SECONDARY membership only (see below). If the user already exists we read its current
# primary group; if we are creating the user we will give it a dedicated primary group named after
# the user (never the job group).
# SECOND INVARIANT, macOS-specific: wa_group must not be a broadly-shared group. It is used
# as the group owner of the config directory (worker.toml, mode 640) and the agent's logs, so
# every member of it can read them. On Linux `useradd` guarantees a dedicated single-member
# primary group, which is why install.sh can reuse the primary group safely. On macOS the
# primary group of any normal account -- including Setup Assistant and MDM-created ones -- is
# `staff` (GID 20), which contains every local user on the host. Reusing that would publish
# worker.toml and the logs to all of them.
broad_groups=(staff admin everyone wheel _unknown nogroup)
is_broad_group() {
    local candidate="$1" g
    for g in "${broad_groups[@]}"; do
        [[ "${candidate}" == "${g}" ]] && return 0
    done
    return 1
}

if user_exists "${wa_user}"; then
    wa_group=$(user_primary_group_name "${wa_user}")
    if [[ -z "${wa_group}" ]]; then
        # Fail here rather than falling back to "${wa_user}". That name has no group record on
        # this path -- the block that creates /Groups/${wa_user} only runs when the USER is
        # being created -- so every later `chown ${wa_user}:${wa_group}` would fail with
        # "invalid group", and under set -e the install would stop somewhere in the middle,
        # possibly after the job group, the group membership and the sudoers rule were already
        # in place. Reachable without any resolver bug: a PrimaryGroupID pointing at a GID whose
        # group record no longer exists (a removed MDM group, say) resolves to nothing.
        echo "ERROR: could not resolve the primary group of the existing user ${wa_user}."
        echo "       Its PrimaryGroupID does not correspond to any group on this host."
        echo "       Fix that account's PrimaryGroupID, or pass --user with a different"
        echo "       account, or omit --user to let the installer create one."
        exit 1
    fi
else
    # Newly created user -> its dedicated primary group has the same name as the user.
    wa_group="${wa_user}"
fi

# Checked on the RESOLVED wa_group, outside the branches above, so both paths are covered.
# Gating this on user_exists would miss `--user staff`: the account does not exist, so
# wa_group becomes "staff", and the group-creation block below then adopts the existing
# `staff` record (GID 20) as the new account's primary group -- the same exposure by a
# different route.
if is_broad_group "${wa_group}"; then
    echo "ERROR: ${wa_group} would be the worker agent user's primary group, and it is shared"
    echo "       by other users on this host. The agent's configuration and logs are"
    echo "       group-owned by that group, so this would make them readable by every one of"
    echo "       its members."
    echo "       Use --user with a dedicated service account whose primary group has no other"
    echo "       members, or omit --user to let the installer create one."
    exit 1
fi

# An existing account already has a home directory recorded in NFSHomeDirectory, and launchd
# derives the daemon's HOME from that record -- not from the plist's WorkingDirectory. Adopt it
# rather than provisioning the default path: otherwise HOME and the CWD point at two different
# directories, anything resolving ~ (botocore's ~/.aws config and caches, for instance) lands
# somewhere the installer never created or chowned, and the directory we did provision goes
# unused.
if user_exists "${wa_user}"; then
    # Read via dscl_read_value: a home directory containing spaces is printed by `dscl -read`
    # on a continuation line rather than inline, so both a field-split and a "^Key: " sed parse
    # come back empty for exactly the case that matters. Silently falling back to the default
    # would provision and chown a directory that is not the account's home while launchd still
    # took HOME from the record.
    existing_homedir=$(dscl_read_value /Users/"${wa_user}" NFSHomeDirectory)
    if [[ -n "${existing_homedir}" ]]; then
        worker_agent_homedir="${existing_homedir}"
    fi
fi

# Default the job group if not provided via --group.
job_group=${job_group:-${default_job_group}}
if [[ ! -z "${job_group}" ]] && [[ ! "${job_group}" =~ ^[a-z_]([a-z0-9_-]{0,31}|[a-z0-9_-]{0,30}\$)$ ]]; then
    echo "ERROR: Not a valid value for --group: ${job_group}"
    usage
fi

# Same broad-group check as wa_group, for the same reason. job_group owns the persistence tree
# (0750) and the session root (0755), and those modes are deliberately loosened so job users
# can reach them -- so a shared group here is worse, not better: `--group staff` would let any
# local user list queues/ and traverse into session directories to read job attachment inputs
# and generated scripts. (credentials/ stays 0700, so AWS credentials remain protected.)
# Syntax validation above does not catch this; `staff` is a perfectly well-formed group name.
if is_broad_group "${job_group}"; then
    echo "ERROR: --group ${job_group} is shared by other users on this host. The worker agent's"
    echo "       persistence and session directories are group-owned by it, so this would give"
    echo "       every member of ${job_group} access to job data."
    echo "       Use a dedicated job group, or omit --group to use ${default_job_group}."
    exit 1
fi

banner
echo

# Output configuration
echo "Farm ID: ${farm_id}"
echo "Fleet ID: ${fleet_id}"
echo "Region: ${region}"
echo "Worker agent user: ${wa_user}"
echo "Worker agent group: ${wa_group}"
echo "Worker job group: ${job_group}"
echo "Scripts path: ${scripts_path}"
echo "Session root directory: ${session_root_dir}"
echo "Worker agent program path: ${worker_agent_program}"
echo "Allow worker agent shutdown: ${allow_shutdown}"
echo "Start launchd service: ${start_service}"
echo "Telemetry opt-out: ${telemetry_opt_out}"
echo "Disallow EC2 instance profile: ${disallow_instance_profile}"

# Confirmation prompt
if [ -z "$confirm" ]; then
    while :
    do
        read -p "Confirm install with the above settings (y/n):" confirm
        if [[ "${confirm}" == "y" ]]; then
            break
        elif [[ "${confirm}" == "n" ]]; then
            echo "Installation aborted"
            exit 1
        else
            echo "Not a valid choice (${confirm}). Please try again."
        fi
    done
fi

echo ""

# --- Create the worker agent user (hidden system account) ---------------------------
# DESIGN CHOICE: dscl (low-level) instead of `sysadminctl -addUser`.
#   * sysadminctl auto-assigns a UID >= 501 (a normal, login-visible account) and offers no
#     supported flag to force a hidden sub-500 system UID.
#   * dscl lets us explicitly set UniqueID (<500), IsHidden, NFSHomeDirectory, UserShell, and
#     PrimaryGroupID -- exactly what a headless daemon account needs, and it keeps the account
#     out of the login window.
# Idempotent: only create when the record is absent.
if ! user_exists "${wa_user}"; then
    echo "Creating worker agent user (${wa_user})"

    # First ensure the user's DEDICATED primary group exists (named after the user).
    # This group -- NOT the job group -- becomes the user's PrimaryGroupID (security invariant).
    if ! group_exists "${wa_user}"; then
        wa_primary_gid=$(find_unused_system_id)
        dscl . -create /Groups/"${wa_user}"
        dscl . -create /Groups/"${wa_user}" PrimaryGroupID "${wa_primary_gid}"
        dscl . -create /Groups/"${wa_user}" RealName "${wa_user}"
    else
        wa_primary_gid=$(dscl_read_value /Groups/"${wa_user}" PrimaryGroupID)
        if [[ -z "${wa_primary_gid}" ]]; then
            # Otherwise the `dscl -create ... PrimaryGroupID ""` below would write an empty
            # value instead of failing with something an operator can act on.
            echo "ERROR: group ${wa_user} exists but has no PrimaryGroupID." >&2
            exit 1
        fi
    fi

    wa_uid=$(find_unused_system_id)
    dscl . -create /Users/"${wa_user}"
    dscl . -create /Users/"${wa_user}" UniqueID "${wa_uid}"
    dscl . -create /Users/"${wa_user}" PrimaryGroupID "${wa_primary_gid}"
    dscl . -create /Users/"${wa_user}" NFSHomeDirectory "${worker_agent_homedir}"
    # /usr/bin/false prevents interactive login (macOS equivalent of a nologin shell).
    dscl . -create /Users/"${wa_user}" UserShell /usr/bin/false
    dscl . -create /Users/"${wa_user}" RealName "AWS Deadline Cloud Worker Agent"
    # IsHidden=1 keeps a UID<500 account out of the login window / user pickers.
    dscl . -create /Users/"${wa_user}" IsHidden 1
    # No usable password for this service account. Password '*' with no
    # AuthenticationAuthority is the shape Apple's own service accounts have -- `dscl . -read
    # /Users/daemon` shows exactly "Password: *" and "UserShell: /usr/bin/false" with no
    # AuthenticationAuthority -- so this follows the platform convention rather than inventing
    # one.
    #
    # Deliberately not claiming more than that: what Directory Services does with a non-hash
    # sentinel in that attribute is not documented, and it is the absence of an
    # AuthenticationAuthority plus UserShell=/usr/bin/false and IsHidden=1 that actually keeps
    # this account out of interactive use, not the '*' by itself. The explicit alternatives
    # (AuthenticationAuthority ';DisabledUser;' or `pwpolicy -disableuser`) describe a
    # *disabled* account, which is a different thing from one that never had credentials, and
    # they add a second mechanism to keep working across releases for no clear gain here.
    dscl . -create /Users/"${wa_user}" Password '*'

    wa_group="${wa_user}"
    echo "Done creating worker agent user (${wa_user})"
else
    echo "Worker agent user ${wa_user} already exists"
fi

# --- Create the home directory (macOS does not auto-create it) ----------------------
# Only provision a directory this installer creates. When --user names a pre-existing account,
# worker_agent_homedir came from its NFSHomeDirectory, which for a service account is
# conventionally a SHARED system path: most of Apple's own _-prefixed accounts record
# /var/empty (root:wheel 0555, also sshd's privsep chroot), and `daemon` records /var/root.
# chown-ing one of those to the agent user and narrowing it to 750 is a host-wide change well
# outside this installer's remit. Some service accounts also use /dev/null, where the old
# `[[ ! -d ]]` test passes and `mkdir -p` then fails with "File exists", aborting the install
# under set -e with an opaque error.
if [[ -e "${worker_agent_homedir}" && ! -d "${worker_agent_homedir}" ]]; then
    warning_lines+=(
        "The home directory of ${wa_user} (${worker_agent_homedir}) is not a directory."
        "Leaving it alone. HOME will point at it regardless, since launchd takes HOME from"
        "the account record, so anything resolving ~ (a botocore credentials cache, for"
        "example) will fail. The service's WorkingDirectory falls back to a real directory"
        "so the agent still starts."
    )
elif [[ ! -d "${worker_agent_homedir}" ]]; then
    echo "Creating worker agent home directory (${worker_agent_homedir})"
    mkdir -p "${worker_agent_homedir}"
    chown "${wa_user}:${wa_group}" "${worker_agent_homedir}"
    chmod 750 "${worker_agent_homedir}"
elif [[ "$(stat -f %Su "${worker_agent_homedir}")" == "${wa_user}" ]]; then
    # Already ours (a re-install): safe to re-assert ownership and mode.
    chown "${wa_user}:${wa_group}" "${worker_agent_homedir}"
    chmod 750 "${worker_agent_homedir}"
else
    warning_lines+=(
        "The home directory of ${wa_user} (${worker_agent_homedir}) already exists and is"
        "owned by $(stat -f %Su "${worker_agent_homedir}"), not ${wa_user}. Leaving its"
        "ownership and permissions unchanged rather than taking over a directory this"
        "installer did not create."
    )
fi

# --- Create the job group -----------------------------------------------------------
# dseditgroup allocates a system GID and creates the group record. Idempotent via group_exists.
if ! group_exists "${job_group}"; then
    echo "Creating job group (${job_group})"
    dseditgroup -o create "${job_group}"
    echo "Done creating job group (${job_group})"
else
    echo "Job group ${job_group} already exists"
fi

# --- Enforce/verify the primary-group security invariant ----------------------------
# The job group must be a SECONDARY membership only, never the agent user's primary group.
current_primary_group=$(user_primary_group_name "${wa_user}")
if [[ "${current_primary_group}" == "${job_group}" ]]; then
    warning_lines+=(
        "The job group (${job_group}) is the primary group of worker agent user (${wa_user}). This is not a secure setup."
        "Consider re-installing and using a dedicated job group."
    )
else
    # Add the agent user to the job group as a SECONDARY member (Linux `usermod -a -G` equivalent).
    # `dseditgroup -o checkmember` reports current membership so this stays idempotent.
    if ! dseditgroup -o checkmember -m "${wa_user}" "${job_group}" &> /dev/null; then
        echo "Adding worker agent user (${wa_user}) to job group (${job_group})"
        dseditgroup -o edit -a "${wa_user}" -t user "${job_group}"
        echo "Done adding worker agent user (${wa_user}) to job group (${job_group})"
    else
        echo "Worker agent user (${wa_user}) is already in job group (${job_group})"
    fi
fi

# --- Sudoers configuration (--allow-shutdown) ---------------------------------------
# macOS shutdown binary lives at /sbin/shutdown (BSD shutdown). The Linux line used
# `/usr/sbin/shutdown now`; the BSD invocation is `/sbin/shutdown -h now` (-h = halt/power off).
# The agent invokes `sudo shutdown -h now` on macOS (startup/entrypoint.py:_host_shutdown);
# sudo resolves `shutdown` to /sbin/shutdown via PATH and matches this rule by full path.
# The sudoers command MUST continue to match that argv exactly for the NOPASSWD rule to apply.
if [[ "${allow_shutdown}" == "yes" ]]; then
    echo "Setting up sudoers shutdown rule at /etc/sudoers.d/deadline-worker-shutdown"
    # /etc/sudoers.d exists and is included by default on macOS.
    mkdir -p /etc/sudoers.d
    # Validated before being installed -- see install_sudoers_file. A rejected file never
    # appears at the real path, so a later step aborting the install cannot leave an
    # unvalidated file behind in /etc/sudoers.d.
    install_sudoers_file /etc/sudoers.d/deadline-worker-shutdown \
"# Allow ${wa_user} user to shutdown the system
${wa_user} ALL=(root) NOPASSWD: /sbin/shutdown -h now
"
    echo "Done setting up sudoers shutdown rule"
elif [ -f /etc/sudoers.d/deadline-worker-shutdown ]; then
    echo "Removing sudoers shutdown rule at /etc/sudoers.d/deadline-worker-shutdown"
    rm /etc/sudoers.d/deadline-worker-shutdown
    echo "Done removing sudoers shutdown rule"
else
    echo "No prior sudoers shutdown rule at /etc/sudoers.d/deadline-worker-shutdown"
fi

# --- Directory provisioning (IDENTICAL to Linux: paths + modes are portable) --------
echo "Provisioning log directory (/var/log/amazon/deadline)"
mkdir -p /var/log/amazon/deadline
chmod 755 /var/log/amazon
chown -R "${wa_user}:${wa_group}" /var/log/amazon/deadline
chmod -R 750 /var/log/amazon/deadline
echo "Done provisioning log directory (/var/log/amazon/deadline)"

echo "Provisioning persistence directory (/var/lib/deadline)"
mkdir -p /var/lib/deadline/queues
mkdir -p /var/lib/deadline/credentials
chown "${wa_user}:${job_group}" \
    /var/lib/deadline \
    /var/lib/deadline/queues
chown "${wa_user}" /var/lib/deadline/credentials
chmod 750 \
    /var/lib/deadline \
    /var/lib/deadline/queues
chmod 700 \
    /var/lib/deadline/credentials
if [ -f /var/lib/deadline/worker.json ]; then
    chown "${wa_user}:${wa_group}" /var/lib/deadline/worker.json
    chmod 600 /var/lib/deadline/worker.json
fi
echo "Done provisioning persistence directory (/var/lib/deadline)"

# DIVERGENCE FROM LINUX, deliberate: the default session root is nested under
# /var/lib/deadline (0750 wa_user:job_group), whereas Linux's /sessions is top-level under /
# (0755). Traversing into a session directory here therefore requires membership in
# ${job_group}, which the 0755 on the session root itself cannot grant -- the search bit is
# missing one level up. That matches the documented model, where every jobRunAsUser is a member
# of the shared job group, and it is deliberately tighter than Linux: a job user outside the
# group cannot reach other queues' session directories. A jobRunAsUser that is NOT in
# ${job_group} will get EACCES on its own session path, which is a misconfiguration the
# worker-host documentation needs to state (macOS cannot put this at / because the root volume
# is sealed read-only).
echo "Provisioning root directory for OpenJD Sessions (${session_root_dir})"
mkdir -p "${session_root_dir}"
chown "${wa_user}:${job_group}" "${session_root_dir}"
chmod 755 "${session_root_dir}"
echo "Done provisioning root directory for OpenJD Sessions (${session_root_dir})"

echo "Provisioning configuration directory (/etc/amazon/deadline)"
mkdir -p /etc/amazon/deadline
chmod 750 /etc/amazon/deadline
cp "${SCRIPT_DIR}/worker.toml.example" /etc/amazon/deadline/
if [ ! -f /etc/amazon/deadline/worker.toml ]; then
    cp "${SCRIPT_DIR}/worker.toml.example" /etc/amazon/deadline/worker.toml
fi
chown -R "root:${wa_group}" /etc/amazon/deadline
chmod 640 /etc/amazon/deadline/worker.toml
echo "Done provisioning configuration directory"

# --- Write farm/fleet/region/session-root/instance-profile via the python config module
# IDENTICAL to Linux -- reuse the same module invocation and flags.
if [[ "${allow_shutdown}" == "yes" ]]; then
   shutdown_on_stop_flag="--shutdown-on-stop"
else
   shutdown_on_stop_flag="--no-shutdown-on-stop"
fi
if [[ "${disallow_instance_profile}" == "yes" ]]; then
   allow_ec2_instance_profile_flag="--no-allow-ec2-instance-profile"
else
   allow_ec2_instance_profile_flag="--allow-ec2-instance-profile"
fi

"${python_interpreter_path}"                \
    -m deadline_worker_agent.config         \
    --farm-id "${farm_id}"                  \
    --fleet-id "${fleet_id}"                \
    "${allow_ec2_instance_profile_flag}"    \
    "${shutdown_on_stop_flag}"              \
    --session-root-dir "${session_root_dir}" \
    --region "${region}"

# Telemetry opt-out (IDENTICAL to Linux). NOTE: uses `sed -i ''` (BSD sed requires an explicit
# empty extension argument for in-place editing, unlike GNU `sed -i`).
if [[ "${telemetry_opt_out}" == "yes" ]]; then
    echo "Opting out of telemetry collection"
    worker_config="/etc/amazon/deadline/worker.toml"
    if grep -q '^\[telemetry\]' "$worker_config" 2>/dev/null; then
        sed -i '' '/^\[telemetry\]/,/^\[/{s/^opt_out.*/opt_out = true/;}' "$worker_config"
        if ! grep -q '^opt_out' "$worker_config"; then
            sed -i '' '/^\[telemetry\]/a\
opt_out = true
' "$worker_config"
        fi
    else
        printf '\n[telemetry]\nopt_out = true\n' >> "$worker_config"
    fi
fi

# --- launchd LaunchDaemon (replaces the systemd unit) -------------------------------
if ! [[ "${no_install_service}" == "yes" ]]; then
    echo "Installing launchd LaunchDaemon to ${launchd_plist}"

    # worker_agent_program is a single path with no embedded arguments, so ProgramArguments is a
    # single-element array. Do NOT word-split it: the venv scripts path can contain spaces on
    # macOS (e.g. a venv under "/Users/My Name/..."). XML-escape it so paths containing
    # &, <, or > cannot corrupt the plist.
    xml_escape() {
        local s="$1"
        s="${s//&/&amp;}"
        s="${s//</&lt;}"
        s="${s//>/&gt;}"
        printf '%s' "${s}"
    }
    prog_args_xml="        <string>$(xml_escape "${worker_agent_program}")</string>"$'\n'
    # WorkingDirectory takes the same class of value as ProgramArguments and needs the same
    # escaping. Since the NFSHomeDirectory adoption above it is read from a Directory Services
    # record rather than being a hard-coded constant, so a home directory containing & < or >
    # would produce a plist that is not well-formed XML -- which launchd rejects with an
    # opaque error, or silently ignores until the next boot when --start was not passed.
    # launchd chdir()s into WorkingDirectory before exec, so an unusable value is fatal rather
    # than cosmetic: the spawn fails with ENOTDIR and KeepAlive throttles the retry, leaving a
    # service that never runs and reports only a nonzero last exit status. worker_agent_homedir
    # can be such a path -- an adopted NFSHomeDirectory of /dev/null, for instance, which the
    # provisioning block above deliberately leaves alone. Fall back to a directory this
    # installer definitely created so the daemon still starts.
    working_directory="${worker_agent_homedir}"
    if [[ ! -d "${working_directory}" ]]; then
        working_directory=/var/lib/deadline
        warning_lines+=(
            "The home directory of ${wa_user} (${worker_agent_homedir}) is not a usable"
            "directory, so the service's WorkingDirectory was set to ${working_directory}"
            "instead. HOME still comes from the account record, so anything resolving ~ will"
            "not work until that directory exists."
        )
    fi
    working_directory_xml="$(xml_escape "${working_directory}")"

    # launchd has no separate "start on boot" and "start on load" controls: RunAtLoad governs
    # both, and loading happens at every boot for /Library/LaunchDaemons plists as well as at
    # `launchctl bootstrap`. So to reproduce the Linux installer's semantics --
    #   * `systemctl enable` always: start on (next) boot
    #   * `systemctl start` only with --start: start now
    # -- the plist is always written boot-ready (RunAtLoad=true plus KeepAlive/SuccessfulExit,
    # the systemd Restart=on-failure analog; note KeepAlive implies a load-time start per
    # launchd.plist(5), which is fine because we want every load to start the daemon), and it
    # is the `launchctl bootstrap` (load-now) that is gated on --start below. Installing the
    # plist into /Library/LaunchDaemons is itself the boot-time registration.

    # NOTE ON MAPPINGS from the systemd unit:
    #   User=                 -> UserName
    #   WorkingDirectory=     -> WorkingDirectory
    #   Environment=AWS_*     -> EnvironmentVariables dict
    #   ExecStart=            -> ProgramArguments (array)
    #   Restart=on-failure    -> KeepAlive { SuccessfulExit = false }  (restart only on failure)
    #   WantedBy=multi-user.target + (systemctl start when --start) -> RunAtLoad, gated on --start
    #   StandardOutput/Error=null  -> StandardOutPath/StandardErrorPath = /dev/null
    #   AmbientCapabilities=CAP_KILL -> OMITTED. No macOS equivalent. The worker agent runtime
    #       already falls back to `pgrep` + `sudo kill` for process cleanup on non-Linux platforms,
    #       so no ambient capability is required here.
    #   VFS env vars (FUS3_PATH/DEADLINE_VFS_PATH) -> OMITTED. VFS is unsupported on macOS.
    cat > "${launchd_plist}" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>${launchd_label}</string>
    <key>UserName</key>
    <string>${wa_user}</string>
    <key>WorkingDirectory</key>
    <string>${working_directory_xml}</string>
    <key>ProgramArguments</key>
    <array>
${prog_args_xml}    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>AWS_REGION</key>
        <string>${region}</string>
        <key>AWS_DEFAULT_REGION</key>
        <string>${region}</string>
    </dict>
    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
    </dict>
    <key>RunAtLoad</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/dev/null</string>
    <key>StandardErrorPath</key>
    <string>/dev/null</string>
</dict>
</plist>
EOF

    # launchd REJECTS a plist that is group- or other-writable, so mode is 644 (NOT 640 like the
    # Linux systemd unit). Ownership must be root:wheel.
    chown root:wheel "${launchd_plist}"
    chmod 644 "${launchd_plist}"
    echo "Done installing launchd LaunchDaemon"

    # Idempotent (re)load: bootout an already-loaded instance so a re-install picks up the new
    # plist. Remember whether it was loaded: a config-only re-run (no --start) over a loaded
    # service must put the service back afterward, matching Linux where a re-run without
    # `systemctl start` leaves a running service running.
    #
    # DIVERGENCE FROM LINUX: this restarts a running agent. launchd has no way to reload a
    # changed plist in place (the systemd `daemon-reload` analog) -- the service must be
    # booted out and back in -- so unlike a Linux config-only re-run, which leaves the
    # running process untouched, a macOS re-install terminates the agent. Any session the
    # agent is currently running is interrupted. Warn the operator rather than doing this
    # silently.
    was_loaded="no"
    if launchctl print "system/${launchd_label}" &> /dev/null; then
        echo "Existing LaunchDaemon detected; unloading"
        was_loaded="yes"
        warning_lines+=(
            "The worker agent service was running and has been restarted to load the updated"
            "configuration. Any session it was running was interrupted. (macOS/launchd cannot"
            "reload a changed LaunchDaemon plist without restarting the service.)"
        )
        # bootout returns non-zero if the service is not loaded; tolerate the race.
        launchctl bootout system "${launchd_plist}" &> /dev/null || true
    fi
    launchctl enable "system/${launchd_label}"

    if [[ "${start_service}" == "yes" ]] || [[ "${was_loaded}" == "yes" ]]; then
        # Load now; RunAtLoad=true makes bootstrap start the daemon immediately (the Linux
        # `systemctl start` analog -- or, in the re-install case, the restore of the
        # previously-loaded service).
        #
        # `bootout` above is asynchronous: launchd may still be unloading the old instance
        # when we bootstrap, which fails transiently ("service already loaded" / EIO). Retry
        # briefly rather than aborting the install on the race.
        echo "Bootstrapping and starting the LaunchDaemon"
        bootstrap_ok="no"
        for _ in $(seq 1 10); do
            if launchctl bootstrap system "${launchd_plist}" &> /dev/null; then
                bootstrap_ok="yes"
                break
            fi
            sleep 1
        done
        if [[ "${bootstrap_ok}" != "yes" ]]; then
            if launchctl print "system/${launchd_label}" &> /dev/null; then
                # bootstrap kept failing because the service is still loaded: the old
                # instance never fully unloaded and won the race. kickstart -k forces it to
                # (re)start so the operator is not left with a stopped agent. launchd is
                # still holding the OLD plist in this case, so the config we just wrote does
                # not take effect until the service is reloaded -- say so.
                echo "Service still loaded after bootout; forcing a restart"
                launchctl kickstart -k "system/${launchd_label}"
                warning_lines+=(
                    "The previous worker agent service could not be unloaded, so launchd is still"
                    "using the previous configuration. Reboot, or run"
                    "\`launchctl bootout system/${launchd_label}\` followed by"
                    "\`launchctl bootstrap system ${launchd_plist}\`, to apply the new configuration."
                )
            else
                # Not loaded, and bootstrap will not take it. Re-run unguarded so the real
                # launchd error reaches the operator; set -e then aborts the install.
                launchctl bootstrap system "${launchd_plist}"
            fi
        fi
        # After a clean bootstrap, RunAtLoad has already started the daemon -- no kickstart
        # here, since an unconditional `-k` would kill and respawn a healthy process (and
        # interrupt its session) for no reason.
        echo "Done starting the service"
    else
        echo "LaunchDaemon installed; it will start on the next boot (use --start to start it now)"
    fi
fi

echo "Done"

# Output warning lines if any
if [ ${#warning_lines[@]} -gt 0 ]; then
    echo
    echo "!!!! WARNING !!!"
    echo
    for i in "${!warning_lines[@]}"; do
        echo "${warning_lines[i]}"
    done
    echo
fi

# OPERATOR NOTES (macOS platform integration; environment-dependent, not verifiable here):
#   * TCC / Full Disk Access: a headless LaunchDaemon may be blocked by TCC from protected paths
#     (Desktop/Documents/removable volumes) and cannot present the consent UI. Fleets likely need
#     an MDM PPPC profile granting Full Disk Access to the ${worker_agent_program} binary. Not an
#     issue for the default session root (/var/lib/deadline/sessions), which is not TCC-protected.
#   * Code signing / Gatekeeper: an unsigned/unnotarized agent binary may be quarantined. Ensure
#     the binary is signed + notarized (or delivered without the com.apple.quarantine xattr).
#     pip-installed console scripts (the standard install path) do not carry the quarantine xattr.
