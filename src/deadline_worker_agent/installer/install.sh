#!/usr/bin/env bash

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

#
# Amazon Deadline Cloud Worker Agent Installer
#
# This script installs the Amazon Deadline Cloud Worker Agent.  The installer provides command-line arguments that
# can be used to configure the installation. The installer supports upgrading over top of a prior
# installation, but the installer will not backup or rollback the prior installation.
#
# Minimally, a farm and fleet ID are required options that must be specified as command-line
# arguments. A minimal installation can be run with:
#
#     ./install.sh --farm-id $FARM_ID --fleet-id $FLEET_ID
#
# The installer:
#
#     1.  Creates OS user for the worker agent if required
#     2.  Creates an OS group for all job users if required
#     3.  Provisions directories used by the worker agent at runtime.
#     4.  Creates an agent configuration file if required and installs an example
#         configuration file.
#     5.  Updates the configuration file with arguments passed to the installer
#     6.  Creates, enables, and starts a systemd service unit that runs the worker agent and
#         restarts it upon failure.

set -euo pipefail

SCRIPT_DIR=$(cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# Defaults
default_wa_user=deadline-worker
default_job_group=deadline-job-users
farm_id=unset
fleet_id=unset
wa_user=$default_wa_user
confirm=""
region="us-west-2"
scripts_path="unset"
worker_agent_program="deadline-worker-agent"
client_library_program="deadline"
allow_shutdown="no"
no_install_service="no"
start_service="no"
telemetry_opt_out="no"
warning_lines=()
vfs_install_path="unset"

usage()
{
    echo "Usage: install.sh --farm-id FARM_ID --fleet-id FLEET_ID"
    echo "                  [--region REGION] [--user USER]"
    echo "                  [--scripts-path SCRIPTS_PATH]"
    echo "                  [-y]"
    echo "                  [--vfs_install_path VFS_INSTALL_PATH]"
    echo ""
    echo "Arguments"
    echo "---------"
    echo "    --farm-id FARM_ID"
    echo "        The Amazon Deadline Cloud Farm ID that the Worker belongs to."
    echo "    --fleet-id FLEET_ID"
    echo "        The Amazon Deadline Cloud Fleet ID that the Worker belongs to."
    echo "    --region REGION"
    echo "        The AWS region of the Amazon Deadline Cloud farm. Defaults to $region."
    echo "    --user USER"
    echo "        A user name that the Amazon Deadline Cloud Worker Agent will run as. Defaults to $default_wa_user."
    echo "    --group GROUP"
    echo "        A group name that the Worker Agent shares with the user(s) that Jobs will be running as."
    echo "        Do not use the primary/effective group of the Worker Agent user specifeid in --user as"
    echo "        this is not a secure configuration. Defaults to $default_job_group."
    echo "    --scripts-path SCRIPTS_PATH"
    echo "        An optional path to the directory that the Worker Agent and Deadline Cloud Library are"
    echo "        installed. This is used as the program path when creating the systemd service for the "
    echo "        Worker Agent. If not specified, the first program named 'deadline-worker-agent' and"
    echo "        'deadline' found in the search path will be used."
    echo "    --allow-shutdown"
    echo "        Dictates whether a sudoers rule is created/deleted allowing the worker agent the"
    echo "        ability to shutdown the host system"
    echo "    --no-install-service"
    echo "        Skips the worker agent systemd service installation"
    echo "    --telemetry-opt-out"
    echo "        Opts out of telemetry collection for the worker agent"
    echo "    --start"
    echo "        Starts the systemd service as part of the installation. By default, the systemd"
    echo "        service is configured to start on system boot, but not started immediately."
    echo "        This option is ignored if --no-install-service is used."
    echo "    -y"
    echo "        Skips a confirmation prompt before performing the installation."
    echo "    --vfs-install-path VFS_INSTALL_PATH"
    echo "        An optional, absolute path to the directory that the Deadline Virtual File System (VFS) is"
    echo "        installed. If it is not specified, the default path /opt/fus3 is used"

    exit 2
}

banner() {
    echo "==========================================================="
    echo "|      Amazon Deadline Cloud Worker Agent Installer       |"
    echo "==========================================================="
}

user_exists() {
    id "$1" &> /dev/null
}

group_exists() {
    getent group "$1" &> /dev/null
}

validate_deadline_id() {
    prefix="$1"
    input="$2"
    [[ "${input}" =~ ^$prefix-[a-f0-9]{32}$ ]]
}

# Validate arguments
PARSED_ARGUMENTS=$(getopt -n install.sh --longoptions farm-id:,fleet-id:,region:,user:,group:,scripts-path:,vfs-install-path:,start,allow-shutdown,no-install-service,telemetry-opt-out -- "y" "$@")
VALID_ARGUMENTS=$?
if [ "${VALID_ARGUMENTS}" != "0" ]; then
    usage
fi

# Additional arguments beyond parsed ones are set as positional arguments
eval set -- "$PARSED_ARGUMENTS"

# Iterate over parsed arguments
while :
do
    case "${1}" in
    --farm-id)               farm_id="$2"               ; shift 2 ;;
    --fleet-id)              fleet_id="$2"              ; shift 2 ;;
    --region)                region="$2"                ; shift 2 ;;
    --user)                  wa_user="$2"               ; shift 2 ;;
    --group)                 job_group="$2"             ; shift 2 ;;
    --scripts-path)          scripts_path="$2"          ; shift 2 ;;
    --vfs-install-path)      vfs_install_path="$2"      ; shift 2 ;;
    --allow-shutdown)        allow_shutdown="yes"       ; shift   ;;
    --no-install-service)    no_install_service="yes"   ; shift   ;;
    --telemetry-opt-out)     telemetry_opt_out="yes"    ; shift   ;;
    --start)                 start_service="yes"        ; shift   ;;
    -y)                      confirm="$1"               ; shift   ;;
    # -- means the end of the arguments; drop this, and break out of the while loop
    --) shift; break ;;
    # If non-valid options were passed, then getopt should have reported an error,
    # which we checked as VALID_ARGUMENTS when getopt was called...
    *) echo "Unexpected option: $1 - this should not happen."
       usage ;;
  esac
done

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
    echo "ERROR: Non a valid value for --fleet-id: ${fleet_id}"
    usage
fi
if [[ "${scripts_path}" == "unset" ]]; then
    set +e
    worker_agent_program=$(which deadline-worker-agent)
    if [[ "$?" != "0" ]]; then
        echo "ERROR: Could not find deadline-worker-agent in search path"
        exit 1
    fi
    client_library_program=$(which deadline)
    if [[ "$?" != "0" ]]; then
        echo "ERROR: Could not find deadline in search path"
        exit 1
    fi
    set -e
elif [[ ! -d "${scripts_path}" ]]; then
    echo "ERROR: The specified scripts path is not found: \"${scripts_path}\""
    usage
else
    set +e
    # We have a provided scripts path, so we append it to the program paths
    worker_agent_program="${scripts_path}"/deadline-worker-agent
    if [[ ! -f "${worker_agent_program}" ]]; then
        echo "ERROR: Could not find deadline-worker-agent in scripts path: \"${worker_agent_program}\""
        exit 1
    fi
    client_library_program="${scripts_path}"/deadline
    if [[ ! -f "${client_library_program}" ]]; then
        echo "ERROR: Could not find deadline in scripts path: \"${client_library_program}\""
        exit 1
    fi
    set -e
fi

if [[ ! -z "${region}" ]] && [[ ! "${region}" =~ ^[a-z]+-[a-z]+-[0-9]+$ ]]; then
    echo "ERROR: Not a valid value for --region: ${region}"
    usage
fi
if [[ ! -z "${wa_user}" ]] && [[ ! "${wa_user}" =~ ^[a-z_]([a-z0-9_-]{0,31}|[a-z0-9_-]{0,30}\$)$ ]]; then
    echo "ERROR: Not a valid value for --user: ${wa_user}"
    usage
fi
# Default the group to wa_user if it wasn't defined via the --group option.
job_group=${job_group:-${default_job_group}}
if [[ ! -z "${job_group}" ]] && [[ ! "${job_group}" =~ ^[a-z_]([a-z0-9_-]{0,31}|[a-z0-9_-]{0,30}\$)$ ]]; then
    echo "ERROR: Not a valid value for --group: ${job_group}"
    usage
fi

if [[ "${vfs_install_path}" == "unset" ]]; then
    vfs_install_path="/opt/fus3"
elif [[ ! -d "${vfs_install_path}" ]]; then
    echo "ERROR: The specified vfs install path is not found: \"${vfs_install_path}\""
    usage
else
    set +e
    deadline_vfs_executable="${vfs_install_path}"/bin/deadline_vfs
    if [[ ! -f "${deadline_vfs_executable}" ]]; then
        echo "Deadline vfs not found at \"${deadline_vfs_executable}\", using fus3 fallback."
        fus3_executable="${vfs_install_path}"/bin/fus3
        if [[ ! -f "${fus3_executable}" ]]; then
            echo "ERROR: Could not find deadline vfs at install path \"${deadline_vfs_executable}\""
            exit 1
        fi
    fi
    set -e
fi


banner
echo

# Ensure sudo is installed
set +e
if ! which sudo &> /dev/null; then
    echo "ERROR: sudo is not installed but is a required dependency of the worker agent."
    exit 1
fi
set -e

# Output configuration
echo "Farm ID: ${farm_id}"
echo "Fleet ID: ${fleet_id}"
echo "Region: ${region}"
echo "Worker agent user: ${wa_user}"
echo "Worker job group: ${job_group}"
echo "Scripts path: ${scripts_path}"
echo "Worker agent program path: ${worker_agent_program}"
echo "Worker agent program path: ${client_library_program}"
echo "Allow worker agent shutdown: ${allow_shutdown}"
echo "Start systemd service: ${start_service}"
echo "Telemetry opt-out: ${telemetry_opt_out}"
echo "VFS install path: ${vfs_install_path}"

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

if ! user_exists "${wa_user}"; then
    echo "Creating worker agent user (${wa_user})"
    useradd -r -m "${wa_user}"
    echo "Done creating worker agent user (${wa_user})"
else
    echo "Worker agent user ${wa_user} already exists"
fi


if ! group_exists "${job_group}"; then
    echo "Creating job group (${job_group})"
    groupadd "${job_group}"
    echo "Done creating job group (${job_group})"
else
    echo "Job group "${job_group}" already exists"
fi

if [[ "$(id -g --name "${wa_user}")" == "${job_group}" ]]; then
    warning_lines+=(
        "The job group (${job_group}) is the primary group of worker agent user (${wa_user}). This is not a secure setup"
        "Consider re-installing and using a dedicated job group."
    )
else
    if ! id -G --name "${wa_user}" | tr ' ' '\n' | egrep --quiet "^${job_group}$"; then
        echo "Adding worker agent user (${wa_user}) to job group (${job_group})"
        usermod -a -G "${job_group}" "${wa_user}"
        echo "Done adding worker agent user (${wa_user}) to job group (${job_group})"
    else
        echo "Worker agent user (${wa_user}) is alread in job group (${job_group})"
    fi
fi


# Sudo configuration
if [[ "${allow_shutdown}" == "yes" ]]; then
    # Allow worker agent user the ability to shutdown the system as root
    echo "Setting up sudoers shutdown rule at /etc/sudoers.d/deadline-worker-shutdown"
    cat > /etc/sudoers.d/deadline-worker-shutdown <<EOF
# Allow ${wa_user} user to shutdown the system
${wa_user} ALL=(root) NOPASSWD: /usr/sbin/shutdown now
EOF
    chmod 440 /etc/sudoers.d/deadline-worker-shutdown
    echo "Done setting up sudoers shutdown rule"
elif [ -f /etc/sudoers.d/deadline-worker-shutdown ]; then
    # Remove any previously created sudoers rule allowing the worker agent user the
    # ability to shutdown the system as root
    echo "Removing sudoers shutdown rule at /etc/sudoers.d/deadline-worker-shutdown"
    rm /etc/sudoers.d/deadline-worker-shutdown
    echo "Done removing sudoers shutdown rule"
else
    echo "No prior sudoers shutdown rule at /etc/sudoers.d/deadline-worker-shutdown"
fi

# Provision log directory
echo "Provisioning log directory (/var/log/amazon/deadline)"
mkdir -p /var/log/amazon/deadline
chown -R "${wa_user}:${wa_user}" /var/log/amazon/deadline
chmod -R 750 /var/log/amazon/deadline
echo "Done provisioning log directory (/var/log/amazon/deadline)"

# Provision ownership/persistence on persistence directory
echo "Provisioning persistence directory (/var/lib/deadline)"
mkdir -p /var/lib/deadline/queues
chown "${wa_user}:${job_group}" \
    /var/lib/deadline \
    /var/lib/deadline/queues
chmod 750 \
    /var/lib/deadline \
    /var/lib/deadline/queues
if [ -f /var/lib/deadline/worker.json ]; then
    chown "${wa_user}:${wa_user}" /var/lib/deadline/worker.json
    chmod 600 /var/lib/deadline/worker.json
fi
echo "Done provisioning persistence directory (/var/lib/deadline)"

echo "Provisioning configuration directory (/etc/amazon/deadline)"
mkdir -p /etc/amazon/deadline
chmod 750 /etc/amazon/deadline
# Copy the example configuration file
cp "${SCRIPT_DIR}/worker.toml.example" /etc/amazon/deadline/
if [ ! -f /etc/amazon/deadline/worker.toml ]; then
    cp "${SCRIPT_DIR}/worker.toml.example" /etc/amazon/deadline/worker.toml
fi
# Ensure the config file has secure permissions
chown -R "root:${wa_user}" /etc/amazon/deadline
chmod 640 /etc/amazon/deadline/worker.toml
echo "Done provisioning configuration directory"

echo "Configuring farm and fleet"
sed -E                                                          \
    --in-place=.bak                                             \
    -e "s,^# farm_id\s*=\s*\"REPLACE-WTIH-WORKER-FARM-ID\"$,farm_id = \"${farm_id}\",g"    \
    -e "s,^# fleet_id\s*=\s*\"REPLACE-WITH-WORKER-FLEET-ID\"$,fleet_id = \"${fleet_id}\",g" \
    /etc/amazon/deadline/worker.toml
echo "Done configuring farm and fleet"

if ! [[ "${no_install_service}" == "yes" ]]; then
    # Set up the service
    echo "Installing systemd service to /etc/systemd/system/deadline-worker.service"
    worker_agent_homedir=$(eval echo ~$wa_user)
    cat > /etc/systemd/system/deadline-worker.service <<EOF
[Unit]
Description=Amazon Deadline Cloud Worker Agent

[Service]
User=${wa_user}
WorkingDirectory=${worker_agent_homedir}
Environment=AWS_REGION=$region AWS_DEFAULT_REGION=$region FUS3_PATH=$vfs_install_path
ExecStart=$worker_agent_program
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF
    chown root:root /etc/systemd/system/deadline-worker.service
    chmod 600 /etc/systemd/system/deadline-worker.service
    echo "Done installing systemd service"

    # Tell systemd to reload units
    echo "Reloading systemd"
    systemctl daemon-reload
    echo "Done reloading systemd"

    # Tell systemd to start the service on system bootup
    systemctl enable deadline-worker

    # Start the service
    if [[ "${start_service}" == "yes" ]]; then
        echo "Starting the service"
        systemctl start deadline-worker
        echo "Done starting the service"
    fi
fi

if [[ "${telemetry_opt_out}" == "yes" ]]; then
    # Set the Deadline Client Lib configuration setting
    echo "Opting out of telemetry collection"
    sudo -u $wa_user $client_library_program config set telemetry.opt_out true
fi

echo "Done"

# Output warning lines if any
if [ ${#warning_lines[@]} -gt 0 ]; then
    echo
    echo "!!!! WARNING !!!"
    echo
    for i in ${!warning_lines[@]}; do
        echo "${warning_lines[i]}"
    done
    echo
fi
