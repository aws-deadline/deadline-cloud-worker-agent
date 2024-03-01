# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import dataclasses
import logging
import os
import re
import secrets
import shutil
import string
import sys
import typing
from argparse import ArgumentParser
from pathlib import Path

from deadline_worker_agent.file_system_operations import (
    _set_windows_permissions,
    FileSystemPermissionEnum,
)

import pywintypes
import win32api
import win32net
import win32netcon
import win32security
import winerror


# Defaults
DEFAULT_WA_USER = "deadline-worker"
DEFAULT_JOB_GROUP = "deadline-job-users"
DEFAULT_PASSWORD_LENGTH = 12


class InstallerFailedException(Exception):
    """Exception raised when the installer fails"""

    pass


@dataclasses.dataclass
class WorkerAgentDirectories:
    deadline_dir: Path
    deadline_log_subdir: Path
    deadline_persistence_subdir: Path
    deadline_config_subdir: Path


def generate_password(length: int = DEFAULT_PASSWORD_LENGTH) -> str:
    """
    Generate password of given length.

    Returns
        str: password
    """
    alphabet = string.ascii_letters + string.digits + string.punctuation
    # Use secrets.choice to ensure a secure random selection of characters
    # https://docs.python.org/3/library/secrets.html#recipes-and-best-practices
    password = "".join(secrets.choice(alphabet) for _ in range(length))
    return password


def print_banner():
    print(
        "===========================================================\n"
        "|      Amazon Deadline Cloud Worker Agent Installer       |\n"
        "===========================================================\n"
    )


def check_user_existence(user_name: str) -> bool:
    """
    Checks if a user exists on the system by attempting to resolve the user's SID.
    This method could be used in both Ad and Non-Ad environments.

    Args:
    user_name (str): The username to check for existence.

    Returns:
    bool: True if the user exists, otherwise False.
    """
    MAX_RETRIES = 5

    retry_count = 0
    while retry_count < MAX_RETRIES:
        try:
            # Resolve the username to an SID
            sid, _, _ = win32security.LookupAccountName(None, user_name)

            # Resolve the SID back to a username as an additional check
            win32security.LookupAccountSid(None, sid)
        except pywintypes.error as e:
            if e.winerror == winerror.ERROR_NONE_MAPPED:
                # LookupAccountSid can throw ERROR_NONE_MAPPED if a network timeout is reached
                # Retry a few times to reduce risk of failing due to temporary network outage
                # See https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-lookupaccountsida#remarks
                retry_count += 1
            else:
                raise
        else:
            return True

    return False


def ensure_local_queue_user_group_exists(group_name: str) -> None:
    """
    Check if a queue user group exists on the system. If it doesn't exit then create it.

    Parameters:
    group (str): The name of the group to check for existence and creation.

    """
    try:
        win32net.NetLocalGroupGetInfo(None, group_name, 1)
    except pywintypes.error as e:
        group_not_found = 2220
        if e.winerror == group_not_found:
            logging.info(f"Creating group {group_name}")
            try:
                win32net.NetLocalGroupAdd(
                    None,
                    1,
                    {
                        "name": group_name,
                        "comment": (
                            "This is a local group created by the Deadline Cloud Worker Agent Installer. "
                            "This group should contain the jobRunAs OS user for all queues associated with "
                            "the worker's fleet"
                        ),
                    },
                )
            except Exception as e:
                logging.error(f"Failed to create group {group_name}. Error: {e}")
                raise
            logging.info("Done creating group")
            return
        else:
            raise
    logging.info(f"Group {group_name} already exists")


def validate_deadline_id(prefix: str, text: str) -> bool:
    """
    Validate a string matches the deadline ID pattern

    Args:
    prefix (str): The prefix
    text (str): The text to validate

    Returns:
    bool: True if it matches the pattern, False otherwise
    """

    pattern = rf"^{re.escape(prefix)}-[a-f0-9]{{32}}$"
    return re.match(pattern, text) is not None


def ensure_local_agent_user(username: str, password: str) -> None:
    """
    Creates a local agent user account on Windows with a specified password and sets the account to never expire.
    The function sets the UF_DONT_EXPIRE_PASSWD flag to ensure the account's password never expires.

    Args:
    username (str): The username of the new agent account.
    password (str): The password for the new agent account. Ensure it meets Windows' password policy requirements.

    """
    if check_user_existence(username):
        logging.info(f"Agent User {username} already exists")
    else:
        logging.info(f"Creating Agent user {username}")
        user_info = {
            "name": username,
            "password": password,
            "priv": win32netcon.USER_PRIV_USER,  # User privilege level, Standard User
            "home_dir": None,
            "comment": "Amazon Deadline Cloud Worker Agent User",
            "flags": win32netcon.UF_DONT_EXPIRE_PASSWD,
            "script_path": None,
        }

        try:
            win32net.NetUserAdd(None, 1, user_info)
            logging.info(f"User '{username}' created successfully.")
        except Exception as e:
            logging.error(f"Failed to create user '{username}'. Error: {e}")
            raise


def grant_account_rights(username: str, rights: list[str]):
    """
    Grants rights to a user account

    Args:
        username (str): Name of user to grant rights to
        rights (list[str]): The rights to grant. See https://learn.microsoft.com/en-us/windows/win32/secauthz/privilege-constants.
            These constants are exposed by the win32security module of pywin32.
    """
    policy_handle = None
    try:
        user_sid, _, _ = win32security.LookupAccountName(None, username)
        policy_handle = win32security.LsaOpenPolicy(None, win32security.POLICY_ALL_ACCESS)
        win32security.LsaAddAccountRights(
            policy_handle,
            user_sid,
            rights,
        )
        logging.info(f"Successfully granted the following rights to {username}: {rights}")
    except Exception as e:
        logging.error(f"Failed to grant user {username} rights ({rights}): {e}")
        raise
    finally:
        if policy_handle is not None:
            win32api.CloseHandle(policy_handle)


def add_user_to_group(group_name: str, user_name: str) -> None:
    """
    Adds a specified user to a specified local group if they are not already a member.

    Parameters:
    - group_name (str): The name of the local group to which the user will be added.
    - user_name (str): The name of the user to be added to the group.
    """
    try:
        group_members_info = win32net.NetLocalGroupGetMembers(None, group_name, 1)
        group_members = [member["name"] for member in group_members_info[0]]

        if user_name not in group_members:
            # The user information must be in a dictionary with 'domainandname' key
            user_info = {"domainandname": user_name}
            win32net.NetLocalGroupAddMembers(
                None,  # the local computer is used.
                group_name,
                3,  # Specifies the domain and name of the new local group member.
                [user_info],
            )
            logging.info(f"User {user_name} is added to group {group_name}.")
        else:
            logging.info(f"User {user_name} is already a member of group {group_name}.")
    except Exception as e:
        logging.error(
            f"An error occurred during adding user {user_name} to the user group {group_name}: {e}"
        )
        raise


def update_config_file(
    deadline_config_sub_directory: str,
    farm_id: str,
    fleet_id: str,
    shutdown_on_stop: typing.Optional[bool] = None,
) -> None:
    """
    Updates the worker configuration file, creating it from the example if it does not exist.
    This function ensures the worker.toml configuration file exists, backs it up, and then
    replaces specific placeholders with the provided values.

    Parameters:
    - deadline_config_sub_directory (str): Subdirectory for Deadline configuration files.
    - farm_id (str): The farm ID to set in the configuration.
    - fleet_id (str): The fleet ID to set in the configuration.
    - shutdown_on_stop (Optional[bool]): The shutdown_on_stop value to set. Does nothing if set to None.
    """
    logging.info("Updating configuration file")

    worker_config_file = os.path.join(deadline_config_sub_directory, "worker.toml")

    # Check if the worker.toml file exists, if not, create it from the example
    if not os.path.isfile(worker_config_file):
        # Directory where the script and example configuration files are located.
        script_dir = os.path.dirname(os.path.realpath(__file__))
        example_config_path = os.path.join(script_dir, "worker.toml.example")
        shutil.copy(example_config_path, worker_config_file)

    # Make a backup of the worker configuration file
    backup_worker_config = worker_config_file + ".bak"
    shutil.copy(worker_config_file, backup_worker_config)

    # Read the content of the worker configuration file
    with open(worker_config_file, "r") as file:
        content = file.read()

    updated_keys = []

    # Replace the placeholders with actual farm_id and fleet_id
    content = re.sub(
        r'^# farm_id\s*=\s*("REPLACE-WITH-WORKER-FARM-ID")$',
        f'farm_id = "{farm_id}"',
        content,
        flags=re.MULTILINE,
    )
    if not re.search(
        rf'^farm_id = "{re.escape(farm_id)}"$',
        content,
        flags=re.MULTILINE,
    ):
        raise InstallerFailedException(f"Failed to configure farm ID in {worker_config_file}")
    else:
        updated_keys.append("farm_id")
    content = re.sub(
        r'^# fleet_id\s*=\s*("REPLACE-WITH-WORKER-FLEET-ID")$',
        f'fleet_id = "{fleet_id}"',
        content,
        flags=re.MULTILINE,
    )
    if not re.search(
        rf'^fleet_id = "{re.escape(fleet_id)}"$',
        content,
        flags=re.MULTILINE,
    ):
        raise InstallerFailedException(f"Failed to configure fleet ID in {worker_config_file}")
    else:
        updated_keys.append("fleet_id")
    if shutdown_on_stop is not None:
        shutdown_on_stop_toml = str(shutdown_on_stop).lower()
        content = re.sub(
            r"^#*\s*shutdown_on_stop\s*=\s*\w+$",
            f"shutdown_on_stop = {shutdown_on_stop_toml}",
            content,
            flags=re.MULTILINE,
        )
        if not re.search(
            rf"^shutdown_on_stop = {re.escape(shutdown_on_stop_toml)}$",
            content,
            flags=re.MULTILINE,
        ):
            raise InstallerFailedException(
                f"Failed to configure shutdown_on_stop in {worker_config_file}"
            )
        else:
            updated_keys.append("shutdown_on_stop")

    # Write the updated content back to the worker configuration file
    with open(worker_config_file, "w") as file:
        file.write(content)

    logging.info(f"Done configuring {updated_keys} in {worker_config_file}")


def provision_directories(agent_username: str) -> WorkerAgentDirectories:
    """
    Creates all required directories for Deadline Worker Agent.
    This function creates the following directories:
    - %PROGRAMDATA%/Amazon/Deadline
    - %PROGRAMDATA%/Amazon/Deadline/Logs
    - %PROGRAMDATA%/Amazon/Deadline/Cache
    - %PROGRAMDATA%/Amazon/Deadline/Config

    Parameters
        agent_username(str): Worker Agent's username used for setting the permission for the directories

    Returns
        WorkerAgentDirectories: all directories created in the function
    """

    program_data_path = os.environ.get("PROGRAMDATA", r"C:\ProgramData")
    deadline_dir = os.path.join(program_data_path, r"Amazon\Deadline")
    logging.info(f"Provisioning root directory ({deadline_dir})")
    os.makedirs(deadline_dir, exist_ok=True)
    _set_windows_permissions(
        path=Path(deadline_dir),
        user=agent_username,
        user_permission=FileSystemPermissionEnum.FULL_CONTROL,
        group="Administrators",
        group_permission=FileSystemPermissionEnum.FULL_CONTROL,
        agent_user_permission=None,
    )
    logging.info(f"Done provisioning root directory ({deadline_dir})")

    deadline_log_subdir = os.path.join(deadline_dir, "Logs")
    logging.info(f"Provisioning log directory ({deadline_log_subdir})")
    os.makedirs(deadline_log_subdir, exist_ok=True)
    logging.info(f"Done provisioning log directory ({deadline_log_subdir})")

    deadline_persistence_subdir = os.path.join(deadline_dir, "Cache")
    logging.info(f"Provisioning persistence directory ({deadline_persistence_subdir})")
    os.makedirs(deadline_persistence_subdir, exist_ok=True)
    logging.info(f"Done provisioning persistence directory ({deadline_persistence_subdir})")

    deadline_config_subdir = os.path.join(deadline_dir, "Config")
    logging.info(f"Provisioning config directory ({deadline_config_subdir})")
    os.makedirs(deadline_config_subdir, exist_ok=True)
    logging.info(f"Done provisioning config directory ({deadline_config_subdir})")

    return WorkerAgentDirectories(
        deadline_dir=Path(deadline_dir),
        deadline_log_subdir=Path(deadline_log_subdir),
        deadline_persistence_subdir=Path(deadline_persistence_subdir),
        deadline_config_subdir=Path(deadline_config_subdir),
    )


def start_windows_installer(
    farm_id: str,
    fleet_id: str,
    region: str,
    worker_agent_program: str,
    allow_shutdown: bool,
    parser: ArgumentParser,
    password: typing.Optional[str] = None,
    user_name: str = DEFAULT_WA_USER,
    group_name: str = DEFAULT_JOB_GROUP,
    no_install_service: bool = False,
    start: bool = False,
    confirm: bool = False,
):
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    # Validate command line arguments
    def print_helping_info_and_exit():
        parser.format_help()
        exit(2)

    if not farm_id:
        logging.error("Farm id not specified")
        print_helping_info_and_exit()
    elif not validate_deadline_id("farm", farm_id):
        logging.error(f"Not a valid value for farm id: {farm_id}")
        print_helping_info_and_exit()
    if not fleet_id:
        logging.error("Fleet id not specified")
        print_helping_info_and_exit()
    elif not validate_deadline_id("fleet", fleet_id):
        logging.error(f"Not a valid value for Fleet id: {fleet_id}")
        print_helping_info_and_exit()
    if not password:
        password = generate_password()

    # Print configuration
    print_banner()
    print(
        f"Farm ID: {farm_id}\n"
        f"Fleet ID: {fleet_id}\n"
        f"Region: {region}\n"
        f"Worker agent user: {user_name}\n"
        f"Worker job group: {group_name}\n"
        f"Worker agent program path: {worker_agent_program}\n"
        f"Allow worker agent shutdown: {allow_shutdown}\n"
        f"Start service: {start}"
    )

    # Confirm installation
    if not confirm:
        while True:
            choice = input("Confirm install (y/n):")
            if choice == "y":
                break
            elif choice == "n":
                logging.warning("Installation aborted")
                sys.exit(1)
            else:
                logging.warning("Not a valid choice, try again")

    # List of required user rights for the worker agent
    worker_user_rights: list[str] = []

    if allow_shutdown:
        # Grant the user privilege to shutdown the machine
        worker_user_rights.append(win32security.SE_SHUTDOWN_NAME)

    # Check if the worker agent user exists, and create it if not
    ensure_local_agent_user(user_name, password)

    # Check if the job group exists, and create it if not
    ensure_local_queue_user_group_exists(group_name)
    # Add the worker agent user to the job group
    add_user_to_group(group_name, user_name)

    agent_dirs = provision_directories(user_name)
    update_config_file(
        str(agent_dirs.deadline_config_subdir),
        farm_id,
        fleet_id,
        # This always sets shutdown_on_stop even if the user did not provide
        # any "shutdown" option to be consistent with POSIX installer
        shutdown_on_stop=allow_shutdown,
    )

    if worker_user_rights:
        # Grant the worker user the necessary rights
        grant_account_rights(user_name, worker_user_rights)
