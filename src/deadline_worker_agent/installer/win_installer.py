# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import dataclasses
import logging
import os
import re
import secrets
import shutil
import string
import sys
import time
from argparse import ArgumentParser
from getpass import getpass
from pathlib import Path
from typing import Any, Optional, Union

import deadline.client.config.config_file
import pywintypes
import win32api
import win32con
import win32net
import win32netcon
import win32profile
import win32security
import win32service
import win32serviceutil
import winerror
from openjd.sessions import BadCredentialsException, WindowsSessionUser
from win32comext.shell import shell

from ..file_system_operations import (
    _set_windows_permissions,
    FileSystemPermissionEnum,
)
from ..windows.win_service import WorkerAgentWindowsService

# Defaults
DEFAULT_WA_USER = "deadline-worker"
DEFAULT_JOB_GROUP = "deadline-job-users"
DEFAULT_PASSWORD_LENGTH = 32

# Environment variable that overrides the config path used by the Deadline client
DEADLINE_CLIENT_CONFIG_PATH_OVERRIDE_ENV_VAR = "DEADLINE_CONFIG_FILE_PATH"


class InstallerFailedException(Exception):
    """Exception raised when the installer fails"""

    pass


@dataclasses.dataclass
class WorkerAgentDirectories:
    deadline_dir: Path
    deadline_log_subdir: Path
    deadline_persistence_subdir: Path
    deadline_config_subdir: Path


def generate_password() -> str:
    """
    Generate password of given length.

    Returns
        str: password
    """
    alphabet = string.ascii_letters + string.digits + string.punctuation
    # Use secrets.choice to ensure a secure random selection of characters
    # https://docs.python.org/3/library/secrets.html#recipes-and-best-practices
    password = "".join(secrets.choice(alphabet) for _ in range(DEFAULT_PASSWORD_LENGTH))
    return password


def print_banner():
    print(
        "===========================================================\n"
        "|      AWS Deadline Cloud Worker Agent Installer       |\n"
        "===========================================================\n"
    )


def check_account_existence(account_name: str) -> bool:
    """
    Checks if an account exists on the system by attempting to resolve the account's SID.
    This method could be used in both Ad and Non-Ad environments.

    Args:
    account_name (str): The account to check for existence.

    Returns:
    bool: True if the account exists, otherwise False.
    """
    MAX_RETRIES = 5

    retry_count = 0
    while retry_count < MAX_RETRIES:
        try:
            # Resolve the account name to an SID
            sid, _, _ = win32security.LookupAccountName(None, account_name)

            # Resolve the SID back to a account name as an additional check
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


def create_local_queue_user_group(group_name: str) -> None:
    """
    Creates the local queue user group.

    Parameters:
    group (str): The name of the group to create.
    """
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


def create_local_agent_user(username: str, password: str) -> None:
    """
    Creates a local agent user account on Windows with a specified password and sets the account to never expire.
    The function sets the UF_DONT_EXPIRE_PASSWD flag to ensure the account's password never expires.

    Args:
    username (str): The username of the new agent account.
    password (str): The password for the new agent account. Ensure it meets Windows' password policy requirements.
    """
    logging.info(f"Creating Agent user {username}")
    user_info = {
        "name": username,
        "password": password,
        "priv": win32netcon.USER_PRIV_USER,  # User privilege level, Standard User
        "home_dir": None,
        "comment": "AWS Deadline Cloud Worker Agent User",
        "flags": win32netcon.UF_DONT_EXPIRE_PASSWD,
        "script_path": None,
    }

    try:
        win32net.NetUserAdd(None, 1, user_info)
    except Exception as e:
        logging.error(f"Failed to create user '{username}'. Error: {e}")
        raise
    else:
        logging.info(f"User '{username}' created successfully.")


def ensure_user_profile_exists(username: str, password: str):
    """
    Ensures a user profile is created by loading it then unloading it.

    Args:
        username (str): The user whose profile to load
        password (str): The user's password
    """
    logging.info(f"Loading user profile for '{username}'")
    logon_token = None
    user_profile = None
    try:
        # https://timgolden.me.uk/pywin32-docs/win32security__LogonUser_meth.html
        logon_token = win32security.LogonUser(
            Username=username,
            LogonType=win32security.LOGON32_LOGON_INTERACTIVE,
            LogonProvider=win32security.LOGON32_PROVIDER_DEFAULT,
            Password=password,
            Domain=None,
        )
        # https://timgolden.me.uk/pywin32-docs/win32profile__LoadUserProfile_meth.html
        user_profile = win32profile.LoadUserProfile(
            logon_token,
            {
                "UserName": username,
                "Flags": win32profile.PI_NOUI,
                "ProfilePath": None,
            },
        )
    except Exception as e:
        logging.error(f"Failed to load user profile for '{username}': {e}")
        raise
    else:
        logging.info("Successfully loaded user profile")
    finally:
        if user_profile is not None:
            assert logon_token is not None
            win32profile.UnloadUserProfile(logon_token, user_profile)
        if logon_token is not None:
            # Pass the handle directly as an int since logon_user returns a ctypes.HANDLE
            # and not a pywin32 PyHANDLE
            win32api.CloseHandle(logon_token)


def grant_account_rights(account_name: str, rights: list[str]):
    """
    Grants rights to an account

    Args:
        account_name (str): Name of account to grant rights to. Can be a user or a group.
        rights (list[str]): The rights to grant. See https://learn.microsoft.com/en-us/windows/win32/secauthz/privilege-constants.
            These constants are exposed by the win32security module of pywin32.
    """
    policy_handle = None
    try:
        acc_sid, _, _ = win32security.LookupAccountName(None, account_name)
        policy_handle = win32security.LsaOpenPolicy(None, win32security.POLICY_ALL_ACCESS)
        win32security.LsaAddAccountRights(
            policy_handle,
            acc_sid,
            rights,
        )
        logging.info(f"Successfully granted the following rights to {account_name}: {rights}")
    except Exception as e:
        logging.error(f"Failed to grant account {account_name} rights ({rights}): {e}")
        raise
    finally:
        if policy_handle is not None:
            win32api.CloseHandle(policy_handle)


def is_user_in_group(group_name: str, user_name: str) -> bool:
    """
    Checks if a user is in a group

    Args:
        group_name (str): The name of the group
        user_name (str): The name of the user

    Returns:
        bool: True if the user is in the group, false otherwise
    """
    try:
        group_members_info = win32net.NetLocalGroupGetMembers(None, group_name, 1)
    except Exception as e:
        logging.error(f"Failed to get group members of '{group_name}': {e}")
        raise

    return any(group_member["name"] == user_name for group_member in group_members_info[0])


def add_user_to_group(group_name: str, user_name: str) -> None:
    """
    Adds a specified user to a specified local group.

    Parameters:
    - group_name (str): The name of the local group to which the user will be added.
    - user_name (str): The name of the user to be added to the group.
    """
    try:
        # The user information must be in a dictionary with 'domainandname' key
        user_info = {"domainandname": user_name}
        win32net.NetLocalGroupAddMembers(
            None,  # the local computer is used.
            group_name,
            3,  # Specifies the domain and name of the new local group member.
            [user_info],
        )
        logging.info(f"User {user_name} is added to group {group_name}.")
    except Exception as e:
        logging.error(
            f"An error occurred during adding user {user_name} to the user group {group_name}: {e}"
        )
        raise


def update_config_file(
    deadline_config_sub_directory: str,
    farm_id: str,
    fleet_id: str,
    shutdown_on_stop: Optional[bool] = None,
    allow_ec2_instance_profile: Optional[bool] = None,
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
    if allow_ec2_instance_profile is not None:
        allow_ec2_instance_profile_toml = str(allow_ec2_instance_profile).lower()
        content = re.sub(
            r"^#*\s*allow_ec2_instance_profile\s*=\s*\w+$",
            f"allow_ec2_instance_profile = {allow_ec2_instance_profile_toml}",
            content,
            flags=re.MULTILINE,
        )
        if not re.search(
            rf"^allow_ec2_instance_profile = {re.escape(allow_ec2_instance_profile_toml)}$",
            content,
            flags=re.MULTILINE,
        ):
            raise InstallerFailedException(
                f"Failed to configure allow_ec2_instance_profile in {worker_config_file}"
            )
        else:
            updated_keys.append("allow_ec2_instance_profile")

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


def update_deadline_client_config(
    user: str,
    settings: dict[str, str],
) -> None:
    """
    Updates the Deadline Client config for the specified user.

    Args:
        user (str): The user to update the Deadline Client config for.
        settings (dict[str, str]]): The key-value pairs of settings to update.

    Raises:
        InstallerFailedException: _description_
    """
    # Build the Deadline client config path for the user
    deadline_client_config_path = deadline.client.config.config_file.CONFIG_FILE_PATH
    if not deadline_client_config_path.startswith("~"):
        raise InstallerFailedException(
            f"Cannot opt out of telemetry: Expected Deadline client config file path to start with a tilde (~), but got: {deadline_client_config_path}\n"
            f"This is because the Deadline client program (version {deadline.client.version}) is not compatible with this version of the Worker agent installer\n"
            f"To opt out of telemetry, please use a compatible version of the Deadline client program or run the following command as the worker user:\n\n"
            "deadline config set telemetry.opt_out true\n"
        )
    user_deadline_client_config_path = f"~{user}" + deadline_client_config_path.removeprefix("~")

    # Opt out of client telemetry for the agent user
    old_environ = os.environ.copy()
    try:
        os.environ[DEADLINE_CLIENT_CONFIG_PATH_OVERRIDE_ENV_VAR] = user_deadline_client_config_path
        for setting_key, setting_value in settings.items():
            deadline.client.config.config_file.set_setting(setting_key, setting_value)
    except Exception as e:
        logging.error(f"Failed to update Deadline Client configuration for user '{user}': {e}")
        raise
    finally:
        os.environ.clear()
        os.environ.update(old_environ)


def _check_and_stop_service(service_name: str):
    """
    Checks the status of a Windows service and stops it if running.

    Parameters:
        service_name (str): The name of the Windows service to check and stop
    """
    try:
        # Check if the service is installed.
        # It will return the SERVICE_STATUS objece
        # https://learn.microsoft.com/en-us/windows/win32/api/winsvc/ns-winsvc-service_status
        status = win32serviceutil.QueryServiceStatus(service_name)
        # The service is installed, now check its state
        if status[1] == win32service.SERVICE_RUNNING:
            logging.info(f"Service '{service_name}' is installed and running.")
            logging.info(f"Before installation, attempting to stop '{service_name}'...")
            win32serviceutil.StopService(service_name)
            service_stop_time_out_in_seconds = 300
            for seconds in range(service_stop_time_out_in_seconds):
                time.sleep(1)
                status = win32serviceutil.QueryServiceStatus(service_name)
                if status[1] == win32service.SERVICE_STOPPED:
                    logging.info(f"Service '{service_name}' has been stopped successfully.")
                    break
            else:
                logging.error(
                    f"Service '{service_name}' could not be stopped within {service_stop_time_out_in_seconds}."
                )
                exit(1)
    except pywintypes.error as e:
        if e.winerror == winerror.ERROR_SERVICE_DOES_NOT_EXIST:
            pass
        else:
            logging.error(
                f"Service '{service_name}' could not be stopped due to the exception: {e}."
            )
            exit(e.winerror)


def _install_service(
    *,
    agent_user_name: str,
    password: str,
) -> None:
    """Installs the Windows Service that hosts the Worker Agent

    Parameters
        agent_user_name(str): Worker Agent's account username
        password(str): The Worker Agent's user account password
    """

    # If the username does not contain the domain, then assume the local domain
    # https://learn.microsoft.com/en-us/windows/win32/secauthn/user-name-formats
    if "\\" not in agent_user_name and "@" not in agent_user_name:
        agent_user_name = f".\\{agent_user_name}"

    # Determine the Windows Service configuration. This uses the same logic as
    # win32serviceutil.HandleCommandLine() so that the service can be debugged
    # using:
    #
    #   python -m deadline_worker_agent.windows.win_service debug
    service_class_str = win32serviceutil.GetServiceClassString(WorkerAgentWindowsService)
    service_name = WorkerAgentWindowsService._svc_name_
    service_display_name = WorkerAgentWindowsService._svc_display_name_
    service_description = getattr(WorkerAgentWindowsService, "_svc_description_", None)
    exe_name = getattr(WorkerAgentWindowsService, "_exe_name_", None)
    exe_args = getattr(WorkerAgentWindowsService, "_exe_args_", None)

    # Check if the service is installed and stop it if running.
    _check_and_stop_service(service_name)

    # Configure the service to start on boot
    startup = win32service.SERVICE_AUTO_START

    logging.info(f'Configuring Windows Service "{service_display_name}"...')
    try:
        win32serviceutil.InstallService(
            service_class_str,
            service_name,
            service_display_name,
            serviceDeps=None,
            startType=startup,
            bRunInteractive=None,
            userName=agent_user_name,
            password=password,
            exeName=exe_name,
            perfMonIni=None,
            perfMonDll=None,
            exeArgs=exe_args,
            description=service_description,
            delayedstart=False,
        )
    except win32service.error as exc:
        if exc.winerror != winerror.ERROR_SERVICE_EXISTS:
            raise
        logging.info(f'Service "{service_display_name}" already exists, updating instead...')
        win32serviceutil.ChangeServiceConfig(
            service_class_str,
            service_name,
            serviceDeps=None,
            startType=startup,
            bRunInteractive=None,
            userName=agent_user_name,
            password=password,
            exeName=exe_name,
            displayName=service_display_name,
            perfMonIni=None,
            perfMonDll=None,
            exeArgs=exe_args,
            description=service_description,
            delayedstart=False,
        )
        logging.info(f'Successfully updated Windows Service "{service_display_name}"')
    else:
        logging.info(f'Successfully created Windows Service "{service_display_name}"')

    logging.info(f'Configuring the failure actions of Windows Service "{service_display_name}"...')
    configure_service_failure_actions(service_name)
    logging.info(
        f'Successfully configured the failure actions for Window Service "{service_display_name}"'
    )


def configure_service_failure_actions(service_name):
    """Configures the failure actions of the Windows Service.

    We use exponential backoff with a base of 2 seconds and doubling each iteration. This grows until
    it reaches ~4m 16s and then repeats indefinitely at this interval. The backoff resets if the service
    heals and stays alive for 20 minutes.

    This uses the ChangeServiceConfig2 win32 API:
    https://learn.microsoft.com/en-us/windows/win32/api/winsvc/nf-winsvc-changeserviceconfig2w

    Notably, the third parameter of ChangeServiceConfig2 expects a SERVICE_FAILURE_ACTIONSW structure.
    whose API reference docs best explains how Windows Service failure actions work:
    https://learn.microsoft.com/en-us/windows/win32/api/winsvc/ns-winsvc-service_failure_actionsw#remarks
    """

    # pywin32's ChangeServiceConfig2 wrapper accepts tuples ofs: (action type, delay in ms)
    # Exponential backoff with base of 2 seconds (2000 ms), doubling each iteration.
    # The backoff grows from 2 seconds to ~4m 16s over 8 attempts totalling 510s (or 8m 30s).
    actions = [(win32service.SC_ACTION_RESTART, 2000 * 2**i) for i in range(8)]

    logging.debug("Opening the Service Control Manager...")
    scm = win32service.OpenSCManager(None, None, win32service.SC_MANAGER_ALL_ACCESS)
    logging.debug("Successfully opened the Service Control Manager")
    try:
        logging.debug(f'Opening the Windows Service "{service_name}"')
        service = win32service.OpenService(scm, service_name, win32service.SERVICE_ALL_ACCESS)
        logging.debug(f'Successfully opened the Windows Service "{service_name}"')

        logging.debug(f'Modifying the failure actions of Windows Service "{service_name}...')
        try:
            win32service.ChangeServiceConfig2(
                service,
                win32service.SERVICE_CONFIG_FAILURE_ACTIONS,
                {
                    # Repeat the last action (restart with ~4m 16s delay) until the service recovers
                    # for 20 minutes (in seconds)
                    "ResetPeriod": 20 * 60,
                    "RebootMsg": None,
                    "Command": None,
                    "Actions": actions,
                },
            )
            logging.debug(
                f'Successfully modified the failure actions of Windows Service "{service_name}...'
            )
        finally:
            logging.debug(f'Closing the Windows Service "{service_name}"..')
            win32service.CloseServiceHandle(service)
            logging.debug(f'Successfully closed the Windows Service "{service_name}"')
    finally:
        logging.debug("Closing the Service Control Manager...")
        win32service.CloseServiceHandle(scm)
        logging.debug("Successfully closed the Service Control Manager")


def _start_service() -> None:
    """Starts the Windows Service hosting the Worker Agent"""
    service_name = WorkerAgentWindowsService._svc_name_

    logging.info(f'Starting service "{service_name}"...')
    try:
        win32serviceutil.StartService(serviceName=service_name)
    except Exception as e:
        logging.warning(f'Failed to start service "{service_name}": {e}')
    else:
        logging.info(f'Successfully started service "{service_name}"')


def get_effective_user_rights(user: str) -> set[str]:
    """
    Gets a set of a user's effective rights. This includes rights granted both directly
    and indirectly via group membership.

    Args:
        user (str): The user to get effective rights for

    Returns:
        set[str]: Set of rights the user effectively has.
    """
    user_sid, _, _ = win32security.LookupAccountName(None, user)
    sids_to_check = [user_sid]

    # Get SIDs of all groups the user is in
    # win32net.NetUserGetLocalGroups includes the LG_INCLUDE_INDIRECT flag by default
    group_names = win32net.NetUserGetLocalGroups(None, user)
    for group in group_names:
        group_sid, _, _ = win32security.LookupAccountName(None, group)
        sids_to_check.append(group_sid)

    policy_handle = win32security.LsaOpenPolicy(None, win32security.POLICY_ALL_ACCESS)
    try:
        effective_rights = set()

        for sid in sids_to_check:
            try:
                account_rights = win32security.LsaEnumerateAccountRights(policy_handle, sid)
            except pywintypes.error as e:
                if e.strerror == "The system cannot find the file specified.":
                    # Account is not directly assigned any rights
                    continue
                else:
                    raise
            else:
                effective_rights.update(account_rights)

        return effective_rights
    finally:
        if policy_handle is not None:
            win32api.CloseHandle(policy_handle)


def set_registry_key_value(
    reg_key: Union[int, str],
    reg_sub_key: Optional[str],
    value_name: str,
    value_type: int,
    value_data: Any,
):
    """
    Sets a value on the specified registry key

    Note: Registry operations also have support for transactional operations if we need it in the future
    See: https://timgolden.me.uk/pywin32-docs/win32api__RegOpenKeyTransacted_meth.html

    Args:
        reg_key (Union[int, str]): The registry key. Can either be a string name which will be used to lookup the value in win32con or int constants from win32con (e.g. win32con.HKEY_LOCAL_MACHINE, etc.)
        reg_sub_key (Optional[str]): The registry sub key
        value_name (str): The name of the value to set
        value_type (int): The type of the value data. Constants are available in win32con (e.g. win32con.REG_SZ)
        value_data (Any): The value data to set
    """
    full_reg_key = f"{reg_key}" + (f":{reg_sub_key}" if reg_sub_key else "")
    if isinstance(reg_key, str):
        assert hasattr(win32con, reg_key), f"{reg_key} not found in win32con"
        reg_key = getattr(win32con, reg_key)
    assert isinstance(reg_key, int)

    logging.info(f"Setting '{value_name}' in registry key '{full_reg_key}'")
    key_handle = None
    try:
        # https://timgolden.me.uk/pywin32-docs/win32api__RegOpenKeyEx_meth.html
        key_handle = win32api.RegOpenKeyEx(
            reg_key,
            reg_sub_key,
            # Note: These two arguments are reversed in the type hints and docs
            # This is the correct order
            0,  # reserved, only use 0
            win32con.KEY_SET_VALUE,
        )
        # https://timgolden.me.uk/pywin32-docs/win32api__RegSetValueEx_meth.html
        win32api.RegSetValueEx(
            key_handle,
            value_name,
            0,  # reserved, only use 0,
            value_type,
            value_data,
        )
    except Exception as e:
        logging.error(f"Failed to set '{value_name}' in registry key '{full_reg_key}': {e}")
        raise
    else:
        logging.info(f"Successfully set '{value_name}' in registry key '{full_reg_key}'")
    finally:
        if key_handle is not None:
            win32api.CloseHandle(key_handle)


def start_windows_installer(
    farm_id: str,
    fleet_id: str,
    region: str,
    allow_shutdown: bool,
    parser: ArgumentParser,
    user_name: str = DEFAULT_WA_USER,
    password: Optional[str] = None,
    group_name: str = DEFAULT_JOB_GROUP,
    install_service: bool = False,
    start_service: bool = False,
    confirm: bool = False,
    telemetry_opt_out: bool = False,
    grant_required_access: bool = False,
    allow_ec2_instance_profile: bool = True,
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

    # Check that user has Administrator privileges
    if not shell.IsUserAnAdmin():
        logging.error(f"User does not have Administrator privileges: {os.environ['USERNAME']}")
        print_helping_info_and_exit()

    # Print configuration
    print_banner()

    if not password:
        if check_account_existence(user_name):
            password = getpass("Agent user password: ")
            try:
                WindowsSessionUser(user_name, password=password)
            except BadCredentialsException:
                print("ERROR: Password incorrect")
                sys.exit(1)
        else:
            password = generate_password()

    print(
        f"Farm ID: {farm_id}\n"
        f"Fleet ID: {fleet_id}\n"
        f"Region: {region}\n"
        f"Worker agent user: {user_name}\n"
        f"Worker job group: {group_name}\n"
        f"Allow worker agent shutdown: {allow_shutdown}\n"
        f"Install Windows service: {install_service}\n"
        f"Start service: {start_service}\n"
        f"Telemetry opt-out: {telemetry_opt_out}\n"
        f"Disallow EC2 instance profile: {not allow_ec2_instance_profile}"
    )
    print()

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

    # Set of user rights to add to the worker agent user
    user_rights_to_grant: set[str] = set()
    if allow_shutdown:
        # User right to shutdown the machine
        user_rights_to_grant.add(win32security.SE_SHUTDOWN_NAME)
    if install_service:
        # User right to logon as a service
        user_rights_to_grant.add(win32security.SE_SERVICE_LOGON_NAME)
        # User right to increase memory quota for a process
        user_rights_to_grant.add(win32security.SE_INCREASE_QUOTA_NAME)
        # User right to replace a process-level token
        user_rights_to_grant.add(win32security.SE_ASSIGNPRIMARYTOKEN_NAME)

    # Check if the worker agent user exists, and create it if not
    agent_user_created = False
    if check_account_existence(user_name):
        logging.info(f"Using existing user ({user_name}) as worker agent user")

        # This is only to verify the credentials. It will raise a BadCredentialsError if the
        # credentials cannot be used to logon the user
        WindowsSessionUser(user=user_name, password=password)
    else:
        create_local_agent_user(user_name, password)
        agent_user_created = True

    # Load the user's profile to ensure it exists
    ensure_user_profile_exists(username=user_name, password=password)

    if is_user_in_group("Administrators", user_name):
        logging.info(f"Agent user '{user_name}' is already an administrator")
    elif not agent_user_created and not grant_required_access:
        logging.error(
            f"The Worker Agent user needs to run as an administrator, but the supplied user ({user_name}) exists "
            "and was not found to be in the Administrators group. Please provide an administrator user, specify a "
            "new username to have one created, or provide the --grant-required-access option to allow the installer "
            "to make the existing user an administrator."
        )
        sys.exit(1)
    else:
        # Add the agent user to Administrators before evaluating missing user rights
        # since it will inherit the user rights that Administrators have
        logging.info(f"Adding '{user_name}' to the Administrators group")
        add_user_to_group(group_name="Administrators", user_name=user_name)

    # Determine which rights we need to grant
    agent_user_rights = get_effective_user_rights(user_name)
    user_rights_to_grant -= agent_user_rights

    # Fail if an existing user was provided but there are rights to add and the user has not explicitly opted in
    if user_rights_to_grant and not agent_user_created and not grant_required_access:
        logging.error(
            f"The existing worker agent user ({user_name}) is missing the following required user rights: {user_rights_to_grant}\n"
            "Provide the --grant-required-access option to allow the installer to grant the missing rights to the user."
        )
        sys.exit(1)

    if user_rights_to_grant:
        grant_account_rights(user_name, list(user_rights_to_grant))
    else:
        logging.info(f"Agent user '{user_name}' has all required user rights")

    # Check if the job group exists, and create it if not
    if check_account_existence(group_name):
        logging.info(f"Using existing group ({group_name}) as the queue user group.")
    else:
        create_local_queue_user_group(group_name)

    if is_user_in_group(group_name, user_name):
        logging.info(f"Agent user '{user_name}' is already in group '{group_name}'")
    else:
        # Add the worker agent user to the job group
        add_user_to_group(group_name, user_name)

    # Create directories and configure their permissions
    agent_dirs = provision_directories(user_name)
    update_config_file(
        str(agent_dirs.deadline_config_subdir),
        farm_id,
        fleet_id,
        # This always sets shutdown_on_stop even if the user did not provide
        # any "shutdown" option to be consistent with POSIX installer
        shutdown_on_stop=allow_shutdown,
        allow_ec2_instance_profile=allow_ec2_instance_profile,
    )

    if telemetry_opt_out:
        logging.info("Opting out of client telemetry")
        update_deadline_client_config(
            user=user_name,
            settings={"telemetry.opt_out": "true"},
        )
        logging.info("Opted out of client telemetry")

    # Install the Windows service if specified
    if install_service:
        _install_service(
            agent_user_name=user_name,
            password=password,
        )

        # Set the AWS region in the service's environment
        logging.info(
            f"Setting region to {region} for {WorkerAgentWindowsService._svc_name_} service"
        )
        set_registry_key_value(
            # Specify attribute name rather than the int constant for readability in logs
            reg_key="HKEY_LOCAL_MACHINE",
            reg_sub_key=f"SYSTEM\\CurrentControlSet\\Services\\{WorkerAgentWindowsService._svc_name_}",
            value_name="Environment",
            value_type=win32con.REG_MULTI_SZ,  # Multi-string value
            value_data=[f"AWS_DEFAULT_REGION={region}"],
        )
        logging.info(
            f"Successfully set region to {region} for {WorkerAgentWindowsService._svc_name_} service"
        )

        # Start the Windows service if specified
        if start_service:
            _start_service()
