# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import logging
import subprocess
import re

import sys
from typing import Optional

import win32com
import win32com.client
import winerror

import pywintypes
import win32net

import win32security

# Defaults
default_wa_user = "deadline-worker"
default_job_group = "deadline-job-users"
logging.basicConfig(level=logging.INFO)


def print_usage():
    logging.info(
        """Arguments
---------
    -FarmId <FarmId>
        The Amazon Deadline Cloud Farm ID that the Worker belongs to.
    -FleetId <FLEET_ID>
        The Amazon Deadline Cloud Fleet ID that the Worker belongs to.
    -Region <REGION>
        The AWS region of the Amazon Deadline Cloud farm. Defaults to $Region.
    -User <USER>
        A user name that the Amazon Deadline Cloud Worker Agent will run as. Defaults to deadline-worker.
    -Group <GROUP>
        A group name that the Worker Agent shares with the user(s) that Jobs will be running as.
        Do not use the primary/effective group of the Worker Agent user specified in -User as
        this is not a secure configuration. Defaults to deadline-job-users.
    -WorkerAgentProgram <WORKER_AGENT_PROGRAM>
        An optional path to the Worker Agent program. This is used as the program path
        when creating the service. If not specified, the first program named
        deadline-worker-agent found in the PATH will be used.
    -NoInstallService
        Skips the worker agent service installation.
    -Start
        Starts the service as part of the installation. By default, the service
        is configured to start on system boot but not started immediately.
        This option is ignored if -NoInstallService is used.
    -Confirm
        Skips a confirmation prompt before performing the installation.
"""
    )


def print_banner():
    logging.info(
        "===========================================================\n"
        "|      Amazon Deadline Cloud Worker Agent Installer       |\n"
        "===========================================================\n"
    )


def check_user_existence(user_name: str) -> Optional[bool]:
    """
    Checks if a user exists on the system by attempting to resolve the user's SID.
    This method could be used in both Ad and Non-Ad environments.

    Args:
    user (str): The username to check for existence.

    Returns:
    bool: True if the user exists, otherwise False.
    """

    try:
        # Resolve the username to an SID
        sid, _, _ = win32security.LookupAccountName(None, user_name)

        # Resolve the SID back to a username as an additional check
        win32security.LookupAccountSid(None, sid)
        return True
    except pywintypes.error as e:
        if e.winerror == winerror.ERROR_NONE_MAPPED:
            return False
        else:
            logging.error(f"Error checking user existence: {e}")
            return None


def create_local_group(group_name: str) -> None:
    """
    Check if a user group exists on the system. If it doesn't exit then create it.

    Parameters:
    group (str): The name of the group to check for existence and creation.

    """
    wmi_service = win32com.client.Dispatch("WbemScripting.SWbemLocator")
    # The WMI namespace root/cimv2 is the default namespace and contains
    # classes for computer hardware and configuration.
    wmi_connect = wmi_service.ConnectServer(".", "root\\cimv2")

    query = f"SELECT * FROM Win32_Group WHERE Name='{group_name}'"
    group_object = wmi_connect.ExecQuery(query)

    if len(group_object) == 0:
        logging.info(f"Creating group {group_name}")
        win32net.NetLocalGroupAdd(
            None,
            1,
            {
                "name": group_name,
                "comment": "This is a local group created by the Agent Installer.",
            },
        )
        logging.info("Done creating group")
    else:
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

    pattern = "^" + prefix + "-[a-f0-9]{32}$"
    return re.match(pattern, text) is not None


def create_local_user_with_powershell(username: str) -> None:
    """
    This function invokes a PowerShell command from Python to create a new local user with the following properties:
    - No password
    - Account never expires

    Args:
    username (str): The username of the new local user to be created.

    Note: This method requires PowerShell access and appropriate permissions to create local users.
    """
    if not check_user_existence(username):
        logging.info(f"Creating user {username}")
        # TODO: Need to figure out Why we create No Password accounts here?
        # Pywin32 doesn't allow us to create account without password.
        powershell_command = f"New-LocalUser -Name {username} -NoPassword -AccountNeverExpires"
        try:
            subprocess.run(["powershell", "-Command", powershell_command], check=True)
            logging.info(f"User {username} created successfully.")
        except subprocess.CalledProcessError as e:
            logging.info(f"Failed to create user: {e}")
            raise e
        logging.info("Done creating user")
    else:
        logging.info(f"User {username} already exists")


def add_user_to_group(group_name, user_name):
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
        logging.info(
            f"An error occurred during adding user {user_name} to the user group {group_name}: {e}"
        )
        raise e


def start_windows_installer(
    farm_id: str,
    fleet_id: str,
    region: str,
    worker_agent_program: str,
    user_name: str = default_wa_user,
    group_name: str = default_job_group,
    no_install_service: bool = False,
    start: bool = False,
    confirm: bool = False,
):
    # Validate command line arguments
    if not farm_id:
        logging.error("-FarmId not specified")
        print_usage()
    elif not validate_deadline_id("farm", farm_id):
        logging.error(f"Not a valid value for -FarmId: {farm_id}")
        print_usage()
    if not fleet_id:
        logging.error("-FleetId not specified")
        print_usage()
    elif not validate_deadline_id("fleet", fleet_id):
        logging.error(f"Not a valid value for -FleetId: {fleet_id}")
        print_usage()

    # Print configuration
    print_banner()
    logging.info(
        f"Farm ID: {farm_id}\n"
        f"Fleet ID: {fleet_id}\n"
        f"Region: {region}\n"
        f"Worker agent user: {user_name}\n"
        f"Worker job group: {group_name}\n"
        f"Worker agent program path: {worker_agent_program}\n"
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

    # Check if the worker agent user exists, and create it if not
    create_local_user_with_powershell(user_name)

    # Check if the job group exists, and create it if not
    create_local_group(group_name)
    # Add the worker agent user to the job group
    add_user_to_group(group_name, user_name)
