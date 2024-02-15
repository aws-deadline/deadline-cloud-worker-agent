# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import logging
import re
import secrets
import string

import sys
from argparse import ArgumentParser
from typing import Optional

import win32netcon
import winerror

import pywintypes
import win32net

import win32security

# Defaults
DEFAULT_WA_USER = "deadline-worker"
DEFAULT_JOB_GROUP = "deadline-job-users"
DEFAULT_PASSWORD_LENGTH = 12
logging.basicConfig(level=logging.INFO)


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
            raise


def ensure_local_group_exists(group_name: str) -> None:
    """
    Check if a user group exists on the system. If it doesn't exit then create it.

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
                        "comment": "This is a local group created by the Agent Installer.",
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
        raise e


def start_windows_installer(
    farm_id: str,
    fleet_id: str,
    region: str,
    worker_agent_program: str,
    password: str,
    parser: ArgumentParser,
    user_name: str = DEFAULT_WA_USER,
    group_name: str = DEFAULT_JOB_GROUP,
    no_install_service: bool = False,
    start: bool = False,
    confirm: bool = False,
):
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
    ensure_local_agent_user(user_name, password)

    # Check if the job group exists, and create it if not
    ensure_local_group_exists(group_name)
    # Add the worker agent user to the job group
    add_user_to_group(group_name, user_name)
