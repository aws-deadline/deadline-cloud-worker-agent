# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import logging
import os
import shutil
import re
from typing import Dict

import win32security
import ntsecuritycon as con


def set_directory_permissions(path: str, user: str, permission_flags: int):
    """
    Sets directory permissions, removes existing inheritance, and adds specific user permissions.
    This function modifies the directory's ACL to include specific permissions for the provided
    user and administrators.

    Args:
    path (str): The directory path to set permissions on.
    user (str): The username to apply the permissions for.
    permission (int): The permission level to set. (e.g., 'Read', 'Write', 'FullControl')

    """

    # Get the security descriptor for the directory
    sd = win32security.GetFileSecurity(path, win32security.DACL_SECURITY_INFORMATION)

    # Create a new DACL
    dacl = win32security.ACL()

    # Add the new ACE for the user
    user_sid, _, _ = win32security.LookupAccountName("", user)
    dacl.AddAccessAllowedAceEx(
        win32security.ACL_REVISION_DS,
        con.OBJECT_INHERIT_ACE | con.CONTAINER_INHERIT_ACE,
        permission_flags,
        user_sid,
    )

    # Add the new ACE for the administrators group
    admins_sid = win32security.CreateWellKnownSid(win32security.WinBuiltinAdministratorsSid, None)
    dacl.AddAccessAllowedAceEx(
        win32security.ACL_REVISION_DS,
        con.OBJECT_INHERIT_ACE | con.CONTAINER_INHERIT_ACE,
        permission_flags,
        admins_sid,
    )

    # Set the DACL to the security descriptor, disabling inheritance
    sd.SetSecurityDescriptorDacl(1, dacl, 0)

    # Apply the modified DACL to the directory
    win32security.SetFileSecurity(path, win32security.DACL_SECURITY_INFORMATION, sd)


def configure_farm_and_fleet(
    deadline_config_sub_directory: str, farm_id: str, fleet_id: str
) -> None:
    """
    Correctly configures farm and fleet settings in a worker configuration file.
    This function ensures the worker.toml configuration file exists, backs it up, and then
    replaces specific placeholders with the provided farm and fleet IDs.

    Parameters:
    - deadline_config_sub_directory (str): Subdirectory for Deadline configuration files.
    - farm_id (str): The farm ID to set in the configuration.
    - fleet_id (str): The fleet ID to set in the configuration.

    """
    logging.info("Configuring farm and fleet")

    worker_config_file = os.path.join(deadline_config_sub_directory, "worker.toml")

    # Check if the worker.toml file exists, if not, create it from the example
    if not os.path.isfile(worker_config_file):
        # Directory where the script and example configuration files are located.
        script_dir = os.path.dirname(os.path.realpath(__file__))
        example_config_path = os.path.join(script_dir, "worker.toml.windows.example")
        shutil.copy(example_config_path, worker_config_file)

    # Make a backup of the worker configuration file
    backup_worker_config = worker_config_file + ".bak"
    shutil.copy(worker_config_file, backup_worker_config)

    # Read the content of the worker configuration file
    with open(worker_config_file, "r") as file:
        content = file.read()

    # Replace the placeholders with actual farm_id and fleet_id
    content = re.sub(
        r'^# farm_id\s*=\s*("REPLACE-WITH-WORKER-FARM-ID")$',
        f'farm_id = "{farm_id}"',
        content,
        flags=re.MULTILINE,
    )
    content = re.sub(
        r'^# fleet_id\s*=\s*("REPLACE-WITH-WORKER-FLEET-ID")$',
        f'fleet_id = "{fleet_id}"',
        content,
        flags=re.MULTILINE,
    )

    # Write the updated content back to the worker configuration file
    with open(worker_config_file, "w") as file:
        file.write(content)

    logging.info("Done farm and fleet Configuration")


def provision_directories(agent_username: str) -> Dict[str, str]:
    """
    Creates all required directories for Deadline Worker Agent.
    This function creates the following directories:
    - %PROGRAMDATA%\Amazon\Deadline
    - %PROGRAMDATA%\Amazon\Deadline\Logs
    - %PROGRAMDATA%\Amazon\Deadline\Cache
    - %PROGRAMDATA%\Amazon\Deadline\Config

    Parameters
        agent_username(str): Worker Agent's username used for setting the permission for the directories

    Returns
        Dict[str, str]: return a Dict containing all directories created in the function
    """

    program_data_path = os.environ.get("PROGRAMDATA", r"C:\ProgramData")
    deadline_dir = os.path.join(program_data_path, r"Amazon\Deadline")
    logging.info(f"Provisioning root directory ({deadline_dir})")
    os.makedirs(deadline_dir, exist_ok=True)
    set_directory_permissions(deadline_dir, agent_username, con.FILE_ALL_ACCESS)
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

    return {
        "deadline_dir": deadline_dir,
        "deadline_log_subdir": deadline_log_subdir,
        "deadline_persistence_subdir": deadline_persistence_subdir,
        "deadline_config_subdir": deadline_config_subdir,
    }
