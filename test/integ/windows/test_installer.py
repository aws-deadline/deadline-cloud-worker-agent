# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# This assertion short-circuits mypy from type checking this module on platforms other than Windows
# https://mypy.readthedocs.io/en/stable/common_issues.html#python-version-and-system-platform-checks
import sys

assert sys.platform == "win32"

import pathlib
import os
import sys
import typing
import uuid
from unittest.mock import patch

import pytest

import win32api
import win32con
import win32net
import win32security
import win32file
import ntsecuritycon

import deadline.client.config.config_file
from deadline_worker_agent.installer import win_installer
from deadline_worker_agent.installer.win_installer import (
    add_user_to_group,
    check_account_existence,
    update_config_file,
    create_local_agent_user,
    create_local_queue_user_group,
    generate_password,
    get_effective_user_rights,
    grant_account_rights,
    provision_directories,
    update_deadline_client_config,
    is_user_in_group,
    WorkerAgentDirectories,
)

try:
    from tomllib import load as load_toml
except ModuleNotFoundError:
    from tomli import load as load_toml


def test_user_existence():
    current_user = win32api.GetUserNameEx(win32api.NameSamCompatible)
    result = check_account_existence(current_user)
    assert result


def test_user_existence_with_without_existing_user():
    result = check_account_existence("ImpossibleUser")
    assert not result


def delete_local_user(username):
    """
    Deletes a local user using pywin32.

    Args:
    username (str): The username of the local user to be deleted.
    """
    try:
        win32net.NetUserDel(None, username)
        print(f"User {username} deleted successfully.")
    except win32net.error as e:
        print(f"Failed to delete user: {e}")


def check_admin_privilege_and_skip_test():
    env_var_value = os.getenv("RUN_AS_ADMIN", "False")
    if env_var_value.lower() != "true":
        pytest.skip(
            "Skipping all tests required Admin permission because RUN_AS_ADMIN is not set or false",
        )


@pytest.fixture
def net_user_get_info():
    # Mock the return value of win32net.NetUserGetInfo
    with patch(
        "win32net.NetUserGetInfo", return_value={"name": "test_user", "full_name": "Test User"}
    ):
        yield


@pytest.fixture
def windows_user_password(net_user_get_info):
    return generate_password("test_user")


@pytest.fixture
def windows_user(windows_user_password):
    """
    Pytest fixture to create a user before the test and ensure it is deleted after the test.
    """
    check_admin_privilege_and_skip_test()
    username = "InstallerTestUser"
    create_local_agent_user(username, windows_user_password)
    yield username
    delete_local_user(username)


def test_create_local_agent_user(windows_user):
    """
    Tests the creation of a local user and validates it exists.
    """
    assert check_account_existence(windows_user)


def test_ensure_user_profile_exists(windows_user, windows_user_password):
    # WHEN
    win_installer.ensure_user_profile_exists(windows_user, windows_user_password)

    # THEN
    # Verify user profile was created by checking that the home directory exists
    assert pathlib.Path(f"~{windows_user}").expanduser().exists()


def delete_group(group_name: str) -> None:
    """
    Delete a local group if it exists.
    """
    if check_account_existence(group_name):
        win32net.NetLocalGroupDel(None, group_name)


@pytest.fixture
def windows_group():
    check_admin_privilege_and_skip_test()
    group_name = "user_group_for_agent_testing_only"
    win32net.NetLocalGroupAdd(None, 1, {"name": group_name})
    yield group_name  # This value will be used in the test function
    # Cleanup after test execution
    delete_group(group_name)


def test_create_local_queue_user_group():
    group_name = "test_create_local_queue_user_group"
    # Ensure the group does not exist initially
    assert not check_account_existence(
        group_name
    ), f"Group '{group_name}' already exists before test."

    try:
        create_local_queue_user_group(group_name)
        assert check_account_existence(
            group_name
        ), f"Group '{group_name}' was not created as expected."
    finally:
        delete_group(group_name)


def test_is_user_in_group(windows_user, windows_group):
    # GIVEN
    assert not is_user_in_group(
        windows_group, windows_user
    ), f"User '{windows_user}' is already in group '{windows_group}'"
    win32net.NetLocalGroupAddMembers(None, windows_group, 3, [{"domainandname": windows_user}])

    # WHEN/THEN
    assert is_user_in_group(windows_group, windows_user)


def test_add_user_to_group(windows_group, windows_user):
    add_user_to_group(windows_group, windows_user)
    assert is_user_in_group(windows_group, windows_user), "User was not added to group as expected."


@pytest.fixture
def setup_example_config(tmp_path):
    # Create an example config file similar to 'worker.toml.example' in the tmp_path
    example_config_path = os.path.join(tmp_path, "worker.toml")
    with open(example_config_path, "w") as f:
        f.write(
            """
[worker]
# farm_id = "REPLACE-WITH-WORKER-FARM-ID"
# fleet_id = "REPLACE-WITH-WORKER-FLEET-ID"
                
[aws]
# allow_ec2_instance_profile = false
                
[os]
# shutdown_on_stop = false
"""
        )
    return str(tmp_path)


def test_update_config_file_updates_values(setup_example_config):
    # GIVEN
    deadline_config_sub_directory = setup_example_config

    farm_id = "123"
    fleet_id = "456"
    shutdown_on_stop = True
    allow_ec2_instance_profile = True

    # WHEN
    update_config_file(
        deadline_config_sub_directory=deadline_config_sub_directory,
        farm_id=farm_id,
        fleet_id=fleet_id,
        shutdown_on_stop=shutdown_on_stop,
        allow_ec2_instance_profile=allow_ec2_instance_profile,
    )

    # THEN
    # Verify that the configuration file was created and placeholders were replaced
    worker_config_file = os.path.join(deadline_config_sub_directory, "worker.toml")
    assert os.path.isfile(worker_config_file), "Worker config file was not created"

    with open(worker_config_file, "rb") as file:
        config_doc = load_toml(file)

    # Check if all values have been correctly replaced
    assert config_doc["worker"]["farm_id"] == farm_id
    assert config_doc["worker"]["fleet_id"] == fleet_id
    assert config_doc["os"]["shutdown_on_stop"] == shutdown_on_stop
    assert config_doc["aws"]["allow_ec2_instance_profile"] == allow_ec2_instance_profile


def test_update_config_file_creates_backup(setup_example_config):
    deadline_config_sub_directory = setup_example_config

    # Call the function under test with some IDs
    update_config_file(
        deadline_config_sub_directory=deadline_config_sub_directory,
        farm_id="test_farm",
        fleet_id="test_fleet",
        allow_ec2_instance_profile=True,
    )

    # Check that both the original and backup files exist
    worker_config_file = os.path.join(deadline_config_sub_directory, "worker.toml")
    backup_worker_config = worker_config_file + ".bak"

    assert os.path.isfile(worker_config_file), "Worker config file was not created"
    assert os.path.isfile(backup_worker_config), "Backup of worker config file was not created"


def verify_least_privilege(windows_user: str, path: pathlib.Path):
    builtin_admin_group_sid, _, _ = win32security.LookupAccountName(None, "Administrators")
    user_sid, _, _ = win32security.LookupAccountName(None, windows_user)
    sd = win32security.GetFileSecurity(
        str(path),
        win32con.DACL_SECURITY_INFORMATION | win32con.OWNER_SECURITY_INFORMATION,
    )
    # Verify ownership
    owner_sid = sd.GetSecurityDescriptorOwner()
    assert (
        builtin_admin_group_sid == owner_sid
    ), f"Expected directory '{path}' to be owned by 'Administrators' but got '{win32security.LookupAccountSid(None, owner_sid)}'"

    # Verify all ACEs
    dacl = sd.GetSecurityDescriptorDacl()
    assert dacl.GetAceCount() == 2, f"Number of aces for {path} was not as expected"
    for ace in [dacl.GetAce(i) for i in range(dacl.GetAceCount())]:
        _ace_info, mask, sid = ace
        ace_type, ace_flags = _ace_info

        assert (
            ace_type == ntsecuritycon.ACCESS_ALLOWED_ACE_TYPE
        ), f"Unexpected ace type found for {path}"
        assert (
            ace_flags == ntsecuritycon.OBJECT_INHERIT_ACE | ntsecuritycon.CONTAINER_INHERIT_ACE
        ), "Unexpected inheritance in ace for  {path}"
        assert (
            # we set ntsecuritycon.GENERIC_ALL but that gets converted to win32File.FILE_ALL_ACCESS
            mask
            == win32file.FILE_ALL_ACCESS
        ), f"Expected only FILE_FULL_ACCESS aces for {path} but found {mask}"
        assert sid in [builtin_admin_group_sid, user_sid], f"Unexpected sid found in ace for {path}"


def test_provision_directories(
    windows_user: str,
    tmp_path: pathlib.Path,
):
    # GIVEN
    root_dir = tmp_path / "ProgramDataTest"
    root_dir.mkdir()
    expected_dirs = WorkerAgentDirectories(
        deadline_dir=root_dir / "Amazon" / "Deadline",
        deadline_log_subdir=root_dir / "Amazon" / "Deadline" / "Logs",
        deadline_persistence_subdir=root_dir / "Amazon" / "Deadline" / "Cache",
        deadline_config_subdir=root_dir / "Amazon" / "Deadline" / "Config",
    )
    assert (
        not expected_dirs.deadline_dir.exists()
    ), f"Cannot test provision_directories because {expected_dirs.deadline_dir} already exists"
    assert (
        not expected_dirs.deadline_log_subdir.exists()
    ), f"Cannot test provision_directories because {expected_dirs.deadline_log_subdir} already exists"
    assert (
        not expected_dirs.deadline_persistence_subdir.exists()
    ), f"Cannot test provision_directories because {expected_dirs.deadline_persistence_subdir} already exists"
    assert (
        not expected_dirs.deadline_config_subdir.exists()
    ), f"Cannot test provision_directories because {expected_dirs.deadline_config_subdir} already exists"

    # WHEN
    with patch.dict(win_installer.os.environ, {"PROGRAMDATA": str(root_dir)}):
        actual_dirs = provision_directories(windows_user)

    # THEN
    assert actual_dirs == expected_dirs
    assert actual_dirs.deadline_dir.exists()
    verify_least_privilege(windows_user, actual_dirs.deadline_dir)
    assert actual_dirs.deadline_log_subdir.exists()
    verify_least_privilege(windows_user, actual_dirs.deadline_log_subdir)
    assert actual_dirs.deadline_persistence_subdir.exists()
    verify_least_privilege(windows_user, actual_dirs.deadline_persistence_subdir)
    assert actual_dirs.deadline_config_subdir.exists()
    verify_least_privilege(windows_user, actual_dirs.deadline_config_subdir)


def test_update_deadline_client_config(tmp_path: pathlib.Path) -> None:
    # GIVEN
    deadline_client_config_path = tmp_path / "deadline_client_config"
    deadline_client_config_path.touch(mode=0o644, exist_ok=False)

    with patch(
        "deadline.client.config.config_file.get_config_file_path",
        return_value=deadline_client_config_path,
    ):
        # WHEN
        update_deadline_client_config(
            user="",  # Doesn't matter, config path is mocked out anyway
            settings={"telemetry.opt_out": "true"},
        )

        # THEN
        assert deadline.client.config.config_file.get_setting("telemetry.opt_out") == "true"


def test_grant_account_rights(windows_user: str):
    # GIVEN
    rights = ["SeCreateSymbolicLinkPrivilege"]

    # WHEN
    grant_account_rights(windows_user, rights)

    # THEN
    user_sid, _, _ = win32security.LookupAccountName(None, windows_user)
    policy_handle = win32security.LsaOpenPolicy(None, win32security.POLICY_ALL_ACCESS)
    try:
        actual_rights = win32security.LsaEnumerateAccountRights(policy_handle, user_sid)
    finally:
        if policy_handle is not None:
            win32api.CloseHandle(policy_handle)

    assert set(rights).issubset(set(actual_rights))


def test_get_effective_user_rights(
    windows_user: str,
    windows_group: str,
) -> None:
    try:
        # GIVEN
        add_user_to_group(
            group_name=windows_group,
            user_name=windows_user,
        )
        grant_account_rights(
            account_name=windows_user,
            rights=[win32security.SE_BACKUP_NAME],
        )
        grant_account_rights(
            account_name=windows_group,
            rights=[win32security.SE_RESTORE_NAME],
        )

        # WHEN
        effective_rights = get_effective_user_rights(windows_user)

        # THEN
        assert effective_rights == set(
            [
                win32security.SE_BACKUP_NAME,
                win32security.SE_RESTORE_NAME,
            ]
        )
    finally:
        # Clean up the added rights since they stick around in Local Security Policy
        # even after the user and group have been deleted
        policy_handle = win32security.LsaOpenPolicy(None, win32security.POLICY_ALL_ACCESS)
        try:
            # Remove backup right from user
            user_sid, _, _ = win32security.LookupAccountName(None, windows_user)
            win32security.LsaRemoveAccountRights(
                policy_handle,
                user_sid,
                AllRights=False,
                UserRights=[win32security.SE_BACKUP_NAME],
            )

            # Remove restore right from group
            group_sid, _, _ = win32security.LookupAccountName(None, windows_group)
            win32security.LsaRemoveAccountRights(
                policy_handle,
                group_sid,
                AllRights=False,
                UserRights=[win32security.SE_RESTORE_NAME],
            )
        finally:
            win32api.CloseHandle(policy_handle)


# TODO: Modify the test user's registry hive instead of the current user's registry hive
# This is currently complicated by the fact a user's registry hive is not loaded by default.
# Running a process as the user causes its registry hive to get loaded (it is unloaded when the process exits)
def test_set_registry_key_value():
    # GIVEN
    reg_key = win32con.HKEY_CURRENT_USER
    reg_sub_key = "Environment"
    value_name = f"TEST-{uuid.uuid4()}"
    value_type = win32con.REG_SZ
    value_data = "TEST_VALUE"

    try:
        # WHEN
        win_installer.set_registry_key_value(
            reg_key, reg_sub_key, value_name, value_type, value_data
        )

        # THEN
        data = get_registry_key_value_data(reg_key, reg_sub_key, value_name)
        print(data)
        assert data[0] == "TEST_VALUE"
    finally:
        handle = None
        try:
            handle = win32api.RegOpenKeyEx(reg_key, reg_sub_key, 0, win32con.KEY_SET_VALUE)
            win32api.RegDeleteValue(handle, value_name)
        finally:
            if handle is not None:
                win32api.CloseHandle(handle)


def get_registry_key_value_data(
    key: int, sub_key: typing.Optional[str], value_name: str
) -> typing.Any:
    """
    Gets a registry key value data. If the key, sub key, or value name do not exist, an error is raised.

    Args:
        key (str): The registry key
        sub_key (typing.Optional[str]): The registry sub key
        value_name (str): The value name to get data for

    Returns:
        typing.Any: The value data
    """
    key_handle = None
    try:
        key_handle = win32api.RegOpenKeyEx(
            key,
            sub_key,
            # Note: These two arguments are reversed in the type hints and docs
            # This is the correct order
            0,
            win32con.KEY_READ,
        )
        return win32api.RegQueryValueEx(key_handle, value_name)
    finally:
        if key_handle is not None:
            win32api.CloseHandle(key_handle)
