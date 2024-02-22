# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import sys

import pytest

import win32api
import win32net

from deadline_worker_agent.installer.win_installer import (
    add_user_to_group,
    check_user_existence,
    configure_farm_and_fleet,
    ensure_local_agent_user,
    ensure_local_queue_user_group_exists,
    generate_password,
)

if sys.platform != "win32":
    pytest.skip("Windows-specific tests", allow_module_level=True)


def test_user_existence():
    current_user = win32api.GetUserNameEx(win32api.NameSamCompatible)
    result = check_user_existence(current_user)
    assert result


def test_user_existence_with_without_existing_user():
    result = check_user_existence("ImpossibleUser")
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
            allow_module_level=True,
        )


@pytest.fixture
def user_setup_and_teardown():
    """
    Pytest fixture to create a user before the test and ensure it is deleted after the test.
    """
    check_admin_privilege_and_skip_test()
    username = "InstallerTestUser"
    ensure_local_agent_user(username, generate_password())
    yield username
    delete_local_user(username)


def test_ensure_local_agent_user(user_setup_and_teardown):
    """
    Tests the creation of a local user and validates it exists.
    """
    assert check_user_existence(user_setup_and_teardown)


def group_exists(group_name: str) -> bool:
    """
    Check if a local group exists.
    """
    try:
        win32net.NetLocalGroupGetInfo(None, group_name, 1)
        return True
    except win32net.error:
        return False


def delete_group(group_name: str) -> None:
    """
    Delete a local group if it exists.
    """
    if group_exists(group_name):
        win32net.NetLocalGroupDel(None, group_name)


def is_user_in_group(group_name, username):
    group_members_info = win32net.NetLocalGroupGetMembers(None, group_name, 1)
    group_members = [member["name"] for member in group_members_info[0]]
    return username in group_members


@pytest.fixture
def setup_and_teardown_group():
    check_admin_privilege_and_skip_test()
    group_name = "user_group_for_agent_testing_only"
    # Ensure the group does not exist before the test
    delete_group(group_name)
    yield group_name  # This value will be used in the test function
    # Cleanup after test execution
    delete_group(group_name)


def test_ensure_local_group_exists(setup_and_teardown_group):
    group_name = setup_and_teardown_group
    # Ensure the group does not exist initially
    assert not group_exists(group_name), "Group already exists before test."
    ensure_local_queue_user_group_exists(group_name)
    assert group_exists(group_name), "Group was not created as expected."


def test_add_user_to_group(setup_and_teardown_group, user_setup_and_teardown):
    group_name = setup_and_teardown_group
    ensure_local_queue_user_group_exists(group_name)
    user_name = user_setup_and_teardown
    add_user_to_group(group_name, user_name)
    assert is_user_in_group(group_name, user_name), "User was not added to group as expected."


@pytest.fixture
def setup_example_config(tmp_path):
    # Create an example config file similar to 'worker.toml.windows.example' in the tmp_path
    example_config_path = os.path.join(tmp_path, "worker.toml")
    with open(example_config_path, "w") as f:
        f.write('# farm_id = "REPLACE-WITH-WORKER-FARM-ID"\n')
        f.write('# fleet_id = "REPLACE-WITH-WORKER-FLEET-ID"')
    return str(tmp_path)


def test_configure_farm_and_fleet_replaces_placeholders(setup_example_config):
    deadline_config_sub_directory = setup_example_config

    farm_id = "123"
    fleet_id = "456"
    configure_farm_and_fleet(deadline_config_sub_directory, farm_id, fleet_id)

    # Verify that the configuration file was created and placeholders were replaced
    worker_config_file = os.path.join(deadline_config_sub_directory, "worker.toml")
    assert os.path.isfile(worker_config_file), "Worker config file was not created"

    with open(worker_config_file, "r") as file:
        content = file.read()

    # Check if the farm_id and fleet_id have been correctly replaced
    assert f'farm_id = "{farm_id}"' in content, "farm_id placeholder was not replaced"
    assert f'fleet_id = "{fleet_id}"' in content, "fleet_id placeholder was not replaced"
    assert "#" not in content, "Comment placeholders were not removed"


def test_configure_farm_and_fleet_creates_backup(setup_example_config):
    deadline_config_sub_directory = setup_example_config

    # Call the function under test with some IDs
    configure_farm_and_fleet(deadline_config_sub_directory, "test_farm", "test_fleet")

    # Check that both the original and backup files exist
    worker_config_file = os.path.join(deadline_config_sub_directory, "worker.toml")
    backup_worker_config = worker_config_file + ".bak"

    assert os.path.isfile(worker_config_file), "Worker config file was not created"
    assert os.path.isfile(backup_worker_config), "Backup of worker config file was not created"
