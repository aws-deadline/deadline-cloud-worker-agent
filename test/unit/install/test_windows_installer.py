# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import sys
import pytest

if sys.platform != "win32":
    pytest.skip("Windows-specific tests", allow_module_level=True)

import sysconfig
import time
from pathlib import Path
from unittest.mock import MagicMock, patch
from deadline_worker_agent import installer as installer_mod
from deadline_worker_agent.installer import ParsedCommandLineArguments, install, generate_password
from deadline_worker_agent.installer.win_installer import (
    check_user_existence,
    create_local_user_with_password,
    validate_deadline_id,
    create_local_group,
    add_user_to_group,
)
import win32net
import win32api


def test_user_existence():
    current_user = win32api.GetUserNameEx(win32api.NameSamCompatible)
    result = check_user_existence(current_user)
    assert result


def test_user_existence_with_without_existing_user():
    result = check_user_existence("ImpossibleUser")
    assert not result


def test_validate_deadline_id():
    assert validate_deadline_id("deadline", "deadline-123e4567e89b12d3a456426655441234")


def test_invalidate_deadline_id1():
    assert not validate_deadline_id("deadline", "deadline-123")


def test_invalidate_deadline_id_with_wrong_prefix():
    assert not validate_deadline_id("deadline", "line-123e4567e89b12d3a456426655441234")


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


@pytest.fixture
def user_setup_and_teardown():
    """
    Pytest fixture to create a user before the test and ensure it is deleted after the test.
    """
    username = "InstallerTestUser"
    create_local_user_with_password(username, generate_password())
    yield username  # This is where the test function will execute
    delete_local_user(username)


def test_create_local_user_with_password(user_setup_and_teardown):
    """
    Tests the creation of a local user and validates it exists.
    """
    username = user_setup_and_teardown
    # Wait for user creation
    time.sleep(0.1)
    try:
        user_info = win32net.NetUserGetInfo(None, username, 1)
    except win32net.error as e:
        pytest.fail(f"User {username} could not be found: {e}")

    assert user_info is not None, "User info should not be None"
    assert user_info["name"] == username, f"Expected username to be '{username}'"


def test_start_windows_installer(
    parsed_args: ParsedCommandLineArguments,
) -> None:
    # GIVEN
    with patch.object(installer_mod, "get_argument_parser") as mock_get_arg_parser:
        with patch.object(installer_mod, "start_windows_installer") as mock_start_windows_installer:
            arg_parser: MagicMock = mock_get_arg_parser.return_value
            arg_parser.parse_args.return_value = parsed_args

            # WHEN
            install()

            # Then
            mock_start_windows_installer.assert_called_once_with(
                farm_id="farm-123e4567e89b12d3a456426655441234",
                fleet_id="fleet-123e4567e89b12d3a456426655444321",
                region="us-west-2",
                worker_agent_program=str(Path(sysconfig.get_path("scripts"))),
                password=parsed_args.password,
                user_name="wa_user",
                group_name="wa_group",
                no_install_service=False,
                start=False,
                confirm=True,
            )


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
    group_name = "user_group_for_agent_testing_only"
    # Ensure the group does not exist before the test
    delete_group(group_name)
    yield group_name  # This value will be used in the test function
    # Cleanup after test execution
    delete_group(group_name)


def test_create_local_group(setup_and_teardown_group):
    group_name = setup_and_teardown_group
    # Ensure the group does not exist initially
    assert not group_exists(group_name), "Group already exists before test."
    create_local_group(group_name)
    assert group_exists(group_name), "Group was not created as expected."


def test_add_user_to_group(setup_and_teardown_group, user_setup_and_teardown):
    group_name = setup_and_teardown_group
    create_local_group(group_name)
    user_name = user_setup_and_teardown
    add_user_to_group(group_name, user_name)
    assert is_user_in_group(group_name, user_name), "User was not added to group as expected."
