# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import string
import sys
import pytest

if sys.platform != "win32":
    pytest.skip("Windows-specific tests", allow_module_level=True)

from pywintypes import error as PyWinTypesError
from deadline_worker_agent.installer.win_installer import (
    create_local_queue_user_group,
    create_local_agent_user,
    generate_password,
    start_windows_installer,
    validate_deadline_id,
)
import sysconfig
from pathlib import Path
from deadline_worker_agent import installer as installer_mod
from deadline_worker_agent.installer import ParsedCommandLineArguments, install
import pytest
from unittest.mock import patch, MagicMock
import win32netcon
from win32comext.shell import shell


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
                farm_id=parsed_args.farm_id,
                fleet_id=parsed_args.fleet_id,
                region=parsed_args.region,
                worker_agent_program=str(Path(sysconfig.get_path("scripts"))),
                no_install_service=not parsed_args.install_service,
                start=parsed_args.service_start,
                confirm=parsed_args.confirmed,
                parser=mock_get_arg_parser(),
                user_name=parsed_args.user,
                group_name=parsed_args.group,
                password=parsed_args.password,
                allow_shutdown=parsed_args.allow_shutdown,
                grant_existing_user_rights=parsed_args.grant_existing_user_rights,
            )


@patch.object(shell, "IsUserAnAdmin")
def test_start_windows_installer_fails_when_run_as_non_admin_user(
    is_user_an_admin, parsed_args: ParsedCommandLineArguments
) -> None:
    # GIVEN
    is_user_an_admin.return_value = False

    with (patch.object(installer_mod, "get_argument_parser") as mock_get_arg_parser,):
        with pytest.raises(SystemExit):
            # WHEN
            start_windows_installer(
                farm_id=parsed_args.farm_id,
                fleet_id=parsed_args.fleet_id,
                region=parsed_args.region,
                worker_agent_program=str(Path(sysconfig.get_path("scripts"))),
                no_install_service=not parsed_args.install_service,
                start=parsed_args.service_start,
                confirm=parsed_args.confirmed,
                parser=mock_get_arg_parser(),
                user_name=parsed_args.user,
                group_name=str(parsed_args.group),
                password=parsed_args.password,
                allow_shutdown=parsed_args.allow_shutdown,
            )

            # THEN
            is_user_an_admin.assert_called_once()


class MockPyWinTypesError(PyWinTypesError):
    def __init__(self, winerror):
        self.winerror = winerror


@pytest.fixture
def group_name():
    return "test_group"


def test_group_creation_failure(group_name):
    with patch("win32net.NetLocalGroupGetInfo", side_effect=MockPyWinTypesError(2220)), patch(
        "win32net.NetLocalGroupAdd", side_effect=Exception("Test Failure")
    ), patch("logging.error") as mock_log_error:
        with pytest.raises(Exception):
            create_local_queue_user_group(group_name)
        mock_log_error.assert_called_with(
            f"Failed to create group {group_name}. Error: Test Failure"
        )


def test_create_local_agent_user_raises_exception_on_creation_failure():
    username = "testuser"
    password = "password123"
    error_message = "System error"
    with patch(
        "deadline_worker_agent.installer.win_installer.check_account_existence", return_value=False
    ), patch("win32net.NetUserAdd") as mocked_net_user_add, patch(
        "deadline_worker_agent.installer.win_installer.logging.error"
    ) as mocked_logging_error:
        mocked_net_user_add.side_effect = Exception(error_message)

        with pytest.raises(Exception):
            create_local_agent_user(username, password)

        mocked_logging_error.assert_called_once_with(
            f"Failed to create user '{username}'. Error: {error_message}"
        )


def test_create_local_agent_user_correct_parameters_passed_to_netuseradd():
    username = "newuser"
    password = "password123"
    with patch(
        "deadline_worker_agent.installer.win_installer.check_account_existence", return_value=False
    ), patch("win32net.NetUserAdd") as mocked_net_user_add:
        create_local_agent_user(username, password)

        expected_user_info = {
            "name": username,
            "password": password,
            "priv": win32netcon.USER_PRIV_USER,
            "home_dir": None,
            "comment": "Amazon Deadline Cloud Worker Agent User",
            "flags": win32netcon.UF_DONT_EXPIRE_PASSWD,
            "script_path": None,
        }

        mocked_net_user_add.assert_called_once_with(None, 1, expected_user_info)


@patch("deadline_worker_agent.installer.win_installer.secrets.choice")
def test_generate_password(mock_choice):
    # Given
    password_length = 27
    characters = string.ascii_letters[:password_length]
    mock_choice.side_effect = characters

    # When
    password = generate_password(password_length)

    # Then
    expected_password = "".join(characters)
    assert password == expected_password


def test_validate_deadline_id():
    assert validate_deadline_id("deadline", "deadline-123e4567e89b12d3a456426655441234")


def test_non_valid_deadline_id1():
    assert not validate_deadline_id("deadline", "deadline-123")


def test_non_valid_deadline_id_with_wrong_prefix():
    assert not validate_deadline_id("deadline", "line-123e4567e89b12d3a456426655441234")
