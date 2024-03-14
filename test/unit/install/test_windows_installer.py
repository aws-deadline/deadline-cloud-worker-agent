# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import string
import sys
import sysconfig

import pytest

if sys.platform != "win32":
    pytest.skip("Windows-specific tests", allow_module_level=True)

from deadline_worker_agent.installer.win_installer import (
    ensure_local_queue_user_group_exists,
    ensure_local_agent_user,
    generate_password,
    start_windows_installer,
    validate_deadline_id,
)
from pathlib import Path
from pywintypes import error as PyWinTypesError
from unittest.mock import call, patch, MagicMock
from win32comext.shell import shell
from win32service import (
    error as win_service_error,
    SERVICE_AUTO_START,
)
from win32serviceutil import GetServiceClassString
import win32netcon
import winerror

from deadline_worker_agent import installer as installer_mod
from deadline_worker_agent.installer import ParsedCommandLineArguments, install
from deadline_worker_agent.installer import win_installer
from deadline_worker_agent.windows.win_service import WorkerAgentWindowsService


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
                worker_agent_program=Path(sysconfig.get_path("scripts")),
                install_service=parsed_args.install_service,
                start_service=parsed_args.service_start,
                confirm=parsed_args.confirmed,
                parser=mock_get_arg_parser(),
                user_name=parsed_args.user,
                group_name=parsed_args.group,
                password=parsed_args.password,
                allow_shutdown=parsed_args.allow_shutdown,
                telemetry_opt_out=parsed_args.telemetry_opt_out,
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
                worker_agent_program=Path(sysconfig.get_path("scripts")),
                install_service=parsed_args.install_service,
                start_service=parsed_args.service_start,
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
            ensure_local_queue_user_group_exists(group_name)
        mock_log_error.assert_called_with(
            f"Failed to create group {group_name}. Error: Test Failure"
        )


def test_unexpected_error_code_handling(group_name):
    with patch("win32net.NetLocalGroupGetInfo", side_effect=MockPyWinTypesError(9999)), patch(
        "win32net.NetLocalGroupAdd"
    ) as mock_group_add, patch("logging.error"):
        with pytest.raises(PyWinTypesError):
            ensure_local_queue_user_group_exists(group_name)
        mock_group_add.assert_not_called()


def test_ensure_local_agent_user_raises_exception_on_creation_failure():
    username = "testuser"
    password = "password123"
    error_message = "System error"
    with patch(
        "deadline_worker_agent.installer.win_installer.check_user_existence", return_value=False
    ), patch("win32net.NetUserAdd") as mocked_net_user_add, patch(
        "deadline_worker_agent.installer.win_installer.logging.error"
    ) as mocked_logging_error:
        mocked_net_user_add.side_effect = Exception(error_message)

        with pytest.raises(Exception):
            ensure_local_agent_user(username, password)

        mocked_logging_error.assert_called_once_with(
            f"Failed to create user '{username}'. Error: {error_message}"
        )


@patch("win32net.NetUserAdd")
def test_ensure_local_agent_user_logs_info_if_user_exists(mock_net_user_add: MagicMock):
    username = "existinguser"
    password = "password123"
    with patch(
        "deadline_worker_agent.installer.win_installer.check_user_existence", return_value=True
    ), patch("deadline_worker_agent.installer.win_installer.logging.info") as mocked_logging_info:
        ensure_local_agent_user(username, password)
        mock_net_user_add.assert_not_called()
        mocked_logging_info.assert_called_once_with(f"Agent User {username} already exists")


def test_ensure_local_agent_user_correct_parameters_passed_to_netuseradd():
    username = "newuser"
    password = "password123"
    with patch(
        "deadline_worker_agent.installer.win_installer.check_user_existence", return_value=False
    ), patch("win32net.NetUserAdd") as mocked_net_user_add:
        ensure_local_agent_user(username, password)

        expected_user_info = {
            "name": username,
            "password": password,
            "priv": win32netcon.USER_PRIV_USER,
            "home_dir": None,
            "comment": "AWS Deadline Cloud Worker Agent User",
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


def test_install_service_fresh_successful() -> None:
    """Tests that the installer calls pywin32's InstallService function to install the
    Windows Service with the correct arguments and that succeeds as a fresh install"""
    # GIVEN
    agent_user_name = "myagentuser"
    password = "apassword"
    expected_service_display_name = WorkerAgentWindowsService._svc_display_name_

    with (
        patch.object(win_installer.win32serviceutil, "InstallService") as mock_install_service,
        patch.object(win_installer.logging, "info") as mock_logging_info,
    ):
        # WHEN
        win_installer._install_service(
            agent_user_name=agent_user_name,
            password=password,
        )

    # THEN
    mock_install_service.assert_called_once_with(
        GetServiceClassString(WorkerAgentWindowsService),
        WorkerAgentWindowsService._svc_name_,
        expected_service_display_name,
        serviceDeps=None,
        startType=SERVICE_AUTO_START,
        bRunInteractive=None,
        userName=f".\\{agent_user_name}",
        password=password,
        exeName=getattr(WorkerAgentWindowsService, "_exe_name_", None),
        perfMonIni=None,
        perfMonDll=None,
        exeArgs=getattr(WorkerAgentWindowsService, "_exe_args_", None),
        description=getattr(WorkerAgentWindowsService, "_svc_description_", None),
        delayedstart=False,
    )
    mock_logging_info.assert_has_calls(
        calls=[
            call(f'Configuring Windows Service "{expected_service_display_name}"...'),
            call(f'Successfully created Windows Service "{expected_service_display_name}"'),
        ],
    )


@pytest.mark.parametrize(
    argnames=("install_service_exception",),
    argvalues=(
        pytest.param(
            win_service_error(
                winerror.ERROR_SERVICE_LOGON_FAILED,
                "InstallService",
                "some error message",
            ),
            id="win-service-error-not-existing",
        ),
        pytest.param(
            Exception("some other error"),
            id="non-win-service-error",
        ),
    ),
)
def test_install_service_fresh_fail(
    install_service_exception: Exception,
) -> None:
    """Tests how the _install_service() function deals with exceptions raised by
    pywin32's InstallService function other than the one we expect to handle if the service
    already exists.

    The exception should not be handled and raised as-is.
    """
    # GIVEN
    agent_user_name = "myagentuser"
    password = "apassword"
    expected_service_display_name = WorkerAgentWindowsService._svc_display_name_

    with (
        patch.object(
            win_installer.win32serviceutil, "InstallService", side_effect=install_service_exception
        ) as mock_install_service,
        patch.object(win_installer.logging, "info") as mock_logging_info,
    ):
        # WHEN
        def when():
            win_installer._install_service(
                agent_user_name=agent_user_name,
                password=password,
            )

        # THEN
        with pytest.raises(type(install_service_exception)) as raise_ctx:
            when()

    assert raise_ctx.value is install_service_exception
    mock_install_service.assert_called_once_with(
        GetServiceClassString(WorkerAgentWindowsService),
        WorkerAgentWindowsService._svc_name_,
        expected_service_display_name,
        serviceDeps=None,
        startType=SERVICE_AUTO_START,
        bRunInteractive=None,
        userName=f".\\{agent_user_name}",
        password=password,
        exeName=getattr(WorkerAgentWindowsService, "_exe_name_", None),
        perfMonIni=None,
        perfMonDll=None,
        exeArgs=getattr(WorkerAgentWindowsService, "_exe_args_", None),
        description=getattr(WorkerAgentWindowsService, "_svc_description_", None),
        delayedstart=False,
    )
    mock_logging_info.assert_called_once_with(
        f'Configuring Windows Service "{expected_service_display_name}"...'
    )


def test_install_service_existing_success() -> None:
    """Tests the behaviour of the _install_service function if the call to pywin32's
    InstallService function fails because the service already exists.

    The function is expected to catch this exception and instead call pywin32's
    ChangeServiceConfig function. This test asserts that ChangeServiceConfig is called
    with the correct arguments."""
    # GIVEN
    agent_user_name = "myagentuser"
    password = "apassword"
    expected_service_display_name = WorkerAgentWindowsService._svc_display_name_
    install_service_error = win_service_error(
        winerror.ERROR_SERVICE_EXISTS,
        "InstallService",
        "service alreadyt exists",
    )

    with (
        patch.object(
            win_installer.win32serviceutil, "InstallService", side_effect=install_service_error
        ) as mock_install_service,
        patch.object(
            win_installer.win32serviceutil, "ChangeServiceConfig"
        ) as mock_change_service_config,
        patch.object(win_installer.logging, "info") as mock_logging_info,
    ):
        # WHEN
        win_installer._install_service(
            agent_user_name=agent_user_name,
            password=password,
        )

    # THEN
    mock_install_service.assert_called_once_with(
        GetServiceClassString(WorkerAgentWindowsService),
        WorkerAgentWindowsService._svc_name_,
        expected_service_display_name,
        serviceDeps=None,
        startType=SERVICE_AUTO_START,
        bRunInteractive=None,
        userName=f".\\{agent_user_name}",
        password=password,
        exeName=getattr(WorkerAgentWindowsService, "_exe_name_", None),
        perfMonIni=None,
        perfMonDll=None,
        exeArgs=getattr(WorkerAgentWindowsService, "_exe_args_", None),
        description=getattr(WorkerAgentWindowsService, "_svc_description_", None),
        delayedstart=False,
    )
    mock_change_service_config.assert_called_once_with(
        GetServiceClassString(WorkerAgentWindowsService),
        WorkerAgentWindowsService._svc_name_,
        serviceDeps=None,
        startType=SERVICE_AUTO_START,
        bRunInteractive=None,
        userName=f".\\{agent_user_name}",
        password=password,
        exeName=getattr(WorkerAgentWindowsService, "_exe_name_", None),
        displayName=expected_service_display_name,
        perfMonIni=None,
        perfMonDll=None,
        exeArgs=getattr(WorkerAgentWindowsService, "_exe_args_", None),
        description=getattr(WorkerAgentWindowsService, "_svc_description_", None),
        delayedstart=False,
    )
    mock_logging_info.assert_has_calls(
        calls=[
            call(f'Configuring Windows Service "{expected_service_display_name}"...'),
            call(f'Service "{expected_service_display_name}" already exists, updating instead...'),
            call(f'Successfully updated Windows Service "{expected_service_display_name}"'),
        ],
    )
