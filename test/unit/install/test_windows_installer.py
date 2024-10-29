# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import sys
import typing
from typing import Generator
from unittest.mock import Mock, call, patch, MagicMock, ANY

import pytest

if sys.platform != "win32":
    pytest.skip("Windows-specific tests", allow_module_level=True)

from deadline_worker_agent import installer as installer_mod
from deadline_worker_agent.installer import ParsedCommandLineArguments, win_installer, install
from deadline_worker_agent.windows.win_service import WorkerAgentWindowsService

import win32con
import win32netcon
import win32profile
import win32security
import win32service
import winerror
from win32comext.shell import shell
from win32service import error as win_service_error
from win32serviceutil import GetServiceClassString


@pytest.fixture
def parsed_kwargs(parsed_args: ParsedCommandLineArguments) -> Generator[dict, None, None]:
    assert parsed_args.region is not None, "Region is required"
    with patch.object(installer_mod, "get_argument_parser") as mock_get_arg_parser:
        arg_parser: MagicMock = mock_get_arg_parser.return_value
        arg_parser.parse_args.return_value = parsed_args
        yield {
            "farm_id": parsed_args.farm_id,
            "fleet_id": parsed_args.fleet_id,
            "region": parsed_args.region,
            "install_service": parsed_args.install_service,
            "start_service": parsed_args.service_start,
            "confirm": parsed_args.confirmed,
            "parser": mock_get_arg_parser(),
            "user_name": parsed_args.user,
            "group_name": str(parsed_args.group),
            "password": parsed_args.password,
            "allow_shutdown": parsed_args.allow_shutdown,
            "telemetry_opt_out": parsed_args.telemetry_opt_out,
            "grant_required_access": parsed_args.grant_required_access,
            "allow_ec2_instance_profile": not parsed_args.disallow_instance_profile,
            "windows_job_user": parsed_args.windows_job_user,
        }


def test_start_windows_installer(parsed_kwargs: dict) -> None:
    # GIVEN
    with patch.object(installer_mod, "start_windows_installer") as mock_start_windows_installer:
        # WHEN
        install()

        # Then
        mock_start_windows_installer.assert_called_once_with(
            **parsed_kwargs,
        )


@patch.object(shell, "IsUserAnAdmin")
def test_start_windows_installer_fails_when_run_as_non_admin_user(
    is_user_an_admin, parsed_kwargs: dict
) -> None:
    # GIVEN
    is_user_an_admin.return_value = False
    with pytest.raises(SystemExit):
        # WHEN
        win_installer.start_windows_installer(**parsed_kwargs)

        # THEN
        is_user_an_admin.assert_called_once()


@patch("deadline_worker_agent.installer.win_installer.check_account_existence", return_value=False)
@patch.object(shell, "IsUserAnAdmin", return_value=True)
def test_start_windows_installer_fails_when_windows_job_user_not_exists(
    is_user_and_admin: MagicMock,
    check_account_existence: MagicMock,
    parsed_kwargs: dict,
) -> None:
    # GIVEN
    with pytest.raises(win_installer.InstallerFailedException) as exc_info:
        # WHEN
        win_installer.start_windows_installer(**parsed_kwargs)

    # THEN
    assert (
        str(exc_info.value)
        == f"Account {parsed_kwargs['windows_job_user']} provided for argument windows-job-user does not exist. "
        "Please create the account before proceeding."
    )


def test_start_windows_installer_fails_when_windows_job_user_is_agent_user(
    parsed_kwargs: dict,
) -> None:
    # GIVEN
    with (
        patch.object(win_installer, "users_equal", return_value=True) as mock_users_equal,
        patch.object(win_installer, "check_account_existence", return_value=True),
        patch.object(shell, "IsUserAnAdmin", return_value=True),
        pytest.raises(win_installer.InstallerFailedException) as exc_info,
    ):
        # WHEN
        win_installer.start_windows_installer(**parsed_kwargs)

    # THEN
    assert (
        str(exc_info.value)
        == f"Argument for windows-job-user cannot be the same as the worker agent user: {parsed_kwargs['user_name']}. "
        "If you wish to run jobs as the agent user, set run_jobs_as_agent_user = true in the agent configuration file."
    )
    mock_users_equal.assert_called_once_with(
        parsed_kwargs["windows_job_user"], parsed_kwargs["user_name"]
    )


# Override the "user" fixture, which feeds into the parsed_kwargs fixture
# with Valid domain username formats. For valid domain user formats, see:
# https://learn.microsoft.com/en-us/windows/win32/secauthn/user-name-formats
@pytest.mark.parametrize(
    argnames="user",
    argvalues=(
        pytest.param("user@domain", id="user principal name"),
        pytest.param(r"domain\username", id="down-level logon name"),
    ),
)
# No job user override
@pytest.mark.parametrize(
    argnames="windows_job_user",
    argvalues=(None,),
)
def test_start_windows_installer_fails_on_domain_worker_user(
    parsed_kwargs: dict,
) -> None:
    # GIVEN
    with (
        patch.object(shell, "IsUserAnAdmin", return_value=True),
        pytest.raises(win_installer.InstallerFailedException) as raise_ctx,
    ):
        # WHEN
        win_installer.start_windows_installer(**parsed_kwargs)

    # THEN
    assert str(raise_ctx.value) == (
        "running worker agent as a domain user is not currently supported. You can "
        "have jobs run as a domain user by configuring the queue job run user to specify a "
        "domain user account."
    )


class TestCreateLocalQueueUserGroup:
    """Tests for the create_local_queue_user_group function"""

    @pytest.fixture
    def group_name(self) -> str:
        return "test_group"

    @pytest.fixture(autouse=True)
    def mock_NetLocalGroupAdd(self) -> typing.Generator[MagicMock, None, None]:
        with patch.object(win_installer.win32net, "NetLocalGroupAdd") as m:
            yield m

    def test_creates_group(self, group_name: str, mock_NetLocalGroupAdd: MagicMock):
        # WHEN
        win_installer.create_local_queue_user_group(group_name)

        # THEN
        mock_NetLocalGroupAdd.assert_called_once_with(
            None,
            1,
            {
                "name": group_name,
                "comment": ANY,
            },
        )

    def test_raises_on_group_creation_failure(
        self,
        group_name: str,
        mock_NetLocalGroupAdd: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ):
        # GIVEN
        mock_NetLocalGroupAdd.side_effect = Exception("Test Failure")

        with pytest.raises(Exception) as raised_exc:
            # WHEN
            win_installer.create_local_queue_user_group(group_name)

        # THEN
        assert raised_exc.value is mock_NetLocalGroupAdd.side_effect
        assert f"Failed to create group {group_name}. Error: Test Failure" in caplog.text


class TestCreateLocalAgentUser:
    """Tests for the create_local_agent_user function"""

    @pytest.fixture
    def username(self) -> str:
        return "testuser"

    @pytest.fixture
    def password(self) -> str:
        return "password123"

    @pytest.fixture(autouse=True)
    def mock_NetUserAdd(self) -> typing.Generator[MagicMock, None, None]:
        with patch.object(win_installer.win32net, "NetUserAdd") as m:
            yield m

    def test_raises_on_creation_failure(
        self,
        username: str,
        password: str,
        mock_NetUserAdd: MagicMock,
    ):
        # GIVEN
        error_message = "System error"
        mock_NetUserAdd.side_effect = Exception(error_message)
        with (
            patch(
                "deadline_worker_agent.installer.win_installer.check_account_existence",
                return_value=False,
            ),
            patch(
                "deadline_worker_agent.installer.win_installer.logging.error"
            ) as mocked_logging_error,
            pytest.raises(Exception) as raised_exc,
        ):
            # WHEN
            win_installer.create_local_agent_user(username, password)

        # THEN
        assert raised_exc.value is mock_NetUserAdd.side_effect
        mocked_logging_error.assert_called_once_with(
            f"Failed to create user '{username}'. Error: {error_message}"
        )

    def test_creates_user(
        self,
        username: str,
        password: str,
        mock_NetUserAdd: MagicMock,
    ):
        # WHEN
        win_installer.create_local_agent_user(username, password)

        # THEN
        expected_user_info = {
            "name": username,
            "password": password,
            "priv": win32netcon.USER_PRIV_USER,
            "home_dir": None,
            "comment": "AWS Deadline Cloud Worker Agent User",
            "flags": win32netcon.UF_DONT_EXPIRE_PASSWD,
            "script_path": None,
        }
        mock_NetUserAdd.assert_called_once_with(None, 1, expected_user_info)


class TestEnsureUserProfileExists:
    """Tests for ensure_user_profile_exists function"""

    @pytest.fixture
    def username(self) -> str:
        return "testuser"

    @pytest.fixture
    def password(self) -> str:
        return "password123"

    @pytest.fixture(autouse=True)
    def mock_LogonUser(self) -> typing.Generator[MagicMock, None, None]:
        with patch.object(win_installer.win32security, "LogonUser") as m:
            yield m

    @pytest.fixture(autouse=True)
    def mock_LoadUserProfile(self) -> typing.Generator[MagicMock, None, None]:
        with patch.object(win_installer.win32profile, "LoadUserProfile") as m:
            yield m

    @pytest.fixture(autouse=True)
    def mock_UnloadUserProfile(self) -> typing.Generator[MagicMock, None, None]:
        with patch.object(win_installer.win32profile, "UnloadUserProfile") as m:
            yield m

    @pytest.fixture(autouse=True)
    def mock_CloseHandle(self) -> typing.Generator[MagicMock, None, None]:
        with patch.object(win_installer.win32api, "CloseHandle") as m:
            yield m

    def test_loads_user_profile(
        self,
        username: str,
        password: str,
        mock_LogonUser: MagicMock,
        mock_LoadUserProfile: MagicMock,
        mock_UnloadUserProfile: MagicMock,
        mock_CloseHandle: MagicMock,
    ):
        # WHEN
        win_installer.ensure_user_profile_exists(username, password)

        # THEN
        mock_LogonUser.assert_called_once_with(
            Username=username,
            LogonType=win32security.LOGON32_LOGON_INTERACTIVE,
            LogonProvider=win32security.LOGON32_PROVIDER_DEFAULT,
            Password=password,
            Domain=None,
        )
        mock_LoadUserProfile.assert_called_once_with(
            mock_LogonUser.return_value,
            {
                "UserName": username,
                "Flags": win32profile.PI_NOUI,
                "ProfilePath": None,
            },
        )
        mock_UnloadUserProfile.assert_called_once_with(
            mock_LogonUser.return_value, mock_LoadUserProfile.return_value
        )
        mock_CloseHandle.assert_called_once_with(mock_LogonUser.return_value)

    def test_raises_on_logon_user_failure(
        self,
        username: str,
        password: str,
        mock_LogonUser: MagicMock,
        mock_UnloadUserProfile: MagicMock,
        mock_CloseHandle: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ):
        # GIVEN
        mock_LogonUser.side_effect = Exception("Failed")

        with pytest.raises(Exception) as raised_exc:
            # WHEN
            win_installer.ensure_user_profile_exists(username, password)

        # THEN
        assert raised_exc.value is mock_LogonUser.side_effect
        assert f"Failed to load user profile for '{username}'" in caplog.text
        mock_UnloadUserProfile.assert_not_called()
        mock_CloseHandle.assert_not_called()

    def test_raises_on_load_user_profile_failure(
        self,
        username: str,
        password: str,
        mock_LogonUser: MagicMock,
        mock_LoadUserProfile: MagicMock,
        mock_UnloadUserProfile: MagicMock,
        mock_CloseHandle: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ):
        # GIVEN
        mock_LoadUserProfile.side_effect = Exception("Failed")

        with pytest.raises(Exception) as raised_exc:
            # WHEN
            win_installer.ensure_user_profile_exists(username, password)

        # THEN
        assert raised_exc.value is mock_LoadUserProfile.side_effect
        assert f"Failed to load user profile for '{username}'" in caplog.text
        mock_UnloadUserProfile.assert_not_called()
        mock_CloseHandle.assert_called_once_with(mock_LogonUser.return_value)


def test_validate_deadline_id():
    assert win_installer.validate_deadline_id(
        "deadline", "deadline-123e4567e89b12d3a456426655441234"
    )


def test_non_valid_deadline_id1():
    assert not win_installer.validate_deadline_id("deadline", "deadline-123")


def test_non_valid_deadline_id_with_wrong_prefix():
    assert not win_installer.validate_deadline_id(
        "deadline", "line-123e4567e89b12d3a456426655441234"
    )


class TestInstallService:
    """Test cases for the install_service() function"""

    @pytest.fixture(autouse=True)
    def mock_configure_service_failure_actions(self) -> typing.Generator[Mock, None, None]:
        with patch.object(
            win_installer, "configure_service_failure_actions", new_callable=Mock
        ) as mock_configure_service_failure_actions:
            yield mock_configure_service_failure_actions

    def test_install_service_fresh_successful(
        self,
        mock_configure_service_failure_actions: Mock,
    ) -> None:
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
            startType=win32service.SERVICE_AUTO_START,
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
                call(
                    f'Configuring the failure actions of Windows Service "{expected_service_display_name}"...'
                ),
                call(
                    f'Successfully configured the failure actions for Window Service "{expected_service_display_name}"'
                ),
            ],
        )
        mock_configure_service_failure_actions.assert_called_once_with(
            WorkerAgentWindowsService._svc_name_
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
        self,
        install_service_exception: Exception,
        mock_configure_service_failure_actions: Mock,
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
                win_installer.win32serviceutil,
                "InstallService",
                side_effect=install_service_exception,
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
            startType=win32service.SERVICE_AUTO_START,
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
        mock_configure_service_failure_actions.assert_not_called()

    def test_install_service_existing_success(
        self,
        mock_configure_service_failure_actions: Mock,
    ) -> None:
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
            startType=win32service.SERVICE_AUTO_START,
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
            startType=win32service.SERVICE_AUTO_START,
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
                call(
                    f'Service "{expected_service_display_name}" already exists, updating instead...'
                ),
                call(f'Successfully updated Windows Service "{expected_service_display_name}"'),
                call(
                    f'Configuring the failure actions of Windows Service "{expected_service_display_name}"...'
                ),
                call(
                    f'Successfully configured the failure actions for Window Service "{expected_service_display_name}"'
                ),
            ],
        )
        mock_configure_service_failure_actions.assert_called_once_with(
            WorkerAgentWindowsService._svc_name_
        )


class TestConfigureServiceFailureActions:
    """Test cases for configure_service_failure_actions()"""

    @pytest.fixture(autouse=True)
    def mock_win32_service(self) -> typing.Generator[Mock, None, None]:
        with patch.object(win_installer, "win32service", new_callable=Mock) as mock_win32_service:
            yield mock_win32_service

    @pytest.fixture
    def mock_open_sc_manager(self, mock_win32_service: Mock) -> Mock:
        return mock_win32_service.OpenSCManager

    @pytest.fixture
    def mock_open_service(self, mock_win32_service: Mock) -> Mock:
        return mock_win32_service.OpenService

    @pytest.fixture
    def mock_close_service_handle(self, mock_win32_service: Mock) -> Mock:
        return mock_win32_service.CloseServiceHandle

    @pytest.fixture
    def mock_change_service_config2(self, mock_win32_service: Mock) -> Mock:
        return mock_win32_service.ChangeServiceConfig2

    @pytest.fixture
    def mock_logging_debug(self) -> typing.Generator[Mock, None, None]:
        with patch.object(win_installer.logging, "debug") as mock_logging_debug:
            yield mock_logging_debug

    @pytest.fixture(params=("svc1", "svc2"))
    def service_name(self, request) -> str:
        return request.param

    def test_success(
        self,
        mock_win32_service: Mock,
        mock_open_sc_manager: Mock,
        mock_open_service: Mock,
        mock_close_service_handle: Mock,
        mock_change_service_config2: Mock,
        service_name: str,
        mock_logging_debug: MagicMock,
    ) -> None:
        # WHEN
        win_installer.configure_service_failure_actions(service_name)

        # THEN
        mock_open_sc_manager.assert_called_once_with(
            None, None, mock_win32_service.SC_MANAGER_ALL_ACCESS
        )
        mock_open_service.assert_called_once_with(
            mock_open_sc_manager.return_value,
            service_name,
            mock_win32_service.SERVICE_ALL_ACCESS,
        )
        mock_change_service_config2.assert_called_once_with(
            mock_open_service.return_value,
            mock_win32_service.SERVICE_CONFIG_FAILURE_ACTIONS,
            {
                "ResetPeriod": 1200,
                "RebootMsg": None,
                "Command": None,
                "Actions": [(mock_win32_service.SC_ACTION_RESTART, 2000 * 2**i) for i in range(8)],
            },
        )
        mock_close_service_handle.assert_has_calls(
            [call(mock_open_service.return_value), call(mock_open_sc_manager.return_value)],
            any_order=False,
        )
        assert mock_close_service_handle.call_count == 2
        mock_logging_debug.assert_has_calls(
            [
                call("Opening the Service Control Manager..."),
                call("Successfully opened the Service Control Manager"),
                call(f'Opening the Windows Service "{service_name}"'),
                call(f'Successfully opened the Windows Service "{service_name}"'),
                call(f'Modifying the failure actions of Windows Service "{service_name}...'),
                call(
                    f'Successfully modified the failure actions of Windows Service "{service_name}...'
                ),
                call(f'Closing the Windows Service "{service_name}"..'),
                call(f'Successfully closed the Windows Service "{service_name}"'),
                call("Closing the Service Control Manager..."),
                call("Successfully closed the Service Control Manager"),
            ],
            any_order=False,
        )

    def test_fail_open_scm(
        self,
        mock_win32_service: Mock,
        mock_open_sc_manager: Mock,
        mock_open_service: Mock,
        mock_close_service_handle: Mock,
        mock_change_service_config2: Mock,
        mock_logging_debug: Mock,
        service_name: str,
    ) -> None:
        # GIVEN
        error = Exception("some error")
        mock_open_sc_manager.side_effect = error

        # WHEN
        def when() -> None:
            win_installer.configure_service_failure_actions(service_name)

        # THEN
        with pytest.raises(type(error)) as raise_ctxt:
            when()
        assert raise_ctxt.value is error
        mock_open_sc_manager.assert_called_once_with(
            None, None, mock_win32_service.SC_MANAGER_ALL_ACCESS
        )
        mock_open_service.assert_not_called()
        mock_change_service_config2.assert_not_called()
        mock_close_service_handle.assert_not_called()
        mock_logging_debug.assert_called_once_with("Opening the Service Control Manager...")

    def test_fail_open_service(
        self,
        mock_win32_service: Mock,
        mock_open_sc_manager: Mock,
        mock_open_service: Mock,
        mock_close_service_handle: Mock,
        mock_change_service_config2: Mock,
        mock_logging_debug: Mock,
        service_name: str,
    ) -> None:
        # GIVEN
        error = Exception("some error")
        mock_open_service.side_effect = error

        # WHEN
        def when() -> None:
            win_installer.configure_service_failure_actions(service_name)

        # THEN
        with pytest.raises(type(error)) as raise_ctxt:
            when()
        assert raise_ctxt.value is error
        mock_open_sc_manager.assert_called_once_with(
            None, None, mock_win32_service.SC_MANAGER_ALL_ACCESS
        )
        mock_open_service.assert_called_once_with(
            mock_open_sc_manager.return_value,
            service_name,
            mock_win32_service.SERVICE_ALL_ACCESS,
        )
        mock_change_service_config2.assert_not_called()
        mock_close_service_handle.assert_called_once_with(mock_open_sc_manager.return_value)
        mock_logging_debug.assert_has_calls(
            [
                call("Opening the Service Control Manager..."),
                call("Successfully opened the Service Control Manager"),
                call(f'Opening the Windows Service "{service_name}"'),
                call("Closing the Service Control Manager..."),
                call("Successfully closed the Service Control Manager"),
            ],
            any_order=False,
        )
        assert mock_logging_debug.call_count == 5

    def test_fail_change_service_config2(
        self,
        mock_win32_service: Mock,
        mock_open_sc_manager: Mock,
        mock_open_service: Mock,
        mock_close_service_handle: Mock,
        mock_change_service_config2: Mock,
        mock_logging_debug: Mock,
        service_name: str,
    ) -> None:
        # GIVEN
        error = Exception("some error")
        mock_change_service_config2.side_effect = error

        # WHEN
        def when() -> None:
            win_installer.configure_service_failure_actions(service_name)

        # THEN
        with pytest.raises(type(error)) as raise_ctxt:
            when()
        assert raise_ctxt.value is error
        mock_open_sc_manager.assert_called_once_with(
            None, None, mock_win32_service.SC_MANAGER_ALL_ACCESS
        )
        mock_open_service.assert_called_once_with(
            mock_open_sc_manager.return_value,
            service_name,
            mock_win32_service.SERVICE_ALL_ACCESS,
        )
        mock_change_service_config2.assert_called_once_with(
            mock_open_service.return_value,
            mock_win32_service.SERVICE_CONFIG_FAILURE_ACTIONS,
            {
                "ResetPeriod": 1200,
                "RebootMsg": None,
                "Command": None,
                "Actions": [(mock_win32_service.SC_ACTION_RESTART, 2000 * 2**i) for i in range(8)],
            },
        )
        mock_close_service_handle.assert_has_calls(
            [call(mock_open_service.return_value), call(mock_open_sc_manager.return_value)],
            any_order=False,
        )
        assert mock_close_service_handle.call_count == 2
        mock_logging_debug.assert_has_calls(
            [
                call("Opening the Service Control Manager..."),
                call("Successfully opened the Service Control Manager"),
                call(f'Opening the Windows Service "{service_name}"'),
                call(f'Successfully opened the Windows Service "{service_name}"'),
                call(f'Modifying the failure actions of Windows Service "{service_name}...'),
                call(f'Closing the Windows Service "{service_name}"..'),
                call(f'Successfully closed the Windows Service "{service_name}"'),
                call("Closing the Service Control Manager..."),
                call("Successfully closed the Service Control Manager"),
            ],
            any_order=False,
        )
        assert mock_logging_debug.call_count == 9


class TestSetRegistryKeyValue:
    """Tests for set_registry_key_value"""

    @pytest.fixture(autouse=True)
    def mock_RegOpenKeyEx(self) -> typing.Generator[MagicMock, None, None]:
        with patch.object(win_installer.win32api, "RegOpenKeyEx") as m:
            yield m

    @pytest.fixture(autouse=True)
    def mock_RegSetValueEx(self) -> typing.Generator[MagicMock, None, None]:
        with patch.object(win_installer.win32api, "RegSetValueEx") as m:
            yield m

    @pytest.fixture(autouse=True)
    def mock_CloseHandle(self) -> typing.Generator[MagicMock, None, None]:
        with patch.object(win_installer.win32api, "CloseHandle") as m:
            yield m

    @pytest.mark.parametrize(
        ["reg_key", "expected_reg_key_call_arg"],
        [
            ["HKEY_LOCAL_MACHINE", win32con.HKEY_LOCAL_MACHINE],
            [win32con.HKEY_USERS, win32con.HKEY_USERS],
        ],
    )
    def test_sets_registry_key_value(
        self,
        reg_key: typing.Union[str, int],
        expected_reg_key_call_arg: int,
        mock_RegOpenKeyEx: MagicMock,
        mock_RegSetValueEx: MagicMock,
        mock_CloseHandle: MagicMock,
    ):
        # GIVEN
        reg_sub_key = "Test\\Sub\\Key"
        value_name = "ValueName"
        value_type = win32con.REG_SZ
        value_data = "ValueData"

        # WHEN
        win_installer.set_registry_key_value(
            reg_key,
            reg_sub_key,
            value_name,
            value_type,
            value_data,
        )

        # THEN
        mock_RegOpenKeyEx.assert_called_once_with(
            expected_reg_key_call_arg,
            reg_sub_key,
            0,
            win32con.KEY_SET_VALUE,
        )
        mock_RegSetValueEx.assert_called_once_with(
            mock_RegOpenKeyEx.return_value,
            value_name,
            0,
            value_type,
            value_data,
        )
        mock_CloseHandle.assert_called_once_with(mock_RegOpenKeyEx.return_value)

    def closes_handle_if_set_value_errors(
        self,
        mock_RegOpenKeyEx: MagicMock,
        mock_RegSetValueEx: MagicMock,
        mock_CloseHandle: MagicMock,
    ):
        # GIVEN
        mock_RegSetValueEx.side_effect = Exception("ERROR")

        with pytest.raises(Exception) as raised_exc:
            # WHEN
            win_installer.set_registry_key_value(
                win32con.HKEY_CURRENT_USER, None, "", win32con.REG_SZ, ""
            )

        # THEN
        assert raised_exc.value is mock_RegSetValueEx.side_effect
        mock_CloseHandle.assert_called_once_with(mock_RegOpenKeyEx.return_value)


class TestProvisionDirectories:
    """Tests for the provision_directories function"""

    @pytest.fixture(autouse=True)
    def mock_os_makedirs(self) -> Generator[MagicMock, None, None]:
        with patch.object(win_installer.os, "makedirs") as mock_os_makedirs:
            yield mock_os_makedirs

    @pytest.fixture(autouse=True)
    def mock_set_windows_permissions(self) -> Generator[MagicMock, None, None]:
        with patch.object(
            win_installer, "_set_windows_permissions"
        ) as mock_set_windows_permissions:
            yield mock_set_windows_permissions

    @pytest.fixture
    def agent_username(self) -> str:
        return "deadline-worker"

    def test_creates_queue_persistence_dir(
        self,
        mock_os_makedirs: MagicMock,
        agent_username: str,
    ) -> None:
        """Tests that provision_directories creates the queue persistence directory"""

        # WHEN
        win_installer.provision_directories(agent_username)

        # THEN
        mock_os_makedirs.assert_any_call(
            os.path.join(os.environ["PROGRAMDATA"], "Amazon", "Deadline", "Cache", "queues"),
            exist_ok=True,
        )
