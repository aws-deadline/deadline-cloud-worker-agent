# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for the Worker Agent configuration"""

from __future__ import annotations
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import MagicMock, patch
from typing import Any, Generator, List, Optional, cast
import logging
import pytest
import os
import sys

from openjd.sessions import SessionUser, PosixSessionUser, WindowsSessionUser

from deadline_worker_agent.config import config as config_mod
from deadline_worker_agent.config.cli_args import ParsedCommandLineArguments


@pytest.fixture(autouse=True)
def mock_worker_settings_cls() -> Generator[MagicMock, None, None]:
    """Patches the WorkerSettings class import in the deadline_worker_agent.config.config module
    module and returns the Mock instance"""

    defaults = {
        "farm_id": None,
        "fleet_id": None,
        "cleanup_session_user_processes": True,
        "profile": None,
        "verbose": None,
        "no_shutdown": None,
        "run_jobs_as_agent_user": None,
        "posix_job_user": None,
        "windows_job_user": None,
        "allow_instance_profile": None,
        "capabilities": None,
        "worker_logs_dir": Path("/var/log/amazon/deadline"),
        "worker_persistence_dir": Path("/var/lib/deadline"),
        "local_session_logs": None,
        "structured_logs": True,
        "host_metrics_logging": True,
        "host_metrics_logging_interval_seconds": 10,
        "retain_session_dir": False,
    }

    class FakeWorkerSettings:
        def __init__(self, **kwargs) -> None:
            attrs = defaults.copy()
            attrs.update(kwargs)
            self.__dict__ = attrs

    with patch.object(
        config_mod, "WorkerSettings", side_effect=FakeWorkerSettings
    ) as mock_worker_settings:
        yield mock_worker_settings


@pytest.fixture
def parsed_args(
    farm_id: str,
    fleet_id: str,
) -> ParsedCommandLineArguments:
    """Fixture to containing mocked pared command-line arguments"""
    parsed_cli_args = ParsedCommandLineArguments()
    parsed_cli_args.farm_id = farm_id
    parsed_cli_args.fleet_id = fleet_id
    return parsed_cli_args


@pytest.fixture(autouse=True)
def arg_parser(
    parsed_args: config_mod.ParsedCommandLineArguments,
) -> Generator[MagicMock, None, None]:
    """Fixture containing the mocked argument parser"""
    with patch.object(config_mod, "get_argument_parser") as get_argument_parser:
        arg_parser = MagicMock()
        get_argument_parser.return_value = arg_parser
        arg_parser.parse_args.return_value = parsed_args
        yield arg_parser


@pytest.fixture
def expected_session_user() -> Optional[SessionUser]:
    if os.name == "posix":
        return PosixSessionUser(user="user", group="group")
    else:
        return WindowsSessionUser(user="windows-user", password="SUP3R_secure_P4$$W0RD")


@pytest.fixture(autouse=os.name == "nt")
def reset_user_password(expected_session_user) -> Generator[Optional[MagicMock], None, None]:
    if sys.platform == "win32":
        with patch.object(config_mod, "reset_user_password") as reset_user_password:
            reset_user_password.return_value = MagicMock()
            reset_user_password.return_value.windows_session_user = expected_session_user
            yield reset_user_password
    else:
        yield None


class TestLoad:
    """Tests for Configuration.load()"""

    @pytest.mark.parametrize(
        ("args",),
        (
            pytest.param(None, id="no-args"),
            pytest.param(["--farm-id", "farm", "--fleet-id", "fleet"], id="args"),
        ),
    )
    def test_calls_parse_args(
        self,
        arg_parser: MagicMock,
        parsed_args: config_mod.ParsedCommandLineArguments,
        args: Optional[List[str]],
    ) -> None:
        """
        Tests to ensure `Configuration.load()` uses the argument parser.

        This asserts that command-line arguments passed when calling `Configuration.load()`
        are forwarded to the argument parser's `.parse_args()` method
        """
        # GIVEN

        # required to avoid validation error
        parsed_args.farm_id = "farm_id"
        parsed_args.fleet_id = "fleet_id"

        parse_args: MagicMock = arg_parser.parse_args

        # WHEN
        config_mod.Configuration.load(args)

        # THEN
        class IsEmptyParsedCommandLineArguments:
            def __eq__(self, other: Any) -> bool:
                return (
                    isinstance(other, config_mod.ParsedCommandLineArguments)
                    and other.profile is None
                    and other.farm_id is None
                    and other.fleet_id is None
                    and other.verbose is None
                )

        parse_args.assert_called_with(args, namespace=IsEmptyParsedCommandLineArguments())

    def test_uses_parsed_farm_id(self, parsed_args: config_mod.ParsedCommandLineArguments) -> None:
        """Tests that the farm ID is parsed from command-line arguments"""
        # GIVEN
        farm_id = "farm_id"
        parsed_args.farm_id = farm_id
        parsed_args.fleet_id = "fleet_id"  # required to avoid a validation error

        # WHEN
        config = config_mod.Configuration.load()

        # THEN

        assert config.farm_id == farm_id

    def test_uses_parsed_fleet_id(self, parsed_args: config_mod.ParsedCommandLineArguments) -> None:
        """Tests that the fleet ID is parsed from command-line arguments"""
        # GIVEN
        fleet_id = "fleet_id"
        parsed_args.fleet_id = fleet_id
        parsed_args.farm_id = "farm_id"  # required to avoid a validation error

        # WHEN
        config = config_mod.Configuration.load()

        # THEN
        assert config.fleet_id == fleet_id

    def test_uses_parsed_profile(self, parsed_args: config_mod.ParsedCommandLineArguments) -> None:
        """Tests that the profile is parsed from command-line arguments"""
        # GIVEN
        profile = "profile"
        parsed_args.profile = profile
        # Must be present or a ConfigurationError is raised
        parsed_args.fleet_id = "fleet_id"
        parsed_args.farm_id = "farm_id"

        # WHEN
        config = config_mod.Configuration.load()

        # THEN
        assert config.profile == profile

    @pytest.mark.parametrize(
        ("verbose",),
        (
            (True,),
            (False,),
        ),
    )
    def test_uses_parsed_verbose(
        self,
        parsed_args: config_mod.ParsedCommandLineArguments,
        verbose: bool,
    ) -> None:
        """Tests that the parsed verbose argument is returned"""
        # GIVEN
        parsed_args.verbose = verbose
        # Must be present or a ConfigurationError is raised
        parsed_args.fleet_id = "fleet_id"
        parsed_args.farm_id = "farm_id"

        # WHEN
        config = config_mod.Configuration.load()

        # THEN
        assert config.verbose == verbose

    @pytest.mark.parametrize(
        ("no_shutdown",),
        (
            pytest.param(True, id="TrueArgument"),
            pytest.param(False, id="FalseArgument"),
        ),
    )
    def test_uses_parsed_no_shutdown(
        self,
        parsed_args: config_mod.ParsedCommandLineArguments,
        no_shutdown: bool,
    ) -> None:
        """Tests that the parsed no_shutdown argument is returned"""
        # GIVEN
        parsed_args.no_shutdown = no_shutdown
        # Must be present or a ConfigurationError is raised
        parsed_args.fleet_id = "fleet_id"
        parsed_args.farm_id = "farm_id"

        # WHEN
        config = config_mod.Configuration.load()

        # THEN
        assert config.no_shutdown == no_shutdown

    @pytest.mark.parametrize(
        ("disallow_instance_profile",),
        (
            pytest.param(True, id="TrueArgument"),
            pytest.param(False, id="FalseArgument"),
        ),
    )
    def test_uses_parsed_disallow_instance_profile(
        self,
        parsed_args: config_mod.ParsedCommandLineArguments,
        disallow_instance_profile: bool,
    ) -> None:
        """Tests that the parsed allow_instance_profile argument is returned"""
        # GIVEN
        parsed_args.disallow_instance_profile = disallow_instance_profile
        # Must be present or a ConfigurationError is raised
        parsed_args.fleet_id = "fleet_id"
        parsed_args.farm_id = "farm_id"

        # WHEN
        config = config_mod.Configuration.load()

        # THEN
        assert config.allow_instance_profile == (not disallow_instance_profile)

    @pytest.mark.parametrize(
        ("cleanup_session_user_processes",),
        (
            pytest.param(True, id="TrueArgument"),
            pytest.param(False, id="FalseArgument"),
        ),
    )
    def test_uses_parsed_cleanup_session_user_processes(
        self,
        parsed_args: config_mod.ParsedCommandLineArguments,
        cleanup_session_user_processes: bool,
    ) -> None:
        """Tests that the parsed cleanup_session_user_processes argument is returned"""
        # GIVEN
        parsed_args.cleanup_session_user_processes = cleanup_session_user_processes
        # Must be present or a ConfigurationError is raised
        parsed_args.fleet_id = "fleet_id"
        parsed_args.farm_id = "farm_id"

        # WHEN
        config = config_mod.Configuration.load()

        # THEN
        assert config.cleanup_session_user_processes == cleanup_session_user_processes

    @pytest.mark.parametrize(
        argnames="worker_logs_dir",
        argvalues=(
            pytest.param(
                Path("/foo"), marks=pytest.mark.skipif(os.name != "posix", reason="Not posix")
            ),
            pytest.param(
                Path("/bar"), marks=pytest.mark.skipif(os.name != "posix", reason="Not posix")
            ),
            pytest.param(
                Path("D:\\foo"), marks=pytest.mark.skipif(os.name != "nt", reason="Not windows")
            ),
            pytest.param(
                Path("D:\\bar"), marks=pytest.mark.skipif(os.name != "nt", reason="Not windows")
            ),
        ),
    )
    def test_uses_worker_logs_dir(
        self,
        parsed_args: config_mod.ParsedCommandLineArguments,
        worker_logs_dir: Path,
    ) -> None:
        # GIVEN
        parsed_args.logs_dir = worker_logs_dir
        parsed_args.farm_id = "farm_id"
        parsed_args.fleet_id = "fleet_id"

        # WHEN
        config = config_mod.Configuration.load()

        # THEN
        assert config.worker_logs_dir == worker_logs_dir

    @pytest.mark.parametrize(
        argnames="persistence_dir",
        argvalues=(
            pytest.param(
                Path("/foo"), marks=pytest.mark.skipif(os.name != "posix", reason="Not posix")
            ),
            pytest.param(
                Path("/bar"), marks=pytest.mark.skipif(os.name != "posix", reason="Not posix")
            ),
            pytest.param(
                Path("D:\\foo"), marks=pytest.mark.skipif(os.name != "nt", reason="Not windows")
            ),
            pytest.param(
                Path("D:\\bar"), marks=pytest.mark.skipif(os.name != "nt", reason="Not windows")
            ),
        ),
    )
    def test_uses_worker_persistence_dir(
        self,
        parsed_args: config_mod.ParsedCommandLineArguments,
        persistence_dir: Path,
    ) -> None:
        # GIVEN
        parsed_args.persistence_dir = persistence_dir
        parsed_args.farm_id = "farm_id"
        parsed_args.fleet_id = "fleet_id"

        # WHEN
        config = config_mod.Configuration.load()

        # THEN
        assert config.worker_persistence_dir == persistence_dir

    @pytest.mark.parametrize(
        argnames="local_session_logs",
        argvalues=(
            True,
            False,
        ),
    )
    def test_uses_local_session_logs(
        self,
        parsed_args: config_mod.ParsedCommandLineArguments,
        local_session_logs: bool,
    ) -> None:
        # GIVEN
        parsed_args.local_session_logs = local_session_logs
        parsed_args.farm_id = "farm_id"
        parsed_args.fleet_id = "fleet_id"

        # WHEN
        config = config_mod.Configuration.load()

        # THEN
        assert config.local_session_logs == local_session_logs

    @pytest.mark.parametrize(
        argnames="retain_session_dir",
        argvalues=(
            True,
            False,
        ),
    )
    def test_uses_retain_session_dir(
        self,
        parsed_args: config_mod.ParsedCommandLineArguments,
        retain_session_dir: bool,
    ) -> None:
        # GIVEN
        parsed_args.retain_session_dir = retain_session_dir
        parsed_args.farm_id = "farm_id"
        parsed_args.fleet_id = "fleet_id"

        # WHEN
        config = config_mod.Configuration.load()

        # THEN
        assert config.retain_session_dir == retain_session_dir


class TestInit:
    """Tests for Configuration.__init__"""

    @pytest.mark.parametrize(
        argnames=("farm_id", "fleet_id", "profile", "verbose"),
        argvalues=(
            pytest.param("a_farm", "a_fleet", None, False),
            pytest.param("b_farm", "b_fleet", "b_profile", False),
            pytest.param("c_farm", "c_fleet", "c_profile", True),
        ),
    )
    def test_valid_args(
        self,
        farm_id: str,
        fleet_id: str,
        profile: Optional[str],
        verbose: bool,
    ) -> None:
        """Tests that valid arguments used to initialize a Configuration instance match the instance's attributes"""
        # WHEN
        cli_args = ParsedCommandLineArguments()
        cli_args.farm_id = farm_id
        cli_args.fleet_id = fleet_id
        cli_args.profile = profile
        cli_args.verbose = verbose
        config = config_mod.Configuration(parsed_cli_args=cli_args)

        # THEN
        # ensure the attributes equal the values passed to the initializer
        assert config.farm_id == farm_id
        assert config.fleet_id == fleet_id
        assert config.profile == profile
        assert config.verbose == verbose

    def test_correct_default_paths(self) -> None:
        """Tests that the Configuration has worker_persistence_dir and worker_credentials_dir
        Path attributes that point to the correct default path:

        POSIX:
            worker_persistence_dir = /var/lib/deadline
            worker_credentials_dir = /var/lib/deadline/credentials
            worker_state_file = /var/lib/deadline/worker.json
        """
        # GIVEN
        cli_args = ParsedCommandLineArguments()
        cli_args.farm_id = "farm-id"
        cli_args.fleet_id = "fleet-id"
        cli_args.profile = None
        cli_args.verbose = False

        # WHEN
        config = config_mod.Configuration(parsed_cli_args=cli_args)

        # THEN
        assert config.worker_persistence_dir == Path("/var/lib/deadline")
        assert config.worker_credentials_dir == Path("/var/lib/deadline/credentials")
        assert config.worker_state_file == Path("/var/lib/deadline/worker.json")

    def test_empty_fleet_id(self) -> None:
        """Tests when no `fleet_id` is supplied a `ConfigurationError` is raised"""
        # GIVEN
        fleet_id = ""
        cli_args = ParsedCommandLineArguments()
        cli_args.fleet_id = fleet_id
        # Avoid validation error on missing farm ID
        cli_args.farm_id = "farm_id"

        with pytest.raises(config_mod.ConfigurationError) as raise_ctx:
            # WHEN
            config_mod.Configuration(parsed_cli_args=cli_args)

        # THEN
        exc = raise_ctx.value
        msg = exc.args[0]
        assert isinstance(msg, str)
        assert msg == f"Fleet ID must be specified, but got {repr(fleet_id)})"

    def test_empty_farm_id(self) -> None:
        """Tests when no `farm_id` is supplied a `ConfigurationError` is raised"""
        # GIVEN
        farm_id = ""
        cli_args = ParsedCommandLineArguments()
        cli_args.fleet_id = "fleet_id"
        # Avoid validation error on missing farm ID
        cli_args.farm_id = farm_id

        with pytest.raises(config_mod.ConfigurationError) as raise_ctx:
            # WHEN
            config_mod.Configuration(parsed_cli_args=cli_args)

        # THEN
        exc = raise_ctx.value
        msg = exc.args[0]
        assert isinstance(msg, str)
        assert msg == f"Farm ID must be specified, but got {repr(farm_id)})"

    @pytest.mark.parametrize(
        argnames="farm_id",
        argvalues=(
            "farm-d3e19e6d4a36407bb9025ec5195c87f1",
            None,
        ),
    )
    def test_farm_id_passed_to_settings_initializer(
        self,
        farm_id: str | None,
        parsed_args: ParsedCommandLineArguments,
        mock_worker_settings_cls: MagicMock,
    ) -> None:
        # GIVEN
        parsed_args.farm_id = farm_id

        with pytest.raises(config_mod.ConfigurationError) if not farm_id else nullcontext():
            # WHEN
            config_mod.Configuration(parsed_cli_args=parsed_args)

        # THEN
        mock_worker_settings_cls.assert_called_once()
        call = mock_worker_settings_cls.call_args_list[0]

        if farm_id is not None:
            assert call.kwargs.get("farm_id") == farm_id
        else:
            assert "farm_id" not in call.kwargs

    @pytest.mark.parametrize(
        argnames="fleet_id",
        argvalues=(
            "fleet-2adaa17733144f14a0d97111b5d48901",
            None,
        ),
    )
    def test_fleet_id_passed_to_settings_initializer(
        self,
        fleet_id: str | None,
        parsed_args: ParsedCommandLineArguments,
        mock_worker_settings_cls: MagicMock,
    ) -> None:
        # GIVEN
        parsed_args.fleet_id = fleet_id

        # WHEN
        with pytest.raises(config_mod.ConfigurationError) if not fleet_id else nullcontext():
            config_mod.Configuration(parsed_cli_args=parsed_args)

        # THEN
        mock_worker_settings_cls.assert_called_once()
        call = mock_worker_settings_cls.call_args_list[0]

        if fleet_id is not None:
            assert call.kwargs.get("fleet_id") == fleet_id
        else:
            assert "fleet_id" not in call.kwargs

    @pytest.mark.parametrize(
        argnames="profile",
        argvalues=(
            "myprofile",
            None,
        ),
    )
    def test_profile_passed_to_settings_initializer(
        self,
        profile: str | None,
        parsed_args: ParsedCommandLineArguments,
        mock_worker_settings_cls: MagicMock,
    ) -> None:
        # GIVEN
        parsed_args.profile = profile

        # WHEN
        config_mod.Configuration(parsed_cli_args=parsed_args)

        # THEN
        mock_worker_settings_cls.assert_called_once()
        call = mock_worker_settings_cls.call_args_list[0]

        if profile is not None:
            assert call.kwargs.get("profile") == profile
        else:
            assert "profile" not in call.kwargs

    @pytest.mark.parametrize(
        argnames="verbose",
        argvalues=(
            True,
            False,
            None,
        ),
    )
    def test_verbose_passed_to_settings_initializer(
        self,
        verbose: bool | None,
        parsed_args: ParsedCommandLineArguments,
        mock_worker_settings_cls: MagicMock,
    ) -> None:
        # GIVEN
        parsed_args.verbose = verbose

        # WHEN
        config_mod.Configuration(parsed_cli_args=parsed_args)

        # THEN
        mock_worker_settings_cls.assert_called_once()
        call = mock_worker_settings_cls.call_args_list[0]

        if verbose is not None:
            assert call.kwargs.get("verbose") == verbose
        else:
            assert "verbose" not in call.kwargs

    @pytest.mark.parametrize(
        argnames="no_shutdown",
        argvalues=(
            True,
            False,
            None,
        ),
    )
    def test_no_shutdown_passed_to_settings_initializer(
        self,
        no_shutdown: bool | None,
        parsed_args: ParsedCommandLineArguments,
        mock_worker_settings_cls: MagicMock,
    ) -> None:
        # GIVEN
        parsed_args.no_shutdown = no_shutdown

        # WHEN
        config_mod.Configuration(parsed_cli_args=parsed_args)

        # THEN
        mock_worker_settings_cls.assert_called_once()
        call = mock_worker_settings_cls.call_args_list[0]

        if no_shutdown is not None:
            assert call.kwargs.get("no_shutdown") == no_shutdown
        else:
            assert "no_shutdown" not in call.kwargs

    @pytest.mark.parametrize(
        argnames="run_jobs_as_agent_user",
        argvalues=(True, False, None),
    )
    def test_run_jobs_as_agent_user_passed_to_settings_initializer(
        self,
        run_jobs_as_agent_user: bool | None,
        parsed_args: ParsedCommandLineArguments,
        mock_worker_settings_cls: MagicMock,
    ) -> None:
        # GIVEN
        parsed_args.run_jobs_as_agent_user = run_jobs_as_agent_user

        # WHEN
        config_mod.Configuration(parsed_cli_args=parsed_args)

        # THEN
        mock_worker_settings_cls.assert_called_once()
        call = mock_worker_settings_cls.call_args_list[0]

        if run_jobs_as_agent_user is not None:
            assert call.kwargs.get("run_jobs_as_agent_user") == run_jobs_as_agent_user
        else:
            assert "run_jobs_as_agent_user" not in call.kwargs

    @pytest.mark.skipif(os.name != "posix", reason="Posix-only test.")
    @pytest.mark.parametrize(
        argnames="posix_job_user",
        argvalues=("user:group", None),
    )
    def test_posix_job_user_passed_to_settings_initializer(
        self,
        posix_job_user: str | None,
        parsed_args: ParsedCommandLineArguments,
        mock_worker_settings_cls: MagicMock,
    ) -> None:
        # GIVEN
        parsed_args.run_jobs_as_agent_user = False
        parsed_args.posix_job_user = posix_job_user

        # WHEN
        config_mod.Configuration(parsed_cli_args=parsed_args)

        # THEN
        mock_worker_settings_cls.assert_called_once()
        call = mock_worker_settings_cls.call_args_list[0]
        if posix_job_user is not None:
            assert call.kwargs.get("posix_job_user") == posix_job_user
        else:
            assert "posix_job_user" not in call.kwargs

    @pytest.mark.skipif(os.name != "nt", reason="windows-only test.")
    @pytest.mark.parametrize(
        argnames="windows_job_user",
        argvalues=("windows-user", None),
    )
    def test_windows_job_user_passed_to_settings_initializer(
        self,
        windows_job_user: str | None,
        parsed_args: ParsedCommandLineArguments,
        mock_worker_settings_cls: MagicMock,
    ) -> None:
        # GIVEN
        parsed_args.run_jobs_as_agent_user = False
        parsed_args.windows_job_user = windows_job_user

        # WHEN
        config_mod.Configuration(parsed_cli_args=parsed_args)

        # THEN
        mock_worker_settings_cls.assert_called_once()
        call = mock_worker_settings_cls.call_args_list[0]
        if windows_job_user is not None:
            assert call.kwargs.get("windows_job_user") == windows_job_user
        else:
            assert "windows_job_user" not in call.kwargs

    @pytest.mark.skipif(os.name != "nt", reason="windows-only test.")
    @pytest.mark.parametrize(
        argnames="windows_job_user",
        argvalues=("windows-user",),
    )
    @patch.object(config_mod, "users_equal", return_value=True)
    @patch.object(config_mod.getpass, "getuser")
    def test_windows_job_user_is_agent_user_fails(
        self,
        getuser: MagicMock,
        users_equal: MagicMock,
        windows_job_user: str | None,
        parsed_args: ParsedCommandLineArguments,
        mock_worker_settings_cls: MagicMock,
    ) -> None:
        # GIVEN
        getuser.return_value = "windows-user"
        parsed_args.run_jobs_as_agent_user = False
        parsed_args.windows_job_user = windows_job_user

        # WHEN
        with pytest.raises(config_mod.ConfigurationError) as exc_info:
            config_mod.Configuration(parsed_cli_args=parsed_args)

        # THEN
        assert (
            str(exc_info.value) == "Windows job user override must not be the user running the "
            f"worker agent: {windows_job_user}. If you wish to run jobs as the agent user, set "
            "run_jobs_as_agent_user = true in the agent configuration file."
        )

    @pytest.mark.parametrize(
        argnames="disallow_instance_profile",
        argvalues=(
            True,
            False,
            None,
        ),
    )
    def test_disallow_instance_profile_passed_to_settings_initializer(
        self,
        disallow_instance_profile: bool | None,
        parsed_args: ParsedCommandLineArguments,
        mock_worker_settings_cls: MagicMock,
    ) -> None:
        # GIVEN
        parsed_args.disallow_instance_profile = disallow_instance_profile

        # WHEN
        config_mod.Configuration(parsed_cli_args=parsed_args)

        # THEN
        mock_worker_settings_cls.assert_called_once()
        call = mock_worker_settings_cls.call_args_list[0]

        if disallow_instance_profile is not None:
            assert "allow_instance_profile" in call.kwargs
            assert call.kwargs.get("allow_instance_profile") == (not disallow_instance_profile)
        else:
            assert "allow_instance_profile" not in call.kwargs

    @pytest.mark.parametrize(
        argnames="cleanup_session_user_processes",
        argvalues=(
            True,
            False,
            None,
        ),
    )
    def test_cleanup_session_user_processes_passed_to_settings_initializer(
        self,
        cleanup_session_user_processes: bool | None,
        parsed_args: ParsedCommandLineArguments,
        mock_worker_settings_cls: MagicMock,
    ) -> None:
        # GIVEN
        parsed_args.cleanup_session_user_processes = cleanup_session_user_processes

        # WHEN
        config_mod.Configuration(parsed_cli_args=parsed_args)

        # THEN
        mock_worker_settings_cls.assert_called_once()
        call = mock_worker_settings_cls.call_args_list[0]

        if cleanup_session_user_processes is not None:
            assert (
                call.kwargs.get("cleanup_session_user_processes") == cleanup_session_user_processes
            )
        else:
            assert "cleanup_session_user_processes" not in call.kwargs

    @pytest.mark.parametrize(
        argnames="logs_dir",
        argvalues=(
            Path("/foo"),
            Path("/bar"),
            None,
        ),
    )
    def test_logs_dir_passed_to_settings_initializer(
        self,
        logs_dir: Path | None,
        parsed_args: ParsedCommandLineArguments,
        mock_worker_settings_cls: MagicMock,
    ) -> None:
        # GIVEN
        parsed_args.logs_dir = logs_dir

        # WHEN
        config_mod.Configuration(parsed_cli_args=parsed_args)

        # THEN
        mock_worker_settings_cls.assert_called_once()
        call = mock_worker_settings_cls.call_args_list[0]

        if logs_dir is not None:
            assert call.kwargs.get("worker_logs_dir") == logs_dir.absolute()
        else:
            assert "worker_logs_dir" not in call.kwargs

    @pytest.mark.parametrize(
        argnames="persistence_dir",
        argvalues=(
            Path("/foo"),
            Path("/bar"),
            None,
        ),
    )
    def test_persistence_dir_passed_to_settings_initializer(
        self,
        persistence_dir: Path | None,
        parsed_args: ParsedCommandLineArguments,
        mock_worker_settings_cls: MagicMock,
    ) -> None:
        # GIVEN
        parsed_args.persistence_dir = persistence_dir

        # WHEN
        config_mod.Configuration(parsed_cli_args=parsed_args)

        # THEN
        mock_worker_settings_cls.assert_called_once()
        call = mock_worker_settings_cls.call_args_list[0]

        if persistence_dir is not None:
            assert call.kwargs.get("worker_persistence_dir") == persistence_dir.absolute()
        else:
            assert "persistence_dir" not in call.kwargs

    @pytest.mark.parametrize(
        argnames="local_session_logs",
        argvalues=(
            True,
            False,
            None,
        ),
    )
    def test_local_session_logs_passed_to_settings_initializer(
        self,
        local_session_logs: bool | None,
        parsed_args: ParsedCommandLineArguments,
        mock_worker_settings_cls: MagicMock,
    ) -> None:
        # GIVEN
        parsed_args.local_session_logs = local_session_logs

        # WHEN
        config_mod.Configuration(parsed_cli_args=parsed_args)

        # THEN
        mock_worker_settings_cls.assert_called_once()
        call = mock_worker_settings_cls.call_args_list[0]

        if local_session_logs is not None:
            assert call.kwargs.get("local_session_logs") == local_session_logs
        else:
            assert "local_session_logs" not in call.kwargs

    @pytest.mark.parametrize(
        argnames="retain_session_dir",
        argvalues=(
            True,
            False,
            None,
        ),
    )
    def test_retain_session_dir_passed_to_settings_initializer(
        self,
        retain_session_dir: bool | None,
        parsed_args: ParsedCommandLineArguments,
        mock_worker_settings_cls: MagicMock,
    ) -> None:
        # GIVEN
        parsed_args.retain_session_dir = retain_session_dir

        # WHEN
        config_mod.Configuration(parsed_cli_args=parsed_args)

        # THEN
        mock_worker_settings_cls.assert_called_once()
        call = mock_worker_settings_cls.call_args_list[0]

        if retain_session_dir is not None:
            assert call.kwargs.get("retain_session_dir") == retain_session_dir
        else:
            assert "retain_session_dir" not in call.kwargs

    @pytest.mark.parametrize(
        argnames=("posix_job_user_setting", "windows_job_user_setting"),
        argvalues=(
            pytest.param(
                "user:group",
                None,
                id="only-has-posix-job-user-setting",
            ),
            pytest.param(
                None,
                "windows-user",
                id="only-has-windows-job-user-setting",
            ),
            pytest.param(
                "user:group",
                "windows-user",
                id="has-posix-and-windows-job-user-setting",
            ),
            pytest.param(
                None,
                None,
                id="no-posix-or-windows-job-user-setting",
            ),
        ),
    )
    def test_uses_worker_settings(
        self,
        posix_job_user_setting: str | None,
        windows_job_user_setting: str | None,
        expected_session_user: SessionUser,
        parsed_args: ParsedCommandLineArguments,
        mock_worker_settings_cls: MagicMock,
        request,
    ) -> None:
        """Tests that when we have a WorkerSettings that defines settings values for
        a configuration, then the returned Configuration object contains the values
        from those WorkerSettings.
        """

        # GIVEN
        mock_worker_settings_cls.side_effect = None
        mock_worker_settings: MagicMock = mock_worker_settings_cls.return_value
        mock_worker_settings.posix_job_user = posix_job_user_setting
        mock_worker_settings.windows_job_user = windows_job_user_setting
        mock_worker_settings.run_jobs_as_agent_user = None

        # Needed because MagicMock does not support gt/lt comparison
        mock_worker_settings.host_metrics_logging_interval_seconds = 10

        # WHEN
        config = config_mod.Configuration(parsed_cli_args=parsed_args)

        # THEN
        # Assert that the attributes are taken from the WorkerSettings instance
        assert config.farm_id is mock_worker_settings.farm_id
        assert config.fleet_id is mock_worker_settings.fleet_id
        assert config.profile is mock_worker_settings.profile
        assert config.verbose is mock_worker_settings.verbose
        assert config.no_shutdown is mock_worker_settings.no_shutdown
        assert config.allow_instance_profile is mock_worker_settings.allow_instance_profile
        assert (
            config.cleanup_session_user_processes
            is mock_worker_settings.cleanup_session_user_processes
        )
        assert config.capabilities is mock_worker_settings.capabilities
        assert (
            config.job_run_as_user_overrides.run_as_agent
            == mock_worker_settings.run_jobs_as_agent_user
        )
        if sys.platform != "win32":
            if posix_job_user_setting is None:
                assert config.job_run_as_user_overrides.job_user is None
            else:
                assert isinstance(config.job_run_as_user_overrides.job_user, PosixSessionUser)
                assert (
                    config.job_run_as_user_overrides.job_user.group == expected_session_user.group  # type: ignore
                )
                assert config.job_run_as_user_overrides.job_user.user == expected_session_user.user
        else:
            if windows_job_user_setting is None:
                assert config.job_run_as_user_overrides.job_user is None
            else:
                assert isinstance(config.job_run_as_user_overrides.job_user, WindowsSessionUser)
                job_user = cast(WindowsSessionUser, config.job_run_as_user_overrides.job_user)
                expected_session_user = cast(WindowsSessionUser, expected_session_user)
                assert job_user.user == expected_session_user.user
                assert job_user.password == expected_session_user.password
        assert config.worker_logs_dir is mock_worker_settings.worker_logs_dir
        assert config.local_session_logs is mock_worker_settings.local_session_logs
        assert config.worker_persistence_dir is mock_worker_settings.worker_persistence_dir
        assert (
            config.worker_credentials_dir
            is mock_worker_settings.worker_persistence_dir / "credentials"
        )
        assert config.host_metrics_logging is mock_worker_settings.host_metrics_logging
        assert (
            config.host_metrics_logging_interval_seconds
            is mock_worker_settings.host_metrics_logging_interval_seconds
        )


class TestLog:
    """Tests for Configuration.log()"""

    @pytest.mark.parametrize(
        ("farm_id", "fleet_id", "profile", "verbose"),
        (
            ("farm_id", "fleet_id", None, False),
            ("farm_id", "fleet_id", "profile", True),
        ),
    )
    @patch.object(config_mod, "_logger")
    def test_defaults(
        self,
        logger_mock: MagicMock,
        farm_id: str,
        fleet_id: str,
        profile: Optional[str],
        verbose: bool,
    ) -> None:
        """Assert that log calls made if the logger is not enabled for the specified level"""
        # GIVEN
        cli_args = ParsedCommandLineArguments()
        cli_args.farm_id = farm_id
        cli_args.fleet_id = fleet_id
        cli_args.profile = profile
        cli_args.verbose = verbose

        config = config_mod.Configuration(parsed_cli_args=cli_args)
        logging_level = logging.DEBUG
        logger_log_mock: MagicMock = logger_mock.log

        # WHEN
        config.log()

        # THEN
        attr_pairs = (
            ("farm_id", farm_id),
            ("fleet_id", fleet_id),
            ("profile", profile),
            ("verbose", verbose),
        )
        for attr, val in attr_pairs:
            logger_log_mock.assert_any_call(logging_level, f"{attr}={val}")

    @patch.object(config_mod, "_logger")
    def test_not_called_if_level_too_low(self, logger_mock: MagicMock) -> None:
        """Assert that log calls are not made if the logger is not enabled for the specified level"""
        # GIVEN
        cli_args = ParsedCommandLineArguments()
        cli_args.farm_id = "farm_id"
        cli_args.fleet_id = "fleet_id"
        config = config_mod.Configuration(parsed_cli_args=cli_args)
        logger_mock.isEnabledFor.return_value = False
        logger_log_mock: MagicMock = logger_mock.log

        # WHEN
        config.log()

        # THEN
        logger_mock.isEnabledFor.assert_called_once()
        logger_log_mock.assert_not_called()

    def test_supplied_logger(self) -> None:
        # GIVEN
        logger = MagicMock()
        logger.isEnabledFor.return_value = True
        cli_args = ParsedCommandLineArguments()
        cli_args.farm_id = "farm_id"
        cli_args.fleet_id = "fleet_id"
        config = config_mod.Configuration(parsed_cli_args=cli_args)
        logger_log_mock: MagicMock = logger.log

        # WHEN
        config.log(logger=logger)

        # THEN
        class Any:
            def __eq__(self, other: object) -> bool:
                return True

        logger_log_mock.assert_called_with(logging.DEBUG, Any())

    def test_supplied_level(self) -> None:
        # GIVEN
        logger = MagicMock()
        logger.isEnabledFor.return_value = True
        cli_args = ParsedCommandLineArguments()
        cli_args.farm_id = "farm_id"
        cli_args.fleet_id = "fleet_id"
        config = config_mod.Configuration(parsed_cli_args=cli_args)
        logger_log_mock: MagicMock = logger.log
        level = logging.INFO

        # WHEN
        config.log(level=level)

        # THEN
        assert all(kall[0][0] == level for kall in logger_log_mock.call_args_list)
