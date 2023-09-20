# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from pathlib import Path
from subprocess import CalledProcessError
from typing import Generator, Optional
from unittest.mock import MagicMock, patch
import os
import sysconfig

import pytest

from deadline_worker_agent.installer import ParsedCommandLineArguments, install
from deadline_worker_agent import installer as installer_mod


@pytest.fixture(autouse=True)
def mock_subprocess_run() -> Generator[MagicMock, None, None]:
    with patch.object(installer_mod, "run") as mock_subprocess_run:
        yield mock_subprocess_run


def test_installer_path(platform: str) -> None:
    """Tests the value of deadline_worker_agent.installer.INSTALLER_PATH"""
    # GIVEN
    if platform == "linux":
        expected_value = Path(installer_mod.__file__).parent / "install.sh"
    else:
        assert False, f"No expected path for platform {platform}"

    # THEN
    assert installer_mod.INSTALLER_PATH[platform] == expected_value


@pytest.fixture
def farm_id() -> str:
    return "farm-1"


@pytest.fixture
def fleet_id() -> str:
    return "fleet-1"


@pytest.fixture
def region() -> str:
    return "us-west-2"


@pytest.fixture
def user() -> str:
    return "wa_user"


@pytest.fixture(params=("wa_group", None))
def group(request: pytest.FixtureRequest) -> Optional[str]:
    return request.param


@pytest.fixture
def service_start() -> bool:
    return False


@pytest.fixture
def confirmed() -> bool:
    return True


@pytest.fixture
def allow_shutdown() -> bool:
    return False


@pytest.fixture
def install_service() -> bool:
    return True


@pytest.fixture
def parsed_args(
    farm_id: str,
    fleet_id: str,
    region: str,
    user: str,
    group: Optional[str],
    service_start: bool,
    confirmed: bool,
    allow_shutdown: bool,
    install_service: bool,
) -> ParsedCommandLineArguments:
    parsed_args = ParsedCommandLineArguments()
    parsed_args.farm_id = farm_id
    parsed_args.fleet_id = fleet_id
    parsed_args.user = user
    parsed_args.group = group
    parsed_args.region = region
    parsed_args.service_start = service_start
    parsed_args.confirmed = confirmed
    parsed_args.allow_shutdown = allow_shutdown
    parsed_args.install_service = install_service
    return parsed_args


@pytest.fixture(
    params=("linux",),
)
def platform(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture(autouse=True)
def mock_sys_platform(platform: str) -> Generator[str, None, None]:
    with patch.object(installer_mod.sys, "platform", new=platform) as mock_sys_platform:
        yield mock_sys_platform


@pytest.fixture
def expected_cmd(
    parsed_args: ParsedCommandLineArguments,
    platform: str,
) -> list[str]:
    expected_cmd = [
        "sudo",
        str(installer_mod.INSTALLER_PATH[platform]),
        "--farm-id",
        parsed_args.farm_id,
        "--fleet-id",
        parsed_args.fleet_id,
        "--region",
        parsed_args.region,
        "--user",
        parsed_args.user,
        "--worker-agent-program",
        os.path.join(sysconfig.get_path("scripts"), "deadline-worker-agent"),
    ]
    if parsed_args.group is not None:
        expected_cmd.extend(("--group", parsed_args.group))
    if parsed_args.confirmed:
        expected_cmd.append("-y")
    if parsed_args.service_start:
        expected_cmd.append("--start")
    if parsed_args.allow_shutdown:
        expected_cmd.append("--allow-shutdown")
    return expected_cmd


@pytest.mark.parametrize(
    argnames="return_code",
    argvalues=(
        1,
        2,
    ),
)
def test_install_handles_nonzero_exit_code(
    mock_subprocess_run: MagicMock,
    parsed_args: ParsedCommandLineArguments,
    expected_cmd: list[str],
    return_code: int,
) -> None:
    """Assert that install() catches CalledProcessError (when the install script returns a
    non-zero exit code) by exiting with the same exit code"""

    # GIVEN
    exception = CalledProcessError(returncode=return_code, cmd=expected_cmd)
    mock_subprocess_run.side_effect = exception

    with (
        patch.object(installer_mod, "get_argument_parser") as mock_get_arg_parser,
        patch.object(installer_mod.sys, "exit") as mock_sys_exit,
    ):
        arg_parser: MagicMock = mock_get_arg_parser.return_value
        arg_parser.parse_args.return_value = parsed_args

        # WHEN
        install()

    # THEN
    mock_sys_exit.assert_called_once_with(return_code)


class TestInstallRunsCommand:
    """Test cases for install()"""

    @pytest.fixture(
        params=("farm-1", "farm-2"),
    )
    def farm_id(self, request: pytest.FixtureRequest) -> str:
        return request.param

    @pytest.fixture(
        params=("fleet-1", "fleet-2"),
    )
    def fleet_id(self, request: pytest.FixtureRequest) -> str:
        return request.param

    @pytest.fixture(
        params=("us-west-2", "us-west-1"),
    )
    def region(self, request: pytest.FixtureRequest) -> str:
        return request.param

    @pytest.fixture(
        params=(True, False),
    )
    def service_start(self, request: pytest.FixtureRequest) -> bool:
        return request.param

    @pytest.fixture(
        params=("wa_user", "another_wa_user"),
    )
    def user(self, request: pytest.FixtureRequest) -> str:
        return request.param

    @pytest.fixture(
        params=(True, False),
    )
    def allow_shutdown(self, request: pytest.FixtureRequest) -> bool:
        return request.param

    @pytest.fixture(
        params=(
            True,
            False,
        ),
        ids=(
            "confirmed-true",
            "confirmed-false",
        ),
    )
    def confirmed(self, request: pytest.FixtureRequest) -> bool:
        return request.param

    def test_runs_expected_subprocess(
        self,
        mock_subprocess_run: MagicMock,
        parsed_args: ParsedCommandLineArguments,
        expected_cmd: list[str],
    ) -> None:
        # GIVEN
        with patch.object(installer_mod, "get_argument_parser") as mock_get_arg_parser:
            arg_parser: MagicMock = mock_get_arg_parser.return_value
            mock_parse_args: MagicMock = arg_parser.parse_args
            mock_parse_args.return_value = parsed_args

            # WHEN
            install()

        # THEN
        mock_subprocess_run.assert_called_once_with(expected_cmd, check=True)
        mock_get_arg_parser.assert_called_once_with()
        mock_parse_args.assert_called_once_with(namespace=ParsedCommandLineArguments)


@pytest.mark.parametrize(
    argnames="platform",
    argvalues=(
        "aix",
        "emscripten",
        "wasi",
        "cygwin",
        "darwin",
    ),
)
def test_unsupported_platform_raises(platform: str, capsys: pytest.CaptureFixture) -> None:
    # THEN
    with pytest.raises(SystemExit) as raise_ctx:
        # WHEN
        install()

    # THEN
    assert raise_ctx.value.code == 1
    capture = capsys.readouterr()

    assert capture.out == f"ERROR: Unsupported platform {platform}\n"
