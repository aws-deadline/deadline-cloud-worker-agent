# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import os
from pathlib import Path
from subprocess import CalledProcessError
from typing import Generator
from unittest.mock import MagicMock, patch
import sysconfig

import pytest

from deadline_worker_agent.installer import (
    ParsedCommandLineArguments,
    install,
)
from deadline_worker_agent import installer as installer_mod


VFS_DEFAULT_INSTALL_PATH = "/opt/deadline_vfs"


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
        expected_value = Path(installer_mod.__file__).parent / "win_installer.sh"

    # THEN
    assert installer_mod.INSTALLER_PATH[platform] == expected_value


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
        "--scripts-path",
        sysconfig.get_path("scripts"),
        "--vfs-install-path",
        parsed_args.vfs_install_path,
    ]
    if parsed_args.group is not None:
        expected_cmd.extend(("--group", parsed_args.group))
    if parsed_args.confirmed:
        expected_cmd.append("-y")
    if parsed_args.service_start:
        expected_cmd.append("--start")
    if parsed_args.allow_shutdown:
        expected_cmd.append("--allow-shutdown")
    if parsed_args.telemetry_opt_out:
        expected_cmd.append("--telemetry-opt-out")
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
        params=(True, False),
    )
    def telemetry_opt_out(self, request: pytest.FixtureRequest) -> bool:
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

    assert capture.out == f"ERROR: Unsupported platform {platform}{os.linesep}"
