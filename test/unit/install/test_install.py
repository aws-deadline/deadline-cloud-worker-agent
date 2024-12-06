# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from pathlib import Path
from subprocess import CalledProcessError
from typing import Generator
from unittest.mock import MagicMock, patch
import sys
import sysconfig
import typing

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
        expected_value = Path(installer_mod.__file__).parent / "win_installer.py"

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
    assert parsed_args.region is not None, "Region is required"
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
        "--python-interpreter-path",
        sys.executable,
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
    if parsed_args.disallow_instance_profile:
        expected_cmd.append("--disallow-instance-profile")
    if not parsed_args.install_service:
        expected_cmd.append("--no-install-service")
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
        params=(
            ParsedCommandLineArguments(
                farm_id="farm-1",
                fleet_id="fleet-1",
                region="us-west-2",
                user="wa-user",
                password="wa-password",
                group="group1",
                service_start=True,
                confirmed=True,
                allow_shutdown=True,
                install_service=True,
                telemetry_opt_out=True,
                vfs_install_path="/install/path",
                grant_required_access=True,
                disallow_instance_profile=True,
                windows_job_user="job-user",
            ),
            ParsedCommandLineArguments(
                farm_id="farm-2",
                fleet_id="fleet-2",
                region="us-east-2",
                user="another-wa-user",
                password="another-wa-password",
                group="group2",
                service_start=False,
                confirmed=False,
                allow_shutdown=False,
                install_service=False,
                telemetry_opt_out=False,
                vfs_install_path="/another/install/path",
                grant_required_access=False,
                disallow_instance_profile=False,
                windows_job_user="another-job-user",
            ),
        )
    )
    def parsed_args(self, request: pytest.FixtureRequest) -> ParsedCommandLineArguments:
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


class TestGetEc2Region:
    """Tests for _get_ec2_region function"""

    @pytest.fixture(autouse=True)
    def mock_requests_get(self) -> Generator[MagicMock, None, None]:
        with patch.object(installer_mod.requests, "get") as m:
            yield m

    @pytest.fixture(autouse=True)
    def mock_requests_put(self) -> Generator[MagicMock, None, None]:
        with patch.object(installer_mod.requests, "put") as m:
            yield m

    def test_gets_ec2_region(self, mock_requests_get: MagicMock, mock_requests_put: MagicMock):
        # GIVEN
        region = "us-east-2"
        az = f"{region}a"

        mock_requests_get.return_value.text = az

        # WHEN
        actual = installer_mod._get_ec2_region()

        # THEN
        assert actual == region
        mock_requests_put.assert_called_once_with(
            url="http://169.254.169.254/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "10"},
        )
        mock_requests_get.assert_called_once_with(
            url="http://169.254.169.254/latest/meta-data/placement/availability-zone",
            headers={"X-aws-ec2-metadata-token": mock_requests_put.return_value.text},
        )

    @pytest.mark.parametrize(
        ["put_side_effect", "get_side_effect"],
        [
            [Exception(), None],  # token request fails
            [None, Exception()],  # az request fails
        ],
    )
    def test_fails_if_request_raises(
        self,
        put_side_effect: typing.Optional[Exception],
        get_side_effect: typing.Optional[Exception],
        mock_requests_put: MagicMock,
        mock_requests_get: MagicMock,
        capfd: pytest.CaptureFixture,
    ):
        # GIVEN
        if put_side_effect:
            mock_requests_put.side_effect = put_side_effect
        if get_side_effect:
            mock_requests_get.side_effect = get_side_effect

        # WHEN
        retval = installer_mod._get_ec2_region()

        # THEN
        assert retval is None
        out, _ = capfd.readouterr()
        assert "Failed to detect AWS region: " in out

    def test_raises_if_empty_token_received(
        self,
        mock_requests_put: MagicMock,
        capfd: pytest.CaptureFixture,
    ):
        # GIVEN
        mock_requests_put.return_value.text = None

        # WHEN
        retval = installer_mod._get_ec2_region()

        # THEN
        assert retval is None
        out, _ = capfd.readouterr()
        assert "Failed to detect AWS region: Received empty IMDSv2 token" in out

    def test_fails_if_empty_az_received(
        self,
        mock_requests_get: MagicMock,
        capfd: pytest.CaptureFixture,
    ):
        # GIVEN
        mock_requests_get.return_value.text = ""

        # WHEN
        retval = installer_mod._get_ec2_region()

        # THEN
        assert retval is None
        out, _ = capfd.readouterr()
        assert "AWS region could not be detected, received empty response from IMDS" in out

    def test_fails_if_nonvalid_az_received(
        self,
        mock_requests_get: MagicMock,
        capfd: pytest.CaptureFixture,
    ):
        # GIVEN
        az = "Not-A-Region-Code-123"
        mock_requests_get.return_value.text = az

        # WHEN
        retval = installer_mod._get_ec2_region()

        # THEN
        assert retval is None
        out, _ = capfd.readouterr()
        assert (
            f"AWS region could not be detected, got unexpected availability zone from IMDS: {az}"
            in out
        )
