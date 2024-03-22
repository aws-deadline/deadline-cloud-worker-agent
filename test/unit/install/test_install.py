# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from pathlib import Path
from subprocess import CalledProcessError
from typing import Generator
from unittest.mock import call, MagicMock, patch
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

    assert capture.out == f"ERROR: Unsupported platform {platform}\n"


class TestGetEc2Region:
    """Tests for _get_ec2_region function"""

    @pytest.fixture(autouse=True)
    def mock_urlopen(self) -> Generator[MagicMock, None, None]:
        with patch.object(installer_mod.urllib.request, "urlopen") as m:
            yield m

    @patch.object(installer_mod.urllib.request, "Request")
    def test_gets_ec2_region(self, mock_Request: MagicMock, mock_urlopen: MagicMock):
        # GIVEN
        region = "us-east-2"
        az = f"{region}a"

        mock_token_request = MagicMock()
        mock_token_response = MagicMock()

        mock_az_request = MagicMock()
        mock_az_response = MagicMock()
        mock_az_response.__enter__().read().decode.return_value = az

        mock_Request.side_effect = [mock_token_request, mock_az_request]
        mock_urlopen.side_effect = [mock_token_response, mock_az_response]

        # WHEN
        actual = installer_mod._get_ec2_region()

        # THEN
        assert actual == region
        mock_Request.assert_has_calls(
            [
                call(
                    url="http://169.254.169.254/latest/api/token",
                    headers={"X-aws-ec2-metadata-token-ttl-seconds": "10"},
                    method="PUT",
                ),
                call(
                    url="http://169.254.169.254/latest/meta-data/placement/availability-zone",
                    headers={
                        "X-aws-ec2-metadata-token": mock_token_response.__enter__()
                        .read()
                        .decode.return_value
                    },
                    method="GET",
                ),
            ]
        )
        mock_urlopen.assert_has_calls(
            [
                call(mock_token_request, timeout=1),
                call(mock_az_request, timeout=1),
            ]
        )

    @pytest.mark.parametrize(
        "side_effect",
        [
            [Exception()],  # token request fails
            [MagicMock(), Exception()],  # az request fails
        ],
    )
    def test_fails_if_urlopen_raises(
        self,
        side_effect: list[Exception],
        mock_urlopen: MagicMock,
        capfd: pytest.CaptureFixture,
    ):
        # GIVEN
        mock_urlopen.side_effect = side_effect

        # WHEN
        retval = installer_mod._get_ec2_region()

        # THEN
        assert retval is None
        out, _ = capfd.readouterr()
        assert "Failed to detect AWS region: " in out

    def test_raises_if_empty_token_received(
        self,
        mock_urlopen: MagicMock,
        capfd: pytest.CaptureFixture,
    ):
        # GIVEN
        mock_token_response = MagicMock()
        mock_token_response.__enter__().read().decode.return_value = None
        mock_urlopen.side_effect = [mock_token_response]

        # WHEN
        retval = installer_mod._get_ec2_region()

        # THEN
        assert retval is None
        out, _ = capfd.readouterr()
        assert "Failed to detect AWS region: Received empty IMDSv2 token" in out

    def test_fails_if_empty_az_received(
        self,
        mock_urlopen: MagicMock,
        capfd: pytest.CaptureFixture,
    ):
        # GIVEN
        mock_az_response = MagicMock()
        mock_az_response.__enter__().read().decode.return_value = ""
        mock_urlopen.side_effect = [MagicMock(), mock_az_response]

        # WHEN
        retval = installer_mod._get_ec2_region()

        # THEN
        assert retval is None
        out, _ = capfd.readouterr()
        assert "AWS region could not be detected, received empty response from IMDS" in out

    def test_fails_if_nonvalid_az_received(
        self,
        mock_urlopen: MagicMock,
        capfd: pytest.CaptureFixture,
    ):
        # GIVEN
        az = "Not-A-Region-Code-123"
        mock_az_response = MagicMock()
        mock_az_response.__enter__().read().decode.return_value = az
        mock_urlopen.side_effect = [MagicMock(), mock_az_response]

        # WHEN
        retval = installer_mod._get_ec2_region()

        # THEN
        assert retval is None
        out, _ = capfd.readouterr()
        assert (
            f"AWS region could not be detected, got unexpected availability zone from IMDS: {az}"
            in out
        )
