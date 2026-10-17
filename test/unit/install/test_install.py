# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from pathlib import Path
from subprocess import CalledProcessError
from typing import Generator
from unittest.mock import ANY, MagicMock, patch
import sys
import sysconfig
import typing

import pytest
import requests

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
        "--session-root-dir",
        str(parsed_args.session_root_dir),
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
                session_root_dir="/sessions/root",
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
                session_root_dir="/different/root",
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
        mock_parse_args.assert_called_once_with(namespace=ANY)
        assert isinstance(mock_parse_args.call_args.kwargs["namespace"], ParsedCommandLineArguments)


@pytest.mark.parametrize(
    argnames="platform",
    argvalues=(
        "aix",
        "emscripten",
        "wasi",
        "cygwin",
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

    @pytest.fixture(autouse=True)
    def az_hop_is_ok(self, mock_requests_get: MagicMock) -> MagicMock:
        """A bare MagicMock's status_code compares unequal to 200, and this hop is now checked."""
        mock_requests_get.return_value.status_code = 200
        return mock_requests_get

    @pytest.fixture(autouse=True)
    def token_is_ok(self, mock_requests_put: MagicMock) -> MagicMock:
        """As above: the code now rejects a non-200 token rather than forwarding its body."""
        mock_requests_put.return_value.status_code = 200
        return mock_requests_put

    @pytest.fixture(autouse=True)
    def no_real_backoff(self) -> Generator[MagicMock, None, None]:
        """Keep the retry backoff out of the suite's wall-clock time."""
        with patch.object(installer_mod.time, "sleep") as m:
            yield m

    @pytest.mark.parametrize(
        ["status_code", "expected_output"],
        [
            pytest.param(403, "IMDSv2 may be disabled", id="imdsv2-disabled"),
            pytest.param(401, "returned HTTP 401", id="unauthorized"),
            pytest.param(404, "returned HTTP 404", id="not-found"),
        ],
    )
    def test_names_the_cause_when_the_token_is_rejected(
        self,
        status_code: int,
        expected_output: str,
        mock_requests_put: MagicMock,
        mock_requests_get: MagicMock,
        no_real_backoff: MagicMock,
        capfd: pytest.CaptureFixture,
    ) -> None:
        """An error body is non-empty, so an emptiness check misses it -- forwarded as a token it
        gets a 401 next hop and surfaces as "unexpected availability zone"."""
        mock_requests_put.return_value.status_code = status_code

        assert installer_mod._get_ec2_region() is None
        out, _ = capfd.readouterr()
        assert expected_output in out
        # Determinate, so no second hop and no retry.
        mock_requests_get.assert_not_called()
        no_real_backoff.assert_not_called()

    def test_names_the_cause_when_the_az_hop_is_rejected(
        self,
        mock_requests_get: MagicMock,
        no_real_backoff: MagicMock,
        capfd: pytest.CaptureFixture,
    ) -> None:
        """Unchecked, the error body becomes `az` and is reported as an unexpected availability
        zone -- the token check's misattribution, one hop later."""
        # 403 rather than 401: a 401 here means a lapsed token and is retried, per
        # test_retries_a_lapsed_token_on_the_az_hop.
        mock_requests_get.return_value.status_code = 403

        assert installer_mod._get_ec2_region() is None
        out, _ = capfd.readouterr()
        assert "HTTP 403 for the availability zone" in out
        no_real_backoff.assert_not_called()

    def test_retries_a_throttled_az_hop(
        self, mock_requests_get: MagicMock, mock_requests_put: MagicMock
    ) -> None:
        """A 429 here arrives as a body, not an exception, so it needs its own check to retry."""
        mock_requests_get.side_effect = [
            MagicMock(status_code=429),
            MagicMock(status_code=200, text="us-east-1a"),
        ]

        assert installer_mod._get_ec2_region() == "us-east-1"
        assert mock_requests_get.call_count == 2

    @pytest.mark.parametrize("status_code", [429, 500, 503])
    def test_retries_a_transient_token_status(
        self, status_code: int, mock_requests_put: MagicMock, mock_requests_get: MagicMock
    ) -> None:
        """Boot is when a user-data install runs and when the metadata service is not yet up, so
        treating 500/503 as final would leave the retry covering half the transient set."""
        mock_requests_put.side_effect = [
            MagicMock(status_code=status_code),
            MagicMock(status_code=200, text="TOKEN"),
        ]
        mock_requests_get.return_value.text = "ap-southeast-2c"

        assert installer_mod._get_ec2_region() == "ap-southeast-2"
        assert mock_requests_put.call_count == 2

    def test_retries_a_lapsed_token_on_the_az_hop(
        self, mock_requests_put: MagicMock, mock_requests_get: MagicMock
    ) -> None:
        """The 10s token TTL can lapse between the two calls, so a fresh token fixes it.
        Transient on this hop only -- a rejected token *request* is not re-obtainable."""
        mock_requests_get.side_effect = [
            MagicMock(status_code=401),
            MagicMock(status_code=200, text="eu-central-1a"),
        ]

        assert installer_mod._get_ec2_region() == "eu-central-1"
        assert mock_requests_put.call_count == 2

    @pytest.mark.parametrize(
        ["az", "reason"],
        [
            pytest.param("", "an empty availability zone", id="empty-az"),
            pytest.param("not-an-az", "an AZ that fails the regex", id="unparseable-az"),
        ],
    )
    def test_does_not_retry_a_determinate_answer(
        self,
        az: str,
        reason: str,
        mock_requests_get: MagicMock,
        mock_requests_put: MagicMock,
        no_real_backoff: MagicMock,
    ) -> None:
        """A workstation install with no --region expects None, so retrying makes one failure read
        as three."""
        mock_requests_get.return_value.text = az

        assert installer_mod._get_ec2_region() is None, reason
        mock_requests_put.assert_called_once()
        no_real_backoff.assert_not_called()

    def test_does_not_retry_a_black_holed_address(
        self, mock_requests_put: MagicMock, no_real_backoff: MagicMock
    ) -> None:
        """ConnectTimeout subclasses both ConnectionError and Timeout, so a handler keyed on
        Timeout would make the workstation case retryable -- five lines and ~4s for an answer
        known on the first attempt."""
        mock_requests_put.side_effect = requests.ConnectTimeout("connect timed out")

        assert installer_mod._get_ec2_region() is None
        mock_requests_put.assert_called_once()
        no_real_backoff.assert_not_called()

    def test_retries_a_slow_answer_rather_than_failing_the_install(
        self, mock_requests_put: MagicMock, mock_requests_get: MagicMock
    ) -> None:
        """`install()` exits 1 on None, and user-data installs run during boot, so bounding the
        request cannot be the whole story."""
        # GIVEN: the first attempt times out, the second answers
        region = "us-west-2"
        mock_requests_put.side_effect = [
            MagicMock(status_code=503),
            MagicMock(status_code=200, text="TOKEN"),
        ]
        mock_requests_get.return_value.text = f"{region}b"

        # WHEN / THEN
        assert installer_mod._get_ec2_region() == region
        assert mock_requests_put.call_count == 2

    def test_gives_up_after_the_attempt_budget(
        self, mock_requests_put: MagicMock, no_real_backoff: MagicMock
    ) -> None:
        # GIVEN
        mock_requests_put.side_effect = [MagicMock(status_code=503)] * installer_mod._IMDS_ATTEMPTS

        # WHEN / THEN
        assert installer_mod._get_ec2_region() is None
        assert mock_requests_put.call_count == installer_mod._IMDS_ATTEMPTS
        # Between attempts, not after the last.
        assert no_real_backoff.call_count == installer_mod._IMDS_ATTEMPTS - 1

    def test_does_not_retry_a_host_that_answers(
        self, mock_requests_get: MagicMock, no_real_backoff: MagicMock
    ) -> None:
        """Control: no delay is paid on an EC2 host, which is the case that matters at boot."""
        mock_requests_get.return_value.text = "eu-west-1a"

        assert installer_mod._get_ec2_region() == "eu-west-1"
        no_real_backoff.assert_not_called()

    def test_gets_ec2_region(self, mock_requests_get: MagicMock, mock_requests_put: MagicMock):
        # GIVEN
        region = "us-east-2"
        az = f"{region}a"

        mock_requests_get.return_value.text = az

        # WHEN
        actual = installer_mod._get_ec2_region()

        # THEN
        assert actual == region
        # Both carry a timeout: unbounded, a non-EC2 host that black-holes the
        # link-local address stalls the installer with no output.
        mock_requests_put.assert_called_once_with(
            url="http://169.254.169.254/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "10"},
            timeout=installer_mod._IMDS_REQUEST_TIMEOUT,
        )
        mock_requests_get.assert_called_once_with(
            url="http://169.254.169.254/latest/meta-data/placement/availability-zone",
            headers={"X-aws-ec2-metadata-token": mock_requests_put.return_value.text},
            timeout=installer_mod._IMDS_REQUEST_TIMEOUT,
        )

    @pytest.mark.parametrize(
        ["put_side_effect", "get_side_effect"],
        [
            [Exception(), None],  # token request fails
            [None, Exception()],  # az request fails
            # A black-holed metadata address: the case the timeout turns into this
            # branch rather than a stall.
            [requests.ConnectTimeout(), None],
            [None, requests.ConnectTimeout()],
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


class TestMacOSInstall:
    """Test cases for macOS (darwin) support in the installer dispatcher."""

    @pytest.fixture(autouse=True)
    def mock_darwin_platform(self) -> Generator[str, None, None]:
        with patch.object(installer_mod.sys, "platform", new="darwin") as m:
            yield m

    def test_installer_path_has_darwin_entry(self) -> None:
        # THEN
        assert (
            installer_mod.INSTALLER_PATH["darwin"]
            == Path(installer_mod.__file__).parent / "install_macos.sh"
        )

    def test_runs_expected_subprocess_on_darwin(
        self,
        mock_subprocess_run: MagicMock,
    ) -> None:
        # GIVEN
        parsed_args = ParsedCommandLineArguments(
            farm_id="farm-1",
            fleet_id="fleet-1",
            region="us-west-2",
            user="wa-user",
            group="job-group",
            service_start=False,
            confirmed=True,
            allow_shutdown=False,
            install_service=True,
            telemetry_opt_out=False,
            vfs_install_path=None,
            disallow_instance_profile=False,
            session_root_dir=Path("/var/lib/deadline/sessions"),
        )

        expected_cmd = [
            "sudo",
            str(installer_mod.INSTALLER_PATH["darwin"]),
            "--farm-id",
            "farm-1",
            "--fleet-id",
            "fleet-1",
            "--region",
            "us-west-2",
            "--user",
            "wa-user",
            "--scripts-path",
            sysconfig.get_path("scripts"),
            "--python-interpreter-path",
            sys.executable,
            "--session-root-dir",
            # install() passes str(args.session_root_dir); a Path stringifies with the host
            # separator, so build the expected value the same way rather than hard-coding it.
            str(Path("/var/lib/deadline/sessions")),
            "--group",
            "job-group",
            "-y",
        ]

        with patch.object(installer_mod, "get_argument_parser") as mock_get_arg_parser:
            arg_parser: MagicMock = mock_get_arg_parser.return_value
            arg_parser.parse_args.return_value = parsed_args

            # WHEN
            install()

        # THEN
        mock_subprocess_run.assert_called_once_with(expected_cmd, check=True)

    def test_vfs_install_path_rejected_on_darwin(
        self,
        mock_subprocess_run: MagicMock,
        capsys: pytest.CaptureFixture,
    ) -> None:
        """--vfs-install-path is unsupported on macOS and must be rejected before dispatch."""
        # GIVEN
        parsed_args = ParsedCommandLineArguments(
            farm_id="farm-1",
            fleet_id="fleet-1",
            region="us-west-2",
            user="wa-user",
            vfs_install_path="/opt/deadline_vfs",
        )

        with patch.object(installer_mod, "get_argument_parser") as mock_get_arg_parser:
            arg_parser: MagicMock = mock_get_arg_parser.return_value
            arg_parser.parse_args.return_value = parsed_args

            # WHEN / THEN
            with pytest.raises(SystemExit) as raise_ctx:
                install()

        assert raise_ctx.value.code == 1
        assert "--vfs-install-path is not supported on macOS." in capsys.readouterr().out
        # AND the install script must not be invoked
        mock_subprocess_run.assert_not_called()
