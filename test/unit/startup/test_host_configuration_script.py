# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests that a host configuration script body is written to disk verbatim.

The body is `hostConfiguration.scriptBody` as the control plane returns it: an opaque
customer-authored shell or PowerShell script, not an OpenJD template. Nothing on its
path through the agent asserts a format-string contract on it, and nothing can.

`HostConfigurationScriptRunner._write_script_file` used to wrap it in
`DataString_2023_09`, whose `__new__` parses it as an OpenJD format string. So ordinary
`{{`/`}}` idioms -- Go templates in `docker --format`, a nested POSIX default
`${A:-${B}}`, a nested JSON object, a nested PowerShell hashtable -- raised
FormatStringError at construction time, before `_materialize_files` was reached. Nothing
was written and the script never ran. See Bea-60617 / P514570804.

The parse had no platform branch upstream of it, so the defect was
platform-independent: the only platform dependence in `_write_script_file` selects the
file *name*, and `DataString.__new__` is pure Python. These tests are written
platform-neutral so the same file covers Windows when run on a Windows host.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Generator
from unittest.mock import MagicMock, patch

import pytest
from botocore.credentials import Credentials

from deadline_worker_agent.config.cli_args import ParsedCommandLineArguments
from deadline_worker_agent.config.config import Configuration
from deadline_worker_agent.startup.host_configuration_script import (
    HostConfigurationScriptRunner,
)

FARM_ID = "farm-00000000000000000000000000000000"
FLEET_ID = "fleet-00000000000000000000000000000000"
WORKER_ID = "worker-00000000000000000000000000000000"

# A Go template in a container health check. Appears in essentially every one.
DOCKER_INSPECT_FORMAT_BODY = "docker inspect --format '{{.State.Running}}' c"


@pytest.fixture(autouse=True)
def config_file_mock() -> Generator[MagicMock, None, None]:
    """Mocks ConfigFile.load() to raise FileNotFoundError.

    Without this, a worker agent config file present in the development environment
    leaks into the Configuration these tests build.
    """
    with patch(
        "deadline_worker_agent.config.config_file.ConfigFile.load",
        side_effect=FileNotFoundError(),
    ) as mock_config_file_load:
        yield mock_config_file_load


@pytest.fixture
def configuration(tmp_path: Path) -> Configuration:
    cli_args = ParsedCommandLineArguments()
    cli_args.farm_id = FARM_ID
    cli_args.fleet_id = FLEET_ID
    cli_args.run_jobs_as_agent_user = False
    cli_args.posix_job_user = "some-user:some-group"
    cli_args.no_shutdown = True
    cli_args.profile = None
    cli_args.verbose = None
    cli_args.disallow_instance_profile = False
    # Direct the logs and persistence state into a temporary directory
    cli_args.logs_dir = tmp_path / "temp-logs-dir"
    cli_args.persistence_dir = tmp_path / "temp-persist-dir"
    config = Configuration(parsed_cli_args=cli_args)

    # These directories need to exist
    cli_args.logs_dir.mkdir()
    cli_args.persistence_dir.mkdir()

    return config


@pytest.fixture
def session_directory(tmp_path: Path) -> Path:
    session_dir = tmp_path / "session"
    session_dir.mkdir()
    return session_dir


@pytest.fixture
def worker_boto3_session() -> MagicMock:
    """The constructor computes env vars from these credentials before super().__init__."""
    session = MagicMock()
    session.get_credentials.return_value = Credentials(
        access_key="access_key_id",
        secret_key="secret_access_key",
        token="session_token",
    )
    return session


@pytest.fixture
def logger() -> MagicMock:
    return MagicMock(spec=logging.Logger)


def _runner(
    *,
    script: str,
    configuration: Configuration,
    session_directory: Path,
    worker_boto3_session: MagicMock,
    logger: MagicMock,
) -> HostConfigurationScriptRunner:
    """Builds the real runner. Nothing on the path under test is mocked."""
    return HostConfigurationScriptRunner(
        logger=logger,
        configuration=configuration,
        worker_id=WORKER_ID,
        session_directory=session_directory,
        worker_boto3_session=worker_boto3_session,
        host_configuration_script=script,
        host_configuration_timeout_seconds=300,
        runas_user=None,  # We cannot use root for tests.
    )


def _expected_script_file_name() -> str:
    return "host_configuration.ps1" if sys.platform == "win32" else "host_configuration.sh"


class TestScriptBodyIsWrittenVerbatim:
    """The contract: whatever the service sent lands on disk byte for byte."""

    @pytest.mark.parametrize(
        "body",
        (
            pytest.param("echo hello", id="control-echo-hello"),
            pytest.param(DOCKER_INSPECT_FORMAT_BODY, id="docker-inspect-format"),
            pytest.param('echo "${A:-${B}}"', id="shell-nested-default"),
            pytest.param(
                'cat <<\'EOF\' > /tmp/c.json\n{"a":{"b":1}}\nEOF\n',
                id="json-heredoc",
            ),
            pytest.param("$h = @{a=@{b=1}}", id="powershell-nested-hashtable"),
            pytest.param("awk '{ if ($1) {print $2}}' /tmp/f", id="awk-nested-brace"),
            # A body authored on Windows. Pins the second half of the contract on
            # every platform: a writer that converts line endings keeps the braces
            # intact but rewrites CRLF, which only `json-heredoc` would catch and
            # only on a Windows host.
            pytest.param(
                'cat <<\'EOF\' > C:\\c.json\r\n{"a":{"b":1}}\r\nEOF\r\n',
                id="crlf-authored",
            ),
        ),
    )
    def test_body_reaches_disk_unchanged(
        self,
        body: str,
        configuration: Configuration,
        session_directory: Path,
        worker_boto3_session: MagicMock,
        logger: MagicMock,
    ) -> None:
        # GIVEN a script body the service returned verbatim. Braces in it are shell,
        # PowerShell, JSON or Go template syntax, never OpenJD interpolation.
        runner = _runner(
            script=body,
            configuration=configuration,
            session_directory=session_directory,
            worker_boto3_session=worker_boto3_session,
            logger=logger,
        )

        # WHEN
        script_file_path = runner._write_script_file()

        # THEN the body is on disk exactly as it arrived. read_bytes() rather than
        # read_text() so a newline translation or re-encoding is caught too.
        written = Path(script_file_path)
        assert written.name == _expected_script_file_name()
        assert written.exists()
        assert written.read_bytes() == body.encode("utf-8")
