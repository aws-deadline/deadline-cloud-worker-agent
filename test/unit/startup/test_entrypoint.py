# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for the Worker Agent entrypoint"""
from __future__ import annotations

import logging
import os
import subprocess
import sys
from typing import Any, Generator, Optional
from unittest.mock import ANY, MagicMock, call, patch
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from deadline_worker_agent.errors import ServiceShutdown
from deadline_worker_agent.log_sync.loggers import ROOT_LOGGER
from deadline_worker_agent.startup import entrypoint as entrypoint_mod
from deadline_worker_agent.startup.bootstrap import (
    WorkerBootstrap,
    WorkerPersistenceInfo,
)
from deadline_worker_agent.aws.deadline import WorkerLogConfig
from deadline_worker_agent.startup.config import Configuration, ConfigurationError

entrypoint = entrypoint_mod.entrypoint


@pytest.fixture
def cloudwatch_log_group() -> str:
    return "cloudwatch_log_group"


@pytest.fixture
def cloudwatch_log_stream() -> str:
    return "cloudwatch_log_stream"


@pytest.fixture
def worker_info(
    worker_id: str,
) -> WorkerPersistenceInfo:
    return WorkerPersistenceInfo(
        worker_id=worker_id,
    )


@pytest.fixture
def worker_log_config(
    cloudwatch_log_group: str,
    cloudwatch_log_stream: str,
) -> WorkerLogConfig:
    return WorkerLogConfig(
        cloudwatch_log_group=cloudwatch_log_group,
        cloudwatch_log_stream=cloudwatch_log_stream,
    )


@pytest.fixture(autouse=True)
def mock_boto_session(
    client: MagicMock,
    s3_client: MagicMock,
    logs_client: MagicMock,
) -> MagicMock:
    mock_session = MagicMock()

    def client_mock(service: str, config: Any) -> MagicMock:
        if service == "deadline":
            return client
        elif service == "s3":
            return s3_client
        elif service == "logs":
            return logs_client
        else:
            raise NotImplementedError(f'Service "{service}" not implemented')

    mock_session.client.side_effect = client_mock
    return mock_session


@pytest.fixture(autouse=True)
def bootstrap_worker_mock(
    worker_info: WorkerPersistenceInfo,
    mock_boto_session: MagicMock,
    worker_log_config: WorkerLogConfig,
) -> Generator[MagicMock, None, None]:
    with patch.object(
        entrypoint_mod,
        "bootstrap_worker",
        return_value=WorkerBootstrap(
            worker_info=worker_info,
            session=mock_boto_session,
            log_config=worker_log_config,
        ),
    ) as bootstrap_worker_mock:
        yield bootstrap_worker_mock


@pytest.fixture(autouse=True)
def mock_stream_cloudwatch_logs() -> Generator[MagicMock, None, None]:
    with patch.object(entrypoint_mod, "stream_cloudwatch_logs") as mock_stream_cloudwatch_logs:
        yield mock_stream_cloudwatch_logs


@pytest.fixture(autouse=True)
def mock_timed_rotating_file_handler() -> Generator[MagicMock, None, None]:
    """This mocks the TimedRotatingFileHandler so that our tests don't perform actual file I/O"""
    with patch.object(entrypoint_mod, "TimedRotatingFileHandler") as mock_obj:
        mock_obj.return_value.level = logging.INFO
        yield mock_obj


@pytest.fixture(autouse=True)
def mock_worker_run() -> Generator[MagicMock, None, None]:
    """Mock the Worker.run() method which is an infinite loop"""
    with patch.object(entrypoint_mod.Worker, "run") as mock_worker_run:
        yield mock_worker_run


@pytest.fixture
def configuration() -> MagicMock:
    config = MagicMock()
    config.verbose = False
    config.farm_id = "farm-123"
    config.fleet_id = "fleet-456"
    config.profile = None
    config.sessions = True
    # Required because MagicMock does not support int comparison
    config.host_metrics_logging_interval_seconds = 10
    return config


@pytest.fixture(autouse=True)
def configuration_load(configuration: MagicMock) -> Generator[MagicMock, None, None]:
    with patch.object(entrypoint_mod.Configuration, "load") as load_mock:
        load_mock.return_value = configuration
        yield load_mock


@pytest.fixture
def mock_system_shutdown() -> Generator[MagicMock, None, None]:
    with patch.object(entrypoint_mod, "_system_shutdown") as system_shutdown_mock:
        yield system_shutdown_mock


@pytest.fixture(autouse=True)
def block_rich_import() -> Generator[None, None, None]:
    with patch.dict("sys.modules", {"rich.logging": None}):
        yield


@pytest.fixture(autouse=True)
def block_telemetry_client() -> Generator[MagicMock, None, None]:
    with patch.object(entrypoint_mod, "record_worker_start_telemetry_event") as telem_mock:
        yield telem_mock


def test_calls_worker_run(
    mock_worker_run: MagicMock,
) -> None:
    """Tests that the Worker.run() method is called by the entrypoint"""
    # WHEN
    entrypoint()

    # THEN
    mock_worker_run.assert_called_once_with()


@patch.object(entrypoint_mod.sys, "exit")
def test_worker_run_exception(
    sys_exit_mock: MagicMock,
    mock_worker_run: MagicMock,
) -> None:
    """Tests that exceptions raised by Worker.run() are logged and the program exits with a non-zero exit code"""
    # GIVEN
    error_msg = "error_msg"
    exception = Exception(error_msg)
    mock_worker_run.side_effect = exception

    with patch.object(entrypoint_mod, "_logger") as logger:
        # WHEN
        entrypoint()

    # THEN
    logger_exception: MagicMock = logger.exception
    logger_exception.assert_called_once_with("Failed running worker: %s", exception)
    sys_exit_mock.assert_called_once_with(1)


def test_configuration_load(
    configuration_load: MagicMock,
) -> None:
    """Tests that the entrypoint loads the Worker Agent configuration"""
    # WHEN
    entrypoint()

    # THEN
    configuration_load.assert_called_once()


@patch.object(entrypoint_mod.sys.stderr, "write")
@patch.object(entrypoint_mod.sys, "exit")
def test_configuration_error(
    sys_exit_mock: MagicMock,
    sys_stderr_write_mock: MagicMock,
    configuration_load: MagicMock,
) -> None:
    """
    Tests that the entrypoint handles Configuration loading errors by
    outputting the error to stderr and exiting with a exit code of 1
    """
    # GIVEN
    error_msg = "error msg"
    exception = ConfigurationError(error_msg)
    configuration_load.side_effect = exception
    # Mock sys.exit() to throw an exception. This is so that program flow won't fall through
    # after the mock is called - as it would for the real sys.exit() implementation
    sys_exit_mock.side_effect = Exception()

    # WHEN
    with pytest.raises(Exception) as raises_ctx:
        entrypoint()

    # THEN
    assert raises_ctx.value is sys_exit_mock.side_effect
    sys_stderr_write_mock.assert_has_calls([call(f"ERROR: {error_msg}{os.linesep}")])
    sys_exit_mock.assert_called_with(1)


def test_configuration_logged(
    configuration: MagicMock,
) -> None:
    """
    Tests that the entrypoint call `Configuration.log()`
    """
    # GIVEN
    configuration_log: MagicMock = configuration.log

    # WHEN
    entrypoint()

    # THEN
    configuration_log.assert_called_with()


@pytest.mark.parametrize(
    ("verbose", "expected_root_log_level", "expected_console_fmt_str"),
    (
        pytest.param(False, logging.INFO, "[%(levelname)8s] %(message)s", id="non-verbose"),
        pytest.param(
            True,
            logging.DEBUG,
            "[%(asctime)s] [%(levelname)8s] [%(name)-50s] --- %(message)s",
            id="verbose",
        ),
    ),
)
@patch.object(entrypoint_mod, "Configuration")
@patch.object(entrypoint_mod.logging, "getLogger")
@patch.object(entrypoint_mod, "_logger")
def test_log_configuration(
    module_logger_mock: MagicMock,
    get_logger_mock: MagicMock,
    _config_mock: MagicMock,
    mock_timed_rotating_file_handler: MagicMock,
    verbose: bool,
    expected_root_log_level: int,
    expected_console_fmt_str: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # GIVEN
    with TemporaryDirectory() as tempdir:
        # Mock verbose configuration setting
        _config_mock.load.return_value.verbose = verbose
        _config_mock.load.return_value.sessions = True
        _config_mock.load().worker_logs_dir = Path(tempdir) / "logs"
        _config_mock.load().worker_state_file = Path(tempdir) / "logs" / "worker_state_file.json"
        _config_mock.load().worker_credentials_dir = Path(tempdir) / "credentials"
        _config_mock.load().worker_logs_dir.mkdir()
        _config_mock.load().worker_credentials_dir.mkdir()
        # Required because MagicMock does not support int comparison
        _config_mock.load().host_metrics_logging_interval_seconds = 10

        # Mock logging.getLogger
        root_logger = MagicMock()
        boto_logger = MagicMock()
        botocore_logger = MagicMock()
        urllib3_connectionpool_logger = MagicMock()
        log_sync_logger = MagicMock()
        job_attachments_logger = MagicMock()

        def get_logger(name=None):
            if not name:
                return root_logger
            elif name == "boto3":
                return boto_logger
            elif name == "botocore":
                return botocore_logger
            elif name == "urllib3.connectionpool":
                return urllib3_connectionpool_logger
            elif name == "deadline_worker_agent.log_sync":
                return log_sync_logger
            elif name == "deadline.job_attachments":
                return job_attachments_logger
            raise NotImplementedError(f"getLogger({repr(name)})")

        get_logger_mock.side_effect = get_logger

        mock_file_logger: MagicMock = mock_timed_rotating_file_handler.return_value

        # WHEN
        entrypoint()

        # THEN
        root_logger.setLevel.assert_called_with(expected_root_log_level)

        # The entrypoint should add three handlers to the root logger
        assert len(root_logger.addHandler.call_args_list) == 3

        # Ensure we have a stderr stream handler
        class StreamHandlerStderrMatcher:
            def __eq__(self, other: Any) -> bool:
                return (
                    isinstance(other, logging.StreamHandler)
                    and other.stream is sys.stderr
                    and isinstance(other.formatter, logging.Formatter)
                    and other.formatter._fmt == expected_console_fmt_str
                )

        root_logger.addHandler.assert_any_call(StreamHandlerStderrMatcher())

        # Ensure we have a TimedRotatingFileHandler
        mock_timed_rotating_file_handler.assert_called_with(
            _config_mock.load().worker_logs_dir / "worker-agent.log",
            when="d",
            interval=1,
        )
        root_logger.addHandler.assert_any_call(mock_file_logger)

        # Ensure boto and botocore loggers are set to WARNING levels
        for logger in (
            boto_logger,
            botocore_logger,
            urllib3_connectionpool_logger,
            log_sync_logger,
        ):
            logger.setLevel.assert_called_once_with(logging.WARNING)

        # Make sure that we log the message that we're contracted to log once successfully
        # started.
        module_logger_mock.info.assert_any_call(
            "Worker successfully bootstrapped and is now running."
        )


@pytest.mark.parametrize(
    argnames=("service_shutdown",),
    argvalues=(
        pytest.param(True, id="service-shutdown"),
        pytest.param(False, id="no-service-shutdown"),
    ),
)
@patch.object(entrypoint_mod._logger, "info")
def test_worker_deletion(
    logger_info_mock: MagicMock,
    worker_info: WorkerPersistenceInfo,
    service_shutdown: bool,
    client: MagicMock,
    configuration: Configuration,
    mock_worker_run: MagicMock,
) -> None:
    # GIVEN
    if service_shutdown:
        mock_worker_run.side_effect = ServiceShutdown()

    # WHEN
    with patch.object(entrypoint_mod, "delete_worker") as mock_delete_worker:
        entrypoint_mod.entrypoint()

    # THEN
    if service_shutdown:
        mock_delete_worker.assert_called_with(
            deadline_client=client,
            config=configuration,
            worker_id=worker_info.worker_id,
        )
        logger_info_mock.assert_has_calls(
            (
                call('Deleting worker with id "%s"', worker_info.worker_id),
                call('Worker "%s" successfully deleted', worker_info.worker_id),
            ),
        )
    else:
        mock_delete_worker.assert_not_called()


@pytest.mark.parametrize(
    ("request_shutdown"),
    [
        pytest.param(True, id="True"),
        pytest.param(False, id="False"),
    ],
)
def test_system_shutdown_called(
    mock_system_shutdown: MagicMock,
    request_shutdown: bool,
    configuration: MagicMock,
    mock_worker_run: MagicMock,
) -> None:
    """
    Tests that when Worker._system_shutdown() is called if the worker state is
    either STOPPING or RUNNING_DRAINING
    """
    # GIVEN
    if request_shutdown:
        mock_worker_run.side_effect = ServiceShutdown()

    # WHEN
    entrypoint()

    # THEN
    if request_shutdown:
        mock_system_shutdown.assert_called_once_with(config=configuration)
    else:
        mock_system_shutdown.assert_not_called()


@pytest.mark.parametrize(
    ("expected_platform", "expected_command"),
    (
        pytest.param("win32", ["shutdown", "-s"], id="windows"),
        pytest.param("linux", ["sudo", "shutdown", "now"], id="linux"),
    ),
)
@patch.object(entrypoint_mod._logger, "info")
@patch.object(entrypoint_mod.subprocess, "Popen")
def test_system_shutdown(
    subprocess_popen_mock: MagicMock,
    logger_info_mock: MagicMock,
    expected_platform: str,
    expected_command: str,
    configuration: MagicMock,
) -> None:
    """
    Tests that entrypoint._system_shutdown() has the correct platform-specific behavior
    """
    # GIVEN
    process: MagicMock = subprocess_popen_mock.return_value
    process.communicate.return_value = (bytes("some", "utf-8"), bytes("error", "utf-8"))
    process.returncode = 0

    configuration.no_shutdown = False
    with patch.object(sys, "platform", expected_platform):
        # WHEN
        entrypoint_mod._system_shutdown(config=configuration)

    # THEN
    logger_info_mock.assert_any_call("Shutting down the instance")
    subprocess_popen_mock.assert_called_once_with(
        expected_command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )


@pytest.mark.parametrize(
    ("expected_platform", "expected_command"),
    (
        pytest.param("win32", ["shutdown", "-s"], id="windows"),
        pytest.param("linux", ["sudo", "shutdown", "now"], id="linux"),
    ),
)
@patch.object(entrypoint_mod, "_logger")
@patch.object(entrypoint_mod.subprocess, "Popen")
def test_system_shutdown_failure(
    subprocess_popen_mock: MagicMock,
    logger_mock: MagicMock,
    expected_platform: str,
    expected_command: str,
    configuration: MagicMock,
) -> None:
    """
    Tests if we log the shutdown failure correctly
    """
    # GIVEN
    stdout = "stdout_msg"
    stderr = "stderr_msg"
    return_code = 1
    process: MagicMock = subprocess_popen_mock.return_value
    process.communicate.return_value = (bytes(stdout, "utf-8"), bytes(stderr, "utf-8"))
    process.returncode = return_code

    handler_0 = MagicMock()
    handler_1 = MagicMock()
    logger_mock.handlers = [handler_0, handler_1]

    configuration.no_shutdown = False
    with patch.object(sys, "platform", expected_platform):
        # WHEN
        entrypoint_mod._system_shutdown(config=configuration)

    # THEN
    logger_mock.info.assert_any_call("Shutting down the instance")
    subprocess_popen_mock.assert_called_once_with(
        expected_command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    logger_mock.error.assert_called_once_with(
        f"Shutdown command ({expected_command}) failed with return code: {return_code}: {stdout}"
    )
    handler_0.flush.assert_called_once()
    handler_1.flush.assert_called_once()


@pytest.mark.parametrize(
    ("expected_platform", "expected_command"),
    (
        pytest.param("win32", ["shutdown", "-s"], id="windows"),
        pytest.param("linux", ["sudo", "shutdown", "now"], id="linux"),
    ),
)
@patch.object(entrypoint_mod._logger, "debug")
@patch.object(entrypoint_mod._logger, "info")
@patch.object(entrypoint_mod.os, "system")
def test_no_shutdown_only_log(
    system_mock: MagicMock,
    logger_info_mock: MagicMock,
    logger_debug_mock: MagicMock,
    expected_platform: str,
    expected_command: str,
    configuration: MagicMock,
) -> None:
    """
    Tests if the system shutdown call is suppressed in a certain case, instead just logs it.
    """
    # GIVEN
    configuration.no_shutdown = True
    with patch.object(sys, "platform", expected_platform):
        # WHEN
        entrypoint_mod._system_shutdown(config=configuration)

    # THEN
    logger_info_mock.assert_called_with("Shutting down the instance")
    logger_debug_mock.assert_called_with(
        f"Skipping system shutdown. The following command would have been run: '{expected_command}'"
    )
    system_mock.assert_not_called()


# TODO: Add register failure test cases


def test_jobs_run_as_user_override(
    configuration: MagicMock,
) -> None:
    """Assert that the Worker is created with the jobs_run_as_overrides kwarg matching the Configuration"""
    # GIVEN
    configuration.jobs_run_as_overrides = MagicMock()
    with patch.object(entrypoint_mod, "Worker") as worker_mock:
        # WHEN
        entrypoint()

        # THEN
        assert worker_mock.call_count == 1
        assert (
            worker_mock.call_args_list[0].kwargs["jobs_run_as_user_override"]
            == configuration.jobs_run_as_overrides
        )


def test_passes_worker_logs_dir(
    configuration: MagicMock,
    tmp_path: Path,
) -> None:
    """Assert that the Worker is passed the worker_logs_dir from the configuration"""
    # GIVEN
    configuration.worker_logs_dir = tmp_path
    with patch.object(entrypoint_mod, "Worker") as worker_mock:
        # WHEN
        entrypoint()

    # THEN
    worker_mock.assert_called_once_with(
        farm_id=ANY,
        fleet_id=ANY,
        worker_id=ANY,
        deadline_client=ANY,
        s3_client=ANY,
        logs_client=ANY,
        boto_session=ANY,
        jobs_run_as_user_override=ANY,
        cleanup_session_user_processes=ANY,
        worker_persistence_dir=ANY,
        worker_logs_dir=tmp_path,
        host_metrics_logging=ANY,
        host_metrics_logging_interval_seconds=ANY,
    )


@patch.object(entrypoint_mod, "_logger")
def test_worker_stop_exception(
    logger_mock: MagicMock,
) -> None:
    """
    Tests that when the Agent gets an exception when trying to call the update_worker() method
    of the deadline boto3 client, that it logs the exception and continues with the shutdown sequence
    """

    # GIVEN
    exc = Exception("a message")
    logger_error_mock: MagicMock = logger_mock.error

    # WHEN
    with patch.object(entrypoint_mod, "update_worker", MagicMock(side_effect=exc)):
        entrypoint()

    # THEN
    logger_error_mock.assert_called_once_with("Failed to stop Worker: %s", exc)


class TestCloudWatchLogStreaming:
    @pytest.fixture(
        params=(
            pytest.param("cloudwatch_log_stream_sequence_token", id="with-sequence-token"),
            pytest.param(None, id="no-sequence-token"),
        )
    )
    def cloudwatch_log_stream_sequence_token(
        self,
        request: pytest.FixtureRequest,
    ) -> Optional[str]:
        return request.param

    def test_cloudwatch_log_streaming(
        self,
        mock_stream_cloudwatch_logs: MagicMock,
        logs_client: MagicMock,
        worker_log_config: WorkerLogConfig,
        configuration: Configuration,
    ) -> None:
        """
        Tests that the entrypoint function uses the stream_cloudwatch_logs context manager function
        and supplies the cloudwatch fields returned by the bootstrap_worker() function
        """
        # GIVEN

        context_mgr: MagicMock = mock_stream_cloudwatch_logs.return_value
        context_mgr_enter: MagicMock = context_mgr.__enter__
        context_mgr_exit: MagicMock = context_mgr.__exit__

        # WHEN
        entrypoint_mod.entrypoint()

        # THEN
        mock_stream_cloudwatch_logs.assert_called_once_with(
            logs_client=logs_client,
            log_group_name=worker_log_config.cloudwatch_log_group,
            log_stream_name=worker_log_config.cloudwatch_log_stream,
            logger=ROOT_LOGGER,
        )
        context_mgr_enter.assert_called_once_with()
        context_mgr_exit.assert_called_once()

    @patch.object(entrypoint_mod.subprocess, "check_output")
    def test_get_gpu_count(
        self,
        check_output_mock: MagicMock,
    ) -> None:
        """
        Tests that the _get_gpu_count function returns the correct number of GPUs
        """
        # GIVEN
        check_output_mock.return_value = b"2"

        # WHEN
        result = entrypoint_mod._get_gpu_count()

        # THEN
        check_output_mock.assert_called_once_with(
            ["nvidia-smi", "--query-gpu=count", "--format=csv,noheader"]
        )
        assert result == 2

    @pytest.mark.parametrize(
        ("exception", "expected_result"),
        (
            pytest.param(FileNotFoundError("nvidia-smi not found"), 0, id="FileNotFoundError"),
            pytest.param(subprocess.CalledProcessError(1, "command"), 0, id="CalledProcessError"),
        ),
    )
    @patch.object(entrypoint_mod.subprocess, "check_output")
    def test_get_gpu_count_nvidia_smi_error(
        self, check_output_mock: MagicMock, exception, expected_result
    ) -> None:
        """
        Tests that the _get_gpu_count function returns 0 when nvidia-smi is not found or fails
        """
        # GIVEN
        check_output_mock.side_effect = exception

        # WHEN
        result = entrypoint_mod._get_gpu_count()

        # THEN
        check_output_mock.assert_called_once_with(
            ["nvidia-smi", "--query-gpu=count", "--format=csv,noheader"]
        )

        assert result == expected_result

    @patch.object(entrypoint_mod.subprocess, "check_output")
    def test_get_gpu_memory(
        self,
        check_output_mock: MagicMock,
    ) -> None:
        """
        Tests that the _get_gpu_memory function returns total memory
        """
        # GIVEN
        check_output_mock.return_value = b"6800 MiB"

        # WHEN
        result = entrypoint_mod._get_gpu_memory()

        # THEN
        check_output_mock.assert_called_once_with(
            ["nvidia-smi", "--query-gpu=memory.total", "--format=csv,noheader"]
        )
        assert result == 6800

    @pytest.mark.parametrize(
        ("exception", "expected_result"),
        (
            pytest.param(FileNotFoundError("nvidia-smi not found"), 0, id="FileNotFoundError"),
            pytest.param(subprocess.CalledProcessError(1, "command"), 0, id="CalledProcessError"),
        ),
    )
    @patch.object(entrypoint_mod.subprocess, "check_output")
    def test_get_gpu_memory_nvidia_smi_error(
        self, check_output_mock: MagicMock, exception, expected_result
    ) -> None:
        """
        Tests that the _get_gpu_memory function returns 0 when nvidia-smi is not found or fails
        """
        # GIVEN
        check_output_mock.side_effect = exception

        # WHEN
        result = entrypoint_mod._get_gpu_memory()

        # THEN
        check_output_mock.assert_called_once_with(
            ["nvidia-smi", "--query-gpu=memory.total", "--format=csv,noheader"]
        )

        assert result == expected_result
