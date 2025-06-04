# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from collections import namedtuple

import logging
import subprocess
from typing import Any, Dict, Generator
from unittest.mock import MagicMock, patch

import pytest
import re

from deadline_worker_agent.metrics import HostMetricsLogger
import deadline_worker_agent.metrics as metrics_mod
from deadline_worker_agent.log_messages import MetricsLogEvent

dioc = namedtuple(
    "dioc",
    [
        "read_count",
        "write_count",
        "read_bytes",
        "write_bytes",
        "read_time",
        "write_time",
    ],
)


@pytest.fixture(autouse=True)
def mock_psutil_module() -> Generator[MagicMock, None, None]:
    """Mock the entire psutil module to prevent future errors due to KeyError from psutil.virtual_memory()"""
    with patch.object(metrics_mod, "psutil") as mock:
        yield mock


class TestHostMetricsLogger:
    BYTES_PATTERN = r"[0-9]+(?:\.[0-9]+)?"
    PERCENT_PATTERN = r"[0-9]{1,3}(?:\.[0-9]+)?"

    @pytest.fixture
    def logger(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def host_metrics_logger(self, logger: MagicMock) -> HostMetricsLogger:
        return HostMetricsLogger(logger=logger, interval_s=1)

    def test_enter(self, host_metrics_logger: HostMetricsLogger):
        # GIVEN
        with patch.object(host_metrics_logger, "log_metrics") as mock_log_metrics:
            # WHEN
            with host_metrics_logger:
                # THEN
                mock_log_metrics.assert_called_once()

    @pytest.mark.parametrize("timer_exists", [True, False])
    def test_exit(
        self,
        timer_exists: bool,
        host_metrics_logger: HostMetricsLogger,
    ):
        # GIVEN
        timer = MagicMock()

        # WHEN
        with patch.object(host_metrics_logger, "__enter__"):
            with host_metrics_logger:
                if timer_exists:
                    host_metrics_logger._timer = timer

        # THEN
        if timer_exists:
            timer.cancel.assert_called_once()
            assert host_metrics_logger._timer is None
        else:
            timer.cancel.assert_not_called()

    def test_set_timer(self, host_metrics_logger: HostMetricsLogger):
        # GIVEN
        with patch.object(metrics_mod, "Timer") as mock_timer_cls:
            # WHEN
            host_metrics_logger._set_timer()

        # THEN
        mock_timer_cls.assert_called_once_with(
            host_metrics_logger.interval_s, host_metrics_logger.log_metrics
        )
        mock_timer_cls.return_value.start.assert_called_once()

    @pytest.fixture
    def mock_subprocess(self) -> Generator[MagicMock, None, None]:
        with patch.object(metrics_mod, "subprocess") as mock:
            mock.CalledProcessError = subprocess.CalledProcessError
            yield mock

    def test_log_metrics_sets_timer(
        self,
        host_metrics_logger: HostMetricsLogger,
    ):
        # GIVEN
        with (
            patch.object(metrics_mod, "psutil"),
            patch.object(host_metrics_logger, "_set_timer") as mock_set_timer,
        ):
            # WHEN
            host_metrics_logger.log_metrics()

        # THEN
        mock_set_timer.assert_called_once()

    # GPU test scenarios
    @pytest.mark.parametrize(
        "metrics_output,expected_metrics",
        [
            pytest.param(
                "75, 2000, 8192, 25\n50, 1000, 8192, 12",
                {
                    "gpu-utilization-percent": "62.5",
                    "gpu-memory-used-mib": "3000",
                    "gpu-memory-total-mib": "16384",
                    "gpu-memory-used-percent": "18.3",
                    "gpu-memory-utilization-percent": "18.5",
                },
                id="multiple_gpus",
            ),
            pytest.param("", {}, id="no_gpus"),
            pytest.param("Failed to get metrics", {}, id="malformed_output"),
            pytest.param("N/A, 2000, 8192, 25", {}, id="missing_values"),
            pytest.param("2000, 8192, 25", {}, id="omitted_values"),
            pytest.param(
                "100, 8192, 8192, 100",
                {
                    "gpu-utilization-percent": "100.0",
                    "gpu-memory-used-mib": "8192",
                    "gpu-memory-total-mib": "8192",
                    "gpu-memory-used-percent": "100.0",
                    "gpu-memory-utilization-percent": "100.0",
                },
                id="full_utlization",
            ),
        ],
    )
    def test_get_gpu_metrics(
        self,
        metrics_output,
        expected_metrics,
        host_metrics_logger,
        mock_subprocess,
    ):
        """Parametrized test for GPU metrics collection with different scenarios"""
        # GIVEN
        mock_subprocess.check_output.side_effect = [metrics_output]

        # WHEN
        gpu_metrics = host_metrics_logger._get_gpu_metrics()

        # THEN
        # Check if the expected metrics are present
        for key, value in expected_metrics.items():
            assert gpu_metrics.get(key) == value
        if expected_metrics:
            assert not host_metrics_logger._host_has_no_gpu

        # Special case checks
        if metrics_output is None:
            assert gpu_metrics == {}

        # Verify subprocess calls
        query_str = "utilization.gpu,memory.used,memory.total,utilization.memory"
        mock_subprocess.check_output.assert_called_with(
            ["nvidia-smi", f"--query-gpu={query_str}", "--format=csv,noheader,nounits"],
            stderr=mock_subprocess.PIPE,
            universal_newlines=True,
        )

    @pytest.mark.parametrize(
        "exception,expected_result",
        [
            pytest.param(FileNotFoundError("nvidia-smi not found"), {}, id="file_not_found"),
            pytest.param(
                subprocess.CalledProcessError(1, "nvidia-smi"), {}, id="called_process_error"
            ),
            pytest.param(Exception("Unexpected error"), {}, id="unexpected_error"),
        ],
    )
    def test_get_gpu_metrics_exceptions(
        self, exception, expected_result, host_metrics_logger, mock_subprocess
    ):
        """Test GPU metrics collection with various exceptions"""
        # GIVEN
        mock_subprocess.check_output.side_effect = exception

        # WHEN
        gpu_metrics = host_metrics_logger._get_gpu_metrics()

        # THEN
        assert gpu_metrics == expected_result
        assert host_metrics_logger._host_has_no_gpu

        # Test that subsequent calls do not attempt to query GPU metrics
        # GIVEN
        mock_subprocess.check_output.reset_mock()

        # WHEN (again)
        host_metrics_logger._get_gpu_metrics()

        # THEN
        mock_subprocess.check_output.assert_not_called()

    class TestLogMetrics:
        @pytest.fixture(autouse=True)
        def mock_timer(self) -> Generator[MagicMock, None, None]:
            # We don't want to actually create/start a timer
            with patch.object(metrics_mod, "Timer") as mock:
                yield mock

        @pytest.fixture
        def virtual_memory(self) -> tuple:
            vm = namedtuple("vm", ["total", "available", "percent", "used", "free"])
            return vm(40, 10, 25, 30, 10)

        @pytest.fixture
        def swap_memory(self) -> tuple:
            sm = namedtuple("sm", ["total", "used", "free", "percent", "sin", "sout"])
            return sm(20, 10, 10, 50, 0, 0)

        @pytest.fixture
        def disk_usage(self) -> tuple:
            du = namedtuple("du", ["total", "used", "free", "percent"])
            return du(100, 25, 75, 25)

        @pytest.fixture
        def cpu_percent(self) -> int:
            return 10

        @pytest.fixture
        def net_io_counters(self) -> tuple:
            nioc = namedtuple(
                "nioc",
                [
                    "bytes_sent",
                    "bytes_recv",
                    "packets_sent",
                    "packets_recv",
                    "errin",
                    "errout",
                    "dropin",
                    "dropout",
                ],
            )
            return nioc(123, 321, 100, 300, 2, 3, 1, 0)

        @pytest.fixture
        def disk_io_counters(self) -> dioc:
            return dioc(123, 321, 123123, 321321, 100, 200)

        @pytest.fixture(autouse=True)
        def mock_psutil(
            self,
            virtual_memory: tuple,
            swap_memory: tuple,
            disk_usage: tuple,
            cpu_percent: int,
            net_io_counters: tuple,
            disk_io_counters: tuple,
        ) -> Generator[MagicMock, None, None]:
            with patch.object(metrics_mod, "psutil") as mock:
                mock.virtual_memory.return_value = virtual_memory
                mock.swap_memory.return_value = swap_memory
                mock.disk_usage.return_value = disk_usage
                mock.cpu_percent.return_value = cpu_percent
                mock.net_io_counters.return_value = net_io_counters
                mock.disk_io_counters.return_value = disk_io_counters

                yield mock

        @pytest.fixture
        def log_metrics(
            self,
            host_metrics_logger: HostMetricsLogger,
            mock_psutil: MagicMock,
        ) -> None:
            with patch.object(host_metrics_logger, "_set_timer"):
                host_metrics_logger.log_metrics()

        @pytest.fixture
        def log_line(self, logger: MagicMock, log_metrics: None) -> str:
            return get_first_and_only_call_arg(logger.info)

        def test_logs_cpu(self, log_line: str):
            # THEN
            assert isinstance(log_line, MetricsLogEvent)
            assert log_line.metrics.get("cpu-usage-percent", "") == "10"

        def test_logs_memory(self, log_line: str):
            # THEN
            assert isinstance(log_line, MetricsLogEvent)
            assert log_line.metrics.get("memory-total-bytes", "") == "40"
            assert log_line.metrics.get("memory-used-bytes", "") == "30"
            assert log_line.metrics.get("memory-used-percent", "") == "25"

        def test_logs_swap(self, log_line: str):
            # THEN
            assert isinstance(log_line, MetricsLogEvent)
            assert log_line.metrics.get("swap-used-bytes", "") == "10"

        def test_logs_disk(self, log_line: str):
            # THEN
            assert isinstance(log_line, MetricsLogEvent)
            assert log_line.metrics.get("total-disk-bytes", "") == "100"
            assert log_line.metrics.get("total-disk-used-bytes", "") == "25"
            assert log_line.metrics.get("total-disk-used-percent", "") == "0.2"
            assert log_line.metrics.get("user-disk-available-bytes", "") == "75"

        def test_logs_disk_rate(
            self,
            host_metrics_logger: HostMetricsLogger,
            logger: MagicMock,
            disk_io_counters: dioc,
        ):
            # GIVEN
            # First call to set up previous disk counters
            with patch.object(host_metrics_logger, "_set_timer"):
                host_metrics_logger.log_metrics()

            # Reset the logger mock to clear the first call
            logger.reset_mock()

            # Increase read_bytes by 1000 and write_bytes by 2000
            new_counters = dioc(
                read_count=disk_io_counters.read_count,
                write_count=disk_io_counters.write_count,
                read_bytes=disk_io_counters.read_bytes + 1000,
                write_bytes=disk_io_counters.write_bytes + 2000,
                read_time=disk_io_counters.read_time,
                write_time=disk_io_counters.write_time,
            )

            # WHEN
            with patch.object(metrics_mod, "psutil") as mock_psutil:
                mock_psutil.disk_io_counters.return_value = new_counters

                with patch.object(host_metrics_logger, "_set_timer"):
                    host_metrics_logger.log_metrics()

            # THEN
            log_line = get_first_and_only_call_arg(logger.info)
            assert isinstance(log_line, MetricsLogEvent)
            # Should report the difference divided by interval (which is 1 second)
            assert log_line.metrics.get("disk-read-bytes-per-second", "") == "1000"
            assert log_line.metrics.get("disk-write-bytes-per-second", "") == "2000"

        def test_logs_network_rate(self, log_line: str):
            # THEN
            assert isinstance(log_line, MetricsLogEvent)
            assert log_line.metrics.get("network-sent-bytes-per-second", "") == "0"
            assert log_line.metrics.get("network-recv-bytes-per-second", "") == "0"

        def test_disk_rate_not_available(
            self,
            mock_psutil: MagicMock,
            host_metrics_logger: HostMetricsLogger,
            logger: MagicMock,
        ):
            # GIVEN
            mock_psutil.disk_io_counters.reset_mock()
            mock_psutil.disk_io_counters.return_value = None

            # WHEN
            host_metrics_logger.log_metrics()

            # THEN
            log_line = get_first_and_only_call_arg(logger.info)
            assert isinstance(log_line, MetricsLogEvent)
            assert log_line.metrics.get("disk-read-bytes-per-second", "") == "NOT_AVAILABLE"
            assert log_line.metrics.get("disk-write-bytes-per-second", "") == "NOT_AVAILABLE"

        def test_disk_rate_not_supported(
            self,
            mock_psutil: MagicMock,
            host_metrics_logger: HostMetricsLogger,
            logger: MagicMock,
        ):
            # GIVEN
            mock_psutil.disk_io_counters.return_value = tuple()

            # WHEN
            host_metrics_logger.log_metrics()

            # THEN
            log_line = get_first_and_only_call_arg(logger.info)
            assert isinstance(log_line, MetricsLogEvent)
            assert log_line.metrics.get("disk-read-bytes-per-second", "") == "NOT_SUPPORTED"
            assert log_line.metrics.get("disk-write-bytes-per-second", "") == "NOT_SUPPORTED"

        def test_network_rate_not_available(
            self,
            mock_psutil: MagicMock,
            host_metrics_logger: HostMetricsLogger,
            logger: MagicMock,
        ):
            # GIVEN
            mock_psutil.net_io_counters.return_value = None

            # WHEN
            host_metrics_logger.log_metrics()

            # THEN
            log_line = get_first_and_only_call_arg(logger.info)
            assert isinstance(log_line, MetricsLogEvent)
            assert log_line.metrics.get("network-sent-bytes-per-second", "") == "NOT_AVAILABLE"
            assert log_line.metrics.get("network-recv-bytes-per-second", "") == "NOT_AVAILABLE"

        @pytest.mark.parametrize(
            "gpu_metrics,gpu_available",
            [
                (
                    {
                        "gpu-utilization-percent": "62.5",
                        "gpu-memory-used-mib": "3000",
                        "gpu-memory-total-mib": "16384",
                        "gpu-memory-used-percent": "18.3",
                        "gpu-memory-utilization-percent": "18.5",
                    },
                    True,
                ),
                (
                    {},
                    False,
                ),
            ],
            ids=["gpu_metrics_available", "no_gpu_device"],
        )
        def test_logs_gpu_metrics(
            self,
            host_metrics_logger: HostMetricsLogger,
            logger: MagicMock,
            gpu_metrics: Dict[str, str],
            gpu_available: bool,
        ):
            # GIVEN
            # gpu_metrics is provided by the parametrize decorator

            # WHEN
            with (
                patch.object(host_metrics_logger, "_get_gpu_metrics", return_value=gpu_metrics),
                patch.object(host_metrics_logger, "_set_timer"),
            ):
                host_metrics_logger.log_metrics()

            # THEN
            log_line = get_first_and_only_call_arg(logger.info)
            assert isinstance(log_line, MetricsLogEvent)

            if gpu_available:
                # Verify GPU metrics are included in the logged metrics
                for key, value in gpu_metrics.items():
                    assert log_line.metrics.get(key, "") == value

                # Verify each metric specifically
                assert log_line.metrics.get("gpu-utilization-percent") == "62.5"
                assert log_line.metrics.get("gpu-memory-used-mib") == "3000"
                assert log_line.metrics.get("gpu-memory-total-mib") == "16384"
                assert log_line.metrics.get("gpu-memory-used-percent") == "18.3"
                assert log_line.metrics.get("gpu-memory-utilization-percent") == "18.5"
            else:
                # Verify no GPU metrics are included when no GPU device is found
                for key in [
                    "gpu-utilization-percent",
                    "gpu-memory-used-mib",
                    "gpu-memory-total-mib",
                    "gpu-memory-used-percent",
                    "gpu-memory-utilization-percent",
                ]:
                    assert key not in log_line.metrics

        def test_log_metrics_correct_encoding(
            self,
            caplog: pytest.LogCaptureFixture,
        ) -> None:
            # GIVEN
            DECIMAL_NUMBER_PATTERN = r"\d+(?:\.\d+)?"
            # fmt: off
            EXPECTED_LOG_MESSAGE_PATTERN = " ".join(
                [
                    "cpu-usage-percent", DECIMAL_NUMBER_PATTERN,
                    "memory-total-bytes", DECIMAL_NUMBER_PATTERN,
                    "memory-used-bytes", DECIMAL_NUMBER_PATTERN,
                    "memory-used-percent", DECIMAL_NUMBER_PATTERN,
                    "swap-used-bytes", DECIMAL_NUMBER_PATTERN,
                    "total-disk-bytes", DECIMAL_NUMBER_PATTERN,
                    "total-disk-used-bytes", DECIMAL_NUMBER_PATTERN,
                    "total-disk-used-percent", DECIMAL_NUMBER_PATTERN,
                    "user-disk-available-bytes", DECIMAL_NUMBER_PATTERN,
                    "network-sent-bytes-per-second", rf"(?:{DECIMAL_NUMBER_PATTERN}|NOT_AVAILABLE)",
                    "network-recv-bytes-per-second", rf"(?:{DECIMAL_NUMBER_PATTERN}|NOT_AVAILABLE)",
                    "disk-read-bytes-per-second", rf"(?:{DECIMAL_NUMBER_PATTERN}|NOT_AVAILABLE|NOT_SUPPORTED)",
                    "disk-write-bytes-per-second", rf"(?:{DECIMAL_NUMBER_PATTERN}|NOT_AVAILABLE|NOT_SUPPORTED)",
                ]
            )
            # fmt: on
            logger = logging.getLogger(__name__)
            caplog.set_level(0, logger.name)
            host_metrics_logger = HostMetricsLogger(logger=logger, interval_s=1)

            # WHEN
            with (
                # We don't want to actually create/start a timer
                patch.object(metrics_mod, "Timer"),
            ):
                host_metrics_logger.log_metrics()

            # THEN
            assert len(caplog.messages) == 1
            assert isinstance(caplog.records[0].msg, MetricsLogEvent)
            assert re.match(EXPECTED_LOG_MESSAGE_PATTERN, caplog.records[0].msg.getMessage())


def get_first_and_only_call_arg(mock: MagicMock) -> Any:
    assert len(mock.mock_calls) == 1
    mock_call = mock.mock_calls[0]
    assert len(mock_call.args) == 1
    return mock_call.args[0]
