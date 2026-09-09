# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from collections import namedtuple

import logging
import subprocess
import threading
from typing import Any, Dict, Generator
from unittest.mock import MagicMock, call, patch

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
        stale_event = host_metrics_logger._stop_event

        with patch.object(metrics_mod, "Thread") as mock_thread_cls:
            # WHEN
            result = host_metrics_logger.__enter__()

        # THEN
        assert result is host_metrics_logger
        # A fresh event, and the thread watches that exact event rather than re-reading
        # the attribute, so an abandoned predecessor can never be revived by a reset
        assert host_metrics_logger._stop_event is not stale_event
        assert not host_metrics_logger._stop_event.is_set()
        mock_thread_cls.assert_called_once_with(
            target=host_metrics_logger._run,
            args=(host_metrics_logger._stop_event,),
            name="HostMetricsLogger",
            daemon=True,
        )
        mock_thread_cls.return_value.start.assert_called_once()
        assert host_metrics_logger._thread is mock_thread_cls.return_value

    def test_enter_does_not_revive_an_abandoned_thread(
        self, host_metrics_logger: HostMetricsLogger
    ):
        """Re-entering must not reset the stop event an abandoned thread is still watching"""
        # GIVEN a thread was started and then abandoned because it would not join
        with patch.object(metrics_mod, "Thread"):
            host_metrics_logger.__enter__()
        abandoned_event = host_metrics_logger._stop_event
        host_metrics_logger._thread = MagicMock(**{"is_alive.return_value": True})
        host_metrics_logger.__exit__(None, None, None)
        assert abandoned_event.is_set()

        # WHEN the logger is entered again
        with patch.object(metrics_mod, "Thread"):
            host_metrics_logger.__enter__()

        # THEN the abandoned thread's event stays set, so that thread still exits
        assert abandoned_event.is_set()
        assert not host_metrics_logger._stop_event.is_set()

    def test_enter_twice_stops_the_first_thread(self, host_metrics_logger: HostMetricsLogger):
        """Re-entering must not orphan a thread that nothing is left able to signal or join"""
        # GIVEN
        first = MagicMock(**{"is_alive.return_value": False})
        second = MagicMock(**{"is_alive.return_value": False})

        with patch.object(metrics_mod, "Thread") as mock_thread_cls:
            mock_thread_cls.side_effect = [first, second]

            # WHEN entered twice with no intervening exit
            host_metrics_logger.__enter__()
            first_event = host_metrics_logger._stop_event
            host_metrics_logger.__enter__()

        # THEN the first thread was signalled and joined rather than left running
        assert first_event.is_set()
        first.join.assert_called_once_with(timeout=HostMetricsLogger.JOIN_TIMEOUT_S)

        # AND the second thread is the one now being tracked, on its own live event
        assert host_metrics_logger._thread is second
        assert host_metrics_logger._stop_event is not first_event
        assert not host_metrics_logger._stop_event.is_set()

    def test_enter_tolerates_thread_start_failure(
        self,
        host_metrics_logger: HostMetricsLogger,
        caplog: pytest.LogCaptureFixture,
    ):
        """Host metrics are best-effort; being unable to start the thread must not fail the Worker"""
        # GIVEN
        caplog.set_level(0)

        with patch.object(metrics_mod, "Thread") as mock_thread_cls:
            mock_thread_cls.return_value.start.side_effect = RuntimeError("can't start new thread")

            # WHEN
            result = host_metrics_logger.__enter__()

        # THEN
        assert result is host_metrics_logger
        # No thread was recorded, so __exit__ has nothing to join
        assert host_metrics_logger._thread is None
        assert any(
            "Failed to start the host metrics thread" in message for message in caplog.messages
        )

        # AND __exit__ is a no-op that does not raise
        host_metrics_logger.__exit__(None, None, None)

    @pytest.mark.parametrize("thread_exists", [True, False])
    def test_exit(
        self,
        thread_exists: bool,
        host_metrics_logger: HostMetricsLogger,
    ):
        # GIVEN
        thread = MagicMock()
        thread.is_alive.return_value = False
        host_metrics_logger._stop_event = MagicMock()
        if thread_exists:
            host_metrics_logger._thread = thread

        # WHEN
        host_metrics_logger.__exit__(None, None, None)

        # THEN
        host_metrics_logger._stop_event.set.assert_called_once()
        if thread_exists:
            thread.join.assert_called_once_with(timeout=HostMetricsLogger.JOIN_TIMEOUT_S)
            assert host_metrics_logger._thread is None
        else:
            thread.join.assert_not_called()

    def test_exit_abandons_thread_that_does_not_join(
        self,
        host_metrics_logger: HostMetricsLogger,
        caplog: pytest.LogCaptureFixture,
    ):
        """A metrics thread hung in a collection must not block shutdown"""
        # GIVEN
        caplog.set_level(0)
        thread = MagicMock()
        thread.is_alive.return_value = True
        host_metrics_logger._thread = thread

        # WHEN
        host_metrics_logger.__exit__(None, None, None)

        # THEN
        thread.join.assert_called_once_with(timeout=HostMetricsLogger.JOIN_TIMEOUT_S)
        assert host_metrics_logger._thread is None
        assert any("did not exit within" in message for message in caplog.messages)

    def test_run_primes_baselines_before_logging(
        self,
        host_metrics_logger: HostMetricsLogger,
    ):
        # GIVEN
        stop_event = MagicMock()
        stop_event.wait.side_effect = [False, False, True]

        # Record the ordering of prime vs. collect on a shared parent mock
        calls = MagicMock()

        # WHEN
        with (
            patch.object(host_metrics_logger, "_prime_metrics", calls.prime),
            patch.object(host_metrics_logger, "log_metrics", calls.log_metrics),
        ):
            host_metrics_logger._run(stop_event)

        # THEN
        assert calls.mock_calls == [call.prime(), call.log_metrics(), call.log_metrics()]
        # Every wait is a full interval, including the first. Shortening the first one to get
        # an earlier startup sample would reintroduce the bug this fixes, because a
        # cpu_percent() call taken moments after priming reports psutil's near-zero again.
        assert stop_event.wait.call_args_list == [call(host_metrics_logger.interval_s)] * 3

    def test_prime_metrics_sets_rate_baselines(
        self,
        host_metrics_logger: HostMetricsLogger,
        mock_psutil_module: MagicMock,
    ):
        """The rate baselines must be primed so the first logged sample reports real rates"""
        # WHEN
        host_metrics_logger._prime_metrics()

        # THEN
        mock_psutil_module.cpu_percent.assert_called_once_with()
        assert host_metrics_logger._prev_network is mock_psutil_module.net_io_counters.return_value
        assert (
            host_metrics_logger._prev_disk_counters
            is mock_psutil_module.disk_io_counters.return_value
        )

    def test_prime_metrics_tolerates_psutil_failure(
        self,
        host_metrics_logger: HostMetricsLogger,
        mock_psutil_module: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ):
        """A failure while priming must not kill the metrics thread"""
        # GIVEN
        caplog.set_level(0)
        mock_psutil_module.cpu_percent.side_effect = RuntimeError("psutil exploded")

        # WHEN
        host_metrics_logger._prime_metrics()

        # THEN
        assert any("Failed to prime host metrics baselines" in msg for msg in caplog.messages)

    def test_run_continues_after_log_metrics_raises(
        self,
        host_metrics_logger: HostMetricsLogger,
        caplog: pytest.LogCaptureFixture,
    ):
        """One failed collection must not stop host metrics for the Worker's lifetime"""
        # GIVEN
        caplog.set_level(0)
        stop_event = MagicMock()
        stop_event.wait.side_effect = [False, False, True]

        # WHEN
        with (
            patch.object(host_metrics_logger, "_prime_metrics"),
            patch.object(
                host_metrics_logger,
                "log_metrics",
                side_effect=[RuntimeError("logging exploded"), None],
            ) as mock_log_metrics,
        ):
            host_metrics_logger._run(stop_event)

        # THEN
        # The loop survived the first failure and collected again
        assert mock_log_metrics.call_count == 2
        assert any("Failed to log host metrics" in msg for msg in caplog.messages)

    def test_thread_lifecycle_end_to_end(
        self,
        logger: MagicMock,
        mock_psutil_module: MagicMock,
    ):
        """
        Exercises the real thread rather than a mocked one.

        Every other test here patches out Thread or the stop event, so none of them would
        catch the thread target being mis-wired, the loop never reaching log_metrics, or
        __exit__ failing to stop the thread. This one starts the genuine thread, waits for
        a real metrics event, and asserts the thread is stopped on the way out.
        """
        # GIVEN
        # This test is about thread lifecycle, so it deliberately asserts nothing about the
        # individual metric formulas -- TestLogMetrics owns those.
        du = namedtuple("du", ["total", "used", "free", "percent"])
        mock_psutil_module.disk_usage.return_value = du(100, 25, 75, 25)
        # The first call is the priming call, whose value is discarded rather than logged.
        # Distinguishing it is what makes the assertion below evidence that the logged sample
        # came from a later call on the metrics thread, rather than from priming.
        mock_psutil_module.cpu_percent.side_effect = [0.0, *([12.5] * 1000)]
        mock_psutil_module.disk_io_counters.return_value = dioc(0, 0, 0, 0, 0, 0)
        mock_psutil_module.net_io_counters.return_value = MagicMock(bytes_sent=0, bytes_recv=0)

        logged = threading.Event()
        logger.info.side_effect = lambda *args, **kwargs: logged.set()

        # A short interval so the test does not wait a full production cycle
        host_metrics_logger = HostMetricsLogger(logger=logger, interval_s=0.01)

        # WHEN
        with patch.object(host_metrics_logger, "_get_gpu_metrics", return_value={}):
            with host_metrics_logger as entered:
                assert entered is host_metrics_logger
                thread = host_metrics_logger._thread
                assert thread is not None
                assert thread.is_alive()
                assert logged.wait(timeout=10), "the metrics thread never logged an event"

        # THEN the thread stopped on its own, and the real values made it through
        assert not thread.is_alive()
        assert host_metrics_logger._thread is None
        assert host_metrics_logger._stop_event.is_set()

        log_event = logger.info.call_args_list[0].args[0]
        assert isinstance(log_event, MetricsLogEvent)
        # 12.5 rather than the priming call's 0.0, so this sample was measured against the
        # baseline primed on this thread rather than being the thread's own first call
        assert log_event.metrics["cpu-usage-percent"] == "12.5"

    @pytest.fixture
    def mock_subprocess(self) -> Generator[MagicMock, None, None]:
        with patch.object(metrics_mod, "subprocess") as mock:
            mock.CalledProcessError = subprocess.CalledProcessError
            mock.TimeoutExpired = subprocess.TimeoutExpired
            yield mock

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
            timeout=HostMetricsLogger.GPU_QUERY_TIMEOUT_S,
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

    def test_get_gpu_metrics_timeout_does_not_disable_gpu_metrics(
        self,
        host_metrics_logger: HostMetricsLogger,
        mock_subprocess: MagicMock,
    ):
        """A transient nvidia-smi hang must not disable GPU metrics for the process lifetime"""
        # GIVEN
        metrics_output = "100, 8192, 8192, 100"
        mock_subprocess.check_output.side_effect = [
            subprocess.TimeoutExpired("nvidia-smi", HostMetricsLogger.GPU_QUERY_TIMEOUT_S),
            metrics_output,
        ]

        # WHEN
        gpu_metrics = host_metrics_logger._get_gpu_metrics()

        # THEN
        assert gpu_metrics == {}
        assert not host_metrics_logger._host_has_no_gpu

        # WHEN (again) the next collection succeeds
        gpu_metrics = host_metrics_logger._get_gpu_metrics()

        # THEN
        assert gpu_metrics["gpu-utilization-percent"] == "100.0"

    class TestLogMetrics:
        @pytest.fixture(autouse=True)
        def no_real_nvidia_smi(self, mock_subprocess: MagicMock) -> MagicMock:
            """These tests call the real log_metrics, which must not fork a real nvidia-smi"""
            return mock_subprocess

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
            # psutil's own "percent" is measured against user-available space, so it does
            # not equal used/total. It is deliberately different here so that the
            # total-disk-used-percent assertion pins down which formula is used.
            return du(100, 25, 75, 40)

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
            # 25/100 bytes used, expressed on a 0-100 scale. Note this is derived from the
            # total/used byte values above, not from psutil's user-space disk.percent (40).
            assert log_line.metrics.get("total-disk-used-percent", "") == "25.0"
            assert log_line.metrics.get("user-disk-available-bytes", "") == "75"

        def test_logs_disk_rate(
            self,
            host_metrics_logger: HostMetricsLogger,
            logger: MagicMock,
            disk_io_counters: dioc,
        ):
            # GIVEN
            # First call to set up previous disk counters
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
            with patch.object(host_metrics_logger, "_get_gpu_metrics", return_value=gpu_metrics):
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
