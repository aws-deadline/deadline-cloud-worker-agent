# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from collections import namedtuple

import logging
from typing import Any, Generator
from unittest.mock import MagicMock, patch

import pytest
import re

from deadline_worker_agent.metrics import HostMetricsLogger
import deadline_worker_agent.metrics as metrics_mod


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
        with patch.object(host_metrics_logger, "_set_timer") as mock_set_timer:
            # WHEN
            with host_metrics_logger:
                # THEN
                mock_set_timer.assert_called_once()

    @pytest.mark.parametrize("timer_exists", [True, False])
    def test_exit(
        self,
        timer_exists: bool,
        host_metrics_logger: HostMetricsLogger,
    ):
        # GIVEN
        timer = MagicMock()

        # WHEN
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
        def disk_io_counters(self) -> tuple:
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
            return dioc(123, 321, 123123, 321321, 100, 200)

        @pytest.fixture
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
            assert re.search(
                rf"cpu-usage-percent {TestHostMetricsLogger.PERCENT_PATTERN}", log_line
            )

        def test_logs_memory(self, log_line: str):
            # THEN
            assert re.search(rf"memory-total-bytes {TestHostMetricsLogger.BYTES_PATTERN}", log_line)
            assert re.search(rf"memory-used-bytes {TestHostMetricsLogger.BYTES_PATTERN}", log_line)
            assert re.search(
                rf"memory-used-percent {TestHostMetricsLogger.PERCENT_PATTERN}", log_line
            )

        def test_logs_swap(self, log_line: str):
            # THEN
            assert re.search(rf"swap-used-bytes {TestHostMetricsLogger.BYTES_PATTERN}", log_line)

        def test_logs_disk(self, log_line: str):
            # THEN
            assert re.search(rf"total-disk-bytes {TestHostMetricsLogger.BYTES_PATTERN}", log_line)
            assert re.search(
                rf"total-disk-used-bytes {TestHostMetricsLogger.BYTES_PATTERN}", log_line
            )
            assert re.search(
                rf"total-disk-used-percent {TestHostMetricsLogger.PERCENT_PATTERN}", log_line
            )
            assert re.search(
                rf"user-disk-available-bytes {TestHostMetricsLogger.BYTES_PATTERN}", log_line
            )

        def test_logs_disk_rate(self, log_line: str):
            # THEN
            assert re.search(
                rf"disk-read-bytes-per-second {TestHostMetricsLogger.BYTES_PATTERN}", log_line
            )
            assert re.search(
                rf"disk-write-bytes-per-second {TestHostMetricsLogger.BYTES_PATTERN}", log_line
            )

        def test_logs_network_rate(self, log_line: str):
            # THEN
            assert re.search(
                rf"network-sent-bytes-per-second {TestHostMetricsLogger.BYTES_PATTERN}",
                log_line,
            )
            assert re.search(
                rf"network-recv-bytes-per-second {TestHostMetricsLogger.BYTES_PATTERN}",
                log_line,
            )

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
            assert re.search(r"disk-read-bytes-per-second NOT_AVAILABLE", log_line)
            assert re.search(r"disk-write-bytes-per-second NOT_AVAILABLE", log_line)

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
            assert re.search(r"disk-read-bytes-per-second NOT_SUPPORTED", log_line)
            assert re.search(r"disk-write-bytes-per-second NOT_SUPPORTED", log_line)

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
            assert re.search(r"network-sent-bytes-per-second NOT_AVAILABLE", log_line)
            assert re.search(r"network-recv-bytes-per-second NOT_AVAILABLE", log_line)

    def test_log_metrics_correct_encoding(self, caplog: pytest.LogCaptureFixture) -> None:
        # GIVEN
        DECIMAL_NUMBER_PATTERN = r"\d+(?:\.\d+)?"
        EXPECTED_LOG_MESSAGE_PATTERN = " ".join(
            # fmt: off
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
            # fmt: on
        )
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
        assert re.match(EXPECTED_LOG_MESSAGE_PATTERN, caplog.messages[0])


def get_first_and_only_call_arg(mock: MagicMock) -> Any:
    assert len(mock.mock_calls) == 1
    mock_call = mock.mock_calls[0]
    assert len(mock_call.args) == 1
    return mock_call.args[0]
