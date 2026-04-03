# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for the _HostConfigTimer class"""

from __future__ import annotations

import logging
import time
from unittest.mock import MagicMock, patch

import pytest

from deadline_worker_agent.startup.host_configuration_script import _HostConfigTimer
from deadline_worker_agent.log_messages import (
    WorkerHostConfigurationLogEvent,
    WorkerHostConfigurationStatus,
)


@pytest.fixture
def logger() -> MagicMock:
    return MagicMock(spec=logging.Logger)


@pytest.fixture
def farm_id() -> str:
    return "farm-00000000000000000000000000000000"


@pytest.fixture
def fleet_id() -> str:
    return "fleet-00000000000000000000000000000000"


@pytest.fixture
def worker_id() -> str:
    return "worker-00000000000000000000000000000000"


class TestHostConfigTimer:
    """Tests for _HostConfigTimer"""

    def test_stop_before_first_interval_emits_no_logs(
        self,
        logger: MagicMock,
        farm_id: str,
        fleet_id: str,
        worker_id: str,
    ) -> None:
        """When the script finishes before the first 30s interval, no progress log is emitted."""
        timer = _HostConfigTimer(
            timeout_seconds=300,
            logger=logger,
            farm_id=farm_id,
            fleet_id=fleet_id,
            worker_id=worker_id,
        )
        timer.start()
        # Stop immediately
        timer.stop()

        logger.info.assert_not_called()
        logger.warning.assert_not_called()

    def test_emits_progress_log_after_interval(
        self,
        logger: MagicMock,
        farm_id: str,
        fleet_id: str,
        worker_id: str,
    ) -> None:
        """Timer emits a progress log after the first interval elapses."""
        # Use a short timeout so the test runs quickly
        timer = _HostConfigTimer(
            timeout_seconds=300,
            logger=logger,
            farm_id=farm_id,
            fleet_id=fleet_id,
            worker_id=worker_id,
        )

        # Patch the interval constants to make the test fast
        with (
            patch.object(_HostConfigTimer, "_NORMAL_INTERVAL_S", 0.1),
            patch.object(_HostConfigTimer, "_ACCELERATED_INTERVAL_S", 0.05),
            patch.object(_HostConfigTimer, "_ACCELERATE_THRESHOLD_S", 60),
        ):
            timer.start()
            # Wait enough for at least one interval to fire
            time.sleep(0.3)
            timer.stop()

        # Should have at least one info log
        assert logger.info.call_count >= 1
        log_event = logger.info.call_args_list[0][0][0]
        assert isinstance(log_event, WorkerHostConfigurationLogEvent)
        assert "Host Config Time" in log_event.msg
        assert "Elapsed:" in log_event.msg
        assert "Remaining:" in log_event.msg
        assert log_event.status == WorkerHostConfigurationStatus.RUNNING

    def test_emits_timeout_warning_when_timeout_reached(
        self,
        logger: MagicMock,
        farm_id: str,
        fleet_id: str,
        worker_id: str,
    ) -> None:
        """Timer emits a WARNING log when the timeout is reached."""
        # Very short timeout so it expires quickly
        timer = _HostConfigTimer(
            timeout_seconds=0,
            logger=logger,
            farm_id=farm_id,
            fleet_id=fleet_id,
            worker_id=worker_id,
        )

        with (
            patch.object(_HostConfigTimer, "_NORMAL_INTERVAL_S", 0.05),
            patch.object(_HostConfigTimer, "_ACCELERATED_INTERVAL_S", 0.05),
        ):
            timer.start()
            time.sleep(0.2)
            timer.stop()

        # Should have a warning about timeout reached
        assert logger.warning.call_count >= 1
        warning_event = logger.warning.call_args_list[0][0][0]
        assert isinstance(warning_event, WorkerHostConfigurationLogEvent)
        assert "timeout expired" in warning_event.msg.lower()
        assert "Worker may be terminated by the service" in warning_event.msg
        assert warning_event.status == WorkerHostConfigurationStatus.FAILED

    def test_accelerates_interval_when_remaining_low(
        self,
        logger: MagicMock,
        farm_id: str,
        fleet_id: str,
        worker_id: str,
    ) -> None:
        """Timer switches to accelerated interval when remaining time <= threshold."""
        # Timeout of 1s with threshold at 60s means we're always in accelerated mode
        timer = _HostConfigTimer(
            timeout_seconds=1,
            logger=logger,
            farm_id=farm_id,
            fleet_id=fleet_id,
            worker_id=worker_id,
        )

        with (
            patch.object(_HostConfigTimer, "_NORMAL_INTERVAL_S", 10),
            patch.object(_HostConfigTimer, "_ACCELERATED_INTERVAL_S", 0.1),
            patch.object(_HostConfigTimer, "_ACCELERATE_THRESHOLD_S", 60),
        ):
            timer.start()
            # With normal interval of 10s, if we weren't accelerating we'd get 0 logs in 0.5s
            # With accelerated interval of 0.1s, we should get at least one
            time.sleep(0.5)
            timer.stop()

        # Should have at least one log due to accelerated interval
        assert logger.info.call_count >= 1

    def test_thread_is_daemon(
        self,
        logger: MagicMock,
        farm_id: str,
        fleet_id: str,
        worker_id: str,
    ) -> None:
        """Timer thread is a daemon thread so it won't prevent process exit."""
        timer = _HostConfigTimer(
            timeout_seconds=300,
            logger=logger,
            farm_id=farm_id,
            fleet_id=fleet_id,
            worker_id=worker_id,
        )
        timer.start()
        assert timer._thread is not None
        assert timer._thread.daemon is True
        timer.stop()

    def test_thread_name(
        self,
        logger: MagicMock,
        farm_id: str,
        fleet_id: str,
        worker_id: str,
    ) -> None:
        """Timer thread has a descriptive name for debugging."""
        timer = _HostConfigTimer(
            timeout_seconds=300,
            logger=logger,
            farm_id=farm_id,
            fleet_id=fleet_id,
            worker_id=worker_id,
        )
        timer.start()
        assert timer._thread is not None
        assert timer._thread.name == "host-config-timer"
        timer.stop()

    def test_stop_is_idempotent(
        self,
        logger: MagicMock,
        farm_id: str,
        fleet_id: str,
        worker_id: str,
    ) -> None:
        """Calling stop() multiple times does not raise."""
        timer = _HostConfigTimer(
            timeout_seconds=300,
            logger=logger,
            farm_id=farm_id,
            fleet_id=fleet_id,
            worker_id=worker_id,
        )
        timer.start()
        timer.stop()
        # Second stop should not raise
        timer.stop()

    def test_stop_without_start(
        self,
        logger: MagicMock,
        farm_id: str,
        fleet_id: str,
        worker_id: str,
    ) -> None:
        """Calling stop() without start() does not raise."""
        timer = _HostConfigTimer(
            timeout_seconds=300,
            logger=logger,
            farm_id=farm_id,
            fleet_id=fleet_id,
            worker_id=worker_id,
        )
        # Should not raise
        timer.stop()

    def test_progress_log_contains_correct_ids(
        self,
        logger: MagicMock,
        farm_id: str,
        fleet_id: str,
        worker_id: str,
    ) -> None:
        """Progress logs contain the correct farm, fleet, and worker IDs."""
        timer = _HostConfigTimer(
            timeout_seconds=300,
            logger=logger,
            farm_id=farm_id,
            fleet_id=fleet_id,
            worker_id=worker_id,
        )

        with (
            patch.object(_HostConfigTimer, "_NORMAL_INTERVAL_S", 0.05),
            patch.object(_HostConfigTimer, "_ACCELERATED_INTERVAL_S", 0.05),
        ):
            timer.start()
            time.sleep(0.15)
            timer.stop()

        assert logger.info.call_count >= 1
        log_event = logger.info.call_args_list[0][0][0]
        assert isinstance(log_event, WorkerHostConfigurationLogEvent)
        assert log_event.farm_id == farm_id
        assert log_event.fleet_id == fleet_id
        assert log_event.worker_id == worker_id

    def test_thread_exits_after_timeout_warning(
        self,
        logger: MagicMock,
        farm_id: str,
        fleet_id: str,
        worker_id: str,
    ) -> None:
        """After emitting the timeout warning, the timer thread exits on its own."""
        timer = _HostConfigTimer(
            timeout_seconds=0,
            logger=logger,
            farm_id=farm_id,
            fleet_id=fleet_id,
            worker_id=worker_id,
        )

        with (
            patch.object(_HostConfigTimer, "_NORMAL_INTERVAL_S", 0.05),
            patch.object(_HostConfigTimer, "_ACCELERATED_INTERVAL_S", 0.05),
        ):
            timer.start()
            time.sleep(0.3)

        # Thread should have exited on its own after the timeout warning
        assert timer._thread is not None
        assert not timer._thread.is_alive()
        # Clean up
        timer.stop()

    def test_no_further_logs_after_timeout_reached(
        self,
        logger: MagicMock,
        farm_id: str,
        fleet_id: str,
        worker_id: str,
    ) -> None:
        """After the timeout is reached, the timer stops logging even if the worker is still
        running (i.e. stop() has not been called). The timer thread self-exits after the
        timeout warning and does not continue emitting progress logs."""
        timer = _HostConfigTimer(
            timeout_seconds=0,
            logger=logger,
            farm_id=farm_id,
            fleet_id=fleet_id,
            worker_id=worker_id,
        )

        with (
            patch.object(_HostConfigTimer, "_NORMAL_INTERVAL_S", 0.05),
            patch.object(_HostConfigTimer, "_ACCELERATED_INTERVAL_S", 0.05),
        ):
            timer.start()
            # Wait for the timeout warning to fire and the thread to exit
            time.sleep(0.3)

        # Record the log counts after the thread has self-exited
        info_count_after_timeout = logger.info.call_count
        warning_count_after_timeout = logger.warning.call_count

        # Wait more time — simulating the worker still running after timeout
        time.sleep(0.3)

        # No additional logs should have been emitted
        assert logger.info.call_count == info_count_after_timeout
        assert logger.warning.call_count == warning_count_after_timeout

        # Exactly one warning (the timeout-reached message)
        assert warning_count_after_timeout == 1

        # Clean up
        timer.stop()
