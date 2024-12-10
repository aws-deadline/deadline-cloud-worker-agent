# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from threading import Event
from typing import Generator
from unittest.mock import ANY, MagicMock, call, patch
from pathlib import Path

import pytest

from deadline_worker_agent import Worker
from deadline_worker_agent.config import JobsRunAsUserOverride
from deadline_worker_agent.errors import ServiceShutdown
import deadline_worker_agent.worker as worker_mod


@pytest.fixture
def asset_sync() -> MagicMock:
    return MagicMock()


@pytest.fixture
def boto_session() -> MagicMock:
    return MagicMock()


@pytest.fixture
def worker_logs_dir(tmp_path: Path) -> Path:
    return tmp_path


@pytest.fixture
def mock_scheduler_cls() -> Generator[MagicMock, None, None]:
    with patch.object(worker_mod, "WorkerScheduler") as mock_scheduler_cls:
        yield mock_scheduler_cls


@pytest.fixture
def scheduler(mock_scheduler_cls: MagicMock) -> MagicMock:
    return mock_scheduler_cls.return_value


@pytest.fixture
def worker(
    boto_session: MagicMock,
    client: MagicMock,
    farm_id: str,
    fleet_id: str,
    job_run_as_user_overrides: JobsRunAsUserOverride,
    logs_client: MagicMock,
    s3_client: MagicMock,
    worker_id: str,
    worker_logs_dir: Path,
    # This is unused, but declaring it as a dependency fixture ensures we mock the scheduler class
    # before we instantiate the Worker instance within this fixture body
    mock_scheduler_cls: MagicMock,
) -> Generator[Worker, None, None]:
    with patch.object(worker_mod, "HostMetricsLogger"):
        yield Worker(
            farm_id=farm_id,
            deadline_client=client,
            boto_session=boto_session,
            fleet_id=fleet_id,
            job_run_as_user_override=job_run_as_user_overrides,
            logs_client=logs_client,
            s3_client=s3_client,
            worker_id=worker_id,
            cleanup_session_user_processes=True,
            worker_persistence_dir=Path("/var/lib/deadline"),
            worker_logs_dir=worker_logs_dir,
            host_metrics_logging=False,
        )


@pytest.fixture
def mock_logger() -> Generator[MagicMock, None, None]:
    """Mocks the logger of the deadline_worker_agent.worker module"""
    with patch.object(worker_mod, "logger", spec=True) as mock_logger:
        yield mock_logger


@pytest.fixture(autouse=True)
def requests_put() -> Generator[MagicMock, None, None]:
    """Mock requests.put()"""
    with patch.object(worker_mod.requests, "put") as mock:
        yield mock


@pytest.fixture(autouse=True)
def requests_get() -> Generator[MagicMock, None, None]:
    """Mock requests.get()"""
    with patch.object(worker_mod.requests, "get") as mock:
        yield mock


@pytest.fixture(autouse=True)
def mock_signal() -> Generator[MagicMock, None, None]:
    """Mock signal import in the module"""
    with patch.object(worker_mod.signal, "signal") as mock_signal:
        yield mock_signal


def test_monitor_rate() -> None:
    """Asserts that Worker._MONITOR_RATE (the rate between polling for spot interruption and ASG
    life-cycle events) is once per second.

    This test only asserts the value. The TestMonitorEc2Shutdown class contains test cases to that
    assert its use.
    """
    assert Worker._EC2_SHUTDOWN_MONITOR_RATE.total_seconds() == 1


@pytest.fixture(autouse=True)
def mock_thread_pool_executor_cls() -> Generator[MagicMock, None, None]:
    """Mocks the ThreadPoolExecutor class in the worker module"""
    with patch.object(worker_mod, "ThreadPoolExecutor") as mock_thread_pool_executor_cls:
        yield mock_thread_pool_executor_cls


@pytest.fixture(autouse=True)
def thread_pool_executor(mock_thread_pool_executor_cls: MagicMock) -> MagicMock:
    """Returns the ThreadPoolExecutor instance mock"""
    return mock_thread_pool_executor_cls.return_value


class TestInit:
    def test_stop_event_created(
        self,
        worker: Worker,
    ) -> None:
        # THEN
        assert isinstance(worker._stop, Event)
        assert not worker._stop.is_set()

    def test_passes_worker_logs_dir(
        self,
        # Not used, but declared in order to have the Worker.__init__() called
        worker: Worker,
        mock_scheduler_cls: MagicMock,
        worker_logs_dir: Path,
    ) -> None:
        """Asserts that when a Worker instance is created, the worker_logs_dir keyword argument is
        passed when creating the WorkerScheduler instance"""
        # THEN
        mock_scheduler_cls.assert_called_once_with(
            deadline=ANY,
            farm_id=ANY,
            fleet_id=ANY,
            worker_id=ANY,
            job_run_as_user_override=ANY,
            boto_session=ANY,
            cleanup_session_user_processes=ANY,
            worker_persistence_dir=ANY,
            worker_logs_dir=worker_logs_dir,
            retain_session_dir=ANY,
            stop=ANY,
        )


class TestRun:
    def test_service_shutdown_raised_not_logged(
        self,
        worker: Worker,
        thread_pool_executor: MagicMock,
        mock_logger: MagicMock,
    ) -> None:
        """Tests that if the Scheduler raises a ServiceShutdown exception, that the exception is
        re-raised and not logged"""

        # GIVEN
        scheduler_future = MagicMock()
        service_shutdown = ServiceShutdown()
        scheduler_future.result.side_effect = service_shutdown
        thread_pool_executor.submit.side_effect = [scheduler_future, MagicMock()]
        logger_exception: MagicMock = mock_logger.exception
        with (
            # wait is called to wait on the first completed fixture
            # we mock it to return the scheduler future
            patch.object(worker_mod, "wait", return_value=([scheduler_future], [])),
            patch.object(worker_mod, "AwsCredentialsRefresher"),
            # THEN
            pytest.raises(ServiceShutdown) as raise_ctx,
        ):
            # WHEN
            worker.run()

        # THEN
        assert raise_ctx.value is service_shutdown
        logger_exception.assert_not_called()


class TestMonitorEc2Shutdown:
    @pytest.fixture
    def is_asg_terminated(self) -> bool:
        return False

    @pytest.fixture
    def mock_is_asg_terminated(
        self,
        worker: Worker,
        is_asg_terminated: bool,
    ) -> Generator[MagicMock, None, None]:
        """Mocks the Worker._is_asg_terminated() method"""
        with patch.object(
            worker, "_is_asg_terminated", return_value=is_asg_terminated
        ) as mock_is_asg_terminated:
            yield mock_is_asg_terminated

    @pytest.mark.parametrize(
        argnames="loop_iterations",
        argvalues=(0, 1, 10),
        ids=(
            "no-loops",
            "one-loop",
            "ten-loops",
        ),
    )
    def test_loops_until_stopped(
        self,
        worker: Worker,
        loop_iterations: int,
    ) -> None:
        """Asserts that the Worker._monitor_ec2_shutdown() method will loop until the Worker._stop
        event is set. We assert that the method calls
        Worker._shutdown.wait(timeout=Worker._MONITOR_RATE) on each loop iteration. If the stop
        event is set and the loop is exited, the method should return None to indicate there was no
        EC2-initiated shutdown.
        """
        # GIVEN

        # mocked return values from shutdown event's wait() method.
        # we return false for the number of loop iterations, followed by true to confirm that the
        # loop is conditional on the event being set.
        wait_side_effect = ([False] * loop_iterations) + [True]
        expected_shutdown_wait_call_count = loop_iterations + 1
        expected_shutdown_wait_calls = [
            call(timeout=Worker._EC2_SHUTDOWN_MONITOR_RATE.total_seconds())
        ] * expected_shutdown_wait_call_count
        with (
            patch.object(worker._stop, "wait", side_effect=wait_side_effect) as mock_shutdown_wait,
            patch.object(worker, "_get_ec2_metadata_imdsv2_token") as mock_get_token,
            patch.object(
                worker, "_get_spot_instance_shutdown_action_timeout", return_value=None
            ) as mock_get_spot,
            patch.object(
                worker, "_is_asg_terminated", return_value=False
            ) as mock_is_asg_terminated,
        ):
            # WHEN
            return_value = worker._monitor_ec2_shutdown()

        # THEN
        mock_shutdown_wait.assert_has_calls(expected_shutdown_wait_calls, any_order=True)
        mock_shutdown_wait.call_count == expected_shutdown_wait_call_count
        # A timedelta should only be returned if an EC2 shutdown notice is detected, not if the stop
        # event was sent by another component (e.g. WorkerScheduler).

        mock_is_asg_terminated.assert_has_calls(
            [call(imdsv2_token=mock_get_token.return_value)] * loop_iterations
        )
        mock_get_spot.assert_has_calls(
            [call(imdsv2_token=mock_get_token.return_value)] * loop_iterations
        )

        assert return_value is None

    def test_no_imds_temporarily_continues_till_stop_called(
        self,
        worker: Worker,
        mock_logger: MagicMock,
    ) -> None:
        """Asserts that when Worker._get_ec2_metadata_imdsv2_token() returns None which indicates
        that IMDS is not available, that Worker._monitor_ec2_shutdown() continues looping until
        _stop called"""
        # GIVEN
        logger_info: MagicMock = mock_logger.info
        wait_side_effect = ([False] * 2) + [True]

        with (
            patch.object(
                worker, "_get_ec2_metadata_imdsv2_token"
            ) as mock_get_ec2_metadata_imdsv2_token,
            patch.object(worker._stop, "wait", side_effect=wait_side_effect),
        ):
            mock_get_ec2_metadata_imdsv2_token.return_value = None

            # WHEN
            result = worker._monitor_ec2_shutdown()

        # THEN
        logger_info.assert_has_calls(
            [
                call(
                    "IMDS unavailable - unable to monitor for spot interruption or ASG life-cycle changes"
                ),
                call(
                    "IMDS unavailable - unable to monitor for spot interruption or ASG life-cycle changes"
                ),
            ]
        )
        assert result is None

    def test_asg_termination(
        self,
        worker: Worker,
        mock_is_asg_terminated: MagicMock,
        mock_logger: MagicMock,
    ) -> None:
        """Asserts that when Worker._is_asg_terminated() returns True that
        Worker._monitor_ec2_shutdown() returns 2 minutes of shutdown grace."""
        # GIVEN
        mock_is_asg_terminated.return_value = True
        logger_info: MagicMock = mock_logger.info

        with patch.object(worker, "_get_ec2_metadata_imdsv2_token"):
            # WHEN
            result = worker._monitor_ec2_shutdown()

        # THEN
        assert result == worker_mod.WorkerShutdown(
            grace_time=timedelta(minutes=2),
            fail_message="The Worker received an auto-scaling life-cycle change event",
        )
        logger_info.assert_called_once_with(
            "Auto-scaling life-cycle change event detected. Termination in %s", timedelta(minutes=2)
        )

    @pytest.mark.parametrize(
        argnames="spot_shutdown_grace",
        argvalues=(
            timedelta(minutes=1),
            timedelta(seconds=15),
        ),
        ids=(
            "spot-shutdown-1-min",
            "spot-shutdown-15-sec",
        ),
    )
    def test_spot_interruption(
        self,
        worker: Worker,
        spot_shutdown_grace: timedelta,
        mock_logger: MagicMock,
    ) -> None:
        """Asserts that if Worker._get_spot_instance_shutdown_action_timeout() returns time
        remaining before a spot interruption termination, that the time remaining is also returned
        from Worker._monitor_ec2_shutdown()."""
        # GIVEN
        logger_info: MagicMock = mock_logger.info
        with (
            patch.object(
                worker,
                "_get_spot_instance_shutdown_action_timeout",
                return_value=spot_shutdown_grace,
            ),
            patch.object(worker, "_get_ec2_metadata_imdsv2_token"),
        ):
            # WHEN
            worker_shutdown = worker._monitor_ec2_shutdown()

        # THEN
        assert isinstance(worker_shutdown, worker_mod.WorkerShutdown)
        assert worker_shutdown == worker_mod.WorkerShutdown(
            grace_time=spot_shutdown_grace,
            fail_message="The Worker received an EC2 spot interruption",
        )
        logger_info.assert_called_once_with(
            "Spot interruption detected. Termination in %s", spot_shutdown_grace
        )


class TestEC2MetadataQueries:
    def test_get_imdsv2_token(self, worker: Worker, requests_put: MagicMock) -> None:
        # GIVEN
        fake_token = "TOKEN_FAKE_VALUE"
        response_mock = MagicMock()
        requests_put.return_value = response_mock
        response_mock.status_code = 200
        response_mock.text = fake_token

        # WHEN
        result = worker._get_ec2_metadata_imdsv2_token()

        # THEN
        assert result == fake_token
        requests_put.assert_called_once_with(
            "http://169.254.169.254/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "10"},
        )

    def test_get_imdsv2_token_cannot_connect(self, worker: Worker, requests_put: MagicMock) -> None:
        # GIVEN
        requests_put.side_effect = worker_mod.requests.ConnectionError("Error")

        # WHEN
        result = worker._get_ec2_metadata_imdsv2_token()

        # THEN
        assert result is None

    def test_get_imdsv2_token_imds_inactive(self, worker: Worker, requests_put: MagicMock) -> None:
        # GIVEN
        response_mock = MagicMock()
        response_mock.status_code = 402
        requests_put.return_value = response_mock

        # WHEN
        result = worker._get_ec2_metadata_imdsv2_token()

        # THEN
        assert result is None

    @pytest.mark.parametrize(
        ("action_type", "is_interrupt"),
        [
            pytest.param("hibernate", False, id="Hibernate"),
            pytest.param("terminate", True, id="Terminate"),
            pytest.param("stop", True, id="Stop"),
        ],
    )
    def test_spot_shutdown(
        self,
        worker: Worker,
        requests_get: MagicMock,
        mock_logger: MagicMock,
        action_type: str,
        is_interrupt: bool,
    ) -> None:
        # GIVEN
        fake_token = "TOKEN_FAKE_VALUE"
        expected_result = timedelta(seconds=30)
        response_mock = MagicMock()
        timeout = datetime.now(timezone.utc) + expected_result
        # See: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-instance-termination-notices.html # noqa: E501
        response_mock.status_code = 200
        response_mock.text = f'{{ "action": "{action_type}", "time": "{timeout.isoformat()}Z" }}'
        requests_get.return_value = response_mock

        # WHEN
        result = worker._get_spot_instance_shutdown_action_timeout(imdsv2_token=fake_token)

        # THEN
        requests_get.assert_called_once_with(
            "http://169.254.169.254/latest/meta-data/spot/instance-action",
            headers={"X-aws-ec2-metadata-token": fake_token},
        )
        if not is_interrupt:
            assert result is None
        else:
            assert isinstance(result, timedelta)
            abs_delta = abs((expected_result - result).total_seconds())
            # The result should be within 1s of the timedelta; it may differ due to the time it
            # takes to run the test.
            assert abs_delta <= 1
            mock_logger.info.assert_called_with(
                f"Spot {action_type} happening at {timeout.isoformat()}Z"
            )

    def test_spot_shutdown_in_past(
        self,
        worker: Worker,
        requests_get: MagicMock,
        mock_logger: MagicMock,
    ) -> None:
        # GIVEN
        fake_token = "TOKEN_FAKE_VALUE"
        response_mock = MagicMock()
        timeout_delta = timedelta(seconds=-10)
        timeout = datetime.utcnow() + timeout_delta
        # See: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-instance-termination-notices.html # noqa: E501
        response_mock.status_code = 200
        response_mock.text = f'{{ "action": "terminate", "time": "{timeout.isoformat()}Z" }}'
        requests_get.return_value = response_mock

        # WHEN
        result = worker._get_spot_instance_shutdown_action_timeout(imdsv2_token=fake_token)

        # THEN
        assert result is None
        mock_logger.error.assert_called_with("Spot terminate time is in the past!")

    def test_spot_shutdown_missing_time(
        self,
        worker: Worker,
        requests_get: MagicMock,
        mock_logger: MagicMock,
    ) -> None:
        # GIVEN
        fake_token = "TOKEN_FAKE_VALUE"
        response_mock = MagicMock()
        # See: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-instance-termination-notices.html # noqa: E501
        response_mock.status_code = 200
        response_mock.text = '{ "action": "terminate" }'
        requests_get.return_value = response_mock

        # WHEN
        result = worker._get_spot_instance_shutdown_action_timeout(imdsv2_token=fake_token)

        # THEN
        assert result is None
        mock_logger.error.assert_called_with(
            "Missing 'time' property from ec2 metadata instance-action response"
        )

    def test_spot_shutdown_cannot_connect(self, worker: Worker, requests_get: MagicMock) -> None:
        # GIVEN
        requests_get.side_effect = worker_mod.requests.ConnectionError("Error")

        # WHEN
        result = worker._get_spot_instance_shutdown_action_timeout(imdsv2_token="token")

        # THEN
        assert result is None

    def test_spot_shutdown_imds_inactive(self, worker: Worker, requests_get: MagicMock) -> None:
        # GIVEN
        response_mock = MagicMock()
        response_mock.status_code = 402
        requests_get.return_value = response_mock

        # WHEN
        result = worker._get_spot_instance_shutdown_action_timeout(imdsv2_token="token")

        # THEN
        assert result is None

    @pytest.mark.parametrize(
        ("lifecycle_state", "expected_result"),
        [
            pytest.param("Terminated", True, id="Terminated"),
            pytest.param("InService", False, id="InService"),
            pytest.param("Detached", False, id="Detached"),
            pytest.param("Warmed:Hibernated", False, id="Warmed:Hibernated"),
            pytest.param("Warmed:Running", False, id="Warmed:Running"),
            pytest.param("Warmed:Stopped", False, id="Warmed:Stopped"),
            pytest.param("Warmed:Terminated", False, id="Warmed:Terminated"),
        ],
    )
    def test_asg_terminate(
        self, worker: Worker, requests_get: MagicMock, lifecycle_state: str, expected_result: bool
    ) -> None:
        # See: https://docs.aws.amazon.com/autoscaling/ec2/userguide/retrieving-target-lifecycle-state-through-imds.html # noqa: E501
        # GIVEN
        fake_token = "TOKEN_FAKE_VALUE"
        response_mock = MagicMock()
        response_mock.status_code = 200
        response_mock.text = lifecycle_state
        requests_get.return_value = response_mock

        # WHEN
        result = worker._is_asg_terminated(imdsv2_token=fake_token)

        # THEN
        assert result == expected_result
        requests_get.assert_called_once_with(
            "http://169.254.169.254/latest/meta-data/autoscaling/target-lifecycle-state",
            headers={"X-aws-ec2-metadata-token": fake_token},
        )

    def test_asg_terminate_cannot_connect(self, worker: Worker, requests_get: MagicMock) -> None:
        # GIVEN
        requests_get.side_effect = worker_mod.requests.ConnectionError("Error")

        # WHEN
        result = worker._is_asg_terminated(imdsv2_token="token")

        # THEN
        assert not result

    def test_asg_terminate_imds_inactive(self, worker: Worker, requests_get: MagicMock) -> None:
        # GIVEN
        response_mock = MagicMock()
        response_mock.status_code = 402
        requests_get.return_value = response_mock

        # WHEN
        result = worker._is_asg_terminated(imdsv2_token="token")

        # THEN
        assert not result
