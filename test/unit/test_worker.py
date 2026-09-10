# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from threading import Event
from typing import Generator
from unittest.mock import MagicMock, call, patch
from pathlib import Path

import pytest
import requests

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
    session_root_dir: Path,
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
            session_root_dir=session_root_dir,
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


class TestImdsProbeCost:
    """Pins what the three probe constants cost together, so prose arithmetic cannot drift.

    Not a guarantee: urllib3's read timeout is per-socket-read inactivity with no `total` set, so
    a peer dribbling bytes can keep one attempt alive indefinitely. A true ceiling would need
    urllib3.Timeout(total=...), i.e. depending directly on a transitive dependency. This is the
    expected cost against a peer that answers or goes silent.
    """

    def test_expected_cost_against_a_silent_peer(self) -> None:
        connect, read = Worker._IMDS_REQUEST_TIMEOUT
        attempts = Worker._IMDS_PROBE_ATTEMPTS

        # Both halves, because requests applies each to its own phase -- one attempt can spend
        # connect + read, not one of the two.
        expected = attempts * (connect + read) + (attempts - 1) * Worker._IMDS_PROBE_BACKOFF_S

        assert expected == 7.0

    def test_a_black_holed_address_costs_less(self) -> None:
        """Nothing answers ARP, so each attempt fails in connect -- the figure above overstates
        what an ordinary workstation or runner pays."""
        connect, _read = Worker._IMDS_REQUEST_TIMEOUT
        attempts = Worker._IMDS_PROBE_ATTEMPTS

        connect_only = attempts * connect + (attempts - 1) * Worker._IMDS_PROBE_BACKOFF_S

        assert connect_only == 4.0


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

    @pytest.mark.parametrize(
        argnames="worker_logs_dir",
        argvalues=(
            pytest.param(Path("/foo"), id="1"),
            pytest.param(Path("/bar"), id="2"),
        ),
    )
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
        mock_scheduler_cls.assert_called_once()
        assert mock_scheduler_cls.call_args.kwargs["worker_logs_dir"] == worker_logs_dir

    @pytest.mark.parametrize(
        argnames="session_root_dir",
        argvalues=(
            pytest.param(Path("/foo"), id="1"),
            pytest.param(Path("/bar"), id="2"),
        ),
    )
    def test_passes_session_root_dir(
        self,
        # Not used, but declared in order to have the Worker.__init__() called
        worker: Worker,
        mock_scheduler_cls: MagicMock,
        session_root_dir: Path,
    ) -> None:
        """Asserts that when a Worker instance is created, the session_root_dir keyword argument is
        passed when creating the WorkerScheduler instance"""
        # THEN
        mock_scheduler_cls.assert_called_once()
        assert mock_scheduler_cls.call_args.kwargs["session_root_dir"] == session_root_dir


class TestRun:
    @pytest.fixture(autouse=True)
    def imds_answers(self, requests_put: MagicMock) -> MagicMock:
        """Let the startup probe succeed first try: otherwise it pays the real backoff twice,
        and a negative verdict leaves the monitor-future branch these tests set up unexercised."""
        requests_put.return_value.status_code = 200
        requests_put.return_value.text = "TOKEN"
        return requests_put

    def test_a_raising_ec2_probe_shuts_the_scheduler_down(
        self,
        worker: Worker,
        thread_pool_executor: MagicMock,
        scheduler: MagicMock,
        mock_logger: MagicMock,
    ) -> None:
        """The scheduler is already submitted when the probe runs, and the `with` joins it on the
        way out -- so without this cleanup that join never returns and SIGTERM cannot kill the
        worker. Holds for any exception type, not only ones the probe's handler catches."""
        # GIVEN
        boom = RuntimeError("something the probe's own handlers do not cover")
        with (
            patch.object(worker, "_is_ec2_host", side_effect=boom),
            patch.object(worker_mod, "AwsCredentialsRefresher"),
            # THEN
            pytest.raises(RuntimeError) as raise_ctx,
        ):
            # WHEN
            worker.run()

        # THEN
        assert raise_ctx.value is boom
        scheduler.shutdown.assert_called_once()
        assert worker._stop.is_set()

    def test_a_raising_monitor_future_does_not_hang_the_executor(
        self,
        worker: Worker,
        thread_pool_executor: MagicMock,
        scheduler: MagicMock,
        mock_logger: MagicMock,
    ) -> None:
        """result() re-raises from outside `except BaseException`, so without the try/finally the
        executor joins a scheduler never asked to stop. Reachable: the monitor parses IMDS
        responses, and JSONDecodeError is not a RequestException."""
        # GIVEN: the monitor future completed by raising
        boom = ValueError("Invalid isoformat string")
        monitor_future = MagicMock()
        monitor_future.result.side_effect = boom
        scheduler_future = MagicMock()
        thread_pool_executor.submit.side_effect = [scheduler_future, monitor_future]

        with (
            patch.object(worker, "_is_ec2_host", return_value=True),
            patch.object(worker_mod, "wait", return_value=([monitor_future], [])),
            patch.object(worker_mod, "AwsCredentialsRefresher"),
            # THEN
            pytest.raises(ValueError) as raise_ctx,
        ):
            # WHEN
            worker.run()

        # THEN: it propagated, and both stops happened. The scheduler assertion is the
        # load-bearing one: _stop alone does not stop the scheduler, because it builds its own
        # Event whenever entrypoint() is called without one.
        assert raise_ctx.value is boom
        scheduler.shutdown.assert_called_once()
        assert worker._stop.is_set()

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
        # At debug, not info: this is the first thing every failing poll logs, so at 1 Hz an
        # info line here buries the warning that names the condition. That warning is emitted
        # once, on the transition, by _log_imds_unanswered.
        mock_logger.debug.assert_has_calls(
            [
                call(
                    "IMDS unavailable - unable to monitor for spot interruption or ASG life-cycle changes"
                ),
                call(
                    "IMDS unavailable - unable to monitor for spot interruption or ASG life-cycle changes"
                ),
            ]
        )
        logger_info.assert_not_called()
        mock_logger.warning.assert_called_once()
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
            timeout=Worker._IMDS_REQUEST_TIMEOUT,
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
            timeout=Worker._IMDS_REQUEST_TIMEOUT,
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
            timeout=Worker._IMDS_REQUEST_TIMEOUT,
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


class TestIsEc2Host:
    """The startup determination that gates EC2 shutdown monitoring.

    Taken once, at startup, and a negative permanently skips spot-interruption and ASG
    life-cycle handling for the life of the process -- so a single slow or throttled
    probe must not decide it.
    """

    @pytest.fixture(autouse=True)
    def no_real_backoff(self, worker: Worker) -> Generator[MagicMock, None, None]:
        """Keep the retry backoff out of the suite's wall-clock time.

        Patched on the Event rather than on time.sleep so the tests still exercise the real
        call the implementation makes. `False` is the return the implementation reads as
        "not stopped, keep probing", so this stands in for a backoff that elapsed.
        """
        with patch.object(worker._stop, "wait", return_value=False) as m:
            yield m

    def test_first_answer_wins(self, worker: Worker, no_real_backoff: MagicMock) -> None:
        with patch.object(worker, "_get_ec2_metadata_imdsv2_token", return_value="TOKEN") as probe:
            assert worker._is_ec2_host() is True

        # No wasted probes on the host where this normally answers in about a millisecond.
        probe.assert_called_once()
        # And no delay paid on an EC2 fleet, which is the case that matters for startup.
        no_real_backoff.assert_not_called()

    def test_a_slow_probe_does_not_decide_it(self, worker: Worker) -> None:
        """Instance boot is when this runs and when IMDS is most likely to be slow."""
        with patch.object(
            worker, "_get_ec2_metadata_imdsv2_token", side_effect=[None, None, "TOKEN"]
        ) as probe:
            assert worker._is_ec2_host() is True

        assert probe.call_count == 3

    def test_backs_off_between_attempts(self, worker: Worker, no_real_backoff: MagicMock) -> None:
        """A throttled IMDS answers immediately, so back-to-back retries would be useless.

        They would meet the same empty token bucket microseconds apart, leaving the retry
        covering only the slow case.
        """
        with patch.object(worker, "_get_ec2_metadata_imdsv2_token", return_value=None):
            assert worker._is_ec2_host() is False

        # Between attempts, not after the last one.
        assert no_real_backoff.call_count == Worker._IMDS_PROBE_ATTEMPTS - 1
        for backoff_call in no_real_backoff.call_args_list:
            assert backoff_call.kwargs["timeout"] == Worker._IMDS_PROBE_BACKOFF_S

    def test_abandons_the_probe_when_stop_is_requested(self, worker: Worker) -> None:
        """Shutdown during the backoff must not hold the startup path for the full budget.

        The monitor this gates would only be asked to stop, so there is nothing to wait for.
        """
        with (
            # One patch, because the implementation reads the flag from wait's return value
            # rather than following it with a second is_set().
            patch.object(worker._stop, "wait", return_value=True),
            patch.object(worker, "_get_ec2_metadata_imdsv2_token", return_value=None) as probe,
        ):
            assert worker._is_ec2_host() is False

        probe.assert_called_once()

    def test_gives_up_after_the_attempt_budget(self, worker: Worker) -> None:
        with patch.object(worker, "_get_ec2_metadata_imdsv2_token", return_value=None) as probe:
            assert worker._is_ec2_host() is False

        assert probe.call_count == Worker._IMDS_PROBE_ATTEMPTS

    def test_says_so_when_monitoring_is_disabled(self, worker: Worker) -> None:
        """Nothing else on this path reports that interruption handling was turned off."""
        with (
            patch.object(worker, "_get_ec2_metadata_imdsv2_token", return_value=None),
            patch.object(worker_mod.logger, "info") as logger_info,
        ):
            worker._is_ec2_host()

        logger_info.assert_called_once()
        assert "not be" in logger_info.call_args.args[0]


class TestImdsTimeoutIsVisible:
    """A timed-out interruption query must not pass for "nothing is shutting down".

    None and False are the caller's "no interruption pending", indistinguishable from a real
    answer -- so a reachable-but-slow IMDS would report all-clear every second and a missed spot
    notice would leave nothing to diagnose.
    """

    def test_a_timed_out_spot_query_warns(
        self, worker: Worker, requests_get: MagicMock, mock_logger: MagicMock
    ) -> None:
        requests_get.side_effect = requests.ReadTimeout("timed out")

        assert worker._get_spot_instance_shutdown_action_timeout(imdsv2_token="TOKEN") is None

        mock_logger.warning.assert_called_once()
        assert "no usable answer" in mock_logger.warning.call_args.args[0]
        assert Worker._IMDS_CAUSE_SLOW_READ in mock_logger.warning.call_args.args

    def test_a_timed_out_asg_query_warns(
        self, worker: Worker, requests_get: MagicMock, mock_logger: MagicMock
    ) -> None:
        requests_get.side_effect = requests.ReadTimeout("timed out")

        assert worker._is_asg_terminated(imdsv2_token="TOKEN") is False

        mock_logger.warning.assert_called_once()

    def test_a_connection_error_does_not_warn(
        self, worker: Worker, requests_get: MagicMock, mock_logger: MagicMock
    ) -> None:
        """Control: the not-on-EC2 case is not a slow IMDS and has its own reporting.

        Warning there would fire on every poll of every non-EC2 worker.
        """
        requests_get.side_effect = requests.ConnectionError("no route")

        assert worker._get_spot_instance_shutdown_action_timeout(imdsv2_token="TOKEN") is None

        mock_logger.warning.assert_not_called()

    def test_it_warns_once_per_outage_not_once_per_poll(
        self, worker: Worker, requests_get: MagicMock, mock_logger: MagicMock
    ) -> None:
        """The monitor polls once a second, so an unconditional warning would bury itself.

        3600 identical lines an hour hides the thing the warning exists to surface.
        """
        requests_get.side_effect = requests.ReadTimeout("timed out")

        for _ in range(5):
            worker._get_spot_instance_shutdown_action_timeout(imdsv2_token="TOKEN")

        mock_logger.warning.assert_called_once()

    def test_a_404_counts_as_answered(
        self, worker: Worker, requests_get: MagicMock, mock_logger: MagicMock
    ) -> None:
        """404 is the normal reply on both endpoints, so it must not read as unanswered.

        /spot/instance-action returns it when no interruption is pending and
        /autoscaling/target-lifecycle-state when the host is not in an ASG -- the steady state of
        a healthy worker. Warning there would fire on every healthy fleet, and because the state
        suppresses repeats it would then stay quiet through a real outage.
        """
        requests_get.return_value = MagicMock(status_code=404)

        assert worker._get_spot_instance_shutdown_action_timeout(imdsv2_token="TOKEN") is None
        assert worker._is_asg_terminated(imdsv2_token="TOKEN") is False

        mock_logger.warning.assert_not_called()
        assert worker._imds_unanswered == set()

    @pytest.mark.parametrize("status_code", [429, 500, 503])
    def test_an_unusable_status_warns_like_a_timeout(
        self, status_code: int, worker: Worker, requests_get: MagicMock, mock_logger: MagicMock
    ) -> None:
        """Getting bytes back is not getting an answer.

        A 429 produces the same silent "no interruption pending" a timeout does, and throttling
        is per-request against a path polled once a second, so it is at least as likely.
        """
        requests_get.return_value = MagicMock(status_code=status_code)

        assert worker._get_spot_instance_shutdown_action_timeout(imdsv2_token="TOKEN") is None

        mock_logger.warning.assert_called_once()
        assert f"HTTP {status_code}" in str(mock_logger.warning.call_args)

    def test_an_error_status_is_not_a_recovery(
        self, worker: Worker, requests_get: MagicMock, mock_logger: MagicMock
    ) -> None:
        """A timeout followed by a 429 has not recovered, and must not say so."""
        requests_get.side_effect = [
            requests.ReadTimeout("timed out"),
            MagicMock(status_code=429),
        ]

        for _ in range(2):
            worker._get_spot_instance_shutdown_action_timeout(imdsv2_token="TOKEN")

        assert not any("is answering" in str(c) for c in mock_logger.info.call_args_list), (
            mock_logger.info.call_args_list
        )
        assert "spot/instance-action" in worker._imds_unanswered

    def test_a_failing_token_hop_warns_once_not_once_per_poll(
        self, worker: Worker, requests_get: MagicMock, mock_logger: MagicMock
    ) -> None:
        """The token hop is the first thing every failing poll hits.

        Left unsuppressed it emits a line a second and buries the warnings from the two queries
        below it -- and after _is_ec2_host gated this thread on IMDS answering, a token that now
        fails is the same condition, not "not on EC2".
        """
        with (
            patch.object(worker, "_get_ec2_metadata_imdsv2_token", return_value=None),
            patch.object(worker._stop, "wait", side_effect=[False, False, False, True]),
        ):
            worker._monitor_ec2_shutdown()

        mock_logger.warning.assert_called_once()
        assert "api/token" in str(mock_logger.warning.call_args)

    def test_an_alternating_path_warns_once_not_on_every_flip(
        self, worker: Worker, requests_get: MagicMock, mock_logger: MagicMock
    ) -> None:
        """A shared IMDS token bucket produces bursty 429s, not a clean outage.

        Clearing the state on the first good answer would re-arm immediately, so alternating
        429/200 would warn every other poll -- worse than the volume the suppression exists to
        avoid, and worse than mainline, which was silent here.
        """
        throttled = MagicMock(status_code=429)
        answered = MagicMock(status_code=404)
        requests_get.side_effect = [throttled, answered] * 6

        for _ in range(12):
            worker._get_spot_instance_shutdown_action_timeout(imdsv2_token="TOKEN")

        mock_logger.warning.assert_called_once()
        assert not any("is answering" in str(c) for c in mock_logger.info.call_args_list)

    def test_a_non_ec2_host_does_not_warn(self, worker: Worker, mock_logger: MagicMock) -> None:
        """Every customer-managed fleet on non-EC2 hardware reaches this on each start.

        It is correct behaviour there, so it must not read as a fault. Mainline logged nothing.
        """
        with patch.object(worker, "_get_ec2_metadata_imdsv2_token", return_value=None):
            with patch.object(worker._stop, "wait", return_value=False):
                assert worker._is_ec2_host() is False

        mock_logger.warning.assert_not_called()
        assert any("not being an" in str(c.args[0]) for c in mock_logger.info.call_args_list)

    def test_one_slow_path_does_not_flap_against_a_fast_one(
        self, worker: Worker, requests_get: MagicMock, mock_logger: MagicMock
    ) -> None:
        """The realistic partial-slowness case, driven through the monitor loop.

        _monitor_ec2_shutdown queries both paths per poll, so a spot query over the read bound
        followed by an ASG query under it is the case a single shared flag got wrong: set by the
        first, cleared by the second, producing a warning and a spurious recovery every second.
        Driven through the loop rather than the queries directly, because that interleaving is
        the whole defect and calling either query alone cannot reproduce it.
        """

        # GIVEN: spot always read-times-out, ASG always answers "not terminating"
        def get(url: str, **kwargs: object) -> MagicMock:
            if "spot/instance-action" in url:
                raise requests.ReadTimeout("timed out")
            return MagicMock(status_code=200, text="InService")

        requests_get.side_effect = get

        # WHEN: three polls, then stop
        with patch.object(worker._stop, "wait", side_effect=[False, False, False, True]):
            with patch.object(worker, "_get_ec2_metadata_imdsv2_token", return_value="TOKEN"):
                worker._monitor_ec2_shutdown()

        # THEN: the slow path is reported once across all three polls, and the fast path never
        # claims a recovery on its behalf.
        assert mock_logger.warning.call_count == 1
        assert not any("answering" in str(c.args[0]) for c in mock_logger.info.call_args_list), (
            mock_logger.info.call_args_list
        )

    def test_a_later_answer_re_arms_the_warning(
        self, worker: Worker, requests_get: MagicMock, mock_logger: MagicMock
    ) -> None:
        """A second outage after a recovery is news again, and the recovery itself is logged."""
        answered = MagicMock(status_code=404)
        requests_get.side_effect = (
            [requests.ReadTimeout("timed out")]
            + [answered] * Worker._IMDS_RECOVERY_ANSWERS
            + [requests.ReadTimeout("timed out")]
        )

        for _ in range(Worker._IMDS_RECOVERY_ANSWERS + 2):
            worker._get_spot_instance_shutdown_action_timeout(imdsv2_token="TOKEN")

        assert mock_logger.warning.call_count == 2
        # The recovery names the path, since only that path recovered.
        assert any(
            "is answering" in c.args[0] and "spot/instance-action" in c.args
            for c in mock_logger.info.call_args_list
        ), mock_logger.info.call_args_list


class TestImdsRequestsAreBounded:
    """The metadata address is link-local, so where nothing answers it the connect waits
    on ARP rather than being refused. Unbounded, that parks the calling thread -- and for
    the token probe that thread is the one the shutdown path returns through.
    """

    def test_token_probe_survives_a_read_timeout(
        self, worker: Worker, requests_put: MagicMock
    ) -> None:
        """ReadTimeout subclasses Timeout, not ConnectionError.

        So this is the case the widened handler adds: against the previous
        `except requests.ConnectionError` alone it escapes and takes the agent down.
        """
        requests_put.side_effect = requests.ReadTimeout("timed out")

        assert worker._get_ec2_metadata_imdsv2_token() is None

    def test_token_probe_survives_a_proxy_error(
        self, worker: Worker, requests_put: MagicMock
    ) -> None:
        """ProxyError subclasses ConnectionError, so this was already handled.

        Kept as documentation of the intent rather than as coverage of the change: a proxy
        configured in the environment intercepts the metadata address, and the right answer
        on this path is still "assume we are not on EC2" rather than an exception out of
        `run()`. It would fail if someone narrowed the handler to ConnectTimeout.
        """
        requests_put.side_effect = requests.exceptions.ProxyError("bad proxy")

        assert worker._get_ec2_metadata_imdsv2_token() is None

    def test_spot_query_survives_a_read_timeout(
        self, worker: Worker, requests_get: MagicMock
    ) -> None:
        requests_get.side_effect = requests.ReadTimeout("timed out")

        assert worker._get_spot_instance_shutdown_action_timeout(imdsv2_token="TOKEN") is None

    def test_asg_query_survives_a_read_timeout(
        self, worker: Worker, requests_get: MagicMock
    ) -> None:
        requests_get.side_effect = requests.ReadTimeout("timed out")

        assert worker._is_asg_terminated(imdsv2_token="TOKEN") is False
