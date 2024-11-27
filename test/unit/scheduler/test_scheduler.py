# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Generator, Optional
from unittest.mock import ANY, MagicMock, Mock, call, patch

from openjd.sessions import ActionState, ActionStatus, SessionUser, PosixSessionUser
from botocore.exceptions import ClientError
import pytest
import os
import sys

from deadline_worker_agent.api_models import (
    AssignedSession,
    EnvironmentAction,
    LogConfiguration,
    TaskRunAction,
)
from deadline_worker_agent.scheduler.scheduler import (
    SessionMap,
    WorkerScheduler,
    UPDATE_WORKER_SCHEDULE_MAX_MESSAGE_CHARS,
)
from deadline_worker_agent.scheduler.session_action_status import SessionActionStatus
from deadline_worker_agent.sessions.job_entities.job_details import (
    JobDetails,
    JobRunAsUser,
    JobRunAsWindowsUser,
)
from deadline_worker_agent.config import JobsRunAsUserOverride
from deadline_worker_agent.errors import ServiceShutdown
import deadline_worker_agent.scheduler.scheduler as scheduler_mod
from deadline_worker_agent.aws.deadline import (
    DeadlineRequestError,
    DeadlineRequestWorkerOfflineError,
    DeadlineRequestUnrecoverableError,
    DeadlineRequestInterrupted,
)
from deadline_worker_agent.file_system_operations import FileSystemPermissionEnum
from openjd.model import SpecificationRevision


@pytest.fixture
def boto_session() -> Mock:
    """A Mock used in place of a boto session"""
    return Mock()


@pytest.fixture
def worker_logs_dir(tmp_path: Path) -> Path:
    return tmp_path


@pytest.fixture
def scheduler(
    farm_id: str,
    fleet_id: str,
    worker_id: str,
    client: MagicMock,
    job_run_as_user_overrides: JobsRunAsUserOverride,
    boto_session: Mock,
    worker_logs_dir: Path,
) -> WorkerScheduler:
    """Fixture for a WorkerScheduler instance"""
    return WorkerScheduler(
        farm_id=farm_id,
        fleet_id=fleet_id,
        worker_id=worker_id,
        deadline=client,
        job_run_as_user_override=job_run_as_user_overrides,
        boto_session=boto_session,
        cleanup_session_user_processes=True,
        worker_persistence_dir=Path("/var/lib/deadline"),
        worker_logs_dir=worker_logs_dir,
    )


@pytest.fixture
def module_logger() -> Generator[MagicMock, None, None]:
    """Mocks the scheduler module's logger"""
    with patch.object(scheduler_mod, "logger") as module_logger:
        yield module_logger


@pytest.fixture(autouse=True)
def mock_session_map_callbacks() -> Generator[None, None, None]:
    # Mock out the callbacks in SessionMap since we're not testing that logic here
    with (
        patch.object(scheduler_mod.SessionMap, "setitem_callback"),
        patch.object(scheduler_mod.SessionMap, "delitem_callback"),
    ):
        yield


class MockSession(MagicMock):
    def __init__(
        self,
        *args,
        **kwargs,
    ):
        super().__init__()
        for key, val in kwargs.items():
            setattr(self, key, val)


@pytest.fixture()
def mock_session() -> Generator[None, None, None]:
    with patch.object(scheduler_mod, "Session", new=MockSession):
        yield


class TestSchedulerRun:
    """Tests for WorkerScheduler.run()"""

    def test_sync_service_shutdown_raised_not_logged(
        self,
        scheduler: WorkerScheduler,
        module_logger: MagicMock,
    ) -> None:
        """Tests that when Scheduler._sync raises a ServiceShutdown exception, the exception is
        re-raised and not logged"""

        # GIVEN
        shutdown = ServiceShutdown()
        logger_exception: MagicMock = module_logger.exception
        with (
            patch.object(scheduler, "_sync", side_effect=shutdown) as mock_sync,
            patch.object(scheduler._shutdown, "is_set", side_effect=[False, False, False]),
            pytest.raises(ServiceShutdown) as raise_ctx,
        ):
            # WHEN
            scheduler.run()

        # THEN
        assert raise_ctx.value is shutdown
        mock_sync.assert_called_once_with(interruptable=True)
        logger_exception.assert_not_called()

    def test_drains_when_worker_shutdown(
        self,
        scheduler: WorkerScheduler,
    ) -> None:
        """Tests that when the Scheduler is shutdown via a local signal that it initiates its drain protocol."""

        # GIVEN
        with (
            patch.object(
                scheduler._shutdown,
                "is_set",
                side_effect=[
                    True,
                ],
            ),
            patch.object(scheduler, "_drain_scheduler") as drain_mock,
        ):
            # WHEN
            scheduler.run()

        # THEN
        drain_mock.assert_called_once()

    def test_drains_when_service_shutdown(
        self,
        scheduler: WorkerScheduler,
    ) -> None:
        """Tests that when the Worker is shutdown by the service that it initiates its drain protocol."""

        # GIVEN
        shutdown = ServiceShutdown()
        with (
            patch.object(scheduler, "_sync", side_effect=shutdown),
            patch.object(
                scheduler._shutdown,
                "is_set",
                side_effect=[
                    False,
                ],
            ),
            patch.object(scheduler, "_drain_scheduler") as drain_mock,
            pytest.raises(ServiceShutdown),
        ):
            # WHEN
            scheduler.run()

        # THEN
        drain_mock.assert_called_once()

    @pytest.mark.parametrize(
        "exception", [Exception("a message"), DeadlineRequestError(Exception("inner"))]
    )
    def test_drains_when_exception(self, scheduler: WorkerScheduler, exception: Exception) -> None:
        """Tests that when the Scheduler's _sync raises an arbitrary exception, that it initiates its drain protocol."""

        # GIVEN
        with (
            patch.object(scheduler, "_sync", side_effect=exception),
            patch.object(
                scheduler._shutdown,
                "is_set",
                side_effect=[
                    False,
                ],
            ),
            patch.object(scheduler, "_drain_scheduler") as drain_mock,
            pytest.raises(Exception) as raise_ctx,
        ):
            # WHEN
            scheduler.run()

        # THEN
        assert raise_ctx.value is exception
        drain_mock.assert_called_once()

    if sys.platform == "win32":

        @pytest.mark.skipif(sys.platform != "win32", reason="Windows Only Test")
        @pytest.mark.parametrize(
            "job_run_as_user_override",
            (
                JobsRunAsUserOverride(run_as_agent=False),
                JobsRunAsUserOverride(
                    run_as_agent=False,
                    job_user=MagicMock(),
                    logon_token=MagicMock(),
                    user_profile=MagicMock(),
                ),
            ),
        )
        @patch.object(scheduler_mod, "unload_and_close")
        def test_win_cleanup_when_worker_shutdown(
            self,
            unload_and_close: MagicMock,
            scheduler: WorkerScheduler,
            job_run_as_user_override: JobsRunAsUserOverride,
        ) -> None:
            """Tests that when the Scheduler is shutdown via a local signal that it unloads the user profile and closes the session token handle"""
            # GIVEN
            scheduler._job_run_as_user_override = job_run_as_user_override

            with (
                patch.object(
                    scheduler._shutdown,
                    "is_set",
                    side_effect=[
                        True,
                    ],
                ),
                patch.object(scheduler, "_drain_scheduler"),
                patch.object(scheduler, "_windows_credentials_resolver") as _cred_resolver_mock,
            ):
                # WHEN
                scheduler.run()

            # THEN
            if job_run_as_user_override.job_user:
                unload_and_close.assert_called_once_with(
                    job_run_as_user_override.user_profile, job_run_as_user_override.logon_token
                )
                _cred_resolver_mock.clear.assert_not_called()
            else:
                unload_and_close.assert_not_called()
                _cred_resolver_mock.clear.assert_called_once()

        @pytest.mark.skipif(sys.platform != "win32", reason="Windows Only Test")
        @pytest.mark.parametrize(
            "job_run_as_user_override",
            (
                JobsRunAsUserOverride(run_as_agent=False),
                JobsRunAsUserOverride(
                    run_as_agent=False,
                    job_user=MagicMock(),
                    logon_token=MagicMock(),
                    user_profile=MagicMock(),
                ),
            ),
        )
        @patch.object(scheduler_mod, "unload_and_close")
        def test_win_cleanup_when_service_shutdown(
            self,
            unload_and_close: MagicMock,
            scheduler: WorkerScheduler,
            job_run_as_user_override: JobsRunAsUserOverride,
        ) -> None:
            """Tests that when the Worker is shutdown by the service that it unloads the user profile and closes the session token handle"""

            # GIVEN
            shutdown = ServiceShutdown()
            scheduler._job_run_as_user_override = job_run_as_user_override
            with (
                patch.object(scheduler, "_sync", side_effect=shutdown),
                patch.object(
                    scheduler._shutdown,
                    "is_set",
                    side_effect=[
                        False,
                    ],
                ),
                patch.object(scheduler, "_drain_scheduler"),
                patch.object(scheduler, "_windows_credentials_resolver") as _cred_resolver_mock,
                pytest.raises(ServiceShutdown),
            ):
                # WHEN
                scheduler.run()

            # THEN
            if job_run_as_user_override.job_user:
                unload_and_close.assert_called_once_with(
                    job_run_as_user_override.user_profile, job_run_as_user_override.logon_token
                )
                _cred_resolver_mock.clear.assert_not_called()
            else:
                unload_and_close.assert_not_called()
                _cred_resolver_mock.clear.assert_called_once()


class TestSchedulerDrain:
    """Tests for WorkerScheduler._drain_scheduler()"""

    def test_noop_drain(self, scheduler: WorkerScheduler) -> None:
        """Test that the drain operation is a straight-shot that does nothing if there are
        no sessions or credentials to end.
        """

        # GIVEN
        with (
            patch.object(scheduler, "_shutdown_sessions", return_value=list()),
            patch.object(scheduler, "_transition_to_stopping") as stopping_mock,
            patch.object(scheduler_mod, "wait") as wait_mock,
            patch.object(scheduler, "_sync") as sync_mock,
        ):
            # WHEN
            scheduler._drain_scheduler()

        # THEN
        stopping_mock.assert_not_called()
        wait_mock.assert_not_called()
        sync_mock.assert_not_called()

    def test_waits_for_session_cancel(self, scheduler: WorkerScheduler) -> None:
        """Test that when the scheduler is undergoing a service-initiated drain that
        it will complete any of the Sessions that it currently has running before exiting."""

        # GIVEN
        mock_futures = [MagicMock(), MagicMock(), MagicMock()]
        scheduler._sessions = SessionMap(
            {"123": MagicMock(), "456": MagicMock(), "789": MagicMock()}
        )
        with (
            patch.object(scheduler, "_shutdown_sessions", return_value=mock_futures),
            patch.object(scheduler, "_transition_to_stopping") as stopping_mock,
            patch.object(scheduler_mod, "wait") as wait_mock,
            patch.object(scheduler, "_sync") as sync_mock,
        ):
            # WHEN
            scheduler._drain_scheduler()

        # THEN
        stopping_mock.assert_not_called()
        wait_mock.assert_called_once_with(mock_futures, timeout=None)
        sync_mock.assert_not_called()

    def test_stopping_and_sync_when_shutdown(self, scheduler: WorkerScheduler) -> None:
        """Test that when the scheduler is undergoing a worker-initiated drain with Sessions
        running and no gracetime defined that it will:
        1. Transition to STOPPING state;
        2. Wait for the sessions to end;
        3. Sync the final action status with the service.
        4. Not pass a timeout when waiting for sessions to complete
        """

        # GIVEN
        mock_futures = [MagicMock(), MagicMock(), MagicMock()]
        scheduler._sessions = SessionMap(
            {"123": MagicMock(), "456": MagicMock(), "789": MagicMock()}
        )
        with (
            patch.object(scheduler, "_shutdown_sessions", return_value=mock_futures),
            patch.object(scheduler, "_transition_to_stopping") as stopping_mock,
            patch.object(scheduler_mod, "wait") as wait_mock,
            patch.object(scheduler, "_sync") as sync_mock,
            patch.object(scheduler._shutdown, "is_set", side_effect=[True, True]),
        ):
            # WHEN
            scheduler._drain_scheduler()

        # THEN
        stopping_mock.assert_called_once()
        assert stopping_mock.call_args.kwargs["timeout"] is not None
        wait_mock.assert_called_once_with(mock_futures, timeout=None)
        sync_mock.assert_called_once()

    def test_stopping_and_sync_when_shutdown_with_gracetime(
        self, scheduler: WorkerScheduler
    ) -> None:
        """Test that when the scheduler is undergoing a worker-initiated drain with Sessions
        running that it will:
        1. Transition to STOPPING state;
        2. Wait for the sessions to end; and
        3. Sync the final action status with the service.
        """

        # GIVEN
        mock_futures = [MagicMock(), MagicMock(), MagicMock()]
        scheduler._sessions = SessionMap(
            {"123": MagicMock(), "456": MagicMock(), "789": MagicMock()}
        )
        timeout = timedelta(seconds=5)
        scheduler._shutdown_grace = timeout
        with (
            patch.object(scheduler, "_shutdown_sessions", return_value=mock_futures),
            patch.object(scheduler, "_transition_to_stopping") as stopping_mock,
            patch.object(scheduler_mod, "wait") as wait_mock,
            patch.object(scheduler, "_sync") as sync_mock,
            patch.object(scheduler._shutdown, "is_set", side_effect=[True, True]),
        ):
            # WHEN
            scheduler._drain_scheduler()

        # THEN
        stopping_mock.assert_called_once()
        assert stopping_mock.call_args.kwargs["timeout"] is not None
        assert stopping_mock.call_args.kwargs["timeout"] < timeout
        wait_mock.assert_called_once()
        assert wait_mock.call_args.args[0] is mock_futures
        assert wait_mock.call_args.kwargs["timeout"] is not None
        assert wait_mock.call_args.kwargs["timeout"] < timeout.total_seconds()
        sync_mock.assert_called_once()

    def test_exits_queue_credentials_managers(self, scheduler: WorkerScheduler) -> None:
        """Test that we cleanup any and all QueueBoto3Credentials that we have when draining."""

        # GIVEN
        queue_boto3_1 = MagicMock()
        queue_boto3_2 = MagicMock()

        creds_1 = scheduler_mod.QueueAwsCredentials(session=queue_boto3_1, refresher=MagicMock())
        creds_2 = scheduler_mod.QueueAwsCredentials(session=queue_boto3_2, refresher=MagicMock())
        scheduler._queue_aws_credentials = {"123": creds_1, "456": creds_2}
        with (
            patch.object(scheduler, "_shutdown_sessions", return_value=list()),
            patch.object(scheduler, "_transition_to_stopping"),
            patch.object(scheduler_mod, "wait"),
            patch.object(scheduler, "_sync"),
        ):
            # WHEN
            scheduler._drain_scheduler()

        # THEN
        queue_boto3_1.cleanup.assert_called_once()
        queue_boto3_2.cleanup.assert_called_once()
        assert len(scheduler._queue_aws_credentials) == 0


class TestTransitionToStopping:
    """Tests for WorkerScheduler._transition_to_stopping()"""

    def test_updates_to_stopping(self, scheduler: WorkerScheduler) -> None:
        """Most basic test. Do we invoke the correct API with the STOPPING state?"""

        # GIVEN
        with patch.object(scheduler_mod, "update_worker") as mock_update_worker:
            # WHEN
            scheduler._transition_to_stopping(timeout=timedelta(seconds=1))

            # THEN
            mock_update_worker.assert_called_once()
            assert mock_update_worker.call_args.kwargs["status"] == "STOPPING"

    @pytest.mark.parametrize(
        "code",
        ["ValidationException", "ResourceNotFoundException", "ConflictException", "AccessDenied"],
    )
    def test_exits_on_exception(self, scheduler: WorkerScheduler, code: str) -> None:
        """Test that we exit when getting an exception that is not retryable."""

        # GIVEN
        with patch.object(scheduler, "_deadline") as mock_deadline_client:
            exception = ClientError(
                error_response={
                    "Error": {
                        "Code": code,
                        "Message": "A message",
                    },
                },
                operation_name="OpName",
            )
            api_mock = MagicMock()
            api_mock.side_effect = (
                exception,
                {},
            )
            mock_deadline_client.update_worker = api_mock

            # WHEN
            scheduler._transition_to_stopping(timeout=timedelta(seconds=1))

            # THEN
            api_mock.assert_called_once()


class TestSchedulerSync:
    """Tests for WorkerScheduler._sync()"""

    @pytest.fixture
    def mock_update_worker_schedule(self) -> Generator[MagicMock, None, None]:
        with patch.object(scheduler_mod, "update_worker_schedule") as mock:
            yield mock

    def test_shutdown(
        self,
        scheduler: WorkerScheduler,
        module_logger: MagicMock,
        mock_update_worker_schedule: MagicMock,
    ) -> None:
        """Tests that when the UpdateWorkerSchedule API returns "STOPPED" in the "desiredWorkerStatus"
        response field, that the shutdown initiation is logged.
        """

        # GIVEN
        mock_update_worker_schedule.return_value = {
            "desiredWorkerStatus": "STOPPED",
        }
        logger_warning: MagicMock = module_logger.warning
        with (
            patch.object(scheduler, "_update_sessions"),
            # THEN
            pytest.raises(ServiceShutdown),
        ):
            # WHEN
            scheduler._sync()

        # THEN
        logger_warning.assert_any_call("Service requested shutdown initiated")

    def test_truncates_message(
        self, scheduler: WorkerScheduler, mock_update_worker_schedule: MagicMock
    ) -> None:
        """Tests that when the UpdateWorkerSchedule API needs to be called with a status message greater than the limit on
        the status field of the UpdateWorkerSchedule API request, the message is truncated
        """

        # GIVEN
        original_message = "x" * (4096 + 1)
        assert len(original_message) > UPDATE_WORKER_SCHEDULE_MAX_MESSAGE_CHARS

        expected_message = "x" * 4096
        assert len(expected_message) <= UPDATE_WORKER_SCHEDULE_MAX_MESSAGE_CHARS

        # WHEN
        with patch.object(
            scheduler,
            "_action_updates_map",
            {
                "id-123": SessionActionStatus(
                    id="id-123",
                    status=ActionStatus(
                        state=ActionState.RUNNING,
                        status_message=original_message,
                    ),
                ),
            },
        ):
            scheduler._sync()

        # THEN
        mock_update_worker_schedule.assert_called_once_with(
            deadline_client=scheduler._deadline,
            farm_id=scheduler._farm_id,
            fleet_id=scheduler._fleet_id,
            worker_id=scheduler._worker_id,
            updated_session_actions={
                "id-123": {
                    "progressMessage": expected_message,
                },
            },
            interrupt_event=scheduler._shutdown,
        )

    @pytest.mark.parametrize(
        "initial_updates_map, actions, expected_statuses",
        (
            pytest.param(
                {
                    "AA": SessionActionStatus(id="AA", completed_status="SUCCEEDED"),
                },
                [
                    {"sessionActionId": "AA", "actionType": "ENV_EXIT"},
                    {"sessionActionId": "BB", "actionType": "ENV_EXIT"},
                ],
                {"AA": "SUCCEEDED", "BB": "FAILED"},
                id="Return existing status",
            ),
            pytest.param(
                dict(),
                [
                    {"sessionActionId": "AA", "actionType": "ENV_EXIT"},
                    {"sessionActionId": "BB", "actionType": "ENV_EXIT"},
                ],
                {"AA": "FAILED", "BB": "FAILED"},
                id="Cases: 1,2,3",  # See comments in _return_sessionactions_from_stopped_session
            ),
            pytest.param(
                dict(),
                [
                    {"sessionActionId": "AA", "actionType": "TASK_RUN"},
                    {"sessionActionId": "BB", "actionType": "TASK_RUN"},
                    {"sessionActionId": "CC", "actionType": "TASK_RUN"},
                    {"sessionActionId": "DD", "actionType": "ENV_EXIT"},
                    {"sessionActionId": "EE", "actionType": "ENV_EXIT"},
                ],
                {
                    "AA": "FAILED",
                    "BB": "NEVER_ATTEMPTED",
                    "CC": "NEVER_ATTEMPTED",
                    "DD": "FAILED",
                    "EE": "FAILED",
                },
                id="Case 4",  # See comments in _return_sessionactions_from_stopped_session
            ),
        ),
    )
    def test_return_sessionactions_from_stopped_session(
        self,
        scheduler: WorkerScheduler,
        initial_updates_map: dict[str, SessionActionStatus],
        actions: list[dict[str, str]],
        expected_statuses: dict[str, str],
    ) -> None:
        # GIVEN
        failure_message = "This is a failure message"
        scheduler._action_updates_map = initial_updates_map

        # WHEN
        scheduler._return_sessionactions_from_stopped_session(
            assigned_session_actions=actions,  # type: ignore[arg-type]
            failure_message=failure_message,
        )

        # THEN
        assert len(scheduler._action_updates_map) == len(expected_statuses)
        assert set(scheduler._action_updates_map.keys()) == set(expected_statuses.keys())
        for id in expected_statuses:
            action_status = scheduler._action_updates_map[id]
            assert action_status.completed_status == expected_statuses[id], id
            assert action_status.id == id
            if expected_statuses[id] == "NEVER_ATTEMPTED":
                # Per contract: NEVER_ATTEMPTED has to start/end time
                assert action_status.start_time is None
                assert action_status.end_time is None
            elif expected_statuses[id] == "FAILED":
                # Per contract: FAILED has both a start & end time
                assert action_status.start_time is not None
                assert action_status.end_time is not None
                assert action_status.start_time <= action_status.end_time

    @pytest.mark.parametrize(
        "exitcode, expected_result",
        [
            pytest.param(None, None, id="None"),
            pytest.param(0, 0, id="Zero"),
            pytest.param(0x7FFFFFFF, 0x7FFFFFFF, id="maxint"),
            pytest.param(-2147483648, -2147483648, id="minint_decimal"),
            pytest.param(0x80000000, -2147483648, id="minint_hex"),
            pytest.param(0xFFFD0000, -196608, id="out-of-range-32bit"),
            pytest.param(0xFFFFFFFD0000, -196608, id="out-of-range-big"),
        ],
    )
    def test_updated_action_to_boto_exitcode(
        self, scheduler: WorkerScheduler, exitcode: Optional[int], expected_result: Optional[int]
    ) -> None:
        # GIVEN
        action_status = SessionActionStatus(
            id="1234", status=ActionStatus(state=ActionState.FAILED, exit_code=exitcode)
        )

        # WHEN
        status_as_boto = scheduler._updated_action_to_boto(action_status)

        # THEN
        if expected_result is None:
            assert status_as_boto.get("processExitCode", "ABSENT") == "ABSENT"
        else:
            assert status_as_boto.get("processExitCode", "FAIL") == expected_result


class TestCreateNewSessions:
    """Tests for WorkerScheduler._create_new_sessions"""

    def test_local_logging(
        self,
        scheduler: WorkerScheduler,
        worker_logs_dir: Path,
    ) -> None:
        """Tests that when creating a new session, that the WorkerScheduler:

        1.  Provisions a directory for the queue with 700 permissions (read/write/traversal for
            owner/agent OS user only)
        2.  Provisions a log file for the session with 600 permissions (read/write permissions for
            owner/agent OS user only)
        3.  Forwards the session log file path to the LogConfiguration.from_boto() class method
        """
        # GIVEN
        queue_id = "queue-abcdef0123456789abcdef0123456789"
        session_id = "session-abcdef0123456789abcdef0123456789"
        assigned_sessions: dict[str, AssignedSession] = {
            session_id: AssignedSession(
                queueId=queue_id,
                jobId="job-abcdef0123456789abcdef0123456789",
                logConfiguration=LogConfiguration(
                    logDriver="awslogs",
                    options={
                        "logGroupName": "logGroup",
                        "logStreamName": "logStreamName",
                    },
                    parameters={
                        "interval": "15",
                    },
                ),
                sessionActions=[
                    EnvironmentAction(
                        actionType="ENV_ENTER",
                        environmentId="env-1",
                        sessionActionId="action-1",
                    ),
                    TaskRunAction(
                        actionType="TASK_RUN",
                        parameters={},
                        sessionActionId="action-2",
                        stepId="step-1",
                        taskId="task-1",
                    ),
                ],
            ),
        }
        queue_log_dir_path = MagicMock()
        session_log_file_path = MagicMock()

        with (
            patch.object(scheduler_mod, "make_directory") as mock_make_directory,
            patch.object(scheduler_mod, "touch_file") as mock_touch_file,
            patch.object(scheduler, "_executor"),
            patch.object(scheduler_mod.LogConfiguration, "from_boto") as mock_log_config_from_boto,
            patch.object(
                scheduler, "_queue_log_dir_path", return_value=queue_log_dir_path
            ) as mock_queue_log_dir,
            patch.object(
                scheduler, "_session_log_file_path", return_value=session_log_file_path
            ) as mock_queue_session_log_file_path,
        ):
            # WHEN
            scheduler._create_new_sessions(assigned_sessions=assigned_sessions)

        # THEN
        mock_queue_log_dir.assert_called_once_with(queue_id=queue_id)
        if os.name == "posix":
            queue_log_dir_path.mkdir.assert_called_once_with(mode=0o700, exist_ok=True)
        else:
            mock_make_directory.assert_called_once_with(
                dir_path=queue_log_dir_path,
                agent_user_permission=FileSystemPermissionEnum.FULL_CONTROL,
                exist_ok=True,
            )
        mock_queue_session_log_file_path.assert_called_once_with(
            session_id=session_id, queue_log_dir=queue_log_dir_path
        )
        if os.name == "posix":
            session_log_file_path.touch.assert_called_once_with(mode=0o600, exist_ok=True)
        else:
            mock_touch_file.assert_called_once()
        mock_log_config_from_boto.assert_called_once()
        assert (
            mock_log_config_from_boto.call_args_list[0].kwargs["session_log_file"]
            == session_log_file_path
        )

    @pytest.mark.parametrize(
        argnames=("mkdir_side_effect", "touch_side_effect"),
        argvalues=(
            pytest.param(PermissionError(), None, id="mkdir-permissions-error"),
            pytest.param(None, PermissionError(), id="touch-permissions-error"),
        ),
    )
    def test_local_logging_os_error(
        self,
        scheduler: WorkerScheduler,
        mkdir_side_effect: Exception | None,
        touch_side_effect: Exception | None,
    ) -> None:
        """Tests that when creating a new session, and the worker encounters an OS error when
        provisioning the session log directory/file that the worker fails the session actions
        and continues.
        """
        # GIVEN
        queue_id = "queue-abcdef0123456789abcdef0123456789"
        session_id = "session-abcdef0123456789abcdef0123456789"
        assigned_sessions: dict[str, AssignedSession] = {
            session_id: AssignedSession(
                queueId=queue_id,
                jobId="job-abcdef0123456789abcdef0123456789",
                logConfiguration=LogConfiguration(
                    logDriver="awslogs",
                    options={
                        "logGroupName": "logGroup",
                        "logStreamName": "logStreamName",
                    },
                    parameters={
                        "interval": "15",
                    },
                ),
                sessionActions=[
                    EnvironmentAction(
                        actionType="ENV_ENTER",
                        environmentId="env-1",
                        sessionActionId="action-1",
                    ),
                ],
            ),
        }
        queue_log_dir_path = MagicMock()
        session_log_file_path = MagicMock()
        if mkdir_side_effect:
            expected_error_msg = (
                f"Failed to create local session log directory on worker: {queue_log_dir_path}"
            )
        else:
            expected_error_msg = (
                f"Failed to create local session log file on worker: {session_log_file_path}"
            )

        with (
            patch.object(scheduler, "_executor"),
            patch.object(scheduler_mod, "make_directory") as mock_make_directory,
            patch.object(scheduler_mod, "touch_file") as mock_touch_file,
            patch.object(scheduler_mod.LogConfiguration, "from_boto") as mock_log_config_from_boto,
            patch.object(
                scheduler, "_queue_log_dir_path", return_value=queue_log_dir_path
            ) as mock_queue_log_dir,
            patch.object(
                scheduler, "_session_log_file_path", return_value=session_log_file_path
            ) as mock_queue_session_log_file_path,
            patch.object(scheduler, "_fail_all_actions") as mock_fail_all_actions,
        ):
            if os.name == "posix":
                queue_log_dir_path.mkdir.side_effect = mkdir_side_effect
                session_log_file_path.touch.side_effect = touch_side_effect
            else:
                mock_make_directory.side_effect = mkdir_side_effect
                mock_touch_file.side_effect = touch_side_effect

            # WHEN
            scheduler._create_new_sessions(assigned_sessions=assigned_sessions)

        # THEN
        mock_queue_log_dir.assert_called_once_with(queue_id=queue_id)
        if os.name == "posix":
            queue_log_dir_path.mkdir.assert_called_once_with(mode=0o700, exist_ok=True)
            if mkdir_side_effect:
                mock_queue_session_log_file_path.assert_not_called()
            else:
                mock_queue_session_log_file_path.assert_called_once()
            if mkdir_side_effect:
                session_log_file_path.touch.asset_not_called()
            else:
                session_log_file_path.touch.assert_called_once()
        else:
            if mkdir_side_effect:
                mock_queue_session_log_file_path.assert_not_called()
            else:
                mock_queue_session_log_file_path.assert_called_once()
            mock_make_directory.assert_called_once()
            if mkdir_side_effect:
                session_log_file_path.touch.asset_not_called()
        mock_log_config_from_boto.assert_not_called()
        mock_fail_all_actions.assert_called_once_with(
            assigned_sessions[session_id],
            error_message=expected_error_msg,
        )

    def test_log_provision_error(
        self,
        scheduler: WorkerScheduler,
    ) -> None:
        """Tests that when a session is assigned with a log provisioning error, that the assigned
        action is marked as FAILED, the rest are marked as NEVER_ATTEMPTED,
        and the scheduler's wakeup event is set so that it makes an
        immediate follow-up UpdateWorkerSchedule request to signal the failure.
        """

        # GIVEN
        session_id = "session-abcdef0123456789abcdef0123456789"
        log_provision_error_msg = "log provision error msg"
        assigned_sessions: dict[str, AssignedSession] = {
            session_id: AssignedSession(
                queueId="queue-abcdef0123456789abcdef0123456789",
                jobId="job-abcdef0123456789abcdef0123456789",
                logConfiguration=LogConfiguration(
                    error=log_provision_error_msg,
                    logDriver="awslogs",
                    options={},
                    parameters={
                        "interval": "15",
                    },
                ),
                sessionActions=[
                    EnvironmentAction(
                        actionType="ENV_ENTER",
                        environmentId="env-1",
                        sessionActionId="action-1",
                    ),
                    TaskRunAction(
                        actionType="TASK_RUN",
                        parameters={},
                        sessionActionId="action-2",
                        stepId="step-1",
                        taskId="task-1",
                    ),
                ],
            ),
        }
        with patch.object(scheduler_mod, "datetime") as datetime_mock:
            datetime_now_mock: MagicMock = datetime_mock.now

            # WHEN
            scheduler._create_new_sessions(assigned_sessions=assigned_sessions)

        # THEN
        for action_num in (1, 2):
            action_id = f"action-{action_num}"
            assert (
                action_update := scheduler._action_updates_map.get(action_id, None)
            ), f"no action update for {action_id}"
            assert action_update.id == action_id
            assert action_update.status is not None
            assert action_update.status.state == ActionState.FAILED
            assert (
                action_update.status.fail_message
                == f"Log provisioning error: {log_provision_error_msg}"
            )
            if action_num == 1:
                assert action_update.completed_status == "FAILED"

                assert action_update.start_time == datetime_now_mock.return_value
                assert action_update.end_time == datetime_now_mock.return_value
            else:
                assert action_update.completed_status == "NEVER_ATTEMPTED"
                assert action_update.start_time is None
                assert action_update.end_time is None

    @pytest.mark.parametrize(
        argnames="job_details_error",
        argvalues=(RuntimeError("job details error"), ValueError("job details error")),
        ids=(
            "RuntimeError",
            "ValueError",
        ),
    )
    def test_job_details_error(
        self,
        scheduler: WorkerScheduler,
        job_details_error: Exception,
    ) -> None:
        """Tests that when a session encounters a job details error, that the first assigned
        action is marked as FAILED, the rest are marked as NEVER_ATTEPTED,
        and the scheduler's wakeup event is set so that it makes an
        immediate follow-up UpdateWorkerSchedule request to signal the failure.
        """
        # GIVEN
        queue_id = "queue-abcdef0123456789abcdef0123456789"
        session_id = "session-abcdef0123456789abcdef0123456789"
        assigned_sessions: dict[str, AssignedSession] = {
            session_id: AssignedSession(
                queueId=queue_id,
                jobId="job-abcdef0123456789abcdef0123456789",
                logConfiguration=LogConfiguration(
                    logDriver="awslogs",
                    options={},
                    parameters={"interval": "15"},
                ),
                sessionActions=[
                    EnvironmentAction(
                        actionType="ENV_ENTER",
                        environmentId="env-1",
                        sessionActionId="action-1",
                    ),
                    TaskRunAction(
                        actionType="TASK_RUN",
                        parameters={},
                        sessionActionId="action-2",
                        stepId="step-1",
                        taskId="task-1",
                    ),
                ],
            ),
        }

        job_entity_mock = MagicMock()
        job_entity_mock.job_details.side_effect = job_details_error

        with (
            patch.object(scheduler_mod, "datetime") as datetime_mock,
            patch.object(scheduler_mod, "JobEntities") as job_entities_mock,
        ):
            job_entities_mock.return_value = job_entity_mock
            datetime_now_mock: MagicMock = datetime_mock.now

            # WHEN
            scheduler._create_new_sessions(assigned_sessions=assigned_sessions)

        # THEN
        for action_num in (1, 2):
            action_id = f"action-{action_num}"
            assert (
                action_update := scheduler._action_updates_map.get(action_id, None)
            ), f"no action update for {action_id}"
            assert action_update.id == action_id
            assert action_update.status is not None
            assert action_update.status.state == ActionState.FAILED
            assert action_update.status.fail_message == str(job_details_error)
            if action_num == 1:
                assert action_update.completed_status == "FAILED"

                assert action_update.start_time == datetime_now_mock.return_value
                assert action_update.end_time == datetime_now_mock.return_value
            else:
                assert action_update.completed_status == "NEVER_ATTEMPTED"
                assert action_update.start_time is None
                assert action_update.end_time is None

    @pytest.mark.skipif(sys.platform != "win32", reason="Windows-only test.")
    def test_job_details_run_as_worker_agent_user_windows(
        self,
        scheduler: WorkerScheduler,
    ) -> None:
        """Tests that when a session encounters a runAs: WORKER_AGENT_USER for Windows os,
        the first assigned action is marked as FAILED, the rest are marked as NEVER_ATTEPTED,
        and the scheduler's wakeup event is set so that it makes an
        immediate follow-up UpdateWorkerSchedule request to signal the failure.
        """
        # GIVEN
        queue_id = "queue-abcdef0123456789abcdef0123456789"
        session_id = "session-abcdef0123456789abcdef0123456789"
        scheduler._job_run_as_user_override = JobsRunAsUserOverride(run_as_agent=False)
        assigned_sessions: dict[str, AssignedSession] = {
            session_id: AssignedSession(
                queueId=queue_id,
                jobId="job-abcdef0123456789abcdef0123456789",
                logConfiguration=LogConfiguration(
                    logDriver="awslogs",
                    options={},
                    parameters={"interval": "15"},
                ),
                sessionActions=[
                    EnvironmentAction(
                        actionType="ENV_ENTER",
                        environmentId="env-1",
                        sessionActionId="action-1",
                    ),
                    TaskRunAction(
                        actionType="TASK_RUN",
                        parameters={},
                        sessionActionId="action-2",
                        stepId="step-1",
                        taskId="task-1",
                    ),
                ],
            ),
        }
        expected_err_msg = "Job cannot run as WORKER_AGENT_USER. Worker Agent is running with Administrator privileges."

        job_entity_mock = MagicMock()
        job_entity_mock.job_details.return_value = JobDetails(
            log_group_name="/aws/deadline/queue-0000",
            schema_version=SpecificationRevision.v2023_09,
            job_run_as_user=JobRunAsUser(is_worker_agent_user=True),
        )

        with (
            patch.object(scheduler_mod, "datetime") as datetime_mock,
            patch.object(scheduler_mod, "JobEntities") as job_entities_mock,
        ):
            job_entities_mock.return_value = job_entity_mock
            datetime_now_mock: MagicMock = datetime_mock.now

            # WHEN
            scheduler._create_new_sessions(assigned_sessions=assigned_sessions)

        # THEN
        for action_num in (1, 2):
            action_id = f"action-{action_num}"
            assert (
                action_update := scheduler._action_updates_map.get(action_id, None)
            ), f"no action update for {action_id}"
            assert action_update.id == action_id
            assert action_update.status is not None
            assert action_update.status.state == ActionState.FAILED
            assert action_update.status.fail_message == expected_err_msg
            if action_num == 1:
                assert action_update.completed_status == "FAILED"
                assert action_update.start_time == datetime_now_mock.return_value
                assert action_update.end_time == datetime_now_mock.return_value
            else:
                assert action_update.completed_status == "NEVER_ATTEMPTED"
                assert action_update.start_time is None
                assert action_update.end_time is None

    @pytest.mark.parametrize(
        "job_details_run_as",
        (
            pytest.param(
                JobRunAsUser(
                    posix=(
                        PosixSessionUser(user="username", group="group")
                        if sys.platform != "win32"
                        else None
                    )
                ),
                marks=pytest.mark.skipif(sys.platform != "win32", reason="POSIX-only test."),
            ),
            pytest.param(
                JobRunAsUser(
                    windows_settings=JobRunAsWindowsUser(user="username", passwordArn="passwordArn")
                ),
                marks=pytest.mark.skipif(sys.platform == "win32", reason="Windows-only test."),
            ),
            JobRunAsUser(is_worker_agent_user=True),
        ),
    )
    @pytest.mark.parametrize("scheduler_run_as_agent", (True, False))
    def test_job_details_run_as_with_run_as_agent_override(
        self,
        scheduler: WorkerScheduler,
        job_details_run_as: JobRunAsUser,
        scheduler_run_as_agent: bool,
        job_user: SessionUser,
        mock_session: MockSession,
    ) -> None:
        """Tests that when a session encounters a runAs: WORKER_AGENT_USER,
        and the agent is configured with a JobsRunAsUserOverride, that the session is not
        marked as FAILED
        """
        # GIVEN
        queue_id = "queue-abcdef0123456789abcdef0123456789"
        session_id = "session-abcdef0123456789abcdef0123456789"
        if scheduler_run_as_agent:
            scheduler._job_run_as_user_override = JobsRunAsUserOverride(run_as_agent=True)
        else:
            scheduler._job_run_as_user_override = JobsRunAsUserOverride(
                run_as_agent=False, job_user=job_user
            )
        assigned_sessions: dict[str, AssignedSession] = {
            session_id: AssignedSession(
                queueId=queue_id,
                jobId="job-abcdef0123456789abcdef0123456789",
                logConfiguration=LogConfiguration(
                    logDriver="awslogs",
                    options={
                        "logGroupName": "logGroup",
                        "logStreamName": "logStreamName",
                    },
                    parameters={
                        "interval": "15",
                    },
                ),
                sessionActions=[
                    EnvironmentAction(
                        actionType="ENV_ENTER",
                        environmentId="env-1",
                        sessionActionId="action-1",
                    ),
                    TaskRunAction(
                        actionType="TASK_RUN",
                        parameters={},
                        sessionActionId="action-2",
                        stepId="step-1",
                        taskId="task-1",
                    ),
                ],
            ),
        }

        job_entity_mock = MagicMock()
        job_entity_mock.job_details.return_value = JobDetails(
            log_group_name="/aws/deadline/queue-0000",
            schema_version=SpecificationRevision.v2023_09,
            job_run_as_user=job_details_run_as,
        )

        # WHEN
        with (patch.object(scheduler_mod, "JobEntities") as job_entities_mock,):
            job_entities_mock.return_value = job_entity_mock
            scheduler._create_new_sessions(assigned_sessions=assigned_sessions)

        # THEN
        if session_id not in scheduler._sessions:
            assert session_id in list(scheduler._sessions.keys())
        assert session_id in scheduler._sessions

        if scheduler_run_as_agent:
            assert scheduler._sessions[session_id].session.os_user is None
        else:
            assert scheduler._sessions[session_id].session.os_user is job_user

    class MockSessionUser(SessionUser):
        user: str

        def __init__(self, user) -> None:
            self.user = user

        def __eq__(self, other) -> bool:
            return self.user == other.user

        @staticmethod
        def _get_process_user() -> str:
            return "user"

    @pytest.mark.parametrize(
        "host_is_posix,job_run_as_user,job_run_as_user_override,expected_result,expected_exception",
        [
            pytest.param(
                True,
                JobRunAsUser(
                    posix=MockSessionUser("posix"),  # type: ignore[arg-type]
                    windows=MockSessionUser("windows"),  # type: ignore[arg-type]
                    windows_settings=None,
                    is_worker_agent_user=False,
                ),
                JobsRunAsUserOverride(run_as_agent=True, job_user=None),
                None,
                None,
                id="Run as agent override",
            ),
            pytest.param(
                True,
                JobRunAsUser(
                    posix=MockSessionUser("posix"),  # type: ignore[arg-type]
                    windows=MockSessionUser("windows"),  # type: ignore[arg-type]
                    windows_settings=None,
                    is_worker_agent_user=False,
                ),
                JobsRunAsUserOverride(run_as_agent=False, job_user=MockSessionUser("override")),
                MockSessionUser("override"),
                None,
                id="Override job user",
            ),
            pytest.param(
                True,
                None,
                JobsRunAsUserOverride(run_as_agent=False, job_user=None),
                None,
                r"^FATAL: Queue does not have a jobRunAsUser\. .*",
                id="Invariant violated: No override or queue user",
            ),
            pytest.param(
                True,
                JobRunAsUser(
                    posix=MockSessionUser("posix"),  # type: ignore[arg-type]
                    windows=MockSessionUser("windows"),  # type: ignore[arg-type]
                    windows_settings=None,
                    is_worker_agent_user=True,
                ),
                JobsRunAsUserOverride(run_as_agent=False, job_user=None),
                None,
                None,
                id="Selects agent user",
            ),
            pytest.param(
                True,
                JobRunAsUser(
                    posix=MockSessionUser("posix"),  # type: ignore[arg-type]
                    windows=MockSessionUser("windows"),  # type: ignore[arg-type]
                    windows_settings=None,
                    is_worker_agent_user=False,
                ),
                JobsRunAsUserOverride(run_as_agent=False, job_user=None),
                MockSessionUser("posix"),
                None,
                id="Selects posix user",
            ),
            pytest.param(
                False,
                JobRunAsUser(
                    posix=MockSessionUser("posix"),  # type: ignore[arg-type]
                    windows=MockSessionUser("windows"),  # type: ignore[arg-type]
                    windows_settings=None,
                    is_worker_agent_user=False,
                ),
                JobsRunAsUserOverride(run_as_agent=False, job_user=None),
                MockSessionUser("windows"),
                None,
                id="Selects windows user",
            ),
            pytest.param(
                True,
                JobRunAsUser(
                    posix=None,
                    windows=None,
                    windows_settings=None,
                    is_worker_agent_user=False,
                ),
                JobsRunAsUserOverride(run_as_agent=False, job_user=None),
                None,
                r"^FATAL: Queue's jobRunAsUser does not define a QUEUE_CONFIGURED_USER for this platform\. .*",
                id="Invariant violated: Missing platform-specific queue user",
            ),
        ],
    )
    def test_determine_user_for_session(
        self,
        host_is_posix: bool,
        job_run_as_user: Optional[JobRunAsUser],
        job_run_as_user_override: JobsRunAsUserOverride,
        expected_result: Optional[SessionUser],
        expected_exception: Optional[str],
    ) -> None:

        # WHEN
        if expected_exception is not None:
            with pytest.raises(ValueError, match=expected_exception):
                WorkerScheduler._determine_user_for_session(
                    host_is_posix=host_is_posix,
                    job_run_as_user=job_run_as_user,
                    job_run_as_user_override=job_run_as_user_override,
                    queue_id="queue-1234",
                    job_id="job-1234",
                    session_id="session-1234",
                )
            return

        result = WorkerScheduler._determine_user_for_session(
            host_is_posix=host_is_posix,
            job_run_as_user=job_run_as_user,
            job_run_as_user_override=job_run_as_user_override,
            queue_id="queue-1234",
            job_id="job-1234",
            session_id="session-1234",
        )

        # THEN
        assert result == expected_result


class TestQueueAwsCredentialsManagement:
    """Tests that validate that we are constructing and destroying credentials objects
    as appropriate."""

    def test_retains_assigned_queue_creds(self, scheduler: WorkerScheduler) -> None:
        """Test that _cleanup_queue_aws_credentials() retains a previously created
        credentials manager object if it has assigned sessions for the same queue.
        """

        # GIVEN
        queue_id = "queue-123456"
        role_arn = "arn:aws:...:RoleArn!"
        hash_key = f"{queue_id}:{role_arn}"
        creds_mock = MagicMock()
        creds_mock.__exit__ = MagicMock()
        scheduler._queue_aws_credentials[hash_key] = creds_mock
        assigned_sessions: dict[str, AssignedSession] = {
            queue_id: {"queueId": queue_id, "jobId": "job-1234", "sessionActions": []}
        }

        # WHEN
        scheduler._cleanup_queue_aws_credentials(assigned_sessions=assigned_sessions)
        creds_mock.__exit__.assert_not_called()

        # THEN
        assert scheduler._queue_aws_credentials.get(hash_key) is not None

    def test_deletes_unassigned_queue_creds(self, scheduler: WorkerScheduler) -> None:
        """Test that we delete a previously created credentials manager object if
        we are no longer working on things from the same queue."""

        # GIVEN
        role_arn = "arn:aws:...:RoleArn!"
        prev_queue_id = "queue-123456"
        new_queue_id = "queue-abcdef"
        prev_hash_key = f"{prev_queue_id}:{role_arn}"
        new_hash_key = f"{new_queue_id}:{role_arn}"
        queue_boto3 = MagicMock()
        creds = scheduler_mod.QueueAwsCredentials(session=queue_boto3, refresher=MagicMock())
        scheduler._queue_aws_credentials[prev_hash_key] = creds
        assigned_sessions: dict[str, AssignedSession] = {
            new_queue_id: {
                "queueId": new_queue_id,
                "jobId": "job-1234",
                "sessionActions": [],
            }
        }

        # WHEN
        scheduler._cleanup_queue_aws_credentials(assigned_sessions=assigned_sessions)

        # THEN
        assert prev_hash_key not in scheduler._queue_aws_credentials
        # New credentials objects are created elsewhere.
        assert new_hash_key not in scheduler._queue_aws_credentials
        assert new_queue_id not in scheduler._queue_aws_credentials
        queue_boto3.cleanup.assert_called_once()

    def test_reuses_existing_credentials(self, scheduler: WorkerScheduler) -> None:
        """Test that we reuse an existing set of Queue credentials in
        _get_queue_aws_credentials_profile if we already have the appropriate one.
        """

        # GIVEN
        queue_id = "queue-123456"
        role_arn = "arn:aws:...:RoleArn!"
        hash_key = f"{queue_id}:{role_arn}"
        queue_boto3 = MagicMock()
        creds = scheduler_mod.QueueAwsCredentials(session=queue_boto3, refresher=MagicMock())
        scheduler._queue_aws_credentials[hash_key] = creds

        # WHEN
        result = scheduler._get_queue_aws_credentials(queue_id, role_arn, "session-1234", None)

        # THEN
        assert result is creds

    def test_creates_new_credentials(
        self,
        scheduler: WorkerScheduler,
        boto_session: MagicMock,
    ) -> None:
        """Test that we create a new set of Queue credentials in _get_queue_aws_credentials
        when we don't already have one cached for the queue.
        """

        with (
            patch.object(scheduler_mod, "QueueBoto3Session") as mock_q_boto3_cls,
            patch.object(scheduler_mod, "AwsCredentialsRefresher") as mock_cred_refresh_cls,
        ):
            # GIVEN
            queue_id = "queue-123456"
            role_arn = "arn:aws:...:RoleArn!"
            hash_key = f"{queue_id}:{role_arn}"
            queue_boto3 = MagicMock()
            mock_refresh = MagicMock()
            mock_q_boto3_cls.return_value = queue_boto3
            mock_cred_refresh_cls.return_value = mock_refresh

            # WHEN
            result = scheduler._get_queue_aws_credentials(queue_id, role_arn, "session-1234", None)

            # THEN
            assert result is not None
            assert result.session is queue_boto3
            assert result.refresher is mock_refresh
            mock_q_boto3_cls.assert_called_once_with(
                deadline_client=scheduler._deadline,
                farm_id=scheduler._farm_id,
                fleet_id=scheduler._fleet_id,
                worker_id=scheduler._worker_id,
                queue_id=queue_id,
                role_arn=role_arn,
                os_user=None,
                interrupt_event=scheduler._shutdown,
                worker_persistence_dir=Path("/var/lib/deadline"),
                region=boto_session.region_name,
            )
            mock_cred_refresh_cls.assert_called_once_with(
                resource={"resource": queue_id, "role_arn": role_arn},
                session=queue_boto3,
                failure_callback=ANY,  # functools.partial(scheduler._queue_credentials_refresh_failed, hash_key),
            )
            assert scheduler._queue_aws_credentials[hash_key] is result

    @pytest.mark.parametrize(
        "exception",
        [
            pytest.param(
                DeadlineRequestWorkerOfflineError(Exception("inner")), id="worker offline"
            ),
            pytest.param(DeadlineRequestUnrecoverableError(Exception("inner")), id="unrecoverable"),
        ],
    )
    def test_new_credentials_raises(self, scheduler: WorkerScheduler, exception: Exception) -> None:
        """Test that when we create a new set of Queue credentials in _get_queue_aws_credentials
        but that raises a terminal exception, then we reraise the exception
        """

        with patch.object(scheduler_mod, "QueueBoto3Session") as mock_q_boto3_cls:
            # GIVEN
            queue_id = "queue-123456"
            role_arn = "arn:aws:...:RoleArn!"
            mock_q_boto3_cls.side_effect = exception

            # WHEN
            with pytest.raises(
                (DeadlineRequestWorkerOfflineError, DeadlineRequestUnrecoverableError)
            ) as exc_context:
                scheduler._get_queue_aws_credentials(queue_id, role_arn, "session-1234", None)

            # THEN
            assert exc_context.value is exception
            assert len(scheduler._queue_aws_credentials) == 0

    @pytest.mark.parametrize(
        "exception",
        [
            pytest.param(DeadlineRequestError(Exception("inner")), id="worker offline"),
            pytest.param(DeadlineRequestInterrupted(Exception("inner")), id="unrecoverable"),
        ],
    )
    def test_new_credentials_returns_none_on_exception(
        self, scheduler: WorkerScheduler, exception: Exception
    ) -> None:
        """Test that when we create a new set of Queue credentials in _get_queue_aws_credentials
        but that raises a recoverable exception, then we just return None
        """

        with patch.object(scheduler_mod, "QueueBoto3Session") as mock_q_boto3_cls:
            # GIVEN
            queue_id = "queue-123456"
            role_arn = "arn:aws:...:RoleArn!"
            mock_q_boto3_cls.side_effect = exception

            # WHEN
            result = scheduler._get_queue_aws_credentials(queue_id, role_arn, "session-1234", None)

            # THEN
            assert result is None
            assert len(scheduler._queue_aws_credentials) == 0


class TestShutdownSessions:
    """Test cases for the WorkerScheduler._shutdown_sessions() method"""

    @pytest.mark.parametrize(
        argnames="shutdown_grace",
        argvalues=(
            timedelta(minutes=1),
            timedelta(seconds=25),
            None,
        ),
        ids=(
            "grace-1-min",
            "grace-25-secs",
            "grace-None",
        ),
    )
    @pytest.mark.parametrize(
        argnames="shutdown_fail_message",
        argvalues=(
            "msg1",
            "msg2",
            None,
        ),
        ids=(
            "fail-msg-1",
            "fail-msg-2",
            "fail-msg-None",
        ),
    )
    def test_stops_sessions(
        self,
        scheduler: WorkerScheduler,
        shutdown_grace: timedelta | None,
        shutdown_fail_message: str | None,
    ) -> None:
        """Tests that when WorkerScheduler._shutdown_sessions() is called, that Session.stop() is
        called for all sessions using the WorkerScheduler._shutdown_grace and
        WorkerScheduler._shutdown_fail_message.
        """

        # GIVEN
        scheduler._shutdown_fail_message = shutdown_fail_message
        scheduler._shutdown_grace = shutdown_grace
        sessions = [MagicMock(), MagicMock()]
        scheduler._sessions = SessionMap(
            {f"session-{i}": session for i, session in enumerate(sessions)}
        )
        expected_executor_calls = [
            call(
                session.session.stop,
                grace_time=shutdown_grace,
                current_action_result="INTERRUPTED",
                fail_message=shutdown_fail_message,
            )
            for session in sessions
        ]

        with patch.object(scheduler, "_executor") as mock_executor:
            # WHEN
            scheduler._shutdown_sessions(shutdown_grace, shutdown_fail_message)

        # THEN
        executor_submit: MagicMock = mock_executor.submit
        executor_submit.assert_has_calls(expected_executor_calls)
        assert len(expected_executor_calls) == executor_submit.call_count


class TestShutdown:
    """Test cases for WorkerScheduler.shutdown()"""

    @pytest.mark.parametrize(
        argnames="fail_message",
        argvalues=(
            "msg1",
            "msg2",
            None,
        ),
        ids=(
            "fail-message-1",
            "fail-message-2",
            "fail-message-None",
        ),
    )
    def test_persists_fail_message(
        self,
        scheduler: WorkerScheduler,
        fail_message: str | None,
    ) -> None:
        """Tests that the fail_message argument passed in to WorkerScheduler.shutdown() is persisted
        to the WorkerScheduler._shutdown_fail_message attribute.
        """
        # WHEN
        scheduler.shutdown(fail_message=fail_message)

        # THEN
        if fail_message is None:
            assert scheduler._shutdown_fail_message is None
        else:
            assert scheduler._shutdown_fail_message == fail_message

    @pytest.mark.parametrize(
        argnames="grace_time",
        argvalues=(
            timedelta(minutes=1),
            timedelta(seconds=25),
            None,
        ),
        ids=(
            "grace-time-1-min",
            "grace-time-25-secs",
            "grace-time-None",
        ),
    )
    def test_persists_grace_time(
        self,
        scheduler: WorkerScheduler,
        grace_time: timedelta | None,
    ) -> None:
        """Tests that the grace_time argument passed in to WorkerScheduler.shutdown() is persisted
        to the WorkerScheduler._shutdown_grace_time attribute.
        """
        # WHEN
        scheduler.shutdown(grace_time=grace_time)

        # THEN
        if grace_time is None:
            assert scheduler._shutdown_grace is None
        else:
            assert scheduler._shutdown_grace == grace_time

    def test_sets_events(
        self,
        scheduler: WorkerScheduler,
    ) -> None:
        """Tests that the events used to signal the shutdown to the Scheduler's thread are set
        in the correct order. This should be:

        1.  Set the WorkerScheduler._shutdown event
        2.  Set the WorkerScheduler._wakeup event

        The order is important because the scheduler does a blocking wait on the
        WorkerScheduler._wakeup event. We need to be sure that the shutdown event is set first so
        that when the scheduler wakes up, it has been set.
        """
        # GIVEN
        with (
            patch.object(scheduler._shutdown, "set") as shutdown_set,
            patch.object(scheduler._wakeup, "set") as wakeup_set,
        ):
            # Ensure WorkerScheduler._shutdown event is set BEFORE tje WorkerScheduler._wakeup
            # event is set
            def shutdown_side_effect() -> None:
                wakeup_set.assert_not_called()

            shutdown_set.side_effect = shutdown_side_effect

            # WHEN
            scheduler.shutdown()

        # THEN
        shutdown_set.assert_called_once_with()
        wakeup_set.assert_called_once_with()


class TestQueueLogDirPath:
    """Test cases for WorkerScheduler._queue_log_dir_path()"""

    @pytest.fixture(params=("queue-1", "queue-2"))
    def queue_id(self, request: pytest.FixtureRequest) -> str:
        return request.param

    def test_correct_queue_path(
        self,
        queue_id: str,
        worker_logs_dir: Path,
        scheduler: WorkerScheduler,
    ) -> None:
        # WHEN
        result = scheduler._queue_log_dir_path(queue_id=queue_id)

        # THEN
        assert result == worker_logs_dir / queue_id


class TestSessionLogPath:
    """Test cases for WorkerScheduler._session_log_file_path()"""

    @pytest.fixture(params=("session-1", "session-2"))
    def session_id(self, request: pytest.FixtureRequest) -> str:
        return request.param

    @pytest.fixture(
        params=(
            Path("foo"),
            Path("bar"),
        ),
    )
    def queue_log_dir(self, request: pytest.FixtureRequest) -> Path:
        return request.param

    def test_correct_queue_path(
        self,
        queue_log_dir: Path,
        session_id: str,
        scheduler: WorkerScheduler,
    ) -> None:
        # WHEN
        result = scheduler._session_log_file_path(
            queue_log_dir=queue_log_dir,
            session_id=session_id,
        )

        # THEN
        assert result == queue_log_dir / f"{session_id}.log"
