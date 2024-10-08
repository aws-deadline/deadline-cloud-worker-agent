# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import PurePosixPath, PureWindowsPath
from threading import Event, RLock
from types import TracebackType
from typing import Generator, Iterable, Literal, Optional
from unittest.mock import patch, MagicMock, ANY

import pytest
from openjd.model import ParameterValue
import os

from openjd.model.v2023_09 import (
    Action,
    Environment,
    EnvironmentActions,
    EnvironmentScript,
    StepActions,
    StepScript,
    StepTemplate,
)
from openjd.sessions import (
    ActionState,
    ActionStatus,
    PathFormat,
    PathMappingRule,
    SessionUser,
    PosixSessionUser,
    WindowsSessionUser,
)

from deadline_worker_agent.api_models import EnvironmentAction, TaskRunAction
from deadline_worker_agent.sessions import Session
import deadline_worker_agent.sessions.session as session_mod
from deadline_worker_agent.sessions.session import (
    LOW_TRANSFER_RATE_THRESHOLD,
    LOW_TRANSFER_COUNT_THRESHOLD,
    CurrentAction,
    SessionActionStatus,
)
from deadline_worker_agent.sessions.actions import (
    EnterEnvironmentAction,
    ExitEnvironmentAction,
    RunStepTaskAction,
)
from deadline_worker_agent.sessions.job_entities import (
    EnvironmentDetails,
    JobAttachmentDetails,
    JobDetails,
    StepDetails,
)
from deadline_worker_agent.log_messages import (
    SessionActionLogEvent,
    SessionActionLogEventSubtype,
)
from deadline.job_attachments.models import (
    Attachments,
    JobAttachmentsFileSystem,
    JobAttachmentS3Settings,
)
from deadline.job_attachments.os_file_permission import (
    FileSystemPermissionSettings,
    PosixFileSystemPermissionSettings,
    WindowsFileSystemPermissionSettings,
    WindowsPermissionEnum,
)

from deadline.job_attachments.progress_tracker import (
    ProgressReportMetadata,
    ProgressStatus,
    SummaryStatistics,
)


@pytest.fixture
def os_user() -> Optional[SessionUser]:
    if os.name == "posix":
        return PosixSessionUser(user="some-user", group="some-group")
    elif os.name == "nt":
        return WindowsSessionUser(user="SomeUser", password="qwe123!@#")
    else:
        return None


@pytest.fixture
def asset_sync() -> MagicMock:
    """A fixture returning a Mock to be passed in place of a deadline.job_attachments. AssetSync
    instance when creating the Worker Agent Session instance"""
    return MagicMock()


@pytest.fixture
def session_action_queue() -> MagicMock:
    """A fixture returning a Mock to be passed in place of a SessionActionQueue when creating the
    Worker Agent Session instance"""
    return MagicMock()


@pytest.fixture
def env() -> dict[str, str] | None:
    """A fixture that represents the dictionary of environment variables and their values supplied
    the Open Job Description Session initializer"""
    return None


@pytest.fixture
def action_start_time() -> datetime:
    """A fixture that represents the start time of an action"""
    return datetime(2023, 1, 2, 3, 4, 5)


@pytest.fixture
def action_complete_time() -> datetime:
    """A fixture that represents the complete time of an action"""
    return datetime(2023, 1, 2, 3, 4, 5)


@pytest.fixture
def mock_openjd_session_cls() -> Generator[MagicMock, None, None]:
    """Mocks the Worker Agent Session module's import of the Open Job Description Session class"""
    with patch.object(session_mod, "OPENJDSession") as mock_openjd_session:
        yield mock_openjd_session


@pytest.fixture
def mock_openjd_session(mock_openjd_session_cls: MagicMock) -> MagicMock:
    """The mocked Open Job Description Session class instance"""
    return mock_openjd_session_cls.return_value


@pytest.fixture
def action_update_callback() -> MagicMock:
    """MagicMock action as the action update callback"""
    return MagicMock()


@pytest.fixture
def action_update_lock() -> MagicMock:
    """MagicMock action as the action update lock"""
    return MagicMock()


@pytest.fixture(autouse=True)
def mock_telemetry_event_for_sync_inputs() -> Generator[MagicMock, None, None]:
    with patch.object(session_mod, "record_sync_inputs_telemetry_event") as mock_telemetry_event:
        yield mock_telemetry_event


@pytest.fixture(autouse=True)
def mock_telemetry_event_for_sync_outputs() -> Generator[MagicMock, None, None]:
    with patch.object(session_mod, "record_sync_outputs_telemetry_event") as mock_telemetry_event:
        yield mock_telemetry_event


@pytest.fixture
def session(
    asset_sync: MagicMock,
    env: dict[str, str] | None,
    job_details: JobDetails,
    os_user: SessionUser | None,
    mock_openjd_session_cls: MagicMock,
    queue_id: str,
    session_action_queue: MagicMock,
    session_id: str,
    action_update_callback: MagicMock,
    action_update_lock: MagicMock,
) -> Session:
    """A fixture that creates and returns the Worker Session"""
    return Session(
        id=session_id,
        asset_sync=asset_sync,
        env=env,
        job_details=job_details,
        os_user=os_user,
        queue=session_action_queue,
        queue_id=queue_id,
        job_id="job-1234",
        action_update_callback=action_update_callback,
        action_update_lock=action_update_lock,
    )


@pytest.fixture
def run_step_task_action(
    action_id: str,
    step_id: str,
    task_id: str,
    command: str,
    on_run_args: list[str],
) -> RunStepTaskAction:
    """A fixture that provides a RunStepTaskAction"""
    return RunStepTaskAction(
        details=StepDetails(
            step_template=StepTemplate(
                name="Test",
                script=StepScript(
                    actions=StepActions(
                        onRun=Action(
                            command=command,
                            args=on_run_args,
                            cancelation=None,
                        ),
                    ),
                ),
            ),
            step_id=step_id,
        ),
        id=action_id,
        task_id=task_id,
        task_parameter_values=dict[str, ParameterValue](),
    )


@pytest.fixture
def enter_env_action(
    action_id: str,
    job_env_id: str,
) -> EnterEnvironmentAction:
    """A fixture that provides a EnterEnvironmentAction"""
    return EnterEnvironmentAction(
        details=EnvironmentDetails(
            environment=Environment(
                name="EnvName",
                script=EnvironmentScript(
                    actions=EnvironmentActions(
                        onEnter=Action(
                            command="test",
                        ),
                    ),
                ),
            ),
        ),
        id=action_id,
        job_env_id=job_env_id,
    )


@pytest.fixture
def exit_env_action(
    action_id: str,
    job_env_id: str,
) -> ExitEnvironmentAction:
    """A fixture that provides a ExitEnvironmentAction"""
    return ExitEnvironmentAction(
        id=action_id,
        environment_id=job_env_id,
    )


@pytest.fixture
def current_action(
    run_step_task_action: RunStepTaskAction,
    action_start_time: datetime,
    session: Session,
) -> CurrentAction:
    """A fixture that provides the current action of the Worker when entering the test case"""
    current_action = CurrentAction(
        definition=run_step_task_action,
        start_time=action_start_time,
    )
    session._current_action = current_action
    return current_action


@pytest.fixture(
    params=(
        ActionStatus(
            exit_code=1,
            state=ActionState.FAILED,
        ),
        ActionStatus(
            exit_code=1,
            state=ActionState.FAILED,
            fail_message="fail message",
        ),
        ActionStatus(
            exit_code=1,
            state=ActionState.FAILED,
            progress=50,
        ),
    ),
    ids=(
        "no-fail-msg-progress",
        "no-progress",
        "no-fail-msg",
    ),
)
def failed_action_status(request: pytest.FixtureRequest) -> ActionStatus:
    """A fixture providing a failed Open Job Description ActionStatus"""
    return request.param


@pytest.fixture(
    params=(
        ActionStatus(
            exit_code=1,
            state=ActionState.CANCELED,
        ),
        ActionStatus(
            exit_code=1,
            state=ActionState.CANCELED,
            fail_message="canceled message",
        ),
        ActionStatus(
            exit_code=1,
            state=ActionState.CANCELED,
            progress=50,
        ),
    ),
    ids=(
        "no-fail-msg-progress",
        "no-progress",
        "no-fail-msg",
    ),
)
def canceled_action_status(request: pytest.FixtureRequest) -> ActionStatus:
    """A fixture providing a canceled Open Job Description ActionStatus"""
    return request.param


@pytest.fixture(
    params=(
        ActionStatus(
            exit_code=0,
            state=ActionState.SUCCESS,
        ),
        ActionStatus(
            exit_code=0,
            state=ActionState.SUCCESS,
            status_message="status message",
        ),
        ActionStatus(
            exit_code=0,
            state=ActionState.SUCCESS,
            progress=99,
        ),
    ),
    ids=(
        "no-status-msg-progress",
        "no-progress",
        "no-status-msg",
    ),
)
def success_action_status(request: pytest.FixtureRequest) -> ActionStatus:
    """A fixture providing a successful Open Job Description ActionStatus"""
    return request.param


@pytest.fixture
def mock_mod_logger() -> Generator[MagicMock, None, None]:
    """Fixture that mocks the session module's logger"""
    with patch.object(session_mod, "logger") as mock_mod_logger:
        yield mock_mod_logger


class TestSessionInit:
    """Test cases for Session.__init__()"""

    def test_uses_action_updated_callback(
        self,
        session: Session,
        mock_openjd_session_cls: MagicMock,
    ) -> None:
        """Asserts that the Session.update_action method is called by the callback supplied to the
        Open Job Description session initializer."""
        # GIVEN
        mock_openjd_session_cls.assert_called_once()
        call = mock_openjd_session_cls.call_args_list[0]
        action_status = ActionStatus(state=ActionState.SUCCESS)

        # THEN
        with patch.object(session, "update_action") as mock_update_action:
            # WHEN
            call.kwargs["callback"](session.id, action_status)
            mock_update_action.assert_called_once_with(action_status)

    def test_creates_current_action_lock(
        self,
        session: Session,
    ) -> None:
        """Asserts that the Session creates a threading.RLock instance and assigns it to the
        _current_action_lock attribute. This test coverage complements additional test cases in this
        file that mock the _current_action_lock."""
        # GIVEN
        # threading.RLock is a function, NOT a class. One easy way to obtain the returned is to
        # instantiate one.
        lock_type = type(RLock())

        # THEN
        assert isinstance(session._current_action_lock, lock_type)

    @pytest.mark.parametrize(
        "path_mapping_rules",
        [
            pytest.param([], id="0 rules"),
            pytest.param(
                [
                    PathMappingRule(
                        source_path_format=PathFormat.POSIX,
                        source_path=PurePosixPath("/source/path"),
                        destination_path=PurePosixPath("/dest/path"),
                    )
                ],
                id="1 rule",
            ),
            pytest.param(
                [
                    PathMappingRule(
                        source_path_format=PathFormat.POSIX,
                        source_path=PurePosixPath("/source/path"),
                        destination_path=PurePosixPath("/dest/path"),
                    ),
                    PathMappingRule(
                        source_path_format=PathFormat.WINDOWS,
                        source_path=PureWindowsPath("C:/windows/source/path"),
                        destination_path=PurePosixPath("/linux/dest/path"),
                    ),
                ],
                id="multiple rules",
            ),
        ],
    )
    def test_has_path_mapping_rules(
        self,
        session: Session,
        mock_openjd_session_cls: MagicMock,
        path_mapping_rules: list[PathMappingRule],
    ):
        """Ensure that when we have path mapping rules that we're passing them to the Open Job Description session"""
        # GIVEN / WHEN / THEN
        assert session is not None
        mock_openjd_session_cls.assert_called_once()
        if path_mapping_rules:
            assert (
                path_mapping_rules == mock_openjd_session_cls.call_args.kwargs["path_mapping_rules"]
            )
        else:
            assert not mock_openjd_session_cls.call_args.kwargs.get("path_mapping_rules", False)

    @pytest.mark.parametrize(
        "env",
        [
            pytest.param([], id="0 env variables"),
            pytest.param(
                [{"DEADLINE_SESSION_ID": "mock_session_id"}],
                id="1 env variable",
            ),
            pytest.param(
                [
                    {
                        "DEADLINE_SESSION_ID": "mock_session_id",
                        "DEADLINE_FARM_ID": "mock_farm_id",
                        "DEADLINE_QUEUE_ID": "mock_queue_id",
                        "DEADLINE_JOB_ID": "mock_job_id",
                        "DEADLINE_FLEET_ID": "mock_fleet_id",
                        "DEADLINE_WORKER_ID": "mock_worker_id",
                    }
                ],
                id="multiple env variables",
            ),
        ],
    )
    def test_has_env_variables(
        self,
        session: Session,
        mock_openjd_session_cls: MagicMock,
        env: dict[str, str],
    ):
        """Ensure that when we have env variables that we're passing them to the Open Job Description session"""
        # GIVEN / WHEN / THEN
        assert session is not None
        mock_openjd_session_cls.assert_called_once()
        if env:
            assert env == mock_openjd_session_cls.call_args.kwargs["os_env_vars"]
        else:
            assert not mock_openjd_session_cls.call_args.kwargs.get("os_env_vars", False)


class TestSessionOuterRun:
    """Test cases for Session.run()"""

    @pytest.fixture(autouse=True)
    def mock_inner_run(
        self,
        session: Session,
    ) -> Generator[MagicMock, None, None]:
        """Fixture to patch Session._run() with a MagicMock and return it"""
        with patch.object(session, "_run") as mock_inner_run:
            yield mock_inner_run

    @pytest.fixture(autouse=True)
    def mock_sync_asset_inputs(
        self,
        session: Session,
    ) -> Generator[MagicMock, None, None]:
        """Fixture to patch Session.sync_asset_inputs with a MagicMock and return it"""
        with patch.object(session, "sync_asset_inputs") as mock_sync_asset_inputs:
            yield mock_sync_asset_inputs

    @pytest.fixture(autouse=True)
    def mock_cleanup(
        self,
        session: Session,
    ) -> Generator[MagicMock, None, None]:
        """Fixture to patch Session._cleanup with a MagicMock and return it"""
        with patch.object(session, "_cleanup") as mock_cleanup:
            yield mock_cleanup

    @pytest.mark.parametrize(
        argnames="inner_run_side_effect",
        argvalues=(
            Exception("some exception"),
            None,
        ),
        ids=(
            "with-exception",
            "no-exception",
        ),
    )
    def test_calls_cleanup(
        self,
        session: Session,
        inner_run_side_effect: Exception | None,
        mock_inner_run: MagicMock,
        mock_cleanup: MagicMock,
    ) -> None:
        """Tests that when Session.run() calls Session._cleanup() regardless of whether the
        internal call to Session._run() succeeds or raises an exception."""
        # GIVEN
        mock_inner_run.side_effect = inner_run_side_effect

        if inner_run_side_effect:
            # THEN
            with pytest.raises(type(inner_run_side_effect)) as raise_ctx:
                # WHEN
                session.run()

            # THEN
            assert raise_ctx.value is inner_run_side_effect
        else:
            # WHEN
            session.run()

        # THEN
        mock_cleanup.assert_called_once_with()

    def test_exception_stops(
        self,
        session: Session,
        mock_inner_run: MagicMock,
    ) -> None:
        """Tests that when Session.run() calls Session._run() and it raises an exception, that the
        Session._stop event is set and the Session._stop_fail_message is set with an appropriate
        message."""
        # GIVEN
        inner_run_exception = Exception("an exception msg")
        mock_inner_run.side_effect = inner_run_exception

        # THEN
        with pytest.raises(Exception) as raise_ctx:
            # WHEN
            session.run()

        # THEN
        assert session._stop.is_set()
        assert (
            session._stop_fail_message
            == f"Worker encountered an unexpected error: {inner_run_exception}"
        )
        assert raise_ctx.value is inner_run_exception

    def test_toggles_whether_running(self, session: Session, mock_inner_run: MagicMock) -> None:
        """Tests that the _stopped_running Event is cleared before running the inner and then set
        once we're done running."""

        # GIVEN
        event_is_set = True

        def inner_run_check_state():
            nonlocal event_is_set
            event_is_set = session._stopped_running.is_set()

        mock_inner_run.side_effect = inner_run_check_state

        # WHEN
        session.run()

        # THEN
        assert not event_is_set
        assert session._stopped_running.is_set()

    def test_not_running_with_cleanup_exception(
        self, session: Session, mock_cleanup: MagicMock
    ) -> None:
        """Tests that the _stopped_running Event is set even when the Session's cleanup method
        raises an exception."""

        # GIVEN
        mock_cleanup.side_effect = Exception("a message")

        # WHEN
        session.run()

        # THEN
        assert session._stopped_running.is_set()

    def test_warm_cache_does_not_throw_error(
        self,
        session: Session,
        session_action_queue: MagicMock,
    ):
        # GIVEN
        session_action_queue._job_entities.cache_entities.side_effect = Exception("An error")

        # WHEN
        session._warm_job_entities_cache()

        # THEN
        # it did not error
        session_action_queue._job_entities.cache_entities.assert_called_once()


class TestSessionSyncAssetInputs:
    @pytest.fixture(autouse=True)
    def mock_asset_sync(self, session: Session) -> Generator[MagicMock, None, None]:
        with patch.object(session, "_asset_sync") as mock_asset_sync:
            yield mock_asset_sync

    # This overrides the job_attachments_file_system fixture in tests/unit/conftest.py which feeds into
    # the job_attachment_details fixture
    @pytest.mark.parametrize(
        "job_attachments_file_system", [e.value for e in JobAttachmentsFileSystem]
    )
    @pytest.mark.skipif(os.name != "posix", reason="Posix-only test.")
    def test_asset_loading_method(
        self,
        session: Session,
        job_attachments_file_system: JobAttachmentsFileSystem,
        mock_asset_sync: MagicMock,
        mock_telemetry_event_for_sync_inputs: MagicMock,
        job_attachment_details: JobAttachmentDetails,
    ) -> None:
        """Tests that the job_attachments_file_system specified in session._job_details is properly passed to the sync_inputs function"""
        # GIVEN
        mock_ja_sync_inputs: MagicMock = mock_asset_sync.sync_inputs
        mock_ja_sync_inputs.return_value = (SummaryStatistics(), {})
        cancel = Event()

        # WHEN
        session.sync_asset_inputs(  # type: ignore
            cancel=cancel,
            job_attachment_details=job_attachment_details,
        )

        # THEN
        mock_ja_sync_inputs.assert_called_with(
            s3_settings=ANY,
            queue_id=ANY,
            job_id=ANY,
            session_dir=ANY,
            attachments=Attachments(
                manifests=ANY,
                fileSystem=job_attachments_file_system,
            ),
            fs_permission_settings=PosixFileSystemPermissionSettings(
                os_user="some-user",
                os_group="some-group",
                dir_mode=0o20,
                file_mode=0o20,
            ),
            storage_profiles_path_mapping_rules={},
            step_dependencies=None,
            on_downloading_files=ANY,
            os_env_vars=None,
        )

        mock_telemetry_event_for_sync_inputs.assert_called_once_with(
            "queue-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            SummaryStatistics(),
        )

    def test_sync_asset_inputs_with_fs_permission_settings(
        self,
        session: Session,
        mock_asset_sync: MagicMock,
        job_attachment_details: JobAttachmentDetails,
    ):
        """
        Tests that sync_inputs function is called with the correct fs_permission_settings
        argument based on the current OS.
        """
        # GIVEN
        mock_sync_inputs: MagicMock = mock_asset_sync.sync_inputs
        mock_sync_inputs.return_value = ({}, {})
        cancel = Event()

        expected_fs_permission_settings: Optional[FileSystemPermissionSettings] = None
        if os.name == "posix":
            expected_fs_permission_settings = PosixFileSystemPermissionSettings(
                os_user="some-user",
                os_group="some-group",
                dir_mode=0o20,
                file_mode=0o20,
            )
        elif os.name == "nt":
            expected_fs_permission_settings = WindowsFileSystemPermissionSettings(
                os_user="SomeUser",
                dir_mode=WindowsPermissionEnum.WRITE,
                file_mode=WindowsPermissionEnum.WRITE,
            )

        # WHEN
        session.sync_asset_inputs(  # type: ignore
            cancel=cancel,
            job_attachment_details=job_attachment_details,
        )

        # THEN
        mock_sync_inputs.assert_called_with(
            s3_settings=ANY,
            queue_id=ANY,
            job_id=ANY,
            session_dir=ANY,
            attachments=Attachments(
                manifests=ANY,
                fileSystem=ANY,
            ),
            fs_permission_settings=expected_fs_permission_settings,
            storage_profiles_path_mapping_rules={},
            step_dependencies=None,
            on_downloading_files=ANY,
            os_env_vars=None,
        )

    @pytest.mark.parametrize(
        "sync_asset_inputs_args_sequence, expected_error",
        [
            (
                [
                    {
                        "job_attachment_details": JobAttachmentDetails(
                            manifests=[],
                            job_attachments_file_system="COPIED",
                        )
                    }
                ],
                False,
            ),
            (
                [
                    {
                        "job_attachment_details": JobAttachmentDetails(
                            manifests=[],
                            job_attachments_file_system="COPIED",
                        )
                    },
                    {"step_dependencies": ["step-1"]},
                ],
                False,
            ),
            (
                [{"step_dependencies": ["step-1"]}],
                True,
            ),
            ([{"job_attachment_details": None}], True),
            ([{"step_dependencies": None}], True),
            ([{"job_attachment_details": None}, {"step_dependencies": None}], True),
        ],
    )
    def test_sync_asset_inputs(
        self,
        session: Session,
        mock_asset_sync: MagicMock,
        mock_telemetry_event_for_sync_inputs: MagicMock,
        sync_asset_inputs_args_sequence: list[dict[str, JobAttachmentDetails | list[str]]],
        expected_error: bool,
    ):
        """
        Tests 'sync_asset_inputs' with a sequence of arguments and checks if it raises an error as expected.
        For each test case, 'sync_asset_inputs' is called with each argument in the 'sync_asset_inputs_args_sequence'.
        It then checks whether the function raises an error or not, which should match the 'expected_error'.
        Also, asserts that 'record_sync_inputs_telemetry_event' is called with the correct arguments.
        """
        # GIVEN
        mock_ja_sync_inputs: MagicMock = mock_asset_sync.sync_inputs
        mock_ja_sync_inputs.return_value = (SummaryStatistics(), {})
        cancel = Event()

        if expected_error:
            # WHEN
            with pytest.raises(RuntimeError) as raise_ctx:
                for args in sync_asset_inputs_args_sequence:
                    session.sync_asset_inputs(cancel=cancel, **args)  # type: ignore
            # THEN
            assert (
                raise_ctx.value.args[0]
                == "Job attachments must be synchronized before downloading Step dependencies."
            )
        else:
            # WHEN
            for args in sync_asset_inputs_args_sequence:
                session.sync_asset_inputs(cancel=cancel, **args)  # type: ignore
            # THEN
            for call in mock_telemetry_event_for_sync_inputs.call_args_list:
                assert call[0] == (
                    "queue-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    SummaryStatistics(),
                )
            assert mock_telemetry_event_for_sync_inputs.call_count == len(
                sync_asset_inputs_args_sequence
            )

    def test_sync_asset_inputs_cancellation_by_low_transfer_rate(
        self,
        session: Session,
        mock_asset_sync: MagicMock,
    ):
        """
        Tests that the session is canceled if it observes a series of alarmingly low transfer rates.
        """

        # Mock out the Job Attachment's sync_inputs function to report multiple consecutive low transfer rates
        # (lower than the threshold) via callback function.
        def mock_sync_inputs(on_downloading_files, *args, **kwargs):
            low_transfer_rate_report = ProgressReportMetadata(
                status=ProgressStatus.DOWNLOAD_IN_PROGRESS,
                progress=0.0,
                transferRate=LOW_TRANSFER_RATE_THRESHOLD / 2,
                progressMessage="",
            )
            for _ in range(LOW_TRANSFER_COUNT_THRESHOLD):
                on_downloading_files(low_transfer_rate_report)
            return ({}, {})

        mock_asset_sync.sync_inputs = mock_sync_inputs
        mock_cancel = MagicMock(spec=Event)

        with (
            patch.object(session, "update_action") as mock_update_action,
            patch.object(
                session_mod, "record_sync_inputs_fail_telemetry_event"
            ) as mock_record_sync_inputs_fail_telemetry_event,
        ):
            session.sync_asset_inputs(  # type: ignore
                cancel=mock_cancel,
                job_attachment_details=JobAttachmentDetails(
                    manifests=[],
                    job_attachments_file_system=JobAttachmentsFileSystem.COPIED,
                ),
            )
        mock_cancel.set.assert_called_once()
        mock_update_action.assert_called_with(
            ActionStatus(
                state=ActionState.FAILED,
                fail_message=(
                    f"Input syncing failed due to successive low transfer rates (< {LOW_TRANSFER_RATE_THRESHOLD / 1000} KB/s). "
                    f"The transfer rate was below the threshold for the last {session._seconds_to_minutes_str(LOW_TRANSFER_COUNT_THRESHOLD)}."
                ),
            ),
        )
        mock_record_sync_inputs_fail_telemetry_event.assert_called_once_with(
            queue_id="queue-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            failure_reason=(
                "Insufficient download speed: "
                f"Input syncing failed due to successive low transfer rates (< {LOW_TRANSFER_RATE_THRESHOLD / 1000} KB/s). "
                f"The transfer rate was below the threshold for the last {session._seconds_to_minutes_str(LOW_TRANSFER_COUNT_THRESHOLD)}."
            ),
        )

    @pytest.mark.parametrize(
        "seconds, expected_str",
        [
            (0, "0 seconds"),
            (1, "1 second"),
            (30, "30 seconds"),
            (60, "1 minute"),
            (61, "1 minute 1 second"),
            (90, "1 minute 30 seconds"),
            (120, "2 minutes"),
            (121, "2 minutes 1 second"),
            (150, "2 minutes 30 seconds"),
        ],
    )
    def test_seconds_to_minutes_str(self, session: Session, seconds: int, expected_str: str):
        assert session._seconds_to_minutes_str(seconds) == expected_str


class TestSessionSyncAssetOutputs:
    @pytest.fixture(autouse=True)
    def mock_asset_sync(self, session: Session) -> Generator[MagicMock, None, None]:
        with patch.object(session, "_asset_sync") as mock_asset_sync:
            yield mock_asset_sync

    def test_sync_asset_outputs(
        self,
        action_id: str,
        queue_id: str,
        step_id: str,
        task_id: str,
        action_start_time: datetime,
        session: Session,
        job_attachment_details: JobAttachmentDetails,
        mock_asset_sync: MagicMock,
        mock_telemetry_event_for_sync_outputs: MagicMock,
    ):
        """
        Tests that session's '_sync_asset_outputs' calls Job Attachment's method 'sync_outputs' correctly.
        Also, asserts that 'record_sync_outputs_telemetry_event' is called once with the correct arguments.
        """
        # GIVEN
        mock_ja_sync_outputs: MagicMock = mock_asset_sync.sync_outputs
        mock_ja_sync_outputs.return_value = SummaryStatistics()
        current_action = CurrentAction(
            definition=RunStepTaskAction(
                details=StepDetails(
                    step_template=StepTemplate(
                        name="Test",
                        script=StepScript(
                            actions=StepActions(
                                onRun=Action(
                                    command="echo",
                                    args=["hello"],
                                ),
                            ),
                        ),
                    ),
                    step_id=step_id,
                ),
                id=action_id,
                task_id=task_id,
                task_parameter_values=dict[str, ParameterValue](),
            ),
            start_time=action_start_time,
        )
        session._job_attachment_details = job_attachment_details

        # WHEN
        session._sync_asset_outputs(current_action=current_action)  # type: ignore

        # THEN
        mock_ja_sync_outputs.assert_called_once_with(
            s3_settings=JobAttachmentS3Settings(
                rootPrefix="job_attachments",
                s3BucketName="job_attachments_bucket",
            ),
            attachments=Attachments(
                manifests=ANY,
                fileSystem=JobAttachmentsFileSystem.COPIED,
            ),
            queue_id=queue_id,
            job_id=ANY,
            step_id=step_id,
            task_id=task_id,
            session_action_id=action_id,
            start_time=ANY,
            session_dir=ANY,
            storage_profiles_path_mapping_rules={},
            on_uploading_files=ANY,
        )
        mock_telemetry_event_for_sync_outputs.assert_called_once_with(
            "queue-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            SummaryStatistics(),
        )


class TestSessionInnerRun:
    """Test cases for Session._run()"""

    def test_locking_semantics(
        self,
        session: Session,
    ) -> None:
        """Test that asserts that the _current_action_lock is entered before the method calls
        Session._start_action() and that _current_action_lock is exited afterwards."""

        # GIVEN
        with (
            patch.object(session, "_action_update_lock") as mock_action_update_lock,
            patch.object(session, "_current_action_lock") as mock_current_action_lock,
            patch.object(session, "_start_action") as mock_start_action,
        ):
            action_update_lock_enter: MagicMock = mock_action_update_lock.__enter__
            action_update_lock_exit: MagicMock = mock_action_update_lock.__exit__
            current_action_lock_enter: MagicMock = mock_current_action_lock.__enter__
            current_action_lock_exit: MagicMock = mock_current_action_lock.__exit__

            # THEN
            # Assert the correct order:
            #     1. self._action_update_lock is acquired
            #     2. self._current_action_lock is acquired
            #     3. Session._replace_assigned_actions_impl() is called
            #     4. self._current_action_lock is released
            #     5. self._action_update_lock is released
            def current_action_lock_enter_side_effect() -> None:
                action_update_lock_enter.assert_called_once_with()
                action_update_lock_exit.assert_not_called()
                current_action_lock_exit.assert_not_called()
                mock_start_action.assert_not_called()

            current_action_lock_enter.side_effect = current_action_lock_enter_side_effect

            def action_update_lock_enter_side_effect() -> None:
                current_action_lock_enter.assert_not_called()
                current_action_lock_exit.assert_not_called()
                action_update_lock_exit.assert_not_called()
                mock_start_action.assert_not_called()

            action_update_lock_enter.side_effect = action_update_lock_enter_side_effect

            def start_action_side_effect() -> CurrentAction | None:
                action_update_lock_enter.assert_called_once()
                current_action_lock_enter.assert_called_once()
                current_action_lock_exit.assert_not_called()
                action_update_lock_exit.assert_not_called()

                # Set the stop event so that the run loop exits
                session._stop.set()
                return None

            mock_start_action.side_effect = start_action_side_effect

            def current_action_lock_exit_side_effect(
                exc_type: type[BaseException] | None,
                exc_val: BaseException | None,
                exc_tb: TracebackType | None,
            ) -> None:
                action_update_lock_enter.assert_called_once()
                current_action_lock_enter.assert_called_once()
                mock_start_action.assert_called()
                action_update_lock_exit.assert_not_called()

            current_action_lock_exit.side_effect = current_action_lock_exit_side_effect

            def action_update_lock_exit_side_effect(
                exc_type: type[BaseException] | None,
                exc_val: BaseException | None,
                exc_tb: TracebackType | None,
            ) -> None:
                action_update_lock_enter.assert_called_once()
                current_action_lock_enter.assert_called_once()
                current_action_lock_exit.assert_called_once()
                mock_start_action.assert_called_once()

            action_update_lock_exit.side_effect = action_update_lock_exit_side_effect

            # WHEN
            session._run()

        # THEN
        action_update_lock_enter.assert_called_once_with()
        action_update_lock_exit.assert_called_once_with(None, None, None)
        current_action_lock_enter.assert_called_once()
        current_action_lock_exit.assert_called_once_with(None, None, None)
        mock_start_action.assert_called_once()


class TestSessionCancelActions:
    """Test cases for Session.cancel_actions()"""

    def test_locking_semantics(
        self,
        session: Session,
    ) -> None:
        """Test that asserts that the _current_action_lock is entered before the method calls
        Session._cancel_actions_impl() and that _current_action_lock is exited afterwards."""
        # GIVEN
        action_ids: list[str] = []

        with (
            patch.object(session, "_cancel_actions_impl") as mock_cancel_actions_impl,
            patch.object(session, "_action_update_lock") as mock_action_update_lock,
            patch.object(session, "_current_action_lock") as mock_current_action_lock,
        ):
            action_update_lock_enter: MagicMock = mock_action_update_lock.__enter__
            action_update_lock_exit: MagicMock = mock_action_update_lock.__exit__
            current_action_lock_enter: MagicMock = mock_current_action_lock.__enter__
            current_action_lock_exit: MagicMock = mock_current_action_lock.__exit__

            # Assert the correct order:
            #     1. self._action_update_lock is acquired
            #     2. self._current_action_lock is acquired
            #     3. Session._cancel_actions_impl() is called
            #     4. self._current_action_lock is released
            #     5. self._action_update_lock is released
            def current_action_lock_enter_side_effect() -> None:
                action_update_lock_enter.assert_called_once_with()
                action_update_lock_exit.assert_not_called()
                current_action_lock_exit.assert_not_called()
                mock_cancel_actions_impl.assert_not_called()

            current_action_lock_enter.side_effect = current_action_lock_enter_side_effect

            def action_update_lock_enter_side_effect() -> None:
                current_action_lock_enter.assert_not_called()
                current_action_lock_exit.assert_not_called()
                action_update_lock_exit.assert_not_called()
                mock_cancel_actions_impl.assert_not_called()

            action_update_lock_enter.side_effect = action_update_lock_enter_side_effect

            def mock_cancel_actions_impl_side_effect(*, action_ids: list[str]) -> None:
                action_update_lock_enter.assert_called_once()
                current_action_lock_enter.assert_called_once()
                current_action_lock_exit.assert_not_called()
                action_update_lock_exit.assert_not_called()

            mock_cancel_actions_impl.side_effect = mock_cancel_actions_impl_side_effect

            def current_action_lock_exit_side_effect(
                exc_type: type[BaseException] | None,
                exc_val: BaseException | None,
                exc_tb: TracebackType | None,
            ) -> None:
                action_update_lock_enter.assert_called_once()
                current_action_lock_enter.assert_called_once()
                mock_cancel_actions_impl.assert_called()
                action_update_lock_exit.assert_not_called()

            current_action_lock_exit.side_effect = current_action_lock_exit_side_effect

            def action_update_lock_exit_side_effect(
                exc_type: type[BaseException] | None,
                exc_val: BaseException | None,
                exc_tb: TracebackType | None,
            ) -> None:
                action_update_lock_enter.assert_called_once()
                current_action_lock_enter.assert_called_once()
                current_action_lock_exit.assert_called_once()
                mock_cancel_actions_impl.assert_called_once()

            action_update_lock_exit.side_effect = action_update_lock_exit_side_effect

            # WHEN
            session.cancel_actions(action_ids=action_ids)

        # THEN
        mock_cancel_actions_impl.assert_called_once_with(action_ids=action_ids)
        action_update_lock_enter.assert_called_once_with()
        action_update_lock_exit.assert_called_once_with(None, None, None)
        current_action_lock_enter.assert_called_once()
        current_action_lock_exit.assert_called_once_with(None, None, None)


class TestSessionCancelActionsImpl:
    """Test cases for Session._cancel_actions_impl()"""

    def test_cancels_current_action(
        self,
        session: Session,
        mock_openjd_session: MagicMock,
        current_action: CurrentAction,
    ) -> None:
        """Asserts that the current action is canceled if cancel_actions() is called with the
        corresponding action ID in the action_ids argument."""
        # GIVEN
        openjd_cancel_action: MagicMock = mock_openjd_session.cancel_action

        # WHEN
        session._cancel_actions_impl(action_ids=[current_action.definition.id])

        # THEN
        openjd_cancel_action.assert_called_once_with(time_limit=None)


class TestSessionReplaceAssignedActions:
    """Test cases for Session.replace_assigned_actions()"""

    def test_locking_semantics(
        self,
        session: Session,
    ) -> None:
        # GIVEN
        actions: list[EnvironmentAction | TaskRunAction] = []

        with (
            patch.object(
                session, "_replace_assigned_actions_impl"
            ) as mock_replace_assigned_actions_impl,
            patch.object(session, "_current_action_lock") as mock_current_action_lock,
        ):
            lock_enter: MagicMock = mock_current_action_lock.__enter__
            lock_exit: MagicMock = mock_current_action_lock.__exit__

            # Assert the correct order:
            #     1. Lock is entered (aka acquired)
            #     2. Session._replace_assigned_actions_impl() is called
            #     3. Lock is exited (aka released)
            def replace_assigned_actions_impl_side_effect(
                *, actions: Iterable[EnvironmentAction | TaskRunAction]
            ) -> None:
                # THEN
                lock_enter.assert_called_once_with()
                lock_exit.assert_not_called()

            mock_replace_assigned_actions_impl.side_effect = (
                replace_assigned_actions_impl_side_effect
            )

            # WHEN
            session.replace_assigned_actions(actions=actions)

        # THEN
        mock_replace_assigned_actions_impl.assert_called_once_with(actions=actions)
        lock_exit.assert_called_once()


class TestSessionUpdateAction:
    """Test cases for Session.update_action()"""

    def test_locking_semantics(
        self,
        session: Session,
        # We don't use the value of this fixture, but requiring it has the side-effect of assigning
        # it as the current action of the session
        current_action: CurrentAction,
        success_action_status: ActionStatus,
    ) -> None:
        """Test that asserts that the _current_action_lock is entered before the method calls
        Session._action_updated_impl() and that _current_action_lock is exited afterwards."""
        # GIVEN
        with (
            patch.object(session, "_current_action_lock") as mock_current_action_lock,
            patch.object(session, "_action_updated_impl") as mock_action_updated_impl,
            patch.object(session, "_report_action_update") as mock_report_action_update,
        ):
            current_action_lock_enter: MagicMock = mock_current_action_lock.__enter__
            current_action_lock_exit: MagicMock = mock_current_action_lock.__exit__

            # Assert the correct order:
            #     1. Lock is entered (aka acquired)
            #     2. Session._action_updated_impl() is called
            #     3. Lock is exited (aka released)
            #     4. Session queue is forwarded the session action update
            def mock_action_updated_impl_side_effect(
                *,
                action_status: ActionStatus,
                now: datetime,
            ) -> None:
                # THEN
                current_action_lock_enter.assert_called_once_with()
                current_action_lock_exit.assert_not_called()

            mock_action_updated_impl.side_effect = mock_action_updated_impl_side_effect

            # WHEN
            session.update_action(success_action_status)

        # THEN
        mock_action_updated_impl.assert_called_once()
        current_action_lock_exit.assert_called_once()
        mock_report_action_update.assert_not_called()

    def test_timeout_messaging(
        self,
        session: Session,
        # We don't use the value of this fixture, but requiring it has the side-effect of assigning
        # it as the current action of the session
        current_action: CurrentAction,
    ) -> None:
        """Test that when an action is reported as TIMEOUT then we:
        1) Cancel all subsequent tasks as NEVER_ATTEMPTED; and
        2) Have an appropriate failure message on the action.
        """
        # GIVEN
        status = ActionStatus(state=ActionState.TIMEOUT, exit_code=-1, progress=24.4)
        with (
            patch.object(session._queue, "cancel_all") as mock_cancel_all,
            patch.object(session, "_report_action_update") as mock_report_action_update,
        ):
            # WHEN
            session.update_action(status)

        # THEN
        mock_cancel_all.assert_called_once_with(message=ANY, ignore_env_exits=True)
        assert "TIMEOUT" in mock_cancel_all.call_args.kwargs["message"]
        mock_report_action_update.assert_called_once()
        session_status = mock_report_action_update.call_args.args[0]
        assert session_status.completed_status == "FAILED"
        called_with_status = session_status.status
        assert called_with_status.state == ActionState.TIMEOUT
        assert "TIMEOUT" in called_with_status.fail_message
        assert called_with_status.exit_code == status.exit_code
        assert called_with_status.progress == status.progress


class TestSessionActionUpdatedImpl:
    """Test cases for Session._action_updated_impl()"""

    @pytest.fixture(autouse=True)
    def mock_report_action_update(self, session: Session) -> Generator[MagicMock, None, None]:
        """Returns a patched mock for Session._report_action_update"""
        with patch.object(session, "_report_action_update") as mock_report_action_update:
            yield mock_report_action_update

    def test_failed_enter_env(
        self,
        action_id: str,
        session: Session,
        session_action_queue: MagicMock,
        action_start_time: datetime,
        action_complete_time: datetime,
        failed_action_status: ActionStatus,
        mock_report_action_update: MagicMock,
    ) -> None:
        """Tests that if a environment enter action fails (the Open Job Description action), that the action
        failure is returned, and that any pending actions other than ENV_EXITS are marked as
        NEVER_ATTEMPTED with a message that explains that the env enter action failed."""
        # GIVEN
        job_env_id = "job_env_id"
        current_action = CurrentAction(
            definition=EnterEnvironmentAction(
                details=EnvironmentDetails(
                    environment=Environment(
                        name="EnvName",
                        script=EnvironmentScript(
                            actions=EnvironmentActions(
                                onEnter=Action(
                                    command="test",
                                ),
                            ),
                        ),
                    ),
                ),
                id=action_id,
                job_env_id=job_env_id,
            ),
            start_time=action_start_time,
        )
        session._current_action = current_action
        queue_cancel_all: MagicMock = session_action_queue.cancel_all
        expected_next_action_message = failed_action_status.fail_message or (
            f"Previous action failed: {current_action.definition.id}"
        )
        expected_action_update = SessionActionStatus(
            id=action_id,
            status=failed_action_status,
            start_time=action_start_time,
            completed_status="FAILED",
            end_time=action_complete_time,
        )

        with patch.object(session, "_sync_asset_outputs") as mock_sync_asset_outputs:
            # WHEN
            session._action_updated_impl(
                action_status=failed_action_status,
                now=action_complete_time,
            )

        # THEN
        mock_report_action_update.assert_called_once_with(expected_action_update)
        queue_cancel_all.assert_called_once_with(
            message=expected_next_action_message,
            ignore_env_exits=True,
        )
        mock_sync_asset_outputs.assert_not_called()
        assert session._current_action is None, "Current session action emptied"

    def test_failed_task_run(
        self,
        action_id: str,
        session: Session,
        session_action_queue: MagicMock,
        step_id: str,
        task_id: str,
        action_start_time: datetime,
        action_complete_time: datetime,
        failed_action_status: ActionStatus,
        mock_report_action_update: MagicMock,
    ) -> None:
        """Tests that if a task run fails (the Open Job Description action), that job attachment output
        sync is not performed, the action failure is returned, and that any pending actions are
        marked as NEVER_ATTEMPTED."""
        # GIVEN
        current_action = CurrentAction(
            definition=RunStepTaskAction(
                details=StepDetails(
                    step_template=StepTemplate(
                        name="Test",
                        script=StepScript(
                            actions=StepActions(
                                onRun=Action(
                                    command="echo",
                                    args=["hello"],
                                ),
                            ),
                        ),
                    ),
                    step_id=step_id,
                ),
                id=action_id,
                task_id=task_id,
                task_parameter_values=dict[str, ParameterValue](),
            ),
            start_time=action_start_time,
        )
        session._current_action = current_action
        queue_cancel_all: MagicMock = session_action_queue.cancel_all
        expected_next_action_message = failed_action_status.fail_message or (
            f"Previous action failed: {current_action.definition.id}"
        )
        expected_action_update = SessionActionStatus(
            id=action_id,
            status=failed_action_status,
            start_time=action_start_time,
            completed_status="FAILED",
            end_time=action_complete_time,
        )

        with patch.object(session, "_sync_asset_outputs") as mock_sync_asset_outputs:
            # WHEN
            session._action_updated_impl(
                action_status=failed_action_status,
                now=action_complete_time,
            )

        # THEN
        mock_report_action_update.assert_called_once_with(expected_action_update)
        queue_cancel_all.assert_called_once_with(
            message=expected_next_action_message,
            ignore_env_exits=True,
        )
        assert session._current_action is None, "Current session action emptied"
        mock_sync_asset_outputs.assert_not_called()

    def test_success_task_run(
        self,
        action_id: str,
        session_action_queue: MagicMock,
        session: Session,
        action_start_time: datetime,
        action_complete_time: datetime,
        step_id: str,
        success_action_status: ActionStatus,
        task_id: str,
        mock_report_action_update: MagicMock,
    ) -> None:
        """Tests that if a task run succeeds (the Open Job Description action), that job attachment output
        sync is performed, and AFTER that, the action success is returned."""
        # GIVEN
        current_action = CurrentAction(
            definition=RunStepTaskAction(
                details=StepDetails(
                    step_template=StepTemplate(
                        name="Test",
                        script=StepScript(
                            actions=StepActions(
                                onRun=Action(
                                    command="echo",
                                    args=["hello"],
                                ),
                            ),
                        ),
                    ),
                    step_id=step_id,
                ),
                id=action_id,
                task_id=task_id,
                task_parameter_values=dict[str, ParameterValue](),
            ),
            start_time=action_start_time,
        )
        session._current_action = current_action
        queue_cancel_all: MagicMock = session_action_queue.cancel_all
        expected_action_update = SessionActionStatus(
            id=action_id,
            status=success_action_status,
            start_time=action_start_time,
            completed_status="SUCCEEDED",
            end_time=action_complete_time,
        )

        def mock_now(*arg, **kwarg) -> datetime:
            return action_complete_time

        with (
            patch.object(session_mod, "datetime") as mock_datetime,
            patch.object(session, "_sync_asset_outputs") as mock_sync_asset_outputs,
        ):
            mock_datetime.now.side_effect = mock_now

            # Assert that reporting the action update happens AFTER syncing the output job
            # attachments.
            def sync_asset_outputs_side_effect(*, current_action: CurrentAction) -> None:
                mock_report_action_update.assert_not_called()

            mock_sync_asset_outputs.side_effect = sync_asset_outputs_side_effect

            # WHEN
            session._action_updated_impl(
                action_status=success_action_status,
                now=action_complete_time,
            )

        # THEN
        mock_report_action_update.assert_called_once_with(expected_action_update)
        queue_cancel_all.assert_not_called()
        assert session._current_action is None, "Current session action emptied"
        mock_sync_asset_outputs.assert_called_once_with(current_action=current_action)

    def test_success_task_run_fail_output_sync(
        self,
        action_id: str,
        session_action_queue: MagicMock,
        session: Session,
        action_start_time: datetime,
        action_complete_time: datetime,
        step_id: str,
        success_action_status: ActionStatus,
        task_id: str,
        mock_report_action_update: MagicMock,
    ) -> None:
        """Tests that if a task run succeeds (the Open Job Description action), but the job attachment output
        sync fails, the action failure is returned, and any pending actions are marked as
        NEVER_ATTEMPTED."""
        # GIVEN
        current_action = CurrentAction(
            definition=RunStepTaskAction(
                details=StepDetails(
                    step_template=StepTemplate(
                        name="Test",
                        script=StepScript(
                            actions=StepActions(
                                onRun=Action(
                                    command="echo",
                                    args=["hello"],
                                ),
                            ),
                        ),
                    ),
                    step_id=step_id,
                ),
                id=action_id,
                task_id=task_id,
                task_parameter_values=dict[str, ParameterValue](),
            ),
            start_time=action_start_time,
        )
        session._current_action = current_action
        queue_cancel_all: MagicMock = session_action_queue.cancel_all
        sync_outputs_exception_msg = "syncing outputs fail message"
        sync_outputs_exception = Exception(sync_outputs_exception_msg)
        expected_fail_action_status = ActionStatus(
            state=ActionState.FAILED,
            fail_message=f"Failed to sync job output attachments for {current_action.definition.id}: {sync_outputs_exception_msg}",
        )
        expected_action_update = SessionActionStatus(
            id=action_id,
            status=expected_fail_action_status,
            start_time=action_start_time,
            completed_status="FAILED",
            end_time=action_complete_time,
        )

        def mock_now(*arg, **kwarg) -> datetime:
            return action_complete_time

        with (
            patch.object(session_mod, "datetime") as mock_datetime,
            patch.object(
                session, "_sync_asset_outputs", side_effect=sync_outputs_exception
            ) as mock_sync_asset_outputs,
        ):
            mock_datetime.now.side_effect = mock_now

            # WHEN
            session._action_updated_impl(
                action_status=success_action_status,
                now=action_complete_time,
            )

        # THEN
        mock_report_action_update.assert_called_once_with(expected_action_update)
        queue_cancel_all.assert_called_once_with(
            message=expected_fail_action_status.fail_message,
            ignore_env_exits=True,
        )
        assert session._current_action is None, "Current session action emptied"
        mock_sync_asset_outputs.assert_called_once_with(current_action=current_action)

    def test_logs_succeeded(
        self,
        action_complete_time: datetime,
        current_action: CurrentAction,
        mock_mod_logger: MagicMock,
        session: Session,
        success_action_status: ActionStatus,
    ) -> None:
        """Tests that succeeded actions are logged"""
        # WHEN
        session._action_updated_impl(
            action_status=success_action_status,
            now=action_complete_time,
        )
        # This because the _action_update_impl submits a future to this thread pool executor
        # The test assertion depends on this future completing and so there's a race condition
        # if we do not wait for the thread pool to shutdown and all futures to complete.
        session._executor.shutdown()

        # THEN
        mock_mod_logger.info.assert_called_once()
        assert isinstance(mock_mod_logger.info.call_args.args[-1], SessionActionLogEvent)
        assert (
            mock_mod_logger.info.call_args.args[-1].subtype
            == SessionActionLogEventSubtype.END.value
        )
        assert mock_mod_logger.info.call_args.args[-1].status == "SUCCEEDED"
        assert mock_mod_logger.info.call_args.args[-1].action_id == current_action.definition.id

    def test_logs_failed(
        self,
        action_complete_time: datetime,
        current_action: CurrentAction,
        mock_mod_logger: MagicMock,
        session: Session,
        failed_action_status: ActionStatus,
    ) -> None:
        """Tests that failed actions are logged"""
        # WHEN
        session._action_updated_impl(
            action_status=failed_action_status,
            now=action_complete_time,
        )

        # THEN
        mock_mod_logger.info.assert_called_once()
        assert isinstance(mock_mod_logger.info.call_args.args[0], SessionActionLogEvent)
        assert (
            mock_mod_logger.info.call_args.args[0].subtype == SessionActionLogEventSubtype.END.value
        )
        assert mock_mod_logger.info.call_args.args[0].status == "FAILED"
        assert mock_mod_logger.info.call_args.args[0].action_id == current_action.definition.id

    def test_logs_canceled(
        self,
        action_complete_time: datetime,
        current_action: CurrentAction,
        mock_mod_logger: MagicMock,
        session: Session,
        canceled_action_status: ActionStatus,
    ) -> None:
        """Tests that canceled actions are logged"""
        # WHEN
        session._action_updated_impl(
            action_status=canceled_action_status,
            now=action_complete_time,
        )

        # THEN
        mock_mod_logger.info.assert_called_once()
        assert isinstance(mock_mod_logger.info.call_args.args[0], SessionActionLogEvent)
        assert (
            mock_mod_logger.info.call_args.args[0].subtype == SessionActionLogEventSubtype.END.value
        )
        assert mock_mod_logger.info.call_args.args[0].status == "CANCELED"
        assert mock_mod_logger.info.call_args.args[0].action_id == current_action.definition.id


@pytest.mark.usefixtures("mock_openjd_session")
class TestStartCancelingCurrentAction:
    """Test cases for Session._start_canceling_current_action()"""

    @pytest.fixture(
        params=(
            timedelta(minutes=1),
            timedelta(seconds=22),
            None,
        ),
        ids=(
            "time-limit-1-min",
            "time-limit-22-secs",
            "time-limit-None",
        ),
    )
    def time_limit(self, request: pytest.FixtureRequest) -> timedelta | None:
        return request.param

    def test_calls_current_action_cancel(
        self,
        session: Session,
        time_limit: timedelta | None,
        current_action: CurrentAction,
    ) -> None:
        """Tests that Session._start_canceling_current_action() calls the current action
        definition's cancel() method and forwards the session and time_limit arguments"""
        # GIVEN
        with patch.object(current_action.definition, "cancel") as mock_current_action_cancel:
            # WHEN
            session._start_canceling_current_action(time_limit=time_limit)

        # THEN
        mock_current_action_cancel.assert_called_once_with(
            session=session,
            time_limit=time_limit,
        )

    def test_logs_cancelation(
        self,
        session: Session,
        mock_mod_logger: MagicMock,
        time_limit: timedelta | None,
        current_action: CurrentAction,
    ) -> None:
        """Tests that Session._start_canceling_current_action() calls the current action
        definition's cancel() method and forwards the session and time_limit arguments"""
        # GIVEN
        logger_info: MagicMock = mock_mod_logger.info

        # WHEN
        session._start_canceling_current_action(time_limit=time_limit)

        # THEN
        logger_info.assert_called_once()
        assert isinstance(logger_info.call_args.args[0], SessionActionLogEvent)
        assert logger_info.call_args.args[0].subtype == SessionActionLogEventSubtype.CANCEL


class TestSessionStop:
    """Tests for Session.stop()"""

    @pytest.fixture(
        params=(
            "INTERRUPTED",
            "FAILED",
        ),
    )
    def current_action_result(
        self, request: pytest.FixtureRequest
    ) -> Literal["INTERRUPTED", "FAILED"]:
        return request.param

    @pytest.fixture(
        params=(
            "INTERRUPTED",
            "FAILED",
        ),
    )
    def fail_message(self, request: pytest.FixtureRequest) -> str | None:
        return request.param

    @pytest.fixture(
        params=(
            timedelta(minutes=1),
            timedelta(seconds=9),
            None,
        ),
    )
    def grace_time(self, request: pytest.FixtureRequest) -> timedelta | None:
        return request.param

    def test_persists_current_action_result(
        self,
        session: Session,
        current_action_result: Literal["INTERRUPTED", "FAILED"],
        fail_message: str | None,
        grace_time: timedelta | None,
    ) -> None:
        """Tests that calling Session.stop() with a current_action_result kwarg that the value is
        persisted to Session._stop_current_action_result"""
        # WHEN
        session.stop(
            current_action_result=current_action_result,
            fail_message=fail_message,
            grace_time=grace_time,
        )

        # THEN
        assert session._stop_current_action_result == current_action_result

    def test_persists_fail_message(
        self,
        session: Session,
        current_action_result: Literal["INTERRUPTED", "FAILED"],
        fail_message: str | None,
        grace_time: timedelta | None,
    ) -> None:
        """Tests that calling Session.stop() with a fail_message kwarg that the value is
        persisted to Session._stop_fail_message"""
        # WHEN
        session.stop(
            current_action_result=current_action_result,
            fail_message=fail_message,
            grace_time=grace_time,
        )

        # THEN
        assert session._stop_fail_message == fail_message

    def test_persists_grace_time(
        self,
        session: Session,
        current_action_result: Literal["INTERRUPTED", "FAILED"],
        fail_message: str | None,
        grace_time: timedelta | None,
    ) -> None:
        """Tests that calling Session.stop() with a grace_time kwarg that the value is
        persisted to Session._stop_grace_time"""
        # WHEN
        session.stop(
            current_action_result=current_action_result,
            fail_message=fail_message,
            grace_time=grace_time,
        )

        # THEN
        assert session._stop_grace_time == grace_time

    def test_sets_stop_event(
        self,
        session: Session,
        current_action_result: Literal["INTERRUPTED", "FAILED"],
        fail_message: str | None,
        grace_time: timedelta | None,
    ) -> None:
        """Tests that calling Session.stop() sets the Session._stop event"""
        # GIVEN
        assert not session._stop.is_set()

        # WHEN
        session.stop(
            current_action_result=current_action_result,
            fail_message=fail_message,
            grace_time=grace_time,
        )

        # THEN
        assert session._stop.is_set()


class TestSessionCleanup:
    """Tests for the Session._cleanup() method"""

    @pytest.mark.parametrize(
        argnames="stop_current_action_result",
        argvalues=(
            "INTERRUPTED",
            "FAILED",
        ),
        ids=(
            "action-interrupted",
            "action-failed",
        ),
    )
    @pytest.mark.parametrize(
        argnames="stop_fail_message",
        argvalues=("msg1", "msg2", None),
        ids=(
            "fail-msg-1",
            "fail-msg-2",
            "fail-msg-None",
        ),
    )
    def test_reports_stop_action_msg(
        self,
        session: Session,
        stop_current_action_result: Literal["INTERRUPTED", "FAILED"],
        stop_fail_message: str | None,
        current_action: CurrentAction,
        action_complete_time: datetime,
    ) -> None:
        # GIVEN
        session._stop_current_action_result = stop_current_action_result
        session._stop_fail_message = stop_fail_message

        with (
            patch.object(session, "_report_action_update") as mock_report_action_update,
            patch.object(session_mod, "datetime") as datetime_mock,
        ):
            datetime_mock.now.return_value = action_complete_time

            # WHEN
            session._cleanup()

        # THEN
        assert session._interrupted is True
        mock_report_action_update.assert_called_once_with(
            SessionActionStatus(
                completed_status=stop_current_action_result,
                start_time=current_action.start_time,
                end_time=action_complete_time,
                id=current_action.definition.id,
                status=ActionStatus(
                    state=ActionState.CANCELED,
                    fail_message=stop_fail_message,
                ),
            )
        )

    def test_calls_queue_cancel_all(
        self,
        session: Session,
        session_action_queue: MagicMock,
    ) -> None:
        """Tests that Session._cleanup() cancels all queued actions as NEVER_ATTEMPTED and forwards
        any previously set failure message from Session._stop_fail_message."""
        # GIVEN
        mock_queue_cancel_all: MagicMock = session_action_queue.cancel_all

        # WHEN
        session._cleanup()

        # THEN
        mock_queue_cancel_all.assert_called_once_with(
            message=session._stop_fail_message,
        )

    def test_calls_openjd_cleanup(
        self,
        session: Session,
        mock_openjd_session: MagicMock,
    ) -> None:
        # GIVEN
        openjd_session_cleanup: MagicMock = mock_openjd_session.cleanup

        # Mock Session._monitor_action which is used to poll the Open Job Description session status
        with patch.object(session, "_monitor_action", return_value=[]):
            # WHEN
            session._cleanup()

        # THEN
        openjd_session_cleanup.assert_called_once_with()

    @pytest.fixture()
    def mock_asset_sync(self, session: Session) -> Generator[MagicMock, None, None]:
        with patch.object(session, "_asset_sync") as mock_asset_sync:
            yield mock_asset_sync

    def test_calls_asset_sync_cleanup(
        self,
        session: Session,
        job_attachment_details: JobAttachmentDetails,
        mock_asset_sync: MagicMock,
        mock_openjd_session: MagicMock,
    ) -> None:
        # GIVEN
        mock_asset_sync_cleanup: MagicMock = mock_asset_sync.cleanup_session
        session._job_attachment_details = job_attachment_details
        assert session._os_user

        # WHEN
        session._cleanup()

        # THEN
        mock_asset_sync_cleanup.assert_called_once_with(
            session_dir=mock_openjd_session.working_directory,
            file_system=job_attachment_details.job_attachments_file_system,
            os_user=session._os_user.user,
        )

    def test_asset_sync_cleanup_calls_with_none_os_user(
        self,
        session: Session,
        job_attachment_details: JobAttachmentDetails,
        mock_asset_sync: MagicMock,
        mock_openjd_session: MagicMock,
    ) -> None:
        # GIVEN
        mock_asset_sync_cleanup: MagicMock = mock_asset_sync.cleanup_session
        session._job_attachment_details = job_attachment_details
        session._os_user = None

        # WHEN
        session._cleanup()

        # THEN
        mock_asset_sync_cleanup.assert_called_once_with(
            session_dir=mock_openjd_session.working_directory,
            file_system=job_attachment_details.job_attachments_file_system,
            os_user=None,
        )


class TestSessionStartAction:
    """Tests for Session._start_action()"""

    @pytest.mark.parametrize(
        argnames="exception_msg",
        argvalues=(
            "msg1",
            "msg2",
        ),
        ids=(
            "exception-1",
            "exception-2",
        ),
    )
    def test_run_exception(
        self,
        exception_msg: str,
        session: Session,
        run_step_task_action: RunStepTaskAction,
        mock_mod_logger: MagicMock,
    ) -> None:
        """Tests that if SessionActionDefinition.start() raises an exception, that:

        1.  the attempt to start the action is logged
        2.  the action is FAILED with a message representing the exception
        3.  actions other than ENV_EXITS in the session action queue are FAILED with a message
            representing the exception
        4.  that the Session._current_action is set to None
        5.  that a warning is logged indicating that attempts to start the action failed
        """

        # GIVEN
        exception = Exception(exception_msg)
        logger_info: MagicMock = mock_mod_logger.info
        logger_error: MagicMock = mock_mod_logger.error

        with (
            patch.object(session, "_report_action_update") as mock_report_action_update,
            patch.object(session._queue, "dequeue", return_value=run_step_task_action),
            patch.object(session._queue, "cancel_all") as mock_queue_cancel_all,
            patch.object(session_mod, "datetime") as datetime_mock,
            patch.object(run_step_task_action, "start", side_effect=exception),
        ):
            now: MagicMock = datetime_mock.now.return_value

            # WHEN
            session._start_action()

        # THEN
        logger_info.assert_called_once()
        assert isinstance(logger_info.call_args.args[0], SessionActionLogEvent)
        assert logger_info.call_args.args[0].subtype == SessionActionLogEventSubtype.START.value
        assert logger_info.call_args.args[0].action_id == run_step_task_action.id

        mock_report_action_update.assert_called_once_with(
            SessionActionStatus(
                completed_status="FAILED",
                start_time=now,
                end_time=now,
                id=run_step_task_action.id,
                status=ActionStatus(
                    state=ActionState.FAILED,
                    fail_message=exception_msg,
                ),
            ),
        )
        mock_queue_cancel_all.assert_called_once_with(
            message=f"Error starting prior action {run_step_task_action.id}",
            ignore_env_exits=True,
        )
        assert session._current_action is None
        logger_error.assert_called_once()
        assert isinstance(logger_error.call_args.args[0], SessionActionLogEvent)
        assert logger_error.call_args.args[0].subtype == SessionActionLogEventSubtype.END.value
        assert logger_error.call_args.args[0].status == "FAILED"
        assert logger_error.call_args.args[0].action_id == run_step_task_action.id

    def test_run_action_with_env_variables(
        self,
        session: Session,
        run_step_task_action: RunStepTaskAction,
        mock_mod_logger: MagicMock,
    ) -> None:
        """
        Tests that env variables are passed from Run step task action when _start_action is successfully called
        """

        # GIVEN
        logger_info: MagicMock = mock_mod_logger.info

        with (
            patch.object(session._queue, "dequeue", return_value=run_step_task_action),
            patch.object(session, "run_task") as session_run_task,
        ):
            # WHEN
            session._start_action()

        # THEN
        logger_info.assert_called_once()
        assert isinstance(logger_info.call_args.args[0], SessionActionLogEvent)
        assert logger_info.call_args.args[0].subtype == SessionActionLogEventSubtype.START
        assert logger_info.call_args.args[0].action_id == run_step_task_action.id

        session_run_task.assert_called_once()
        session_run_task.call_args.kwargs["os_env_vars"] == {
            "DEADLINE_SESSIONACTION_ID": run_step_task_action.id,
            "DEADLINE_TASK_ID": run_step_task_action.task_id,
        }

    def test_enter_env_action_called_with_env_variables(
        self,
        session: Session,
        enter_env_action: EnterEnvironmentAction,
        mock_mod_logger: MagicMock,
    ) -> None:
        """Tests that env variables are passed when enter environment action is called"""
        # GIVEN
        logger_info: MagicMock = mock_mod_logger.info

        with (
            patch.object(session._queue, "dequeue", return_value=enter_env_action),
            patch.object(session, "enter_environment") as session_enter_env,
        ):
            # WHEN
            session._start_action()

        # THEN
        logger_info.assert_called_once()
        assert isinstance(logger_info.call_args.args[0], SessionActionLogEvent)
        assert logger_info.call_args.args[0].subtype == SessionActionLogEventSubtype.START
        assert logger_info.call_args.args[0].action_id == enter_env_action.id

        session_enter_env.assert_called_once()
        session_enter_env.call_args.kwargs["os_env_vars"] == {
            "DEADLINE_SESSIONACTION_ID": enter_env_action.id,
        }

    def test_exit_env_action_called_with_env_variables(
        self,
        session: Session,
        exit_env_action: ExitEnvironmentAction,
        mock_mod_logger: MagicMock,
    ) -> None:
        """Tests that env variables are passed when exit environment action is called"""
        # GIVEN
        logger_info: MagicMock = mock_mod_logger.info

        with (
            patch.object(session._queue, "dequeue", return_value=exit_env_action),
            patch.object(session, "exit_environment") as session_exit_env,
        ):
            # WHEN
            session._start_action()

        # THEN
        logger_info.assert_called_once()
        assert isinstance(logger_info.call_args.args[0], SessionActionLogEvent)
        assert logger_info.call_args.args[0].subtype == SessionActionLogEventSubtype.START
        assert logger_info.call_args.args[0].action_id == exit_env_action.id

        session_exit_env.assert_called_once()
        session_exit_env.call_args.kwargs["os_env_vars"] == {
            "DEADLINE_SESSIONACTION_ID": exit_env_action.id,
        }
