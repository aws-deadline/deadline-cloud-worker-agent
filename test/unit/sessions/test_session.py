# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import os
from collections.abc import Generator, Iterable
from concurrent.futures import wait
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath, PureWindowsPath
from threading import RLock
from types import TracebackType
from typing import Literal, Optional
from unittest.mock import ANY, MagicMock, patch

import pytest

from openjd.model import ParameterValue
from openjd.model.v2023_09 import (
    Action,
    Environment,
    EnvironmentActions,
    EnvironmentScript,
    StepActions,
    StepScript,
    StepTemplate,
    CommandString,
    ArgListType,
    ArgString,
    ExtensionName,
)
from openjd.sessions import (
    ActionState,
    ActionStatus,
    PathFormat,
    PathMappingRule,
    PosixSessionUser,
    SessionUser,
    WindowsSessionUser,
)

from deadline_worker_agent.api_models import (
    EnvironmentAction,
    TaskRunAction,
    AttachmentUploadAction,
    ManifestInfo,
)
from deadline_worker_agent.sessions import Session
import deadline_worker_agent.sessions.session as session_mod
from deadline_worker_agent.sessions.session import (
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
    UploadManifestInfo,
)
import deadline_worker_agent.sessions.log_config as log_config_mod
from deadline_worker_agent.sessions.job_entities.job_attachment_details import (
    JobAttachmentManifestProperties,
)
from deadline_worker_agent.sessions.attachment_models import (
    WorkerManifestProperties,
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
    session_root_dir: Path,
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
        session_root_dir=session_root_dir,
    )


@pytest.fixture
def run_step_task_action(
    action_id: str,
    step_id: str,
    task_id: str,
    command: CommandString,
    on_run_args: ArgListType,
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
                            command=CommandString("test"),
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

    @pytest.mark.parametrize(
        argnames="session_root_dir",
        argvalues=(
            pytest.param(Path("/foo"), id="1"),
            pytest.param(Path("/bar"), id="1"),
        ),
    )
    def test_has_session_root_dir(
        self,
        session: Session,
        mock_openjd_session_cls: MagicMock,
        session_root_dir: Path,
    ) -> None:
        """Ensure that Session passes session_root_dir when creating the Open Job Description session"""
        # THEN
        mock_openjd_session_cls.assert_called_once()
        assert (
            mock_openjd_session_cls.call_args.kwargs["session_root_directory"] == session_root_dir
        )


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

    @pytest.fixture
    def mock_action_output_log_filter(self) -> Generator[MagicMock, None, None]:
        """Returns a patched mock for ActionOutputCaptureFilter"""
        with patch(
            "deadline_worker_agent.sessions.session.ActionOutputCaptureFilter"
        ) as mock_filter:
            yield mock_filter

    @pytest.fixture
    def mock_openjd_log(self) -> Generator[MagicMock, None, None]:
        """Returns a patched mock for OPENJD_LOG"""
        with patch("deadline_worker_agent.sessions.session.OPENJD_LOG") as mock_log:
            yield mock_log

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
                                    command=CommandString("test"),
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
            manifests=None,
        )

        # WHEN
        future = session._action_updated_impl(
            action_status=failed_action_status,
            now=action_complete_time,
        )
        if future:
            wait([future])

        # THEN
        mock_report_action_update.assert_called_once_with(expected_action_update)
        queue_cancel_all.assert_called_once_with(
            message=expected_next_action_message,
            ignore_env_exits=True,
        )
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
                                    command=CommandString("echo"),
                                    args=[ArgString("hello")],
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
            manifests=None,
        )

        # WHEN
        future = session._action_updated_impl(
            action_status=failed_action_status,
            now=action_complete_time,
        )
        if future:
            wait([future])

        # THEN
        mock_report_action_update.assert_called_once_with(expected_action_update)
        queue_cancel_all.assert_called_once_with(
            message=expected_next_action_message,
            ignore_env_exits=True,
        )
        assert session._current_action is None, "Current session action emptied"

    def test_success_task_run_attachment_upload(
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
        session._job_attachment_details = MagicMock()

        current_action = CurrentAction(
            definition=RunStepTaskAction(
                details=StepDetails(
                    step_template=StepTemplate(
                        name="Test",
                        script=StepScript(
                            actions=StepActions(
                                onRun=Action(
                                    command=CommandString("echo"),
                                    args=[ArgString("hello")],
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

        def mock_now(*arg, **kwarg) -> datetime:
            return action_complete_time

        with (
            patch.object(session_mod, "datetime") as mock_datetime,
        ):
            mock_datetime.now.side_effect = mock_now

            # WHEN
            future = session._action_updated_impl(
                action_status=success_action_status,
                now=action_complete_time,
            )
            if future:
                wait([future])

        # THEN
        mock_report_action_update.assert_not_called()
        queue_cancel_all.assert_not_called()
        assert session._output_sync_target_action == current_action
        assert session._current_action is None, "Current session action emptied"
        session_action_queue.insert_front.assert_called_once_with(
            action=AttachmentUploadAction(
                sessionActionId=action_id,
                actionType="SYNC_OUTPUT_JOB_ATTACHMENTS",
                stepId=step_id,
                taskId=task_id,
                startTime=action_start_time.timestamp(),
            )
        )

    def test_success_task_run_attachment_upload_with_no_output_manifest(
        self,
        session: Session,
        job_attachment_details: JobAttachmentDetails,
    ) -> None:
        # Set up a mock output sync action
        mocked_upload_action = MagicMock()
        # Set up a mock output sync target action
        output_sync_target_action = MagicMock()

        session._current_action = mocked_upload_action
        session._output_sync_target_action = output_sync_target_action
        session._job_attachment_details = job_attachment_details

        # WHEN
        # Call with a completed action status
        completed_action_status = ActionStatus(state=ActionState.SUCCESS)

        action_complete_time = datetime.now()

        with (
            patch.object(
                session_mod,
                "OPENJD_ACTION_STATE_TO_DEADLINE_COMPLETED_STATUS",
                {ActionState.SUCCESS: "SUCCEEDED"},
            ),
            patch.object(session, "_handle_action_update") as mocked_handle_action_upload,
        ):
            session._action_updated_impl(
                action_status=completed_action_status,
                now=action_complete_time,
            )

        expected_manifest_list: list[ManifestInfo] = [{}]

        mocked_handle_action_upload.assert_called_once_with(
            False,
            completed_action_status,
            output_sync_target_action,
            action_complete_time,
            expected_manifest_list,
        )

    def test_success_task_run_attachment_upload_with_manifest(
        self,
        session: Session,
        job_attachment_details: JobAttachmentDetails,
    ) -> None:
        # Set up a mock output sync action
        mocked_upload_action = MagicMock()
        # Set up a mock output sync target action
        output_sync_target_action = MagicMock()

        job_attachment_details.manifests.extend(
            [
                # Add a second asset root to the manifests. We won't get an output for this one.
                JobAttachmentManifestProperties(root_path="no_output", root_path_format="posix"),
                # Add a thir asset root to the manifests.
                JobAttachmentManifestProperties(root_path="root_path2", root_path_format="posix"),
            ]
        )

        session._current_action = mocked_upload_action
        session._output_sync_target_action = output_sync_target_action
        session._job_attachment_details = job_attachment_details
        session._upload_manifest_list = [
            # Extra manifest. It's not in job attachments details and should be
            # omitted from the expected output.
            UploadManifestInfo(
                "fake_path1",
                "fake_hash1",
                "fake_source_path1",
            ),
            # Two Manifests match root paths in job attachments details
            UploadManifestInfo(
                "fake_path",
                "fake_hash",
                job_attachment_details.manifests[0].root_path,
            ),
            UploadManifestInfo(
                "fake_path1",
                "fake_hash1",
                "root_path2",
            ),
        ]

        # WHEN
        # Call with a completed action status
        completed_action_status = ActionStatus(state=ActionState.SUCCESS)

        action_complete_time = datetime.now()

        with (
            patch.object(
                session_mod,
                "OPENJD_ACTION_STATE_TO_DEADLINE_COMPLETED_STATUS",
                {ActionState.SUCCESS: "SUCCEEDED"},
            ),
            patch.object(session, "_handle_action_update") as mocked_handle_action_upload,
        ):
            session._action_updated_impl(
                action_status=completed_action_status,
                now=action_complete_time,
            )

        expected_manifest_list = [
            # manifest information for the source path that matches the root.
            {
                "outputManifestPath": "fake_path",
                "outputManifestHash": "fake_hash",
            },
            # There was no output information for the 2nd root path. It should have an empty manifest info.
            {},
            # This manifest also matches a root path. We want to make sure that we're matching the order defined
            # in job attachment details.
            {
                "outputManifestPath": "fake_path1",
                "outputManifestHash": "fake_hash1",
            },
        ]

        mocked_handle_action_upload.assert_called_once_with(
            False,
            completed_action_status,
            output_sync_target_action,
            action_complete_time,
            expected_manifest_list,
        )

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
        future = session._action_updated_impl(
            action_status=success_action_status,
            now=action_complete_time,
        )
        if future:
            wait([future])

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
        future = session._action_updated_impl(
            action_status=failed_action_status,
            now=action_complete_time,
        )
        if future:
            wait([future])

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
        future = session._action_updated_impl(
            action_status=canceled_action_status,
            now=action_complete_time,
        )
        if future:
            wait([future])

        # THEN
        mock_mod_logger.info.assert_called_once()
        assert isinstance(mock_mod_logger.info.call_args.args[0], SessionActionLogEvent)
        assert (
            mock_mod_logger.info.call_args.args[0].subtype == SessionActionLogEventSubtype.END.value
        )
        assert mock_mod_logger.info.call_args.args[0].status == "CANCELED"
        assert mock_mod_logger.info.call_args.args[0].action_id == current_action.definition.id

    def test_action_output_capture_filter_integration_on_output_sync_creation(
        self,
        action_id: str,
        session: Session,
        action_start_time: datetime,
        step_id: str,
        success_action_status: ActionStatus,
        task_id: str,
        mock_action_output_log_filter: MagicMock,
        mock_openjd_log: MagicMock,
    ) -> None:
        """Tests that ActionOutputCaptureFilter is properly integrated when a task run succeeds"""
        # GIVEN
        session._job_attachment_details = MagicMock()

        current_action = CurrentAction(
            definition=RunStepTaskAction(
                details=StepDetails(
                    step_template=StepTemplate(
                        name="Test",
                        script=StepScript(
                            actions=StepActions(
                                onRun=Action(
                                    command=CommandString("echo"),
                                    args=[ArgString("hello")],
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
        mock_filter_instance = MagicMock()
        mock_action_output_log_filter.return_value = mock_filter_instance

        # WHEN
        session._action_updated_impl(
            action_status=success_action_status,
            now=datetime.now(),
        )

        # THEN
        # Verify filter was created with correct parameters
        mock_action_output_log_filter.assert_called_once()
        _, kwargs = mock_action_output_log_filter.call_args
        assert kwargs["session_id"] == session.id
        assert callable(kwargs["callback"])

        # Verify filter was added to OPENJD_LOG
        mock_openjd_log.addFilter.assert_called_once_with(mock_filter_instance)

        # Verify output sync target action was set
        assert session._output_sync_target_action == current_action
        assert session._current_action is None

    def test_action_output_filter_removed_on_output_sync_completion(
        self,
        session: Session,
        mock_openjd_log: MagicMock,
    ) -> None:
        """Tests that ActionOutputCaptureFilter is removed when output sync completes"""
        # GIVEN
        # Set up a mock filter
        mock_filter = MagicMock()
        session._action_output_log_filter = mock_filter

        # Set up a mock output sync action
        mocked_upload_action = MagicMock()
        # Set up a mock output sync target action
        output_sync_target_action = MagicMock()

        session._current_action = mocked_upload_action
        session._output_sync_target_action = output_sync_target_action

        # WHEN
        # Call with a completed action status
        completed_action_status = ActionStatus(state=ActionState.SUCCESS)

        with patch.object(
            session_mod,
            "OPENJD_ACTION_STATE_TO_DEADLINE_COMPLETED_STATUS",
            {ActionState.SUCCESS: "SUCCEEDED"},
        ):
            session._action_updated_impl(
                action_status=completed_action_status,
                now=datetime.now(),
            )

        # THEN
        # Verify filter was removed
        mock_openjd_log.removeFilter.assert_called_once_with(mock_filter)
        # Verify output sync target action was cleared
        assert session._output_sync_target_action is None

    def test_action_output_capture_filter_ja_upload_callback_invalid(
        self,
        session: Session,
    ) -> None:
        """Tests that the callback for ja_upload type correctly handles incorrectly formatted value"""

        # WHEN
        session._action_output_log_filter_callback(
            log_config_mod.ActionOutputMessageKind.JA_UPLOAD, "NOT A VALID LIST OF MANIFEST INFOS"
        )

        assert session._upload_manifest_list == []

        session._action_output_log_filter_callback(
            log_config_mod.ActionOutputMessageKind.JA_UPLOAD, '[{"not":"real"}]'
        )

        assert session._upload_manifest_list == []

    def test_action_output_capture_filter_ja_upload_callback_valid(
        self,
        session: Session,
    ) -> None:
        """Tests that the callback for ja_upload type correctly handles properly formatted value"""

        # WHEN
        session._action_output_log_filter_callback(
            log_config_mod.ActionOutputMessageKind.JA_UPLOAD,
            '[{"source_path": "test", "output_manifest_path":"test", "output_manifest_hash":"test"}]',
        )

        assert len(session._upload_manifest_list) == 1
        assert all(isinstance(item, UploadManifestInfo) for item in session._upload_manifest_list)

    def test_progress_update_excludes_manifests(
        self,
        session: Session,
        mock_report_action_update: MagicMock,
    ) -> None:
        """Test that progress updates (RUNNING state) do not include manifests field"""
        # GIVEN - A running task action with progress
        action_id = "test-action-123"
        step_id = "step-456"
        task_id = "task-789"

        current_action = CurrentAction(
            definition=RunStepTaskAction(
                details=StepDetails(
                    step_template=StepTemplate(
                        name="Test",
                        script=StepScript(
                            actions=StepActions(
                                onRun=Action(
                                    command=CommandString("echo"),
                                    args=[ArgString("hello")],
                                    cancelation=None,
                                )
                            )
                        ),
                    ),
                    step_id=step_id,
                ),
                id=action_id,
                task_id=task_id,
                task_parameter_values=dict[str, ParameterValue](),
            ),
            start_time=datetime.now(tz=timezone.utc),
        )
        session._current_action = current_action

        # Progress update (RUNNING state, no completed_status)
        progress_action_status = ActionStatus(
            state=ActionState.RUNNING, progress=50.0, status_message="Processing files..."
        )

        # WHEN - Progress update is reported
        session._action_updated_impl(
            action_status=progress_action_status,
            now=datetime.now(tz=timezone.utc),
        )

        # THEN - No manifests field should be included
        mock_report_action_update.assert_called_once()
        call_args = mock_report_action_update.call_args[0][0]
        assert isinstance(call_args, SessionActionStatus)
        assert call_args.manifests is None  # Should be None for progress updates

    def test_failed_task_excludes_manifests(
        self,
        session: Session,
        mock_report_action_update: MagicMock,
    ) -> None:
        """Test that failed task actions do not include manifests field"""
        # GIVEN - A failed task action
        action_id = "test-action-123"
        step_id = "step-456"
        task_id = "task-789"

        current_action = CurrentAction(
            definition=RunStepTaskAction(
                details=StepDetails(
                    step_template=StepTemplate(
                        name="Test",
                        script=StepScript(
                            actions=StepActions(
                                onRun=Action(
                                    command=CommandString("echo"),
                                    args=[ArgString("hello")],
                                    cancelation=None,
                                )
                            )
                        ),
                    ),
                    step_id=step_id,
                ),
                id=action_id,
                task_id=task_id,
                task_parameter_values=dict[str, ParameterValue](),
            ),
            start_time=datetime.now(tz=timezone.utc),
        )
        session._current_action = current_action

        # Failed action status
        failed_action_status = ActionStatus(state=ActionState.FAILED, fail_message="Task failed")

        # WHEN - Failed action is reported
        with patch.object(
            session_mod,
            "OPENJD_ACTION_STATE_TO_DEADLINE_COMPLETED_STATUS",
            {ActionState.FAILED: "FAILED"},
        ):
            session._action_updated_impl(
                action_status=failed_action_status,
                now=datetime.now(tz=timezone.utc),
            )

        # THEN - No manifests field should be included
        mock_report_action_update.assert_called_once()
        call_args = mock_report_action_update.call_args[0][0]
        assert isinstance(call_args, SessionActionStatus)
        assert call_args.manifests is None  # Should be None for failed actions

    def test_successful_task_without_job_attachments_includes_none_manifests(
        self,
        session: Session,
        mock_report_action_update: MagicMock,
    ) -> None:
        """Test that manifests are None when job attachments are not used in output sync flow"""
        # Set up a mock output sync action
        mocked_upload_action = MagicMock()
        # Set up a mock output sync target action
        output_sync_target_action = MagicMock()

        session._current_action = mocked_upload_action
        session._output_sync_target_action = output_sync_target_action
        # Explicitly set job_attachment_details to None to simulate no job attachments
        session._job_attachment_details = None

        # WHEN - Call with a completed action status
        completed_action_status = ActionStatus(state=ActionState.SUCCESS)
        action_complete_time = datetime.now()

        with (
            patch.object(
                session_mod,
                "OPENJD_ACTION_STATE_TO_DEADLINE_COMPLETED_STATUS",
                {ActionState.SUCCESS: "SUCCEEDED"},
            ),
            patch.object(session, "_handle_action_update") as mocked_handle_action_upload,
        ):
            session._action_updated_impl(
                action_status=completed_action_status,
                now=action_complete_time,
            )

        # THEN - manifests_list should be None when job attachments are not used
        mocked_handle_action_upload.assert_called_once_with(
            False,
            completed_action_status,
            output_sync_target_action,
            action_complete_time,
            None,  # manifests_list should be None when job attachments are not used
        )


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

    @pytest.fixture
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
        step_id: str,
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
            "DEADLINE_STEP_ID": step_id,
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

    def test_session_includes_redacted_env_vars_extension(
        self,
        session_id: str,
        job_details: MagicMock,
        session_action_queue: MagicMock,
        session_root_dir: Path,
        os_user: SessionUser,
        queue_id: str,
        action_update_callback: MagicMock,
        action_update_lock: MagicMock,
        asset_sync: MagicMock,
    ) -> None:
        """Tests that the REDACTED_ENV_VARS extension is included in the supported extensions
        This test should be updated when BatchGetJobEntity returns the list of requested extensions
        to use those intead - Session will likely take those extensions as a parameter of some sort"""
        # GIVEN
        from deadline_worker_agent.sessions.session import Session

        # WHEN
        with patch("deadline_worker_agent.sessions.session.OPENJDSession") as mock_openjd_session:
            _ = Session(
                id=session_id,
                job_details=job_details,
                queue=session_action_queue,
                queue_id=queue_id,
                job_id="job-1234",
                asset_sync=asset_sync,
                session_root_dir=session_root_dir,
                os_user=os_user,
                action_update_callback=action_update_callback,
                action_update_lock=action_update_lock,
                retain_session_dir=False,
            )

        # THEN
        # Verify that the REDACTED_ENV_VARS extension is included in the supported extensions
        mock_openjd_session.assert_called_once()
        _, kwargs = mock_openjd_session.call_args
        assert "revision_extensions" in kwargs
        revision_extensions = kwargs["revision_extensions"]
        # Check that REDACTED_ENV_VARS is in the list of extensions
        assert ExtensionName.REDACTED_ENV_VARS.value in revision_extensions.extensions


class TestSessionWorkerManifestProperties:
    """Test cases for Session worker manifest properties dictionary functionality."""

    @pytest.fixture
    def worker_manifest_properties(self) -> WorkerManifestProperties:
        """Fixture providing a WorkerManifestProperties instance for testing."""
        from deadline.job_attachments.models import ManifestProperties, PathFormat

        manifest_props = ManifestProperties(
            rootPath="/source/path",
            rootPathFormat=PathFormat.POSIX,
            fileSystemLocationName="shared_storage",
            inputManifestPath="input.json",
            inputManifestHash="hash123",
            outputRelativeDirectories=["out1", "out2"],
        )

        return WorkerManifestProperties(
            manifest_properties=manifest_props,
            local_root_path="/local/root",
            local_manifest_paths=["/local/manifest.json"],
        )

    @pytest.fixture
    def second_worker_manifest_properties(self) -> WorkerManifestProperties:
        """Fixture providing a second WorkerManifestProperties instance for testing."""
        from deadline.job_attachments.models import ManifestProperties, PathFormat

        manifest_props = ManifestProperties(
            rootPath="/another/source/path",
            rootPathFormat=PathFormat.WINDOWS,
            fileSystemLocationName="another_storage",
            inputManifestPath="another_input.json",
            inputManifestHash="hash456",
            outputRelativeDirectories=["output"],
        )

        return WorkerManifestProperties(
            manifest_properties=manifest_props,
            local_root_path="/another/local/root",
            local_manifest_paths=["/another/local/manifest.json"],
        )

    def test_worker_manifest_properties_dict_property_initial_state(self, session: Session):
        """Test that worker_manifest_properties_by_local_root property returns empty dict initially."""
        # WHEN
        result = session.worker_manifest_properties_by_local_root

        # THEN
        assert result == {}

    def test_set_worker_manifest_properties(
        self, session: Session, worker_manifest_properties: WorkerManifestProperties
    ):
        """Test setting worker manifest properties in the session dictionary."""
        # WHEN
        session.set_worker_manifest_properties(worker_manifest_properties)

        # THEN
        assert (
            worker_manifest_properties.local_root_path
            in session._worker_manifest_properties_by_local_root
        )
        assert (
            session._worker_manifest_properties_by_local_root[
                worker_manifest_properties.local_root_path
            ]
            == worker_manifest_properties
        )

    def test_set_multiple_worker_manifest_properties(
        self,
        session: Session,
        worker_manifest_properties: WorkerManifestProperties,
        second_worker_manifest_properties: WorkerManifestProperties,
    ):
        """Test setting multiple worker manifest properties in the session dictionary."""
        # WHEN
        session.set_worker_manifest_properties(worker_manifest_properties)
        session.set_worker_manifest_properties(second_worker_manifest_properties)

        # THEN
        assert len(session._worker_manifest_properties_by_local_root) == 2
        assert (
            session._worker_manifest_properties_by_local_root[
                worker_manifest_properties.local_root_path
            ]
            == worker_manifest_properties
        )
        assert (
            session._worker_manifest_properties_by_local_root[
                second_worker_manifest_properties.local_root_path
            ]
            == second_worker_manifest_properties
        )

    def test_set_worker_manifest_properties_overwrites_existing(
        self,
        session: Session,
        worker_manifest_properties: WorkerManifestProperties,
        second_worker_manifest_properties: WorkerManifestProperties,
    ):
        """Test that setting worker manifest properties overwrites existing entry with same key."""
        # GIVEN - Modify second properties to have same local_root_path as first
        second_worker_manifest_properties.local_root_path = (
            worker_manifest_properties.local_root_path
        )
        session.set_worker_manifest_properties(worker_manifest_properties)

        # WHEN
        session.set_worker_manifest_properties(second_worker_manifest_properties)

        # THEN
        assert len(session._worker_manifest_properties_by_local_root) == 1
        assert (
            session._worker_manifest_properties_by_local_root[
                worker_manifest_properties.local_root_path
            ]
            == second_worker_manifest_properties
        )

    def test_add_local_manifest_path_existing_key(
        self, session: Session, worker_manifest_properties: WorkerManifestProperties
    ):
        """Test adding local manifest path to existing worker manifest properties."""
        # GIVEN
        new_manifest_path = "/new/manifest/path.json"
        session.set_worker_manifest_properties(worker_manifest_properties)
        initial_paths = worker_manifest_properties.local_manifest_paths.copy()

        # WHEN
        returned_props = session.add_local_manifest_path(
            worker_manifest_properties.local_root_path, new_manifest_path
        )

        # THEN
        updated_props = session.worker_manifest_properties_by_local_root[
            worker_manifest_properties.local_root_path
        ]
        assert returned_props is updated_props  # Verify return value is the same object
        assert len(updated_props.local_manifest_paths) == len(initial_paths) + 1
        assert new_manifest_path in updated_props.local_manifest_paths
        assert all(path in updated_props.local_manifest_paths for path in initial_paths)

    def test_add_local_manifest_path_nonexistent_key(self, session: Session):
        """Test adding local manifest path to nonexistent key raises ValueError."""
        # GIVEN
        nonexistent_path = "/nonexistent/path"
        manifest_path = "/some/manifest.json"

        # WHEN / THEN
        with pytest.raises(
            ValueError,
            match=f"Worker manifest properties not found for local_root_path: {nonexistent_path}",
        ):
            session.add_local_manifest_path(nonexistent_path, manifest_path)

    def test_add_multiple_local_manifest_paths(
        self, session: Session, worker_manifest_properties: WorkerManifestProperties
    ):
        """Test adding multiple local manifest paths to same worker manifest properties."""
        # GIVEN
        new_paths = ["/path1.json", "/path2.json", "/path3.json"]
        session.set_worker_manifest_properties(worker_manifest_properties)
        initial_count = len(worker_manifest_properties.local_manifest_paths)

        # WHEN
        returned_props_list = []
        for path in new_paths:
            returned_props = session.add_local_manifest_path(
                worker_manifest_properties.local_root_path, path
            )
            returned_props_list.append(returned_props)

            # THEN
            updated_props = session.worker_manifest_properties_by_local_root[
                worker_manifest_properties.local_root_path
            ]
            assert returned_props is updated_props
            assert path in updated_props.local_manifest_paths

        # THEN
        updated_props = session.worker_manifest_properties_by_local_root[
            worker_manifest_properties.local_root_path
        ]
        assert len(updated_props.local_manifest_paths) == initial_count + len(new_paths)

    def test_worker_manifest_properties_dict_property_reflects_changes(
        self, session: Session, worker_manifest_properties: WorkerManifestProperties
    ):
        """Test that worker_manifest_properties_by_local_root property reflects changes made to internal dict."""
        # WHEN
        session.set_worker_manifest_properties(worker_manifest_properties)
        result = session.worker_manifest_properties_by_local_root

        # THEN
        assert result == {worker_manifest_properties.local_root_path: worker_manifest_properties}
        assert (
            result is session._worker_manifest_properties_by_local_root
        )  # Should return the same reference

    def test_worker_manifest_properties_integration_workflow(
        self,
        session: Session,
        worker_manifest_properties: WorkerManifestProperties,
        second_worker_manifest_properties: WorkerManifestProperties,
    ):
        """Test a complete workflow of setting, getting, and modifying worker manifest properties."""
        # GIVEN
        additional_manifest_path = "/additional/manifest.json"

        # WHEN - Set initial properties
        session.set_worker_manifest_properties(worker_manifest_properties)
        session.set_worker_manifest_properties(second_worker_manifest_properties)

        # WHEN - Add manifest path to first properties
        returned_props = session.add_local_manifest_path(
            worker_manifest_properties.local_root_path, additional_manifest_path
        )

        # WHEN - Get all properties
        properties_dict = session.worker_manifest_properties_by_local_root

        # THEN - Verify all operations worked correctly
        assert len(properties_dict) == 2
        assert (
            returned_props is properties_dict[worker_manifest_properties.local_root_path]
        )  # Verify return value is correct
        assert (
            additional_manifest_path
            in properties_dict[worker_manifest_properties.local_root_path].local_manifest_paths
        )
        assert (
            additional_manifest_path
            not in properties_dict[
                second_worker_manifest_properties.local_root_path
            ].local_manifest_paths
        )

    def test_worker_manifest_properties_with_empty_local_manifest_paths(self, session: Session):
        """Test worker manifest properties functionality with empty local_manifest_paths."""
        from deadline.job_attachments.models import ManifestProperties, PathFormat

        # GIVEN
        manifest_props = ManifestProperties(
            rootPath="/source/path",
            rootPathFormat=PathFormat.POSIX,
        )

        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props,
            local_root_path="/local/root",
            # local_manifest_paths defaults to empty list
        )

        # WHEN
        session.set_worker_manifest_properties(worker_props)
        session.add_local_manifest_path(worker_props.local_root_path, "/first/manifest.json")
        session.add_local_manifest_path(worker_props.local_root_path, "/second/manifest.json")

        # THEN
        result = session.worker_manifest_properties_by_local_root[worker_props.local_root_path]
        assert sorted(result.local_manifest_paths) == [
            "/first/manifest.json",
            "/second/manifest.json",
        ]


class TestRunAttachmentSyncTask:
    """Test cases for Session._run_attachment_sync_task()

    This method is a thin wrapper around _run_task_without_session_env, so we only test
    that it correctly delegates to the underlying method with the right parameters.
    """

    @pytest.mark.parametrize(
        "os_env_vars,log_task_banner",
        [
            (None, True),
            ({"ENV_VAR1": "value1", "ENV_VAR2": "value2"}, True),
            (None, False),
            ({"ENV_VAR1": "value1"}, False),
        ],
        ids=["defaults", "with_env_vars", "no_banner", "all_params"],
    )
    def test_delegates_to_run_task_without_session_env(
        self,
        session: Session,
        mock_openjd_session: MagicMock,
        os_env_vars: dict[str, str] | None,
        log_task_banner: bool,
    ) -> None:
        """Tests that _run_attachment_sync_task correctly delegates to _run_task_without_session_env."""
        # GIVEN
        step_script_model = MagicMock()

        # WHEN
        session._run_attachment_sync_task(
            step_script=step_script_model,
            task_parameter_values=dict[str, ParameterValue](),
            os_env_vars=os_env_vars,
            log_task_banner=log_task_banner,
        )

        # THEN
        mock_openjd_session._run_task_without_session_env.assert_called_once_with(
            step_script=step_script_model,
            task_parameter_values=dict[str, ParameterValue](),
            os_env_vars=os_env_vars,
            log_task_banner=log_task_banner,
        )

    def test_propagates_exception_from_openjd_session(
        self,
        session: Session,
        mock_openjd_session: MagicMock,
    ) -> None:
        """Tests that exceptions from the underlying Open Job Description session are propagated."""
        # GIVEN
        expected_exception = RuntimeError("Task execution failed")
        mock_openjd_session._run_task_without_session_env.side_effect = expected_exception

        # WHEN / THEN
        with pytest.raises(RuntimeError) as exc_info:
            session._run_attachment_sync_task(
                step_script=MagicMock(),
                task_parameter_values={},
            )

        assert exc_info.value is expected_exception
