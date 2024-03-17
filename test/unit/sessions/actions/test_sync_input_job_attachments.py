# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from concurrent.futures import CancelledError as FutureCancelledError
from typing import Callable, TYPE_CHECKING
from unittest.mock import Mock, patch

from deadline.job_attachments.exceptions import AssetSyncCancelledError
from openjd.sessions import ActionState, ActionStatus
import pytest

from deadline_worker_agent.sessions.actions import SyncInputJobAttachmentsAction
from deadline_worker_agent.sessions.job_entities import StepDetails

if TYPE_CHECKING:
    from deadline_worker_agent.sessions.job_entities import JobAttachmentDetails
    from concurrent.futures import Future


@pytest.fixture
def executor() -> Mock:
    return Mock()


@pytest.fixture
def session_id() -> str:
    return "session_id"


@pytest.fixture
def session(session_id: str) -> Mock:
    session = Mock()
    session.id = session_id
    return session


@pytest.fixture
def action_id() -> str:
    return "sessionaction-abc123"


@pytest.fixture
def action(
    action_id: str,
    job_attachment_details: JobAttachmentDetails,
) -> SyncInputJobAttachmentsAction:
    return SyncInputJobAttachmentsAction(
        id=action_id,
        session_id="session-1234",
        job_attachment_details=job_attachment_details,
    )


class TestStart:
    """Tests for SyncInputJobAttachmentsAction.start()"""

    def test_submits_sync_asset_inputs_future(
        self,
        executor: Mock,
        session: Mock,
        action: SyncInputJobAttachmentsAction,
        job_attachment_details: JobAttachmentDetails,
    ) -> None:
        """
        Tests that SyncInputJobAttachmentsAction.start() submits a future to call
        Session.sync_asset_inputs() method and passes the action's cancel event.
        """
        # WHEN
        action.start(session=session, executor=executor)

        # THEN
        executor.submit.assert_called_once_with(
            session.sync_asset_inputs,
            cancel=action._cancel,
            job_attachment_details=job_attachment_details,
        )

    @pytest.mark.parametrize(
        argnames="step_dependencies",
        argvalues=(
            ["step-1"],
            ["step-1", "step-2"],
        ),
        ids=(
            "step-dependencies-1",
            "step-dependencies-2",
        ),
    )
    def test_submits_sync_asset_inputs_future_with_given_step_details(
        self,
        executor: Mock,
        session: Mock,
        step_template: Mock,
        action_id: str,
        step_dependencies: list[str],
    ) -> None:
        """
        Tests that SyncInputJobAttachmentsAction.start() submits a future to call
        Session.sync_asset_inputs() method and passes the action's cancel event
        when the SyncInputJobAttachmentsAction has step details.
        """
        # WHEN
        action = SyncInputJobAttachmentsAction(
            id=action_id,
            session_id="session-1234",
            step_details=StepDetails(
                step_template=step_template,
                dependencies=step_dependencies,
            ),
        )
        action.start(session=session, executor=executor)

        # THEN
        executor.submit.assert_called_once_with(
            session.sync_asset_inputs,
            cancel=action._cancel,
            job_attachment_details=None,
            step_dependencies=step_dependencies,
        )

    def test_updates_action_to_running(
        self,
        executor: Mock,
        session: Mock,
        action: SyncInputJobAttachmentsAction,
    ) -> None:
        """
        Tests that SyncInputJobAttachmentsAction.start() calls the Session's update_action()
        method to report the action as RUNNING
        """
        # WHEN
        action.start(session=session, executor=executor)

        # THEN
        session.update_action.assert_called_once_with(
            action_status=ActionStatus(state=ActionState.RUNNING),
        )

    def test_adds_done_callback(
        self,
        executor: Mock,
        session: Mock,
        action: SyncInputJobAttachmentsAction,
    ) -> None:
        """
        Tests that SyncInputJobAttachmentsAction.start() adds a done callback to the future that
        calls SyncInputJobAttachmentsAction._on_done and passes the session passed into the start()
        method as an argument
        """
        # GIVEN
        future: Mock = executor.submit.return_value
        future_add_done_callback: Mock = future.add_done_callback
        with patch.object(action, "_on_done") as mock_on_done:
            # WHEN
            action.start(session=session, executor=executor)

            # THEN
            future_add_done_callback.assert_called_once()
            callback: Callable[[Future], None] = future_add_done_callback.call_args_list[0].args[0]

            # WHEN
            callback(future)

            # THEN
            mock_on_done.assert_called_once_with(future, session=session)


class TestOnDone:
    """Tests for SyncInputJobAttachmentsAction._on_done() which is the callback invoked when the
    asset input sync future completes"""

    @pytest.fixture
    def update_action(
        self,
        session: Mock,
    ) -> Mock:
        return session.update_action

    def test_handles_success(
        self,
        session: Mock,
        action: SyncInputJobAttachmentsAction,
        update_action: Mock,
    ) -> None:
        """
        Tests that when the future succeeds that Session.update_action() is called with
        state=ActionState.SUCCESS
        """
        # GIVEN
        future = Mock(
            **{
                "result.return_value": None,
            },
        )

        # WHEN
        action._on_done(future=future, session=session)

        # THEN
        update_action.assert_called_once_with(
            action_status=ActionStatus(state=ActionState.SUCCESS),
        )

    @pytest.mark.parametrize(
        argnames="cancel_exception",
        argvalues=(
            FutureCancelledError(),
            AssetSyncCancelledError("some message"),
        ),
        ids=(
            "concurrent.futures.CancelledError",
            "deadline.job_attachments.exceptions.AssetSyncCancelledError",
        ),
    )
    def test_handles_cancelation(
        self,
        session: Mock,
        action: SyncInputJobAttachmentsAction,
        update_action: Mock,
        cancel_exception: Exception,
    ) -> None:
        """Tests that when the future raises one of:

        - concurrent.futures.CancelledError (future was canceled before starting)
        - deadline.job_attachments.exceptions.AssetSyncCancelledError

        that Session.update_action() is called with state=ActionState.CANCELED
        """
        # GIVEN
        future = Mock(
            **{
                "result.side_effect": cancel_exception,
            },
        )

        # WHEN
        action._on_done(future=future, session=session)

        # THEN
        update_action.assert_called_once_with(
            action_status=ActionStatus(
                state=ActionState.CANCELED,
                fail_message="Canceled",
            ),
        )

    @pytest.mark.parametrize(
        argnames="fail_exception_msg",
        argvalues=(
            "msg",
            "msg2",
        ),
        ids=(
            "fail-exception-1",
            "fail-exception-2",
        ),
    )
    def test_handles_failure(
        self,
        session: Mock,
        action: SyncInputJobAttachmentsAction,
        update_action: Mock,
        fail_exception_msg: str,
    ) -> None:
        """Tests that when the future raises one of:

        - concurrent.futures.CancelledError (future was canceled before starting)
        - deadline.job_attachments.exceptions.AssetSyncCancelledError

        that Session.update_action() is called with state=ActionState.CANCELED
        """
        # GIVEN
        future = Mock(
            **{
                "result.side_effect": Exception(fail_exception_msg),
            },
        )

        # WHEN
        action._on_done(future=future, session=session)

        # THEN
        update_action.assert_called_once_with(
            action_status=ActionStatus(
                state=ActionState.FAILED,
                fail_message=fail_exception_msg,
            ),
        )
