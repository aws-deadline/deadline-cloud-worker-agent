# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from concurrent.futures import Executor
from datetime import timedelta
from unittest.mock import Mock
from deadline_worker_agent.sessions import Session
import re

from openjd.sessions import ActionState, ActionStatus
import pytest

from deadline_worker_agent.sessions.actions.openjd_action import OpenjdAction
from deadline_worker_agent.sessions.errors import CancelationError


class DerivedOpenjdAction(OpenjdAction):
    def __init__(self, *, id: str) -> None:
        super().__init__(id=id)
        self.start_mock = Mock()

    def start(self, *, session: Session, executor: Executor) -> None:
        pass

    def human_readable(self) -> str:
        return "not important to test"


@pytest.fixture
def session() -> Mock:
    return Mock()


class TestCancel:
    """Tests for the OpenjdAction.cancel() method"""

    @pytest.fixture(
        params=(
            timedelta(minutes=1),
            timedelta(seconds=15),
            None,
        ),
        ids=(
            "timeout-1min",
            "timeout-15sec",
            "timeout-None",
        ),
    )
    def time_limit(self, request: pytest.FixtureRequest) -> timedelta | None:
        return request.param

    def test_calls_openjd_session_cancel_action(
        self,
        session: Mock,
        time_limit: timedelta | None,
    ) -> None:
        """Tests that when calling OpenjdAction.cancel(), the session's Open Job Description session action is
        canceled"""

        # GIVEN
        action = DerivedOpenjdAction(id="my-id")

        # WHEN
        action.cancel(session=session, time_limit=time_limit)

        # THEN
        session._session.cancel_action.assert_called_once_with(time_limit=time_limit)

    @pytest.mark.parametrize(
        argnames="action_status",
        argvalues=(
            ActionStatus(state=ActionState.FAILED),
            ActionStatus(state=ActionState.SUCCESS),
            None,
        ),
        ids=(
            "failed",
            "success",
            "not-run",
        ),
    )
    def test_handles_runtime_errors(
        self,
        session: Mock,
        time_limit: timedelta | None,
        action_status: ActionStatus | None,
    ) -> None:
        """Tests that when calling OpenjdAction.cancel(), the session's Open Job Description session action is
        canceled"""

        # GIVEN
        action = DerivedOpenjdAction(id="my-id")
        error_msg = "error msg"
        session._session.cancel_action.side_effect = RuntimeError(error_msg)
        session._session.action_status = action_status

        with pytest.raises(CancelationError) as raise_ctx:
            # WHEN
            action.cancel(session=session, time_limit=time_limit)

        # THEN
        session._session.cancel_action.assert_called_once()
        if action_status:
            raise_ctx.match(
                re.escape(
                    f"Could not cancel {action.id}. It completed as {action_status.state.name}"
                )
            )
        else:
            raise_ctx.match(re.escape(f"Could not cancel {action.id}. No action was run"))
