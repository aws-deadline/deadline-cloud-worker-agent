# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from unittest.mock import MagicMock, Mock

import pytest

from deadline_worker_agent.scheduler.session_queue import (
    EnvironmentQueueEntry,
    SessionActionQueue,
    _step_id_from_environment_id,
)
from deadline_worker_agent.sessions.actions import EnterEnvironmentAction
from deadline_worker_agent.api_models import EnvironmentAction


class TestStepIdFromEnvironmentId:
    """Unit tests for the _step_id_from_environment_id helper."""

    @pytest.mark.parametrize(
        "environment_id, expected",
        [
            pytest.param(
                "STEP:step-0771968389a54c26adf4afd80bac1b82:Identifier",
                "step-0771968389a54c26adf4afd80bac1b82",
                id="step-scoped",
            ),
            pytest.param(
                "JOB:job-ac95524e7128498b9082375f8e3d7665:TestEnvironment",
                None,
                id="job-scoped",
            ),
            pytest.param(
                "STEP:step-abc:name:with:colons",
                "step-abc",
                id="colons-in-name",
            ),
            pytest.param(
                "STEP:step-abc",
                None,
                id="no-second-colon",
            ),
            pytest.param(
                "STEP::envname",
                None,
                id="empty-step-id",
            ),
            pytest.param(
                "",
                None,
                id="empty-string",
            ),
            pytest.param(
                "step-abc:name",
                None,
                id="no-STEP-prefix",
            ),
        ],
    )
    def test(self, environment_id: str, expected: str | None) -> None:
        assert _step_id_from_environment_id(environment_id) == expected


class TestDequeueStepContext:
    """Integration tests: dequeue() provides step context via environment id parsing."""

    @pytest.fixture
    def job_entities(self) -> MagicMock:
        mock = MagicMock()
        step_template = MagicMock()
        step_template.name = "MyStep"
        step_template.let = ["VAR=value"]
        mock.step_details.return_value.step_template = step_template
        return mock

    @pytest.fixture
    def session_queue(self, job_entities: MagicMock) -> SessionActionQueue:
        return SessionActionQueue(
            queue_id="queue-1",
            job_id="job-aaa",
            session_id="session-bbb",
            job_entities=job_entities,
            action_update_callback=Mock(),
        )

    def test_step_scoped_env_gets_step_context(
        self, session_queue: SessionActionQueue, job_entities: MagicMock
    ) -> None:
        """A STEP:-prefixed env-enter receives step name and let bindings."""
        entry = EnvironmentQueueEntry(
            cancel=Mock(),
            definition=EnvironmentAction(
                sessionActionId="sa-1",
                actionType="ENV_ENTER",
                environmentId="STEP:step-abc123:MyEnv",
            ),
        )
        session_queue._actions = [entry]
        session_queue._actions_by_id["sa-1"] = entry

        result = session_queue.dequeue()

        assert isinstance(result, EnterEnvironmentAction)
        assert result._step_name == "MyStep"
        assert result._extra_let_bindings == ["VAR=value"]
        job_entities.step_details.assert_called_once_with(step_id="step-abc123")

    def test_job_scoped_env_gets_no_step_context(
        self, session_queue: SessionActionQueue, job_entities: MagicMock
    ) -> None:
        """A JOB:-prefixed env-enter gets no step context and step_details is NOT called."""
        entry = EnvironmentQueueEntry(
            cancel=Mock(),
            definition=EnvironmentAction(
                sessionActionId="sa-2",
                actionType="ENV_ENTER",
                environmentId="JOB:job-def456:SharedEnv",
            ),
        )
        session_queue._actions = [entry]
        session_queue._actions_by_id["sa-2"] = entry

        result = session_queue.dequeue()

        assert isinstance(result, EnterEnvironmentAction)
        assert result._step_name is None
        assert result._extra_let_bindings is None
        job_entities.step_details.assert_not_called()

    def test_two_consecutive_step_envs_both_get_context(
        self, session_queue: SessionActionQueue, job_entities: MagicMock
    ) -> None:
        """Multiple step-scoped env-enters for the same step all receive context."""
        entry1 = EnvironmentQueueEntry(
            cancel=Mock(),
            definition=EnvironmentAction(
                sessionActionId="sa-3",
                actionType="ENV_ENTER",
                environmentId="STEP:step-same:Env1",
            ),
        )
        entry2 = EnvironmentQueueEntry(
            cancel=Mock(),
            definition=EnvironmentAction(
                sessionActionId="sa-4",
                actionType="ENV_ENTER",
                environmentId="STEP:step-same:Env2",
            ),
        )
        session_queue._actions = [entry1, entry2]
        session_queue._actions_by_id["sa-3"] = entry1
        session_queue._actions_by_id["sa-4"] = entry2

        result1 = session_queue.dequeue()
        result2 = session_queue.dequeue()

        assert isinstance(result1, EnterEnvironmentAction)
        assert isinstance(result2, EnterEnvironmentAction)
        assert result1._step_name == "MyStep"
        assert result2._step_name == "MyStep"

    def test_step_details_raises_graceful_degradation(
        self, session_queue: SessionActionQueue, job_entities: MagicMock
    ) -> None:
        """When step_details raises ValueError, the action is produced with no step context."""
        job_entities.step_details.side_effect = ValueError("not found")
        entry = EnvironmentQueueEntry(
            cancel=Mock(),
            definition=EnvironmentAction(
                sessionActionId="sa-5",
                actionType="ENV_ENTER",
                environmentId="STEP:step-bad:Broken",
            ),
        )
        session_queue._actions = [entry]
        session_queue._actions_by_id["sa-5"] = entry

        result = session_queue.dequeue()

        assert isinstance(result, EnterEnvironmentAction)
        assert result._step_name is None
        assert result._extra_let_bindings is None
