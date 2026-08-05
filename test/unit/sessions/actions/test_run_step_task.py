# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from unittest.mock import Mock
import pytest

from deadline_worker_agent.sessions.actions.run_step_task import RunStepTaskAction
from deadline_worker_agent.sessions.job_entities.step_details import StepDetails


@pytest.fixture
def mock_step_details():
    mock = Mock(spec=StepDetails)
    mock.step_id = "step-123"
    mock.step_template = Mock()
    mock.step_template.name = "step-name"
    mock.step_template.script = Mock()
    return mock


@pytest.fixture
def mock_session():
    session = Mock()
    session.run_task = Mock()
    return session


@pytest.fixture
def mock_executor():
    return Mock()


class TestRunStepTaskAction:
    """Tests for RunStepTaskAction with optional task_id."""

    def test_init_with_task_id(self, mock_step_details):
        """Test creating RunStepTaskAction with task_id."""
        action = RunStepTaskAction(
            id="action-123",
            details=mock_step_details,
            task_id="task-456",
            task_parameter_values={},
        )

        assert action.task_id == "task-456"
        assert action._id == "action-123"

    def test_init_without_task_id(self, mock_step_details):
        """Test creating RunStepTaskAction without task_id."""
        action = RunStepTaskAction(
            id="action-123",
            details=mock_step_details,
            task_parameter_values={},
        )

        assert action.task_id is None
        assert action._id == "action-123"

    def test_start_with_task_id(self, mock_step_details, mock_session, mock_executor):
        """Test start() includes DEADLINE_TASK_ID when task_id is provided."""
        action = RunStepTaskAction(
            id="action-123",
            details=mock_step_details,
            task_id="task-456",
            task_parameter_values={},
        )

        action.start(session=mock_session, executor=mock_executor)

        mock_session.run_task.assert_called_once()
        call_args = mock_session.run_task.call_args[1]

        assert "os_env_vars" in call_args
        env_vars = call_args["os_env_vars"]
        assert env_vars["DEADLINE_STEP_ID"] == "step-123"
        assert env_vars["DEADLINE_TASK_ID"] == "task-456"
        assert env_vars["DEADLINE_SESSIONACTION_ID"] == "action-123"

    def test_start_without_task_id(self, mock_step_details, mock_session, mock_executor):
        """Test start() excludes DEADLINE_TASK_ID when task_id is None."""
        action = RunStepTaskAction(
            id="action-123",
            details=mock_step_details,
            task_parameter_values={},
        )

        action.start(session=mock_session, executor=mock_executor)

        mock_session.run_task.assert_called_once()
        call_args = mock_session.run_task.call_args[1]

        assert "os_env_vars" in call_args
        env_vars = call_args["os_env_vars"]
        assert env_vars["DEADLINE_STEP_ID"] == "step-123"
        assert env_vars["DEADLINE_SESSIONACTION_ID"] == "action-123"
        assert "DEADLINE_TASK_ID" not in env_vars
