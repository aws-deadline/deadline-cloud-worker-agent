# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from unittest.mock import Mock, patch
import pytest

from deadline_worker_agent.api_models import TaskRunAction, AttachmentUploadAction
from deadline_worker_agent.scheduler.session_queue import (
    SessionActionQueue,
    TaskRunQueueEntry,
    AttachmentUploadActionQueueEntry,
)
from deadline_worker_agent.sessions.actions import RunStepTaskAction
from deadline_worker_agent.sessions.actions import (
    AttachmentUploadAction as AttachmentUploadActionImpl,
)


class TestSessionActionQueueOptionalTaskId:
    """Tests for SessionActionQueue handling optional task_id."""

    @pytest.fixture
    def mock_job_entities(self):
        job_entities = Mock()
        job_entities.step_details.return_value = Mock()
        return job_entities

    @pytest.fixture
    def session_queue(self, mock_job_entities):
        return SessionActionQueue(
            queue_id="queue-123",
            job_id="job-456",
            session_id="session-789",
            job_entities=mock_job_entities,
            action_update_callback=Mock(),
        )

    def test_dequeue_task_run_action_without_task_id(self, session_queue, mock_job_entities):
        """Test dequeue creates RunStepTaskAction when TaskRunAction has no taskId."""
        # Arrange
        action_definition = TaskRunAction(
            sessionActionId="action-123",
            actionType="TASK_RUN",
            stepId="step-456",
            # No taskId field
        )

        queue_entry = TaskRunQueueEntry(
            cancel=Mock(),
            definition=action_definition,
        )
        session_queue._actions = [queue_entry]
        session_queue._actions_by_id = {"action-123": queue_entry}

        # Mock the step details
        mock_step_details = Mock()
        mock_job_entities.step_details.return_value = mock_step_details

        # Act
        with patch(
            "deadline_worker_agent.scheduler.session_queue.parameters_from_api_response",
            return_value={},
        ):
            action = session_queue.dequeue()

        # Assert
        assert isinstance(action, RunStepTaskAction)
        assert action.task_id is None
        assert action._id == "action-123"

    def test_dequeue_task_run_action_with_task_id(self, session_queue, mock_job_entities):
        """Test dequeue creates RunStepTaskAction when TaskRunAction has taskId."""
        # Arrange
        action_definition = TaskRunAction(
            sessionActionId="action-123",
            actionType="TASK_RUN",
            stepId="step-456",
            taskId="task-789",
        )

        queue_entry = TaskRunQueueEntry(
            cancel=Mock(),
            definition=action_definition,
        )
        session_queue._actions = [queue_entry]
        session_queue._actions_by_id = {"action-123": queue_entry}

        # Mock the step details
        mock_step_details = Mock()
        mock_job_entities.step_details.return_value = mock_step_details

        # Act
        with patch(
            "deadline_worker_agent.scheduler.session_queue.parameters_from_api_response",
            return_value={},
        ):
            action = session_queue.dequeue()

        # Assert
        assert isinstance(action, RunStepTaskAction)
        assert action.task_id == "task-789"
        assert action._id == "action-123"

    def test_dequeue_attachment_upload_action_without_task_id(self, session_queue):
        """Test dequeue creates AttachmentUploadAction when definition has no taskId."""
        # Arrange
        action_definition = AttachmentUploadAction(
            sessionActionId="action-123",
            actionType="SYNC_OUTPUT_JOB_ATTACHMENTS",
            stepId="step-456",
            startTime=1234567890.0,
        )

        queue_entry = AttachmentUploadActionQueueEntry(
            cancel=Mock(),
            definition=action_definition,
        )
        session_queue._actions = [queue_entry]
        session_queue._actions_by_id = {"action-123": queue_entry}

        # Act
        action = session_queue.dequeue()

        # Assert
        assert isinstance(action, AttachmentUploadActionImpl)
        assert action._task_id is None
        assert action._step_id == "step-456"

    def test_dequeue_attachment_upload_action_with_task_id(self, session_queue):
        """Test dequeue creates AttachmentUploadAction when definition has taskId."""
        # Arrange
        action_definition = AttachmentUploadAction(
            sessionActionId="action-123",
            actionType="SYNC_OUTPUT_JOB_ATTACHMENTS",
            stepId="step-456",
            taskId="task-789",
            startTime=1234567890.0,
        )

        queue_entry = AttachmentUploadActionQueueEntry(
            cancel=Mock(),
            definition=action_definition,
        )
        session_queue._actions = [queue_entry]
        session_queue._actions_by_id = {"action-123": queue_entry}

        # Act
        action = session_queue.dequeue()

        # Assert
        assert isinstance(action, AttachmentUploadActionImpl)
        assert action._task_id == "task-789"
        assert action._step_id == "step-456"
