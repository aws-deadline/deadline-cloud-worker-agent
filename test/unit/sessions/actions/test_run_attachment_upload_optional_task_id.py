# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from unittest.mock import Mock, patch
import pytest

from deadline_worker_agent.sessions.actions.run_attachment_upload import AttachmentUploadAction
from deadline_worker_agent.sessions.attachment_models import WorkerManifestProperties


@pytest.fixture
def mock_session():
    session = Mock()
    session._run_attachment_sync_task = Mock()

    # Mock the worker manifest properties
    mock_worker_props = Mock(spec=WorkerManifestProperties)
    mock_worker_props.to_dict.return_value = {"test": "data"}
    session.get_worker_manifest_properties_list.return_value = [mock_worker_props]

    # Mock job details
    session._job_details.job_attachment_settings.s3_bucket_name = "test-bucket"
    session._job_details.job_attachment_settings.root_prefix = "test-prefix"

    return session


@pytest.fixture
def mock_executor():
    return Mock()


class TestAttachmentUploadActionOptionalTaskId:
    """Tests for AttachmentUploadAction with optional task_id."""

    def test_init_with_task_id(self):
        """Test creating AttachmentUploadAction with task_id."""
        action = AttachmentUploadAction(
            id="action-123",
            session_id="session-456",
            step_id="step-789",
            task_id="task-abc",
            start_time=1234567890.0,
        )

        assert action._task_id == "task-abc"
        assert action._step_id == "step-789"

    def test_init_without_task_id(self):
        """Test creating AttachmentUploadAction without task_id."""
        action = AttachmentUploadAction(
            id="action-123",
            session_id="session-456",
            step_id="step-789",
            start_time=1234567890.0,
        )

        assert action._task_id is None
        assert action._step_id == "step-789"

    @patch("os.path.exists", return_value=False)
    def test_start_with_task_id_includes_env_var(self, mock_exists, mock_session, mock_executor):
        """Test start() includes DEADLINE_TASK_ID when task_id is provided."""
        action = AttachmentUploadAction(
            id="action-123",
            session_id="session-456",
            step_id="step-789",
            task_id="task-abc",
            start_time=1234567890.0,
        )

        action.start(session=mock_session, executor=mock_executor)

        mock_session._run_attachment_sync_task.assert_called_once()
        call_args = mock_session._run_attachment_sync_task.call_args[1]

        assert "os_env_vars" in call_args
        env_vars = call_args["os_env_vars"]
        assert env_vars["DEADLINE_SESSIONACTION_ID"] == "action-123"
        assert env_vars["DEADLINE_STEP_ID"] == "step-789"
        assert env_vars["DEADLINE_TASK_ID"] == "task-abc"

    @patch("os.path.exists", return_value=False)
    def test_start_without_task_id_excludes_env_var(self, mock_exists, mock_session, mock_executor):
        """Test start() excludes DEADLINE_TASK_ID when task_id is None."""
        action = AttachmentUploadAction(
            id="action-123",
            session_id="session-456",
            step_id="step-789",
            start_time=1234567890.0,
        )

        action.start(session=mock_session, executor=mock_executor)

        mock_session._run_attachment_sync_task.assert_called_once()
        call_args = mock_session._run_attachment_sync_task.call_args[1]

        assert "os_env_vars" in call_args
        env_vars = call_args["os_env_vars"]
        assert env_vars["DEADLINE_SESSIONACTION_ID"] == "action-123"
        assert env_vars["DEADLINE_STEP_ID"] == "step-789"
        assert "DEADLINE_TASK_ID" not in env_vars
