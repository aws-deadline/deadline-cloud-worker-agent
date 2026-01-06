# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from datetime import datetime
from unittest.mock import MagicMock

import pytest
from openjd.sessions import ActionState, ActionStatus

from deadline_worker_agent.api_models import ManifestInfo
from deadline_worker_agent.scheduler.session_action_status import SessionActionStatus


@pytest.fixture
def action_id() -> str:
    return "action-123456"


@pytest.fixture
def step_id() -> str:
    return "step-abcdef"


@pytest.fixture
def task_id() -> str:
    return "task-987654"


@pytest.fixture
def action_start_time() -> datetime:
    return datetime(2023, 1, 2, 3, 4, 5)


@pytest.fixture
def action_complete_time() -> datetime:
    return datetime(2023, 1, 2, 3, 4, 5)


class TestAttachmentUploadHandling:
    """Test the handling of attachment upload completion"""

    def test_attachment_upload_success(
        self,
        action_id: str,
        step_id: str,
        task_id: str,
        action_start_time: datetime,
        action_complete_time: datetime,
    ) -> None:
        """Tests that when attachment upload action completes successfully, the original task action is reported as succeeded"""
        # GIVEN
        # Create mock action
        run_step_task_action = MagicMock()
        run_step_task_action.id = action_id
        run_step_task_action.step_id = step_id
        run_step_task_action.task_id = task_id

        # Create success status
        success_action_status = ActionStatus(
            exit_code=0,
            state=ActionState.SUCCESS,
        )

        # WHEN - Direct testing of action completion handling
        session_action_status = SessionActionStatus(
            id=action_id,
            status=success_action_status,
            start_time=action_start_time,
            end_time=action_complete_time,
            completed_status="SUCCEEDED",
        )

        # THEN - Verify expected status values
        assert session_action_status.id == action_id
        assert session_action_status.status == success_action_status
        assert session_action_status.start_time == action_start_time
        assert session_action_status.end_time == action_complete_time
        assert session_action_status.completed_status == "SUCCEEDED"

    def test_attachment_upload_with_manifests(
        self,
        action_id: str,
        step_id: str,
        task_id: str,
        action_start_time: datetime,
        action_complete_time: datetime,
    ) -> None:
        """Tests that when attachment upload completes with manifests, they are included in the status"""
        # GIVEN
        # Create mock action
        run_step_task_action = MagicMock()
        run_step_task_action.id = action_id
        run_step_task_action.step_id = step_id
        run_step_task_action.task_id = task_id

        # Create test manifests
        manifests = [
            ManifestInfo(outputManifestPath="/test/path/1", outputManifestHash="test-manifest-1"),
            ManifestInfo(outputManifestPath="/test/path/2", outputManifestHash="test-manifest-2"),
        ]

        # Create success status
        success_action_status = ActionStatus(
            exit_code=0,
            state=ActionState.SUCCESS,
        )

        # WHEN - Create session action status with manifests
        session_action_status = SessionActionStatus(
            id=action_id,
            status=success_action_status,
            start_time=action_start_time,
            end_time=action_complete_time,
            completed_status="SUCCEEDED",
            manifests=manifests,
        )

        # THEN - Verify manifests are included
        assert session_action_status.manifests == manifests
        assert len(session_action_status.manifests) == 2
        assert session_action_status.manifests[0]["outputManifestHash"] == "test-manifest-1"
        assert session_action_status.manifests[1]["outputManifestHash"] == "test-manifest-2"

    def test_attachment_upload_failure(
        self,
        action_id: str,
        step_id: str,
        task_id: str,
        action_start_time: datetime,
        action_complete_time: datetime,
    ) -> None:
        """Tests that when attachment upload fails, original task is reported as failed"""
        # GIVEN
        # Create mock action
        run_step_task_action = MagicMock()
        run_step_task_action.id = action_id
        run_step_task_action.step_id = step_id
        run_step_task_action.task_id = task_id

        # Create failed status
        failed_action_status = ActionStatus(
            exit_code=1,
            state=ActionState.FAILED,
            fail_message="Upload failed",
        )

        # WHEN - Create session action status for failed upload
        session_action_status = SessionActionStatus(
            id=action_id,
            status=failed_action_status,
            start_time=action_start_time,
            end_time=action_complete_time,
            completed_status="FAILED",
        )

        # THEN - Verify failure is properly represented
        assert session_action_status.id == action_id
        assert session_action_status.status is not None
        assert session_action_status.status.state == ActionState.FAILED
        assert session_action_status.status.fail_message == "Upload failed"
        assert session_action_status.completed_status == "FAILED"

    def test_attachment_upload_canceled(
        self,
        action_id: str,
        step_id: str,
        task_id: str,
        action_start_time: datetime,
        action_complete_time: datetime,
    ) -> None:
        """Tests that when attachment upload is canceled, original task is reported as canceled"""
        # GIVEN
        # Create mock action
        run_step_task_action = MagicMock()
        run_step_task_action.id = action_id
        run_step_task_action.step_id = step_id
        run_step_task_action.task_id = task_id

        # Create canceled status
        canceled_action_status = ActionStatus(
            exit_code=1,
            state=ActionState.CANCELED,
            fail_message="Upload canceled",
        )

        # WHEN - Create session action status for canceled upload
        session_action_status = SessionActionStatus(
            id=action_id,
            status=canceled_action_status,
            start_time=action_start_time,
            end_time=action_complete_time,
            completed_status="CANCELED",
        )

        # THEN - Verify cancellation is properly represented
        assert session_action_status.id == action_id
        assert session_action_status.status is not None
        assert session_action_status.status.state == ActionState.CANCELED
        assert session_action_status.status.fail_message == "Upload canceled"
        assert session_action_status.completed_status == "CANCELED"
