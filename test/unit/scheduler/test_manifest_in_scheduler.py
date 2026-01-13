# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

from deadline_worker_agent.scheduler.scheduler import WorkerScheduler
from deadline_worker_agent.scheduler.session_action_status import SessionActionStatus
from deadline_worker_agent.api_models import ManifestInfo
from deadline_worker_agent.feature_flag import MANIFEST_REPORTING_FEATURE
from openjd.sessions import ActionStatus, ActionState


@pytest.mark.skipif(
    not MANIFEST_REPORTING_FEATURE,
    reason="Only relevant when MANIFEST_REPORTING_FEATURE is enabled",
)
class TestSchedulerManifests:
    @pytest.fixture
    def scheduler(self):
        # Create a minimal scheduler for testing
        scheduler = MagicMock()
        scheduler._updated_action_to_boto = WorkerScheduler._updated_action_to_boto.__get__(
            scheduler, WorkerScheduler
        )
        return scheduler

    def test_updated_action_to_boto_includes_manifests(self, scheduler):
        # Arrange
        now = datetime.now(timezone.utc)
        action_status = ActionStatus(state=ActionState.SUCCESS)
        manifests = [
            ManifestInfo(
                outputManifestPath="s3://bucket/Manifests/key1",
                outputManifestHash="hash1",
            ),
            ManifestInfo(
                outputManifestPath="s3://bucket/Manifests/key2",
                outputManifestHash="hash2",
            ),
            ManifestInfo(),  # Empty object for a root with no changes
        ]

        action_updated = SessionActionStatus(
            id="test_action_id",
            status=action_status,
            start_time=now,
            end_time=now,
            completed_status="SUCCEEDED",
            manifests=manifests,
        )

        # Act
        result = scheduler._updated_action_to_boto(action_updated)

        # Assert
        assert "manifests" in result
        assert result["manifests"] == manifests

    @patch("deadline_worker_agent.scheduler.scheduler.MANIFEST_REPORTING_FEATURE", False)
    def test_updated_action_to_boto_excludes_manifests_when_feature_disabled(self, scheduler):
        # Arrange
        now = datetime.now(timezone.utc)
        action_status = ActionStatus(state=ActionState.SUCCESS)
        manifests = [
            ManifestInfo(
                outputManifestPath="s3://bucket/Manifests/key1",
                outputManifestHash="hash1",
            ),
        ]

        action_updated = SessionActionStatus(
            id="test_action_id",
            status=action_status,
            start_time=now,
            end_time=now,
            completed_status="SUCCEEDED",
            manifests=manifests,
        )

        # Act
        result = scheduler._updated_action_to_boto(action_updated)

        # Assert
        assert "manifests" not in result

    def test_updated_action_to_boto_excludes_manifests_when_none(self, scheduler):
        # Arrange
        now = datetime.now(timezone.utc)
        action_status = ActionStatus(state=ActionState.SUCCESS)

        action_updated = SessionActionStatus(
            id="test_action_id",
            status=action_status,
            start_time=now,
            end_time=now,
            completed_status="SUCCEEDED",
            # No manifests field
        )

        # Act
        result = scheduler._updated_action_to_boto(action_updated)

        # Assert
        assert "manifests" not in result

    def test_updated_action_to_boto_handles_empty_manifests_list(self, scheduler):
        # Arrange
        now = datetime.now(timezone.utc)
        action_status = ActionStatus(state=ActionState.SUCCESS)

        action_updated = SessionActionStatus(
            id="test_action_id",
            status=action_status,
            start_time=now,
            end_time=now,
            completed_status="SUCCEEDED",
            manifests=[],  # Empty list
        )

        # Act
        result = scheduler._updated_action_to_boto(action_updated)

        # Assert
        assert "manifests" in result
        assert result["manifests"] == []
