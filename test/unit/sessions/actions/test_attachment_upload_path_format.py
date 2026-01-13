# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import pytest
from unittest.mock import patch, MagicMock
from typing import Generator

import deadline_worker_agent.sessions.actions.scripts.attachment_upload as attachment_upload_mod
from deadline_worker_agent.sessions.actions.scripts.attachment_upload import upload_output_assets
from deadline_worker_agent.sessions.attachment_models import WorkerManifestProperties


@pytest.fixture(autouse=True)
def mock_record_attachment_upload_fail_telemetry_event() -> Generator[MagicMock, None, None]:
    with patch.object(attachment_upload_mod, "record_attachment_upload_fail_telemetry_event") as m:
        yield m


@pytest.fixture(autouse=True)
def mock_record_attachment_upload_telemetry_event() -> Generator[MagicMock, None, None]:
    with patch.object(attachment_upload_mod, "record_attachment_upload_telemetry_event") as m:
        yield m


@pytest.fixture(autouse=True)
def mock_record_attachment_upload_latencies_telemetry_event() -> Generator[MagicMock, None, None]:
    with patch.object(
        attachment_upload_mod, "record_attachment_upload_latencies_telemetry_event"
    ) as m:
        yield m


@pytest.fixture(autouse=True)
def mock_record_success_fail_telemetry_event() -> Generator[MagicMock, None, None]:
    with patch.object(attachment_upload_mod, "record_success_fail_telemetry_event") as m:
        yield m


class TestAttachmentUploadPathFormat:
    """Test that upload_output_assets uses correct path format based on feature flag."""

    @pytest.fixture
    def mock_env_vars(self):
        """Mock environment variables for upload."""
        return {
            "DEADLINE_FARM_ID": "farm-123",
            "DEADLINE_QUEUE_ID": "queue-456",
            "DEADLINE_JOB_ID": "job-789",
            "DEADLINE_STEP_ID": "step-abc",
            "DEADLINE_TASK_ID": "task-def",
            "DEADLINE_SESSIONACTION_ID": "sessionaction-ghi-1",
            "DEADLINE_SESSIONACTION_START_TIME": "1234567890.0",
        }

    @pytest.fixture
    def mock_worker_manifest_properties(self):
        """Mock worker manifest properties."""
        mock_props = MagicMock(spec=WorkerManifestProperties)
        mock_props.root_path = "/test/root"
        mock_props.local_root_path = "/local/test/root"
        mock_props.get_hashed_source_path.return_value = "hashed_path"
        mock_props.as_output_metadata.return_value = {}
        return [mock_props]

    @pytest.fixture
    def mock_root_path_to_output_manifest(self):
        """Mock root path to output manifest mapping."""
        return {"/test/root": "/path/to/output/manifest.json"}

    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload.S3AssetUploader")
    @patch(
        "deadline_worker_agent.sessions.actions.scripts.attachment_upload.JobAttachmentS3Settings"
    )
    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload.decode_manifest")
    @patch("builtins.open")
    def test_old_format_when_feature_disabled(
        self,
        mock_open,
        mock_decode_manifest,
        mock_s3_settings,
        mock_uploader_class,
        mock_env_vars,
        mock_worker_manifest_properties,
        mock_root_path_to_output_manifest,
    ):
        """Test that old format is used when MANIFEST_REPORTING_FEATURE=false."""
        # Arrange
        mock_env_vars["MANIFEST_REPORTING_FEATURE"] = "false"
        mock_s3_settings.partial_session_action_manifest_prefix.return_value = "test/path"
        mock_s3_settings.from_s3_root_uri.return_value = MagicMock()

        mock_uploader = MagicMock()
        mock_uploader_class.return_value = mock_uploader
        mock_uploader.upload_assets.return_value = ("key", "data")

        mock_decode_manifest.return_value = MagicMock()
        mock_open.return_value.__enter__.return_value.read.return_value = "{}"

        with patch.dict(os.environ, mock_env_vars):
            # Act
            upload_output_assets(
                "s3://bucket/root",
                mock_worker_manifest_properties,
                mock_root_path_to_output_manifest,
            )

            # Assert: Should call old format method with task_id
            mock_s3_settings.partial_session_action_manifest_prefix.assert_called_once()
            call_args = mock_s3_settings.partial_session_action_manifest_prefix.call_args[1]
            assert "task_id" in call_args
            assert call_args["task_id"] == "task-def"

    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload.S3AssetUploader")
    @patch(
        "deadline_worker_agent.sessions.actions.scripts.attachment_upload.JobAttachmentS3Settings"
    )
    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload.decode_manifest")
    @patch("builtins.open")
    def test_old_format_when_feature_enabled_but_task_id_present(
        self,
        mock_open,
        mock_decode_manifest,
        mock_s3_settings,
        mock_uploader_class,
        mock_env_vars,
        mock_worker_manifest_properties,
        mock_root_path_to_output_manifest,
    ):
        """Test that old format is used when MANIFEST_REPORTING_FEATURE=true but task_id is present."""
        # Arrange
        mock_env_vars["MANIFEST_REPORTING_FEATURE"] = "true"
        # Keep task_id present
        mock_s3_settings.partial_session_action_manifest_prefix.return_value = "test/path"
        mock_s3_settings.from_s3_root_uri.return_value = MagicMock()

        mock_uploader = MagicMock()
        mock_uploader_class.return_value = mock_uploader
        mock_uploader.upload_assets.return_value = ("key", "data")

        mock_decode_manifest.return_value = MagicMock()
        mock_open.return_value.__enter__.return_value.read.return_value = "{}"

        with patch.dict(os.environ, mock_env_vars):
            # Act
            upload_output_assets(
                "s3://bucket/root",
                mock_worker_manifest_properties,
                mock_root_path_to_output_manifest,
            )

            # Assert: Should call old format method with task_id
            mock_s3_settings.partial_session_action_manifest_prefix.assert_called_once()
            call_args = mock_s3_settings.partial_session_action_manifest_prefix.call_args[1]
            assert "task_id" in call_args
            assert call_args["task_id"] == "task-def"

    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload.S3AssetUploader")
    @patch(
        "deadline_worker_agent.sessions.actions.scripts.attachment_upload.JobAttachmentS3Settings"
    )
    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload.decode_manifest")
    @patch("builtins.open")
    def test_new_format_when_feature_enabled_task_id_not_present(
        self,
        mock_open,
        mock_decode_manifest,
        mock_s3_settings,
        mock_uploader_class,
        mock_env_vars,
        mock_worker_manifest_properties,
        mock_root_path_to_output_manifest,
    ):
        """Test that new format is used when MANIFEST_REPORTING_FEATURE=true."""
        # Arrange
        mock_env_vars["MANIFEST_REPORTING_FEATURE"] = "true"
        # Remove task_id to simulate new format
        del mock_env_vars["DEADLINE_TASK_ID"]

        mock_s3_settings.partial_session_action_manifest_prefix_without_task.return_value = (
            "test/path"
        )
        mock_s3_settings.from_s3_root_uri.return_value = MagicMock()

        mock_uploader = MagicMock()
        mock_uploader_class.return_value = mock_uploader
        mock_uploader.upload_assets.return_value = ("key", "data")

        mock_decode_manifest.return_value = MagicMock()
        mock_open.return_value.__enter__.return_value.read.return_value = "{}"

        with patch.dict(os.environ, mock_env_vars):
            # Act
            upload_output_assets(
                "s3://bucket/root",
                mock_worker_manifest_properties,
                mock_root_path_to_output_manifest,
            )

            # Assert: Should call new format method without task_id
            mock_s3_settings.partial_session_action_manifest_prefix_without_task.assert_called_once()
            call_args = (
                mock_s3_settings.partial_session_action_manifest_prefix_without_task.call_args[1]
            )
            assert "task_id" not in call_args

    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload.S3AssetUploader")
    @patch(
        "deadline_worker_agent.sessions.actions.scripts.attachment_upload.JobAttachmentS3Settings"
    )
    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload.decode_manifest")
    @patch("builtins.open")
    def test_default_behavior_is_old_format(
        self,
        mock_open,
        mock_decode_manifest,
        mock_s3_settings,
        mock_uploader_class,
        mock_env_vars,
        mock_worker_manifest_properties,
        mock_root_path_to_output_manifest,
    ):
        """Test that old format is used by default when feature flag not set."""
        # Arrange: No MANIFEST_REPORTING_FEATURE in environment
        mock_s3_settings.partial_session_action_manifest_prefix.return_value = "test/path"
        mock_s3_settings.from_s3_root_uri.return_value = MagicMock()

        mock_uploader = MagicMock()
        mock_uploader_class.return_value = mock_uploader
        mock_uploader.upload_assets.return_value = ("key", "data")

        mock_decode_manifest.return_value = MagicMock()
        mock_open.return_value.__enter__.return_value.read.return_value = "{}"

        with patch.dict(os.environ, mock_env_vars):
            # Act
            upload_output_assets(
                "s3://bucket/root",
                mock_worker_manifest_properties,
                mock_root_path_to_output_manifest,
            )

            # Assert: Should default to old format with task_id
            mock_s3_settings.partial_session_action_manifest_prefix.assert_called_once()
            call_args = mock_s3_settings.partial_session_action_manifest_prefix.call_args[1]
            assert "task_id" in call_args
            assert call_args["task_id"] == "task-def"
