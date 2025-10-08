# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import json
import pytest
from unittest.mock import Mock, patch, mock_open

from deadline_worker_agent.sessions.actions.scripts.attachment_upload import (
    parse_args,
    parse_worker_manifest_properties,
    merge,
    snapshot,
    upload_output_assets,
    main,
)


class TestAttachmentUpload:
    @pytest.fixture
    def worker_properties_file_path(self) -> str:
        return "/path/to/worker_properties.json"

    @pytest.fixture
    def valid_args(self, worker_properties_file_path: str) -> list[str]:
        return [
            "-s3",
            "s3://test-bucket/path",
            "-wp",
            worker_properties_file_path,
        ]

    @pytest.fixture
    def mock_worker_manifest_properties_data(self) -> list[dict]:
        return [
            {
                "root_path": "/source/path1",
                "local_root_path": "/local/path1",
                "local_manifest_paths": ["/path/to/manifest1.json"],
                "manifest_properties": {
                    "rootPath": "/source/path1",
                    "rootPathFormat": "posix",
                    "outputRelativeDirectories": ["output"],
                },
            }
        ]

    def test_parse_args(self, worker_properties_file_path: str, valid_args: list[str]):
        # Test valid arguments
        args = parse_args(valid_args)
        assert args.s3_uri == "s3://test-bucket/path"
        assert args.worker_properties == worker_properties_file_path

    def test_parse_args_missing_required(self):
        # Test missing required argument
        invalid_args = [
            "-s3",
            "s3://test-bucket/path",
        ]
        with pytest.raises(SystemExit):
            parse_args(invalid_args)

    @patch("builtins.open", new_callable=mock_open)
    @patch("json.load")
    def test_parse_worker_manifest_properties(
        self,
        mock_json_load: Mock,
        mock_file_open: Mock,
        mock_worker_manifest_properties_data: list[dict],
    ):
        # GIVEN
        mock_json_load.return_value = mock_worker_manifest_properties_data

        # WHEN
        with patch(
            "deadline_worker_agent.sessions.attachment_models.WorkerManifestProperties.from_dict"
        ) as mock_from_dict:
            mock_worker_props = Mock()
            mock_from_dict.return_value = mock_worker_props

            result = parse_worker_manifest_properties("/test/path.json")

        # THEN
        assert len(result) == 1
        assert result[0] == mock_worker_props
        mock_from_dict.assert_called_once_with(mock_worker_manifest_properties_data[0])

    def test_parse_worker_manifest_properties_file_error(self):
        # Test file I/O error handling
        with patch("builtins.open", side_effect=IOError("File not found")):
            with pytest.raises(ValueError, match="Error reading worker properties file"):
                parse_worker_manifest_properties("/nonexistent/path.json")

    def test_parse_worker_manifest_properties_json_error(self):
        # Test JSON parsing error handling
        with patch("builtins.open", mock_open(read_data="invalid json")):
            with patch("json.load", side_effect=json.JSONDecodeError("msg", "doc", 0)):
                with pytest.raises(ValueError, match="Error reading worker properties file"):
                    parse_worker_manifest_properties("/test/path.json")

    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload._manifest_merge")
    def test_merge(self, mock_manifest_merge: Mock):
        # GIVEN
        mock_worker_props = Mock()
        mock_worker_props.local_manifest_paths = ["/path/to/manifest.json"]
        mock_worker_props.root_path = "/source/path"
        mock_worker_props.to_path_mapping_rule.return_value.source_path = "/source/path"

        mock_merge_result = Mock()
        mock_merge_result.local_manifest_path = "/merged/manifest.json"
        mock_manifest_merge.return_value = mock_merge_result

        # WHEN
        result = merge([mock_worker_props])

        # THEN
        assert result == {"/source/path": "/merged/manifest.json"}
        mock_manifest_merge.assert_called_once_with(
            root="/source/path",
            manifest_files=["/path/to/manifest.json"],
            destination="manifest_merge",
            name="merge",
        )

    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload._manifest_snapshot")
    def test_snapshot(self, mock_manifest_snapshot: Mock):
        # GIVEN
        mock_worker_props1 = Mock()
        mock_worker_props1.local_root_path = "/local/path1"
        mock_worker_props1.root_path = "/source/path1"
        mock_worker_props1.local_output_relative_directories.return_value = ["output"]

        mock_worker_props2 = Mock()
        mock_worker_props2.local_root_path = "/local/path2"
        mock_worker_props2.root_path = "/source/path2"
        mock_worker_props2.local_output_relative_directories.return_value = ["output"]

        mock_snapshot_result = Mock()
        mock_snapshot_result.manifest = "/snapshot/manifest.json"
        mock_manifest_snapshot.return_value = mock_snapshot_result

        base_manifests = {"/source/path1": "/base/manifest.json", "/source/path2": None}

        # WHEN
        result = snapshot([mock_worker_props1, mock_worker_props2], base_manifests)

        # THEN
        assert result == {
            "/source/path1": "/snapshot/manifest.json",
            "/source/path2": "/snapshot/manifest.json",
        }
        assert mock_manifest_snapshot.call_count == 2
        mock_manifest_snapshot.assert_any_call(
            root="/local/path1",
            destination="manifest_snapshot",
            diff="/base/manifest.json",
            include=["output/**"],
            name="output",
        )
        mock_manifest_snapshot.assert_any_call(
            root="/local/path2",
            destination="manifest_snapshot",
            diff=None,
            include=["output/**"],
            name="output",
        )

    @patch.dict(
        "os.environ",
        {
            "DEADLINE_FARM_ID": "farm-123",
            "DEADLINE_QUEUE_ID": "queue-456",
            "DEADLINE_JOB_ID": "job-789",
            "DEADLINE_STEP_ID": "step-abc",
            "DEADLINE_TASK_ID": "task-def",
            "DEADLINE_SESSIONACTION_ID": "action-ghi",
        },
    )
    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload.S3AssetUploader")
    @patch(
        "deadline_worker_agent.sessions.actions.scripts.attachment_upload.JobAttachmentS3Settings"
    )
    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload.decode_manifest")
    @patch("builtins.open", new_callable=mock_open, read_data='{"files": []}')
    def test_upload_output_assets(
        self, mock_file, mock_decode, mock_s3_settings, mock_uploader_class
    ):
        # GIVEN
        mock_worker_props = Mock()
        mock_worker_props.root_path = "/source/path"
        mock_worker_props.local_root_path = "/local/path"
        mock_worker_props.get_hashed_source_path.return_value = "hash123"
        mock_worker_props.as_output_metadata.return_value = {"Metadata": {"key": "value"}}

        mock_uploader = Mock()
        mock_uploader_class.return_value = mock_uploader
        mock_uploader.upload_assets.return_value = ("manifest_key", "hash_data")

        mock_s3_settings_instance = Mock()
        mock_s3_settings.from_s3_root_uri.return_value = mock_s3_settings_instance
        mock_s3_settings_instance.to_s3_root_uri.return_value = "s3://bucket/path"

        mock_decode.return_value = {"files": []}

        root_to_manifest = {"/source/path": "/output/manifest.json"}

        # WHEN
        result = upload_output_assets("s3://bucket/path", [mock_worker_props], root_to_manifest)

        # THEN
        assert len(result) == 1
        assert result[0].source_path == "/source/path"
        mock_uploader.upload_assets.assert_called_once()

    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload.upload_output_assets")
    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload.snapshot")
    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload.merge")
    @patch(
        "deadline_worker_agent.sessions.actions.scripts.attachment_upload.parse_worker_manifest_properties"
    )
    def test_main_with_output(
        self,
        mock_parse_props: Mock,
        mock_merge: Mock,
        mock_snapshot: Mock,
        mock_upload: Mock,
        valid_args: list[str],
        mock_worker_manifest_properties_data: list[dict],
    ):
        # GIVEN
        mock_worker_props = Mock()
        mock_parse_props.return_value = [mock_worker_props]
        mock_merge.return_value = {"/source/path": "/base/manifest.json"}
        mock_snapshot.return_value = {"/source/path": "/output/manifest.json"}
        mock_upload.return_value = []

        # WHEN
        main(valid_args)

        # THEN
        mock_parse_props.assert_called_once_with("/path/to/worker_properties.json")
        mock_merge.assert_called_once_with(worker_manifest_properties=[mock_worker_props])
        mock_snapshot.assert_called_once_with(
            worker_manifest_properties=[mock_worker_props],
            root_path_to_base_manifest={"/source/path": "/base/manifest.json"},
        )
        mock_upload.assert_called_once_with(
            s3_uri="s3://test-bucket/path",
            worker_manifest_properties=[mock_worker_props],
            root_path_to_output_manifest={"/source/path": "/output/manifest.json"},
        )

    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload.snapshot")
    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload.merge")
    @patch(
        "deadline_worker_agent.sessions.actions.scripts.attachment_upload.parse_worker_manifest_properties"
    )
    def test_main_no_output(
        self, mock_parse_props: Mock, mock_merge: Mock, mock_snapshot: Mock, valid_args: list[str]
    ):
        # GIVEN
        mock_worker_props = Mock()
        mock_parse_props.return_value = [mock_worker_props]
        mock_merge.return_value = {"/source/path": "/base/manifest.json"}
        mock_snapshot.return_value = {}  # No output to upload

        # WHEN
        main(valid_args)

        # THEN
        mock_parse_props.assert_called_once_with("/path/to/worker_properties.json")
        mock_merge.assert_called_once_with(worker_manifest_properties=[mock_worker_props])
        mock_snapshot.assert_called_once_with(
            worker_manifest_properties=[mock_worker_props],
            root_path_to_base_manifest={"/source/path": "/base/manifest.json"},
        )
