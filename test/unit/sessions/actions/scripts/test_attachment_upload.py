# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from unittest.mock import patch, Mock
import pytest
import tempfile
import os

from deadline_worker_agent.sessions.actions.scripts.attachment_upload import main, parse_args


@pytest.fixture
def path_mapping_file_path():
    with tempfile.TemporaryDirectory() as tmpdir_path:
        path_mapping_file_path: str = os.path.join(tmpdir_path, "mapping.json")
        yield path_mapping_file_path


@pytest.fixture
def valid_args(path_mapping_file_path: str):
    return [
        "--path-mapping",
        path_mapping_file_path,
        "--s3-uri",
        "s3://test-bucket/path",
        "--manifest-map",
        '{"root1": "/path/to/manifest1"}',
    ]


class TestAttachmentUpload:

    def test_parse_args(self, path_mapping_file_path: str, valid_args: dict):
        # Test valid arguments
        args = parse_args(valid_args)
        assert args.path_mapping == path_mapping_file_path
        assert args.s3_uri == "s3://test-bucket/path"
        assert args.manifest_map == {"root1": "/path/to/manifest1"}

    def test_parse_args_missing_required(self, path_mapping_file_path: str):
        # Test missing required argument
        invalid_args = [
            "--path-mapping",
            path_mapping_file_path,
            "--manifest-map",
            '{"root1": "/path/to/manifest1"}',
        ]
        with pytest.raises(SystemExit):
            parse_args(invalid_args)

    def test_parse_args_invalid_json(self, path_mapping_file_path: str):
        # Test invalid JSON in manifest-map
        invalid_args = [
            "--path-mapping",
            path_mapping_file_path,
            "--s3-uri",
            "s3://test-bucket/path",
            "--manifest-map",
            "invalid-json",
        ]
        with pytest.raises(SystemExit):
            parse_args(invalid_args)

    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload.snapshot")
    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload.upload")
    def test_main_with_manifests(
        self, mock_upload: Mock, mock_snapshot: Mock, path_mapping_file_path: str, valid_args: dict
    ):
        # Setup mock for snapshot to return some manifests
        mock_snapshot.return_value = ["manifest1", "manifest2"]

        # Run main with test arguments
        main(valid_args)

        # Verify snapshot was called with correct arguments
        mock_snapshot.assert_called_once_with(
            manifest_paths_by_root={"root1": "/path/to/manifest1"}
        )

        # Verify upload was called with correct arguments
        mock_upload.assert_called_once_with(
            manifests=["manifest1", "manifest2"],
            s3_root_uri="s3://test-bucket/path",
            path_mapping_rules=path_mapping_file_path,
        )

    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload.snapshot")
    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload.upload")
    def test_main_no_manifests(self, mock_upload: Mock, mock_snapshot: Mock, valid_args: dict):
        # Setup mock for snapshot to return empty list
        mock_snapshot.return_value = []

        # Run main with test arguments
        main(valid_args)

        # Verify snapshot was called
        mock_snapshot.assert_called_once()

        # Verify upload was not called when no manifests
        mock_upload.assert_not_called()
