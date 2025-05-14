# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from unittest.mock import patch, Mock
import pytest
import tempfile
import os
import json

from deadline_worker_agent.sessions.actions.scripts.attachment_upload import (
    main,
    parse_args,
    merge,
    snapshot,
)


@pytest.fixture
def path_mapping_file_path():
    with tempfile.TemporaryDirectory() as tmpdir_path:
        path_mapping_file_path: str = os.path.join(tmpdir_path, "mapping.json")
        # Write the path mapping rules to the file
        path_mapping_rules = {
            "path_mapping_rules": [
                {"destination_path": "/root1", "source_path": "/source_root1"},
                {"destination_path": "/root2", "source_path": "/source_root2"},
            ]
        }
        with open(path_mapping_file_path, "w") as f:
            json.dump(path_mapping_rules, f)
        yield path_mapping_file_path


@pytest.fixture
def valid_args(path_mapping_file_path: str):
    return [
        "--path-mapping",
        path_mapping_file_path,
        "--s3-uri",
        "s3://test-bucket/path",
        "--manifest-paths-by-root",
        '{"root1": ["/path/to/manifest1"]}',
        "--out-rel-dirs-by-root",
        "{}",
    ]


@pytest.fixture
def valid_args_with_snapshot_include_dirs(path_mapping_file_path: str):
    return [
        "--path-mapping",
        path_mapping_file_path,
        "--s3-uri",
        "s3://test-bucket/path",
        "--manifest-paths-by-root",
        '{"root1": ["/path/to/manifest1"]}',
        "--out-rel-dirs-by-root",
        '{"root1": ["/path/to/include/dir1", "/path/to/include/dir2"]}',
    ]


@pytest.fixture
def valid_args_merge(path_mapping_file_path: str):
    return [
        "--path-mapping",
        path_mapping_file_path,
        "--s3-uri",
        "s3://test-bucket/path",
        "--manifest-paths-by-root",
        '{"root1": ["/path/to/manifest1", "/path/to/manifest2"]}',
        "--out-rel-dirs-by-root",
        "{}",
    ]


class TestAttachmentUpload:
    def test_parse_args(self, path_mapping_file_path: str, valid_args: dict):
        # Test valid arguments
        args = parse_args(valid_args)
        assert args.path_mapping == path_mapping_file_path
        assert args.s3_uri == "s3://test-bucket/path"
        assert args.manifest_paths_by_root == {"root1": ["/path/to/manifest1"]}

    def test_parse_args_missing_required(self, path_mapping_file_path: str):
        # Test missing required argument
        invalid_args = [
            "--path-mapping",
            path_mapping_file_path,
            "--manifest-paths-by-root",
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
            "--manifest-paths-by-root",
            "invalid-json",
        ]
        with pytest.raises(SystemExit):
            parse_args(invalid_args)

    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload.merge")
    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload.snapshot")
    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload.upload")
    def test_main_with_manifests_and_include_dirs(
        self,
        mock_upload: Mock,
        mock_snapshot: Mock,
        mock_merge: Mock,
        path_mapping_file_path: str,
        valid_args_with_snapshot_include_dirs: dict,
    ):
        # Setup mock for merge to return some manifests
        mock_merge.return_value = {"root1": "/path/to/manifest1"}

        # Setup mock for snapshot to return some manifests
        mock_snapshot.return_value = ["manifest1", "manifest2"]

        # Run main with test arguments that include out_rel_dirs_by_root
        main(valid_args_with_snapshot_include_dirs)

        mock_merge.assert_called_once_with(
            manifest_paths_by_root={"root1": ["/path/to/manifest1"]},
            path_mapping_rules_file=path_mapping_file_path,
        )

        # Verify snapshot was called with correct arguments including the out_rel_dirs_by_root
        mock_snapshot.assert_called_once_with(
            manifest_path_by_root={"root1": "/path/to/manifest1"},
            out_rel_dirs_by_root={"root1": ["/path/to/include/dir1", "/path/to/include/dir2"]},
        )

        # Verify upload was called with correct arguments
        mock_upload.assert_called_once_with(
            manifests=["manifest1", "manifest2"],
            s3_root_uri="s3://test-bucket/path",
            path_mapping_rules=path_mapping_file_path,
        )

    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload.merge")
    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload.snapshot")
    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload.upload")
    def test_main_with_manifests_merge(
        self,
        mock_upload: Mock,
        mock_snapshot: Mock,
        mock_merge: Mock,
        path_mapping_file_path: str,
        valid_args_merge: dict,
    ):
        # Setup mock for merge to return merged manifests
        merged_manifest_path_by_root = {"root1": "/path/to/merged/manifest"}
        mock_merge.return_value = merged_manifest_path_by_root

        # Setup mock for snapshot to return some manifests
        mock_snapshot.return_value = ["manifest1", "manifest2"]

        # Run main with test arguments
        main(valid_args_merge)

        mock_merge.assert_called_once_with(
            manifest_paths_by_root={"root1": ["/path/to/manifest1", "/path/to/manifest2"]},
            path_mapping_rules_file=path_mapping_file_path,
        )

        # Verify snapshot was called with correct arguments
        mock_snapshot.assert_called_once_with(
            manifest_path_by_root=merged_manifest_path_by_root, out_rel_dirs_by_root={}
        )

        # Verify upload was called with correct arguments
        mock_upload.assert_called_once_with(
            manifests=["manifest1", "manifest2"],
            s3_root_uri="s3://test-bucket/path",
            path_mapping_rules=path_mapping_file_path,
        )

    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload.merge")
    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload.snapshot")
    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload.upload")
    def test_main_no_manifests(
        self,
        mock_upload: Mock,
        mock_snapshot: Mock,
        mock_merge: Mock,
        valid_args: dict,
        path_mapping_file_path: str,
    ):
        # Setup mock for merge to return some manifests
        mock_merge.return_value = dict()

        # Setup mock for snapshot to return empty list
        mock_snapshot.return_value = []

        # Run main with test arguments
        main(valid_args)

        # Verify merge was called with correct arguments
        mock_merge.assert_called_once_with(
            manifest_paths_by_root={"root1": ["/path/to/manifest1"]},
            path_mapping_rules_file=path_mapping_file_path,
        )

        # Verify snapshot was called
        mock_snapshot.assert_called_once()

        # Verify upload was not called when no manifests
        mock_upload.assert_not_called()

    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload._manifest_merge")
    def test_merge_multiple_manifests(self, mock_manifest_merge: Mock, path_mapping_file_path: str):
        # Test case where a root has multiple manifests
        mock_manifest_merge.side_effect = [
            Mock(manifest_root="/source_root1", local_manifest_path="/merged/manifest/path"),
        ]

        input_data = {
            "/root1": ["/path/to/manifest1", "/path/to/manifest2"],
            "/root2": ["/path/to/single_manifest"],
        }

        result = merge(input_data, path_mapping_file_path)

        expected = {"/root1": "/merged/manifest/path", "/root2": "/path/to/single_manifest"}

        assert result == expected
        assert mock_manifest_merge.call_count == 1

        # Verify _manifest_merge was called with the correct arguments
        mock_manifest_merge.assert_called_once_with(
            root="/source_root1",  # Should use the source path from mapping
            manifest_files=["/path/to/manifest1", "/path/to/manifest2"],
            destination=os.path.join(os.getcwd(), "manifest"),
            name="merge",
        )

    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload._manifest_snapshot")
    def test__manifest_snapshot_diff_include(self, mock_manifest_snapshot: Mock):
        # Setup mock for _manifest_snapshot to return some manifests
        mock_manifest_snapshot.side_effect = [
            Mock(
                manifest="/path/to/result1",
            ),
            Mock(
                manifest="/path/to/result2",
            ),
        ]

        # Define test input data
        manifest_path_by_root = {
            "/root1": "/path/to/base/manifest1",
            "/root2": "/path/to/base/manifest2",
        }

        out_rel_dirs_by_root = {
            "/root1": ["/path/to/include/dir1", "/path/to/include/dir2"],
            "/root2": ["/path/to/include/dir3"],
        }

        # Call the function under test
        result = snapshot(manifest_path_by_root, out_rel_dirs_by_root)

        # Verify the results
        assert result == ["/path/to/result1", "/path/to/result2"]

        # Verify _manifest_snapshot was called with the correct arguments
        assert mock_manifest_snapshot.call_count == 2

        # Check first call
        mock_manifest_snapshot.assert_any_call(
            root="/root1",
            destination=os.path.join(os.getcwd(), "diff"),
            name=f"output-{os.path.basename('/path/to/base/manifest1')}",
            diff="/path/to/base/manifest1",
            include=["/path/to/include/dir1/**", "/path/to/include/dir2/**"],
        )

        # Check second call
        mock_manifest_snapshot.assert_any_call(
            root="/root2",
            destination=os.path.join(os.getcwd(), "diff"),
            name=f"output-{os.path.basename('/path/to/base/manifest2')}",
            diff="/path/to/base/manifest2",
            include=["/path/to/include/dir3/**"],
        )
