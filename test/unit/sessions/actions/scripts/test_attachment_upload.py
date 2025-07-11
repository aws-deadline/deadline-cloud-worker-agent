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
    upload,
)


@pytest.fixture
def tmpdir_path():
    with tempfile.TemporaryDirectory() as tmpdir_path:
        yield tmpdir_path


@pytest.fixture
def path_mapping_file_path(tmpdir_path):
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
            destination=os.path.join(os.getcwd(), "merge"),
            name="merge",
        )

    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload._manifest_snapshot")
    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload.time")
    def test_manifest_snapshot_diff_include(
        self, mock_time: Mock, mock_manifest_snapshot: Mock, tmpdir_path: str
    ):
        # Freeze time to a specific timestamp
        frozen_timestamp = 1625097600.0  # 2021-07-01 00:00:00 UTC
        mock_time.time.return_value = frozen_timestamp

        # Create the result files that will be returned by the mock
        result1_path = os.path.join(tmpdir_path, "result1")
        result2_path = os.path.join(tmpdir_path, "result2")

        # Write some content to the result files
        with open(result1_path, "w") as f:
            f.write("This is result1 content")

        with open(result2_path, "w") as f:
            f.write("This is result2 content")

        # Setup mock for _manifest_snapshot to return some manifests
        mock_manifest_snapshot.side_effect = [
            Mock(
                manifest=result1_path,
            ),
            Mock(
                manifest=result2_path,
            ),
        ]

        # Create base directory and files
        base_dir = os.path.join(tmpdir_path, "base")
        os.makedirs(base_dir, exist_ok=True)

        hash1_job_path = os.path.join(base_dir, "hash1_job")
        with open(hash1_job_path, "w") as f:
            f.write("hash1_job content")

        merge_hash2_path = os.path.join(base_dir, "merge-hash2-timestamp.manifest")
        with open(merge_hash2_path, "w") as f:
            f.write("merge-hash2-timestamp.manifest content")

        # Define test input data
        manifest_path_by_root = {
            "/root1": hash1_job_path,
            "/root2": merge_hash2_path,
        }

        # Create include directories
        include_dir1 = os.path.join(tmpdir_path, "include", "dir1")
        include_dir2 = os.path.join(tmpdir_path, "include", "dir2")
        include_dir3 = os.path.join(tmpdir_path, "include", "dir3")

        out_rel_dirs_by_root = {
            "/root1": [include_dir1, include_dir2],
            "/root2": [include_dir3],
        }

        # Call the function under test
        result = snapshot(manifest_path_by_root, out_rel_dirs_by_root)

        # Verify the results
        expected_results = [
            os.path.join(tmpdir_path, "hash1_output"),
            os.path.join(tmpdir_path, "hash2_output"),
        ]
        assert result == expected_results

        # Verify that the files in expected_results actually exist on disk
        for file_path in expected_results:
            assert os.path.exists(file_path), f"File {file_path} does not exist on disk"
            assert os.path.isfile(file_path), f"Path {file_path} is not a file"
            with open(file_path, "r") as f:
                content = f.read()
                reusult = "result1" if "hash1" in file_path else "result2"
                assert f"This is {reusult} content" in content, (
                    f"File {file_path} doesn't contain expected content"
                )

        # Verify _manifest_snapshot was called with the correct arguments
        assert mock_manifest_snapshot.call_count == 2

        # Verify time.time() was called to generate the timestamp for the diff directory
        mock_time.time.assert_called_once()

        # Verify the diff directory name contains the expected timestamp (milliseconds)
        expected_timestamp_ms = int(frozen_timestamp * 1000)
        expected_diff_dir = os.path.join(os.getcwd(), f"diff-{expected_timestamp_ms}")

        # Check first call
        mock_manifest_snapshot.assert_any_call(
            root="/root1",
            destination=expected_diff_dir,
            name=f"output-{os.path.basename(hash1_job_path)}",
            diff=hash1_job_path,
            include=[f"{include_dir1}/**", f"{include_dir2}/**"],
        )

        # Check second call
        mock_manifest_snapshot.assert_any_call(
            root="/root2",
            destination=expected_diff_dir,
            name=f"output-{os.path.basename(merge_hash2_path)}",
            diff=merge_hash2_path,
            include=[f"{include_dir3}/**"],
        )

    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload.api.attachment_upload")
    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload.boto3.session.Session")
    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload.time")
    def test_upload_with_environment_variables(
        self, mock_time, mock_boto3_session, mock_attachment_upload
    ):
        # Setup mock for datetime
        mock_time.time.return_value = 1747952223.4090126

        # Setup mock for boto3 session
        mock_session = Mock()
        mock_boto3_session.return_value = mock_session

        # Setup environment variables
        with patch.dict(
            os.environ,
            {
                "DEADLINE_FARM_ID": "farm-123",
                "DEADLINE_QUEUE_ID": "queue-456",
                "DEADLINE_JOB_ID": "job-789",
                "DEADLINE_STEP_ID": "step-012",
                "DEADLINE_TASK_ID": "task-345",
                "DEADLINE_SESSIONACTION_ID": "sessionaction-678",
            },
        ):
            # Call the function under test
            manifests = ["manifest1", "manifest2"]
            s3_root_uri = "s3://test-bucket/path"
            path_mapping_rules = "/path/to/mapping.json"

            upload(s3_root_uri, path_mapping_rules, manifests)

            # Expected S3 path based on environment variables and datetime
            expected_s3_path = "farm-123/queue-456/job-789/step-012/task-345/2025-05-22T22:17:03.409012Z_sessionaction-678"

            # Verify attachment_upload was called with correct arguments
            mock_attachment_upload.assert_called_once_with(
                manifests=manifests,
                s3_root_uri=s3_root_uri,
                boto3_session=mock_session,
                path_mapping_rules=path_mapping_rules,
                upload_manifest_path=expected_s3_path,
            )
