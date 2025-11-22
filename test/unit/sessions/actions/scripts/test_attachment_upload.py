# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import json
import pytest
from pathlib import Path
from unittest.mock import Mock, call, patch, mock_open
from typing import Optional, Generator

import deadline_worker_agent.sessions.actions.scripts.attachment_upload as attachment_upload_mod
from deadline.job_attachments.asset_manifests.decode import decode_manifest
from deadline.job_attachments.progress_tracker import ProgressStatus, ProgressTracker
from deadline.job_attachments.models import JobAttachmentS3Settings
from deadline_worker_agent.sessions.attachment_models import (
    WorkerManifestProperties,
    ManifestProperties,
    PathFormat,
)
from deadline_worker_agent.sessions.actions.scripts.attachment_upload import (
    parse_args,
    parse_worker_manifest_properties,
    merge,
    snapshot,
    upload_output_assets,
    main,
)


@pytest.fixture(autouse=True)
def mock_record_attachment_upload_fail_telemetry_event() -> Generator[Mock, None, None]:
    with patch.object(attachment_upload_mod, "record_attachment_upload_fail_telemetry_event") as m:
        yield m


@pytest.fixture(autouse=True)
def mock_record_attachment_upload_telemetry_event() -> Generator[Mock, None, None]:
    with patch.object(attachment_upload_mod, "record_attachment_upload_telemetry_event") as m:
        yield m


@pytest.fixture(autouse=True)
def mock_record_attachment_upload_latencies_telemetry_event() -> Generator[Mock, None, None]:
    with patch.object(
        attachment_upload_mod, "record_attachment_upload_latencies_telemetry_event"
    ) as m:
        yield m


@pytest.fixture(autouse=True)
def mock_record_success_fail_telemetry_event() -> Generator[Mock, None, None]:
    with patch.object(attachment_upload_mod, "record_success_fail_telemetry_event") as m:
        yield m


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
            print_function_callback=print,
        )

    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload._manifest_snapshot")
    def test_snapshot_mixed_output_directories(self, mock_manifest_snapshot: Mock):
        # GIVEN
        mock_worker_props1 = Mock()
        mock_worker_props1.local_root_path = "/local/path1"
        mock_worker_props1.root_path = "/source/path1"
        mock_worker_props1.local_output_relative_directories.return_value = None
        mock_worker_props1.output_relative_directories = None

        mock_worker_props2 = Mock()
        mock_worker_props2.local_root_path = "/local/path2"
        mock_worker_props2.root_path = "/source/path2"
        mock_worker_props2.local_output_relative_directories.return_value = ["output"]

        mock_worker_props3 = Mock()
        mock_worker_props3.local_root_path = "/local/path3"
        mock_worker_props3.root_path = "/source/path3"
        mock_worker_props3.local_output_relative_directories.return_value = []
        mock_worker_props3.output_relative_directories = []

        mock_worker_props4 = Mock()
        mock_worker_props4.local_root_path = "/local/path4"
        mock_worker_props4.root_path = "/source/path4"
        mock_worker_props4.local_output_relative_directories.return_value = ["."]

        mock_snapshot_result = Mock()
        mock_snapshot_result.manifest = "/snapshot/manifest.json"
        mock_manifest_snapshot.return_value = mock_snapshot_result

        base_manifests = {
            "/source/path1": None,
            "/source/path2": "/base/manifest.json",
            "/source/path3": None,
            "/source/path4": None,
        }

        # WHEN
        result = snapshot(
            [mock_worker_props1, mock_worker_props2, mock_worker_props3, mock_worker_props4],
            base_manifests,
        )

        # THEN
        assert result == {
            "/source/path2": "/snapshot/manifest.json",
            "/source/path4": "/snapshot/manifest.json",
        }
        assert mock_manifest_snapshot.call_count == 2
        mock_manifest_snapshot.assert_any_call(
            root="/local/path2",
            destination="manifest_snapshot",
            diff="/base/manifest.json",
            include=["output/**"],
            name="output",
            print_function_callback=print,
        )
        mock_manifest_snapshot.assert_any_call(
            root="/local/path4",
            destination="manifest_snapshot",
            diff=None,
            include=["./**"],
            name="output",
            print_function_callback=print,
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
            print_function_callback=print,
        )
        mock_manifest_snapshot.assert_any_call(
            root="/local/path2",
            destination="manifest_snapshot",
            diff=None,
            include=["output/**"],
            name="output",
            print_function_callback=print,
        )

    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_upload._manifest_snapshot")
    def test_snapshot_with_special_characters_in_directory_names(
        self, mock_manifest_snapshot: Mock
    ):
        # GIVEN - Test that glob.escape is used for directory names with special characters
        mock_worker_props = Mock()
        mock_worker_props.local_root_path = "/local/path"
        mock_worker_props.root_path = "/source/path"
        mock_worker_props.local_output_relative_directories.return_value = [
            "output[test]",  # Contains glob special characters
            "output*dir",  # Contains glob wildcard
            "output?dir",  # Contains glob single char wildcard
            "[data_]*dir2?",  # Complex case with multiple special chars
        ]

        mock_snapshot_result = Mock()
        mock_snapshot_result.manifest = "/snapshot/manifest.json"
        mock_manifest_snapshot.return_value = mock_snapshot_result

        # WHEN
        result = snapshot([mock_worker_props], {"/source/path": None})

        # THEN
        assert result == {"/source/path": "/snapshot/manifest.json"}
        mock_manifest_snapshot.assert_called_once_with(
            root="/local/path",
            destination="manifest_snapshot",
            diff=None,
            include=[
                "output[[]test]/**",  # Escaped brackets
                "output[*]dir/**",  # Escaped asterisk
                "output[?]dir/**",  # Escaped question mark
                "[[]data_][*]dir2[?]/**",  # Complex case with multiple special chars
            ],
            name="output",
            print_function_callback=print,
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
            "DEADLINE_SESSIONACTION_START_TIME": "1234567890.0",
        },
    )
    @patch.object(attachment_upload_mod, "ProgressTracker")
    @patch.object(attachment_upload_mod.config_file, "get_cache_directory")
    @patch.object(attachment_upload_mod, "S3AssetUploader")
    def test_upload_output_assets(
        self,
        mock_uploader_class: Mock,
        mock_get_cache_directory: Mock,
        mock_progress_tracker: Mock,
        mock_record_attachment_upload_telemetry_event: Mock,
    ):
        # GIVEN
        worker_props = [
            WorkerManifestProperties(
                manifest_properties=ManifestProperties(
                    rootPath="/root/path/one",
                    rootPathFormat=PathFormat.POSIX,
                ),
                local_root_path="/local/path/one",
            ),
            WorkerManifestProperties(
                manifest_properties=ManifestProperties(
                    rootPath="/root/path/two",
                    rootPathFormat=PathFormat.POSIX,
                ),
                local_root_path="/local/path/two",
            ),
        ]

        mock_upload_assets: Mock = mock_uploader_class.return_value.upload_assets
        mock_upload_assets.return_value = ("manifest_key", "hash_data")

        s3_settings = JobAttachmentS3Settings(
            s3BucketName="bucket",
            rootPrefix="path",
        )

        manifest_one = {
            "hashAlg": "xxh128",
            "manifestVersion": "2023-03-03",
            "paths": [
                {
                    "path": "one.txt",
                    "hash": "abc123",
                    "size": 123,
                    "mtime": 456,
                },
            ],
            "totalSize": 123,
        }
        manifest_two = {
            "hashAlg": "xxh128",
            "manifestVersion": "2023-03-03",
            "paths": [
                {
                    "path": "two.txt",
                    "hash": "xyz987",
                    "size": 987,
                    "mtime": 654,
                },
                {
                    "path": "three.txt",
                    "hash": "testhash",
                    "size": 123,
                    "mtime": 789,
                },
            ],
            "totalSize": 123 + 987,
        }

        # Since we're mocking the actual upload the progress tracker doesn't get any updates
        # Prepopulate the tracker with expected values and make sure it gets recorded
        expected_total_bytes = 123 + 123 + 987
        expected_total_files = 1 + 2
        progress_tracker_stub = ProgressTracker(
            status=ProgressStatus.NONE,
            total_files=expected_total_files,
            total_bytes=expected_total_bytes,
        )
        progress_tracker_stub.processed_bytes = expected_total_bytes
        progress_tracker_stub.processed_files = expected_total_files
        mock_progress_tracker.return_value = progress_tracker_stub

        with patch.object(
            attachment_upload_mod,
            "open",
            side_effect=[
                mock_open(read_data=json.dumps(manifest_one)).return_value,
                mock_open(read_data=json.dumps(manifest_two)).return_value,
            ],
        ):
            # WHEN
            result = upload_output_assets(
                s3_uri=s3_settings.to_s3_root_uri(),
                worker_manifest_properties=worker_props,
                root_path_to_output_manifest={
                    worker_props[0].root_path: "/output1/manifest.json",
                    worker_props[1].root_path: "/output2/manifest.json",
                },
            )

        # THEN
        assert len(result) == 2
        assert result[0].source_path == worker_props[0].root_path
        assert result[1].source_path == worker_props[1].root_path

        expected_partial_manifest_prefix = (
            JobAttachmentS3Settings.partial_session_action_manifest_prefix(
                farm_id="farm-123",
                queue_id="queue-456",
                job_id="job-789",
                step_id="step-abc",
                task_id="task-def",
                session_action_id="action-ghi",
                time=1234567890.0,
            )
        )
        mock_upload_assets.assert_has_calls(
            calls=[
                call(
                    job_attachment_settings=s3_settings,
                    manifest=decode_manifest(json.dumps(manifest_one)),
                    partial_manifest_prefix=expected_partial_manifest_prefix,
                    manifest_file_name=f"{worker_props[0].get_hashed_source_path()}_output",
                    manifest_metadata=worker_props[0].as_output_metadata(),
                    source_root=Path(worker_props[0].root_path),
                    asset_root=Path(worker_props[0].local_root_path),
                    s3_check_cache_dir=mock_get_cache_directory.return_value,
                    progress_tracker=progress_tracker_stub,
                ),
                call(
                    job_attachment_settings=s3_settings,
                    manifest=decode_manifest(json.dumps(manifest_two)),
                    partial_manifest_prefix=expected_partial_manifest_prefix,
                    manifest_file_name=f"{worker_props[1].get_hashed_source_path()}_output",
                    manifest_metadata=worker_props[1].as_output_metadata(),
                    source_root=Path(worker_props[1].root_path),
                    asset_root=Path(worker_props[1].local_root_path),
                    s3_check_cache_dir=mock_get_cache_directory.return_value,
                    progress_tracker=progress_tracker_stub,
                ),
            ],
        )

        # Verify telemetry
        mock_record_attachment_upload_telemetry_event.assert_called_once_with(
            queue_id="queue-unknown",
            upload_summary=progress_tracker_stub.get_summary_statistics(),
            manifest_total_bytes=expected_total_bytes,
            manifest_total_files=expected_total_files,
        )

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


class TestTelemetry:
    """Test cases for telemetry functionality."""

    @patch.object(attachment_upload_mod, "record_attachment_upload_fail_telemetry_event")
    @patch("builtins.open", side_effect=FileNotFoundError("File not found"))
    def test_failure_telemetry_decorator_on_parse_worker_manifest_properties(
        self, mock_open: Mock, mock_fail_telemetry: Mock
    ):
        """Test that @failure_telemetry decorator records failures for parse_worker_manifest_properties."""
        with pytest.raises(ValueError):
            # WHEN
            parse_worker_manifest_properties("/nonexistent/file.json")

        # THEN
        mock_fail_telemetry.assert_called_once_with(
            queue_id="queue-unknown",
            failure_reason="parse_worker_manifest_properties: ValueError",
        )

    @patch.object(attachment_upload_mod, "record_attachment_upload_fail_telemetry_event")
    @patch.object(attachment_upload_mod, "_manifest_merge")
    def test_failure_telemetry_decorator_on_merge(
        self, mock_manifest_merge: Mock, mock_fail_telemetry: Mock
    ):
        """Test that @failure_telemetry decorator records failures for merge function."""
        # GIVEN
        mock_manifest_merge.side_effect = Exception("Merge failed")

        with pytest.raises(Exception, match="Merge failed"):
            # WHEN
            merge([Mock()])

        # THEN
        mock_fail_telemetry.assert_called_once_with(
            queue_id="queue-unknown",
            failure_reason="merge: Exception",
        )

    @patch.object(attachment_upload_mod, "record_attachment_upload_fail_telemetry_event")
    @patch.object(attachment_upload_mod, "_manifest_snapshot")
    def test_failure_telemetry_decorator_on_snapshot(
        self, mock_manifest_snapshot: Mock, mock_fail_telemetry: Mock
    ):
        """Test that @failure_telemetry decorator records failures for snapshot function."""
        # GIVEN
        mock_worker_manifest_properties = Mock()
        mock_worker_manifest_properties.local_output_relative_directories.return_value = ["test"]
        mock_manifest_snapshot.side_effect = Exception("Snapshot failed")

        with pytest.raises(Exception, match="Snapshot failed"):
            # WHEN
            snapshot([mock_worker_manifest_properties], Mock())

        # THEN
        mock_fail_telemetry.assert_called_once_with(
            queue_id="queue-unknown",
            failure_reason="snapshot: Exception",
        )

    @patch.object(
        attachment_upload_mod.JobAttachmentS3Settings, "partial_session_action_manifest_prefix"
    )
    @patch.object(attachment_upload_mod, "record_attachment_upload_fail_telemetry_event")
    def test_failure_telemetry_decorator_on_upload_output_assets(
        self,
        mock_fail_telemetry: Mock,
        mock_partial_session_action_manifest_prefix: Mock,
    ):
        """Test that @failure_telemetry decorator records failures for upload_output_assets function."""
        # GIVEN
        with (
            # Force a KeyError failure by clearing the environment variables
            # which the function uses to get DEADLINE_FARM_ID, etc.
            patch.dict(attachment_upload_mod.os.environ, {}),
            pytest.raises(KeyError),
        ):
            # WHEN
            upload_output_assets(Mock(), [Mock()], Mock())

        # THEN
        mock_fail_telemetry.assert_called_once_with(
            queue_id="queue-unknown",
            failure_reason="upload_output_assets: KeyError",
        )

    @pytest.mark.parametrize(
        argnames=["root_path_to_output_manifest"],
        argvalues=[[{"test": "value"}], [{}]],
        ids=["with output", "no output"],
    )
    @patch.dict(
        attachment_upload_mod.os.environ,
        {
            "DEADLINE_FARM_ID": "DEADLINE_FARM_ID",
            "DEADLINE_QUEUE_ID": "DEADLINE_QUEUE_ID",
            "DEADLINE_JOB_ID": "DEADLINE_JOB_ID",
            "DEADLINE_STEP_ID": "DEADLINE_STEP_ID",
            "DEADLINE_TASK_ID": "DEADLINE_TASK_ID",
            "DEADLINE_SESSIONACTION_ID": "DEADLINE_SESSIONACTION_ID",
            "DEADLINE_SESSIONACTION_START_TIME": "123.456",
        },
    )  # Mock the environment variables
    @patch.object(attachment_upload_mod, "record_attachment_upload_latencies_telemetry_event")
    @patch.object(attachment_upload_mod, "snapshot")
    @patch.object(attachment_upload_mod, "merge")
    @patch.object(attachment_upload_mod, "parse_worker_manifest_properties")
    def test_latencies_telemetry(
        self,
        mock_parse: Mock,
        mock_merge: Mock,
        mock_snapshot: Mock,
        mock_latencies_telemetry: Mock,
        root_path_to_output_manifest: Optional[Mock],
    ):
        """Test that latencies telemetry is recorded whether output is uploaded or not"""
        # GIVEN
        mock_snapshot.return_value = root_path_to_output_manifest
        args = ["-s3", "s3://test-bucket/path", "-wp", "/path/to/worker.json"]

        # WHEN
        main(args)

        # THEN
        mock_latencies_telemetry.assert_called_once()
        call_args = mock_latencies_telemetry.call_args
        assert call_args[1]["queue_id"] == "queue-unknown"
        assert "latencies" in call_args[1]

        # Verify latencies structure
        latencies = call_args[1]["latencies"]
        assert "merge" in latencies
        assert "snapshot" in latencies
        assert "parse_worker_manifest_properties" in latencies
        assert "upload_output_assets" in latencies
        assert "total" in latencies
