# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import json
import os
import tempfile
from unittest.mock import Mock, patch, mock_open, ANY
from typing import Generator
import pytest

from deadline.job_attachments.progress_tracker import (
    DownloadSummaryStatistics,
    ProgressReportMetadata,
    ProgressStatus,
)
from deadline.job_attachments.models import ManifestProperties, PathFormat, JobAttachmentS3Settings
from deadline.job_attachments.asset_manifests.v2023_03_03.asset_manifest import AssetManifest
from deadline_worker_agent.sessions.attachment_models import WorkerManifestProperties

# Import the functions we want to test
import deadline_worker_agent.sessions.actions.scripts.attachment_download as attachment_download_mod
from deadline_worker_agent.sessions.actions.scripts.attachment_download import (
    load_worker_manifest_properties,
    build_merged_manifests_by_root,
    perform_download,
    main,
    _seconds_to_minutes_str,
)


class TestLoadWorkerManifestProperties:
    """Test cases for load_worker_manifest_properties function."""

    @pytest.fixture
    def worker_properties_file(self):
        """Create a temporary worker properties file for testing."""
        manifest_props = ManifestProperties(
            rootPath="/test/root/path",
            rootPathFormat=PathFormat.POSIX,
            fileSystemLocationName="test-location",
            inputManifestPath="/test/input/manifest.json",
            inputManifestHash="test-hash-123",
            outputRelativeDirectories=["/output/dir1", "/output/dir2"],
        )
        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props,
            local_root_path="/local/test/path",
            local_manifest_paths=["/local/manifest1.json", "/local/manifest2.json"],
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump([worker_props.to_dict()], f)
            temp_file_path = f.name

        yield temp_file_path
        if os.path.exists(temp_file_path):
            os.unlink(temp_file_path)

    def test_load_worker_manifest_properties_success(self, worker_properties_file):
        """Test successful loading of worker manifest properties."""
        # WHEN
        result = load_worker_manifest_properties(worker_properties_file)

        # THEN
        assert len(result) == 1
        assert isinstance(result[0], WorkerManifestProperties)
        assert result[0].local_root_path == "/local/test/path"
        assert result[0].local_manifest_paths == ["/local/manifest1.json", "/local/manifest2.json"]

    def test_load_worker_manifest_properties_file_not_found(self):
        """Test handling of non-existent worker properties file."""
        with pytest.raises(FileNotFoundError):
            load_worker_manifest_properties("/non/existent/file.json")

    def test_load_worker_manifest_properties_invalid_json(self):
        """Test handling of invalid JSON in worker properties file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("invalid json content")
            temp_file_path = f.name

        try:
            with pytest.raises(json.JSONDecodeError):
                load_worker_manifest_properties(temp_file_path)
        finally:
            os.unlink(temp_file_path)

    def test_load_worker_manifest_properties_empty_list(self):
        """Test loading empty worker properties list."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump([], f)
            temp_file_path = f.name

        try:
            result = load_worker_manifest_properties(temp_file_path)
            assert result == []
        finally:
            os.unlink(temp_file_path)


class TestBuildMergedManifestsByRoot:
    """Test cases for build_merged_manifests_by_root function."""

    @pytest.fixture
    def mock_manifest_properties(self):
        """Create a mock ManifestProperties object for testing."""
        return ManifestProperties(
            rootPath="/test/root/path",
            rootPathFormat=PathFormat.POSIX,
            fileSystemLocationName="test-location",
            inputManifestPath="/test/input/manifest.json",
            inputManifestHash="test-hash-123",
            outputRelativeDirectories=["/output/dir1", "/output/dir2"],
        )

    @pytest.fixture
    def worker_manifest_properties(self):
        """Create a WorkerManifestProperties object for testing."""
        manifest_props = ManifestProperties(
            rootPath="/test/root/path",
            rootPathFormat=PathFormat.POSIX,
            fileSystemLocationName="test-location",
            inputManifestPath="/test/input/manifest.json",
            inputManifestHash="test-hash-123",
            outputRelativeDirectories=["/output/dir1", "/output/dir2"],
        )
        return WorkerManifestProperties(
            manifest_properties=manifest_props,
            local_root_path="/local/test/path",
            local_manifest_paths=["/local/manifest1.json", "/local/manifest2.json"],
            local_input_manifest_path="/local/manifest1.json",
        )

    @pytest.fixture
    def mock_manifest_content(self):
        """Create mock manifest file content."""
        return json.dumps(
            {
                "manifestVersion": "2023-03-03",
                "hashAlg": "xxh128",
                "totalSize": 1024,
                "paths": [
                    {"path": "/test/file1.txt", "hash": "abc123", "size": 1024, "mtime": 1234567890}
                ],
            }
        )

    @pytest.fixture
    def mock_asset_manifest(self):
        """Create a mock AssetManifest object."""
        mock_manifest = Mock(spec=AssetManifest)
        mock_manifest.get_default_hash_alg.return_value = "xxh128"
        return mock_manifest

    @patch("builtins.open", new_callable=mock_open)
    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_download.decode_manifest")
    def test_build_merged_manifests_by_root_success(
        self, mock_decode_manifest, mock_file_open, worker_manifest_properties
    ):
        """Test successful building of merged manifests by root."""
        # GIVEN
        mock_file_open.return_value.read.return_value = json.dumps(
            {
                "manifestVersion": "2023-03-03",
                "hashAlg": "xxh128",
                "totalSize": 1024,
                "paths": [
                    {"path": "/test/file1.txt", "hash": "abc123", "size": 1024, "mtime": 1234567890}
                ],
            }
        )
        mock_decode_manifest.return_value = Mock(spec=AssetManifest)

        # WHEN
        result = build_merged_manifests_by_root([worker_manifest_properties])

        # THEN
        assert len(result) == 1
        assert "/local/test/path" in result
        mock_decode_manifest.assert_called_once()

    def test_build_merged_manifests_by_root_empty_manifest_paths(self):
        """Test handling of worker properties with empty manifest paths."""
        # GIVEN
        manifest_props = ManifestProperties(
            rootPath="/test/root/path",
            rootPathFormat=PathFormat.POSIX,
            fileSystemLocationName="test-location",
            inputManifestPath="/test/input/manifest.json",
            inputManifestHash="test-hash-123",
            outputRelativeDirectories=["/output/dir1", "/output/dir2"],
        )
        worker_prop = WorkerManifestProperties(
            manifest_properties=manifest_props,
            local_root_path="/local/test/path",
            local_manifest_paths=[],
        )

        # WHEN
        result = build_merged_manifests_by_root([worker_prop])

        # THEN
        assert result == {}

    def test_build_merged_manifests_by_root_no_manifest_paths(self, mock_manifest_properties):
        """Test handling of worker properties with None manifest paths."""
        # GIVEN
        worker_prop = WorkerManifestProperties(
            manifest_properties=mock_manifest_properties,
            local_root_path="/local/test/path",
            local_manifest_paths=None,
        )

        # WHEN
        result = build_merged_manifests_by_root([worker_prop])

        # THEN
        assert result == {}

    @patch("builtins.open", new_callable=mock_open)
    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_download.decode_manifest")
    def test_build_merged_manifests_by_root_multiple_properties(
        self, mock_decode_manifest, mock_file_open, mock_manifest_properties
    ):
        """Test building manifests with multiple worker properties."""
        # GIVEN
        valid_manifest = json.dumps(
            {
                "manifestVersion": "2023-03-03",
                "hashAlg": "xxh128",
                "totalSize": 1024,
                "paths": [
                    {"path": "/test/file1.txt", "hash": "abc123", "size": 1024, "mtime": 1234567890}
                ],
            }
        )
        mock_file_open.return_value.read.return_value = valid_manifest
        mock_decode_manifest.return_value = Mock()

        worker_props = [
            WorkerManifestProperties(
                manifest_properties=mock_manifest_properties,
                local_root_path="/local/path1",
                local_manifest_paths=["/manifest1.json"],
                local_input_manifest_path="/manifest1.json",
            ),
            WorkerManifestProperties(
                manifest_properties=mock_manifest_properties,
                local_root_path="/local/path2",
                local_manifest_paths=["/manifest2.json"],
                local_input_manifest_path="/manifest2.json",
            ),
        ]

        # WHEN
        result = build_merged_manifests_by_root(worker_props)

        # THEN
        assert len(result) == 2
        assert "/local/path1" in result
        assert "/local/path2" in result
        assert mock_decode_manifest.call_count == 2

    @patch("builtins.open", side_effect=FileNotFoundError("Manifest file not found"))
    def test_build_merged_manifests_by_root_manifest_file_not_found(
        self, mock_file_open, worker_manifest_properties
    ):
        """Test handling of missing manifest files."""
        with pytest.raises(FileNotFoundError):
            build_merged_manifests_by_root([worker_manifest_properties])


class TestPerformDownload:
    """Test cases for perform_download function."""

    @pytest.fixture(autouse=True)
    def mock_boto3(self) -> Generator[Mock, None, None]:
        """Mock boto3 session for testing."""
        with patch.object(attachment_download_mod, "boto3") as m:
            yield m

    @pytest.fixture(autouse=True)
    def mock_download_files(self) -> Generator[Mock, None, None]:
        with patch.object(attachment_download_mod, "download_files_from_manifests") as m:
            yield m

    @pytest.fixture(autouse=True)
    def mock_success_telemetry(self) -> Generator[Mock, None, None]:
        with patch.object(attachment_download_mod, "record_sync_inputs_telemetry_event") as m:
            yield m

    @pytest.fixture(autouse=True)
    def mock_fail_telemetry(self) -> Generator[Mock, None, None]:
        with patch.object(attachment_download_mod, "record_sync_inputs_fail_telemetry_event") as m:
            yield m

    def test_perform_download_success(self, mock_success_telemetry, mock_download_files):
        """Test successful download with telemetry recording."""
        # GIVEN
        from deadline.job_attachments.progress_tracker import SummaryStatistics

        mock_download_summary = Mock()
        mock_summary_stats = SummaryStatistics(
            total_time=1.5,
            total_files=10,
            total_bytes=1024,
            processed_files=10,
            processed_bytes=1024,
            skipped_files=0,
            skipped_bytes=0,
            transfer_rate=682.67,
        )
        mock_download_summary.convert_to_summary_statistics.return_value = mock_summary_stats
        mock_download_files.return_value = mock_download_summary

        s3_settings = JobAttachmentS3Settings.from_s3_root_uri("s3://test-bucket/test-prefix")

        # WHEN
        result = perform_download(s3_settings, {}, "test-queue")

        # THEN
        assert result == mock_download_summary
        mock_download_files.assert_called_once()
        mock_success_telemetry.assert_called_once_with("test-queue", mock_summary_stats)

    def test_perform_download_failure(self, mock_fail_telemetry, mock_download_files):
        """Test download failure with telemetry recording."""
        # GIVEN
        mock_download_files.side_effect = Exception("Download failed")

        s3_settings = JobAttachmentS3Settings.from_s3_root_uri("s3://test-bucket/test-prefix")

        # WHEN/THEN
        with pytest.raises(Exception, match="Download failed"):
            perform_download(s3_settings, {}, "test-queue")

        mock_fail_telemetry.assert_called_once_with(
            queue_id="test-queue",
            failure_reason="Error downloading files: Download failed",
        )

    def test_progress_reporting(
        self,
        mock_download_files: Mock,
        capsys: pytest.CaptureFixture,
    ):
        """
        Tests that attachment_download reports progress and status
        """
        # GIVEN
        s3_settings = JobAttachmentS3Settings.from_s3_root_uri("s3://test-bucket/test-prefix")

        # Mock out the Job Attachment's download_files_from_manifests function to
        # report progress
        def fake_download_files_from_manifests(on_downloading_files, *args, **kwargs):
            for i in range(10):
                on_downloading_files(
                    ProgressReportMetadata(
                        status=ProgressStatus.DOWNLOAD_IN_PROGRESS,
                        progress=i * 10,
                        transferRate=10 * 10**9,
                        progressMessage=f"test: {i}",
                    )
                )
            return DownloadSummaryStatistics()

        mock_download_files.side_effect = fake_download_files_from_manifests

        # WHEN
        perform_download(
            s3_settings=s3_settings,
            manifests_by_root={},
            queue_id="test-queue",
        )

        # THEN
        stdout = capsys.readouterr().out
        for msg in [f"openjd_progress: {i * 10}" for i in range(10)]:
            assert msg in stdout
        for msg in [f"openjd_status: test: {i}" for i in range(10)]:
            assert msg in stdout

    def test_cancellation_by_low_transfer_rate(
        self,
        mock_fail_telemetry: Mock,
        mock_download_files: Mock,
        capsys: pytest.CaptureFixture,
    ):
        """
        Tests that the session is canceled if it observes a series of alarmingly low transfer rates.
        """
        # GIVEN
        s3_settings = JobAttachmentS3Settings.from_s3_root_uri("s3://test-bucket/test-prefix")

        # Mock out the Job Attachment's download_files_from_manifests function to
        # report multiple consecutive low transfer rates (lower than the threshold) via callback function.
        def fake_download_files_from_manifests(on_downloading_files, *args, **kwargs):
            low_transfer_rate_report = ProgressReportMetadata(
                status=ProgressStatus.DOWNLOAD_IN_PROGRESS,
                progress=0.0,
                transferRate=(10 * 10**3) / 2,
                progressMessage="",
            )
            for _ in range(60):
                on_downloading_files(low_transfer_rate_report)

            return DownloadSummaryStatistics()

        mock_download_files.side_effect = fake_download_files_from_manifests

        # WHEN
        perform_download(
            s3_settings=s3_settings,
            manifests_by_root={},
            queue_id="test-queue",
        )

        # THEN
        assert (
            "openjd_fail: Input syncing failed due to successive low transfer rates (< 10.0 KB/s). "
            "The transfer rate was below the threshold for the last 1 minute."
        ) in capsys.readouterr().out
        mock_fail_telemetry.assert_called_once_with(
            queue_id="test-queue",
            failure_reason=(
                "Insufficient download speed: "
                "Input syncing failed due to successive low transfer rates (< 10.0 KB/s). "
                "The transfer rate was below the threshold for the last 1 minute."
            ),
        )


class TestMainFunction:
    """Test cases for main function."""

    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_download.perform_download")
    @patch(
        "deadline_worker_agent.sessions.actions.scripts.attachment_download.build_merged_manifests_by_root"
    )
    @patch(
        "deadline_worker_agent.sessions.actions.scripts.attachment_download.load_worker_manifest_properties"
    )
    @patch("time.perf_counter")
    @patch("argparse.ArgumentParser.parse_args")
    @patch.dict("os.environ", {"DEADLINE_QUEUE_ID": "test-queue-id"})
    def test_main_success(
        self,
        mock_parse_args,
        mock_perf_counter,
        mock_load_worker_props,
        mock_build_manifests,
        mock_perform_download,
    ):
        """Test successful main function execution."""
        # GIVEN
        mock_args = Mock()
        mock_args.s3_uri = "s3://test-bucket/test-prefix"
        mock_args.worker_properties = "/path/to/worker/props.json"
        mock_parse_args.return_value = mock_args

        mock_perf_counter.side_effect = [0.0, 5.5]  # start_time, end_time

        mock_worker_props = [Mock()]
        mock_load_worker_props.return_value = mock_worker_props

        mock_manifests = {"root": Mock()}
        mock_build_manifests.return_value = mock_manifests

        mock_download_summary = Mock()
        mock_perform_download.return_value = mock_download_summary

        # WHEN
        main()

        # THEN
        mock_load_worker_props.assert_called_once_with("/path/to/worker/props.json")
        mock_build_manifests.assert_called_once_with(mock_worker_props)
        mock_perform_download.assert_called_once()

        # Verify perform_download was called with correct arguments
        mock_perform_download.assert_called_once_with(
            ANY,  # s3_settings - we'll verify the bucket name separately
            mock_manifests,
            "test-queue-id",
        )
        # Verify S3 settings bucket name
        call_args = mock_perform_download.call_args
        s3_settings = call_args[0][0]
        assert s3_settings.s3BucketName == "test-bucket"

    @patch("deadline_worker_agent.sessions.actions.scripts.attachment_download.perform_download")
    @patch(
        "deadline_worker_agent.sessions.actions.scripts.attachment_download.build_merged_manifests_by_root"
    )
    @patch(
        "deadline_worker_agent.sessions.actions.scripts.attachment_download.load_worker_manifest_properties"
    )
    @patch("time.perf_counter")
    @patch("argparse.ArgumentParser.parse_args")
    @patch.dict("os.environ", {}, clear=True)  # Clear environment to test default
    def test_main_with_default_queue_id(
        self,
        mock_parse_args,
        mock_perf_counter,
        mock_load_worker_props,
        mock_build_manifests,
        mock_perform_download,
    ):
        """Test main function execution with default queue ID when environment variable is not set."""
        # GIVEN
        mock_args = Mock()
        mock_args.s3_uri = "s3://test-bucket/test-prefix"
        mock_args.worker_properties = "/path/to/worker/props.json"
        mock_parse_args.return_value = mock_args

        mock_perf_counter.side_effect = [0.0, 5.5]  # start_time, end_time

        mock_worker_props = [Mock()]
        mock_load_worker_props.return_value = mock_worker_props

        mock_manifests = {"root": Mock()}
        mock_build_manifests.return_value = mock_manifests

        mock_download_summary = Mock()
        mock_perform_download.return_value = mock_download_summary

        # WHEN
        main()

        # THEN
        # Verify perform_download was called with default queue_id
        mock_perform_download.assert_called_once_with(
            ANY,  # s3_settings
            mock_manifests,
            "queue-unknown",
        )


@pytest.mark.parametrize(
    "seconds, expected_str",
    [
        (0, "0 seconds"),
        (1, "1 second"),
        (30, "30 seconds"),
        (60, "1 minute"),
        (61, "1 minute 1 second"),
        (90, "1 minute 30 seconds"),
        (120, "2 minutes"),
        (121, "2 minutes 1 second"),
        (150, "2 minutes 30 seconds"),
    ],
)
def test_seconds_to_minutes_str(seconds: int, expected_str: str):
    assert _seconds_to_minutes_str(seconds) == expected_str
