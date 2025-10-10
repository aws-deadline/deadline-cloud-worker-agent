# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

#! /usr/bin/env python3
import argparse
import dataclasses
import functools
import json
import sys
import time
import os
import boto3
import boto3.session
from typing import cast, Any, Callable, Dict, List, TypeVar

from deadline.job_attachments.download import download_files_from_manifests
from deadline.job_attachments.asset_manifests.decode import decode_manifest
from deadline.job_attachments.asset_manifests import BaseAssetManifest
from deadline.job_attachments.models import JobAttachmentS3Settings
from deadline.job_attachments.progress_tracker import (
    DownloadSummaryStatistics,
    ProgressReportMetadata,
)
from deadline_worker_agent.sessions.attachment_models import WorkerManifestProperties
from deadline_worker_agent.aws.deadline import (
    record_attachment_download_fail_telemetry_event,
    record_attachment_download_latencies_telemetry_event,
    record_attachment_download_telemetry_event,
    record_success_fail_telemetry_event,
)

_queue_id = os.environ.get("DEADLINE_QUEUE_ID", "queue-unknown")  # Just for telemetry

# During a SYNC_INPUT_JOB_ATTACHMENTS session action, the transfer rate is periodically reported through
# a callback function. If a transfer rate lower than LOW_TRANSFER_RATE_THRESHOLD is observed in a series
# for LOW_TRANSFER_COUNT_THRESHOLD times, it is considered concerning or potentially stalled, and the
# session action is canceled.
LOW_TRANSFER_RATE_THRESHOLD = 10 * 10**3  # 10 KB/s÷
LOW_TRANSFER_COUNT_THRESHOLD = (
    60  # Each progress report takes 1 sec at the longest, so 60 reports amount to 1 min in total.
)


F = TypeVar("F", bound=Callable[..., Any])


def failure_telemetry(function: F) -> F:
    """Decorator to record failure telemetry on a function"""

    @functools.wraps(function)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return function(*args, **kwargs)
        except Exception as e:
            record_attachment_download_fail_telemetry_event(
                queue_id=_queue_id,
                failure_reason=f"{function.__name__}: {type(e).__name__}",
            )
            raise e

    return cast(F, wrapper)


def _seconds_to_minutes_str(seconds: int) -> str:
    minutes = seconds // 60
    remaining_seconds = seconds % 60
    if minutes > 0 and remaining_seconds > 0:
        return f"{minutes} minute{'s' if minutes != 1 else ''} {remaining_seconds} second{'s' if remaining_seconds != 1 else ''}"
    elif minutes > 0:
        return f"{minutes} minute{'s' if minutes != 1 else ''}"
    elif remaining_seconds == 0:
        return "0 seconds"
    else:
        return f"{remaining_seconds} second{'s' if remaining_seconds != 1 else ''}"


@dataclasses.dataclass
class AttachmentDownloadLatencies:
    """Stores metrics for function latencies in this script"""

    load_worker_manifest_properties: int = 0
    build_merged_manifests_by_root: int = 0
    perform_download: int = 0
    total: int = 0


@failure_telemetry
def load_worker_manifest_properties(worker_properties_file: str) -> List[WorkerManifestProperties]:
    """
    Load and parse worker manifest properties from a JSON file.

    Args:
        worker_properties_file: Path to the worker properties JSON file

    Returns:
        List of WorkerManifestProperties objects

    Raises:
        FileNotFoundError: If the worker properties file doesn't exist
        json.JSONDecodeError: If the file contains invalid JSON
        ValueError: If the JSON structure is invalid
    """
    with open(worker_properties_file, "r") as f:
        worker_manifest_properties_data = json.load(f)

    if isinstance(worker_manifest_properties_data, dict):
        raise ValueError("Expected a list but got a dictionary in worker properties file")

    return [WorkerManifestProperties.from_dict(item) for item in worker_manifest_properties_data]


@failure_telemetry
def build_merged_manifests_by_root(
    worker_manifest_properties: List[WorkerManifestProperties],
) -> Dict[str, BaseAssetManifest]:
    """
    Build a dictionary mapping local root paths to decoded asset manifests.

    Args:
        worker_manifest_properties: List of WorkerManifestProperties objects

    Returns:
        Dictionary mapping local_root_path to BaseAssetManifest objects

    Raises:
        FileNotFoundError: If a manifest file doesn't exist
        ValueError: If manifest decoding fails
    """
    manifests_by_root = {}

    for worker_prop in worker_manifest_properties:
        if worker_prop.local_input_manifest_path:
            with open(worker_prop.local_input_manifest_path, "r") as manifest_file:
                manifests_by_root[worker_prop.local_root_path] = decode_manifest(
                    manifest_file.read()
                )
        else:
            print(f"Root {worker_prop.root_path} contains no input manifest to sync.")

    return manifests_by_root


@failure_telemetry
def perform_download(
    s3_settings: JobAttachmentS3Settings,
    manifests_by_root: Dict[str, BaseAssetManifest],
) -> DownloadSummaryStatistics:
    """
    Perform the actual download of files from S3 using the provided manifests.

    Args:
        s3_settings: S3 settings for the download
        manifests_by_root: Dictionary mapping root paths to manifests
        queue_id: Queue ID for telemetry

    Returns:
        DownloadSummaryStatistics object containing download results
    """
    low_transfer_count = 0

    def progress_handler(job_attachments_download_status: ProgressReportMetadata) -> bool:
        """
        Callback for Job Attachments' download_files_from_manifests() to track the download progress.
        Returns True if the operation should continue as normal or False to cancel.
        """
        # Check the transfer rate from the progress report. It monitors for a series of
        # alarmingly low transfer rates, and if the count exceeds the specified threshold,
        # cancels the download and fails the current (SYNC_INPUT_JOB_ATTACHMENTS) action.
        nonlocal low_transfer_count
        transfer_rate = job_attachments_download_status.transferRate

        if transfer_rate < LOW_TRANSFER_RATE_THRESHOLD:
            low_transfer_count += 1
        else:
            low_transfer_count = 0
        if low_transfer_count >= LOW_TRANSFER_COUNT_THRESHOLD:
            fail_message = (
                f"Input syncing failed due to successive low transfer rates (< {LOW_TRANSFER_RATE_THRESHOLD / 1000} KB/s). "
                f"The transfer rate was below the threshold for the last {_seconds_to_minutes_str(LOW_TRANSFER_COUNT_THRESHOLD)}."
            )
            print(f"openjd_fail: {fail_message}", flush=True)
            record_attachment_download_fail_telemetry_event(
                queue_id=_queue_id,
                failure_reason=f"Insufficient download speed: {fail_message}",
            )
            return False

        print(f"openjd_progress: {job_attachments_download_status.progress}")
        print(f"openjd_status: {job_attachments_download_status.progressMessage}")
        sys.stdout.flush()
        return True

    download_summary_statistics = download_files_from_manifests(
        s3_bucket=s3_settings.s3BucketName,
        manifests_by_root=manifests_by_root,
        cas_prefix=s3_settings.full_cas_prefix(),
        session=boto3.session.Session(),
        on_downloading_files=progress_handler,
    )
    record_attachment_download_telemetry_event(
        queue_id=_queue_id,
        summary=download_summary_statistics.convert_to_summary_statistics(),
    )
    return download_summary_statistics


@record_success_fail_telemetry_event(metric_name="attachment_download")
def main() -> None:
    """Main function to handle command line execution."""
    total_start_time = time.perf_counter_ns()

    parser = argparse.ArgumentParser()
    parser.add_argument("-s3", "--s3-uri", type=str, help="S3 root URI", required=True)
    parser.add_argument(
        "-wp",
        "--worker-properties",
        type=str,
        help="Worker manifest properties file",
        required=False,
    )

    args = parser.parse_args()

    latencies = AttachmentDownloadLatencies()

    start_t = time.perf_counter_ns()
    worker_manifest_properties = load_worker_manifest_properties(args.worker_properties)
    latencies.load_worker_manifest_properties = time.perf_counter_ns() - start_t

    start_t = time.perf_counter_ns()
    manifests_by_root = build_merged_manifests_by_root(worker_manifest_properties)
    latencies.build_merged_manifests_by_root = time.perf_counter_ns() - start_t

    print("\nStarting download...")

    s3_settings = JobAttachmentS3Settings.from_s3_root_uri(args.s3_uri)

    start_t = time.perf_counter_ns()
    perform_download(s3_settings, manifests_by_root)
    latencies.perform_download = time.perf_counter_ns() - start_t

    total = time.perf_counter_ns() - total_start_time
    latencies.total = total
    print(f"Finished downloading after {round(total * 10**-9, 2)} seconds")

    record_attachment_download_latencies_telemetry_event(
        queue_id=_queue_id,
        latencies=dataclasses.asdict(latencies),
    )


if __name__ == "__main__":
    main()
