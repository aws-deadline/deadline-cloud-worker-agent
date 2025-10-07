# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

#! /usr/bin/env python3
import argparse
import json
import time
import os
import boto3
from typing import Dict, List

from deadline.job_attachments.download import download_files_from_manifests
from deadline.job_attachments.asset_manifests.decode import decode_manifest
from deadline.job_attachments.asset_manifests import BaseAssetManifest
from deadline.job_attachments.models import JobAttachmentS3Settings
from deadline.job_attachments.progress_tracker import DownloadSummaryStatistics
from deadline_worker_agent.sessions.attachment_models import WorkerManifestProperties
from deadline_worker_agent.aws.deadline import (
    record_sync_inputs_fail_telemetry_event,
    record_sync_inputs_telemetry_event,
)


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


def perform_download(
    s3_settings: JobAttachmentS3Settings,
    manifests_by_root: Dict[str, BaseAssetManifest],
    queue_id: str,
) -> DownloadSummaryStatistics:
    """
    Perform the actual download of files from S3 using the provided manifests.

    Args:
        s3_settings: S3 settings for the download
        manifests_by_root: Dictionary mapping root paths to manifests
        queue_id: Queue ID for telemetry

    Returns:
        DownloadSummaryStatistics object containing download results

    Raises:
        Exception: Any exception that occurs during download (after recording telemetry)
    """
    try:
        download_summary_statistics = download_files_from_manifests(
            s3_bucket=s3_settings.s3BucketName,
            manifests_by_root=manifests_by_root,
            cas_prefix=s3_settings.full_cas_prefix(),
            session=boto3.session.Session(),
        )

        # Record successful download telemetry
        record_sync_inputs_telemetry_event(
            queue_id, download_summary_statistics.convert_to_summary_statistics()
        )

        return download_summary_statistics

    except Exception as exc:
        # Record failure telemetry
        record_sync_inputs_fail_telemetry_event(
            queue_id=queue_id,
            failure_reason=f"Error downloading files: {exc}",
        )
        raise


def main() -> None:
    """Main function to handle command line execution."""
    start_time = time.perf_counter()

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

    worker_manifest_properties = load_worker_manifest_properties(args.worker_properties)

    manifests_by_root = build_merged_manifests_by_root(worker_manifest_properties)

    print("\nStarting download...")

    s3_settings = JobAttachmentS3Settings.from_s3_root_uri(args.s3_uri)

    perform_download(
        s3_settings, manifests_by_root, os.environ.get("DEADLINE_QUEUE_ID", "queue-unknown")
    )

    total = time.perf_counter() - start_time
    print(f"Finished downloading after {total} seconds")


if __name__ == "__main__":
    main()
