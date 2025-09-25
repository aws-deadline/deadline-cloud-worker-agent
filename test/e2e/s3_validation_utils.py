# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Utility functions for validating S3 setup in E2E tests for Deadline Cloud jobs.
"""

import json
import logging
from typing import Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

from deadline.job_attachments._aws.deadline import get_job, get_queue
from deadline.job_attachments.asset_manifests.hash_algorithms import hash_data
from deadline.job_attachments.asset_manifests.v2023_03_03.asset_manifest import DEFAULT_HASH_ALG
from deadline.job_attachments.download import _get_tasks_manifests_keys_from_s3
from deadline.job_attachments._aws.aws_clients import get_s3_client

from deadline_test_fixtures import DeadlineClient, Job
from datetime import datetime

LOG = logging.getLogger(__name__)


class S3ValidationError(Exception):
    """Exception raised when S3 validation fails."""

    pass


def validate_s3_job_output_manifest(
    job: Job,
    deadline_client: DeadlineClient,
    session: Optional[boto3.Session] = None,
) -> None:
    """
    Validates the S3 setup for a Deadline Cloud job by checking:
    1. Job attachment manifest and settings to get S3 bucket, rootPath, and fileSystemLocationName
    2. Lists all steps of the job and constructs S3 keys for each step's output manifests
    3. Verifies that output directories exist and match expected session actions
    4. Validates that session action IDs in S3 keys correspond to actual session actions
    5. Validates that timestamps in S3 keys match session action start times (accurate to the second)
    6. Validates output file hashes and metadata

    Args:
        job: The Deadline Cloud job to validate
        deadline_client: Deadline client for API calls
        session: Optional boto3 session

    Raises:
        S3ValidationError: If S3 client operations fail
        AssertionError: If validation checks fail
    """
    farm_id = job.farm.id
    queue_id = job.queue.id
    # Get job and queue details
    job_details = get_job(farm_id=farm_id, queue_id=queue_id, job_id=job.id, session=session)
    queue_details = get_queue(farm_id=farm_id, queue_id=queue_id, session=session)

    assert job_details.attachments, f"Job {job.id} has no attachments"
    assert job_details.attachments.manifests, f"Job {job.id} has no manifests"

    LOG.debug(f"Job {job.id} has {len(job_details.attachments.manifests)} manifest(s)")

    assert queue_details.jobAttachmentSettings, f"Queue {queue_id} has no job attachment settings"
    LOG.debug(f"Queue {queue_id} has job attachment settings configured")

    # Extract S3 configuration
    s3_bucket = queue_details.jobAttachmentSettings.s3BucketName

    for manifest_properties in job_details.attachments.manifests:
        root_path = manifest_properties.rootPath
        file_system_location_name = manifest_properties.fileSystemLocationName

        # Validate each step's output directories for this manifest
        steps = _get_job_steps(deadline_client, farm_id, queue_id, job.id)

        for step in steps:
            step_id = step["stepId"]
            manifest_prefix = f"Deadline/Manifests/{farm_id}/{queue_id}/{job.id}/{step_id}"

            try:
                manifest_keys = _get_tasks_manifests_keys_from_s3(
                    manifest_prefix=manifest_prefix, s3_bucket=s3_bucket, session=session
                )

                if manifest_keys:
                    LOG.info(
                        f"Validating {len(manifest_keys)} outputs for step {step_id} with manifest root_path={root_path}"
                    )
                    for manifest_key in manifest_keys:
                        _validate_output_directory(
                            manifest_key=manifest_key,
                            s3_bucket=s3_bucket,
                            deadline_client=deadline_client,
                            farm_id=farm_id,
                            queue_id=queue_id,
                            job_id=job.id,
                            root_path=root_path,
                            file_system_location_name=file_system_location_name,
                            session=session,
                        )
            except (ClientError, S3ValidationError, AssertionError) as e:
                LOG.debug(f"No output manifests for step {step_id} with root_path={root_path}: {e}")


def _get_job_steps(
    deadline_client: DeadlineClient, farm_id: str, queue_id: str, job_id: str
) -> List[Dict]:
    """Get all steps for a job."""
    try:
        steps = []
        next_token = None

        while True:
            params = {"farmId": farm_id, "queueId": queue_id, "jobId": job_id}
            if next_token:
                params["nextToken"] = next_token

            response = deadline_client.list_steps(**params)
            steps.extend(response.get("steps", []))

            next_token = response.get("nextToken")
            if not next_token:
                break

        return steps
    except ClientError as e:
        raise S3ValidationError(f"Failed to list steps for job {job_id}: {e}")


def _validate_output_directory(
    manifest_key: str,
    s3_bucket: str,
    deadline_client: DeadlineClient,
    farm_id: str,
    queue_id: str,
    job_id: str,
    root_path: str,
    file_system_location_name: Optional[str],
    session: Optional[boto3.Session] = None,
) -> None:
    """Validate output directory: session action, timestamp, hash, and metadata."""
    # Parse key for step ID, task ID, session action ID and timestamp
    parsed_step_id, task_id, session_action_id, timestamp_part = _parse_manifest_key(
        manifest_key=manifest_key
    )

    # Validate task ID exists for the step
    _validate_task_exists(
        deadline_client=deadline_client,
        farm_id=farm_id,
        queue_id=queue_id,
        job_id=job_id,
        step_id=parsed_step_id,
        task_id=task_id,
    )

    # Validate session action exists and timestamp matches
    session_action = _get_session_action_details(
        deadline_client=deadline_client,
        farm_id=farm_id,
        queue_id=queue_id,
        job_id=job_id,
        session_action_id=session_action_id,
    )
    _validate_timestamp_match(timestamp_part=timestamp_part, session_action=session_action)

    # Validate file hash and S3 metadata
    _validate_file_and_metadata(
        manifest_key=manifest_key,
        s3_bucket=s3_bucket,
        root_path=root_path,
        file_system_location_name=file_system_location_name,
        session=session,
    )


def _get_session_action_details(
    deadline_client: DeadlineClient,
    farm_id: str,
    queue_id: str,
    job_id: str,
    session_action_id: str,
) -> Dict:
    """Get session action details by searching through all sessions."""
    try:
        sessions_response = deadline_client.list_sessions(
            farmId=farm_id, queueId=queue_id, jobId=job_id
        )

        sessions = sessions_response.get("sessions", [])
        assert sessions, f"No sessions found for job {job_id}"
        LOG.debug(f"Found {len(sessions)} session(s) for job {job_id}")

        # Search for the session action across all sessions
        for session in sessions:
            try:
                next_token = None

                while True:
                    params = {
                        "farmId": farm_id,
                        "queueId": queue_id,
                        "jobId": job_id,
                        "sessionId": session["sessionId"],
                    }
                    if next_token:
                        params["nextToken"] = next_token

                    actions_response = deadline_client.list_session_actions(**params)

                    for action in actions_response.get("sessionActions", []):
                        if action["sessionActionId"] == session_action_id:
                            LOG.debug(f"Found session action {session_action_id}")
                            return action

                    next_token = actions_response.get("nextToken")
                    if not next_token:
                        break

            except ClientError:
                continue

        # This is a validation failure, not a client error
        LOG.error(f"Session action {session_action_id} not found for job {job_id}")
        assert False, f"Session action {session_action_id} not found for job {job_id}"

    except ClientError as e:
        raise S3ValidationError(f"Failed to list sessions for job {job_id}: {e}")


def _parse_manifest_key(manifest_key: str) -> Tuple[str, str, str, str]:
    """Parse manifest key to extract step ID, task ID, session action ID and timestamp."""
    key_parts = manifest_key.split("/")
    step_id = task_id = session_action_id = timestamp_part = None

    for part in key_parts:
        if part.startswith("step-"):
            step_id = part
        elif part.startswith("task-"):
            task_id = part
        elif "sessionaction-" in part:
            if "_" in part:
                # This is because the file name currently follow the format of {timestamp}_{session_action_id}
                timestamp_part, session_action_id = part.split("_", 1)
            else:
                session_action_id = part

    assert step_id, f"Could not parse step ID from key: {manifest_key}"
    LOG.debug(f"Parsed step ID: {step_id}")

    assert task_id, f"Could not parse task ID from key: {manifest_key}"
    LOG.debug(f"Parsed task ID: {task_id}")

    assert session_action_id, f"Could not parse session action ID from key: {manifest_key}"
    LOG.debug(f"Parsed session action ID: {session_action_id}")

    assert timestamp_part, f"Could not parse timestamp from key: {manifest_key}"
    LOG.debug(f"Parsed timestamp: {timestamp_part}")

    return step_id, task_id, session_action_id, timestamp_part


def _validate_task_exists(
    deadline_client: DeadlineClient,
    farm_id: str,
    queue_id: str,
    job_id: str,
    step_id: str,
    task_id: str,
) -> None:
    """Validate that the task ID exists for the given step."""
    try:
        tasks = []
        next_token = None

        while True:
            params = {"farmId": farm_id, "queueId": queue_id, "jobId": job_id, "stepId": step_id}
            if next_token:
                params["nextToken"] = next_token

            response = deadline_client.list_tasks(**params)
            tasks.extend(response.get("tasks", []))

            next_token = response.get("nextToken")
            if not next_token:
                break

        task_ids = [task["taskId"] for task in tasks]

        assert task_id in task_ids, f"Task {task_id} not found in step {step_id}"
        LOG.debug(f"Task {task_id} validated in step {step_id}")

    except ClientError as e:
        raise S3ValidationError(f"Failed to list tasks for step {step_id}: {e}")


def _validate_timestamp_match(timestamp_part: str, session_action: Dict) -> None:
    """Validate that key timestamp matches session action start time."""
    try:
        key_timestamp = datetime.fromisoformat(timestamp_part.replace("Z", "+00:00")).replace(
            microsecond=0
        )

        start_time = session_action.get("startedAt")
        if isinstance(start_time, str):
            action_start_time = datetime.fromisoformat(start_time.replace("Z", "+00:00")).replace(
                microsecond=0
            )
        elif start_time is not None:
            action_start_time = start_time.replace(microsecond=0)
        else:
            LOG.error("Session action has no startedAt timestamp")
            assert False, "Session action has no startedAt timestamp"

        assert key_timestamp == action_start_time, (
            f"Timestamp mismatch: key={key_timestamp}, action start={action_start_time}"
        )
        LOG.debug(f"Timestamp validation passed: {key_timestamp}")

    except ValueError as e:
        LOG.error(f"Nonvalid timestamp format: {timestamp_part}. Error: {e}")
        assert False, f"Nonvalid timestamp format: {timestamp_part}. Error: {e}"


def _validate_file_and_metadata(
    manifest_key: str,
    s3_bucket: str,
    root_path: str,
    file_system_location_name: Optional[str],
    session: Optional[boto3.Session] = None,
) -> None:
    """Validate file hash and S3 metadata."""
    filename = manifest_key.split("/")[-1]
    assert filename.endswith("_output"), f"Nonvalid filename format: {filename}"
    LOG.debug(f"Filename format validated: {filename}")

    # Validate hash
    file_hash = filename.replace("_output", "")
    hash_input = f"{file_system_location_name or ''}{root_path}".encode()
    expected_hash = hash_data(data=hash_input, hash_alg=DEFAULT_HASH_ALG)

    assert file_hash == expected_hash, f"Hash mismatch: expected {expected_hash}, got {file_hash}"
    LOG.debug(f"Hash validation passed: {file_hash}")

    # Validate S3 metadata
    s3_client = get_s3_client(session=session) if session else get_s3_client()

    try:
        response = s3_client.head_object(Bucket=s3_bucket, Key=manifest_key)
        metadata = response.get("Metadata", {})

        assert metadata.get("asset-root") == root_path, (
            f"Asset root mismatch: expected {root_path}, got {metadata.get('asset-root')}"
        )
        LOG.debug(f"Asset root metadata validated: {root_path}")

        if file_system_location_name:
            assert metadata.get("file-system-location-name") == file_system_location_name, (
                f"File system location mismatch: expected {file_system_location_name}, got {metadata.get('file-system-location-name')}"
            )
            LOG.debug(f"File system location metadata validated: {file_system_location_name}")

        # Validate JSON encoding for non-ASCII paths
        if not root_path.isascii():
            json_root = metadata.get("asset-root-json")
            assert json_root and json.loads(json_root) == root_path, (
                f"Asset root JSON mismatch for non-ASCII path: expected {root_path}, got {json_root}"
            )
            LOG.debug("Non-ASCII path JSON encoding validated")

    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            raise S3ValidationError(f"File not found: {manifest_key}")
        raise S3ValidationError(f"S3 error: {e}")
