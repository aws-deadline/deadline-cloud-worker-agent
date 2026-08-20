# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
import dataclasses
import boto3
import os
import logging
import filecmp
import json
import yaml

from glob import glob
from typing import Any, Dict, Optional, List
from pathlib import Path
from configparser import ConfigParser
from deadline.job_attachments import download
from deadline.job_attachments.download import get_s3_client
from deadline.job_attachments.models import JobAttachmentS3Settings
from deadline_test_fixtures import (
    DeadlineClient,
    EC2InstanceWorker,
    Job,
    Farm,
    Queue,
)
from deadline.client.api import create_job_from_job_bundle  # type: ignore
import backoff
from e2e.conftest import DeadlineResources

LOG = logging.getLogger(__name__)


def _get_queue_attachment_settings(
    deadline_client: DeadlineClient, farm_id: str, queue_id: str
) -> Optional[JobAttachmentS3Settings]:
    """Get job attachment settings from a queue using the Deadline API directly."""
    response = deadline_client.get_queue(farmId=farm_id, queueId=queue_id)
    settings = response.get("jobAttachmentSettings")
    if settings and settings.get("s3BucketName"):
        return JobAttachmentS3Settings(
            s3BucketName=settings["s3BucketName"],
            rootPrefix=settings["rootPrefix"],
        )
    return None


def wait_for_job_output(
    job: Job,
    deadline_client: DeadlineClient,
    deadline_resources: DeadlineResources,
    output_root_path: Optional[str] = None,
) -> dict[str, list[str]]:
    job.wait_until_complete(client=deadline_client, max_retries=20)

    job_attachment_settings = _get_queue_attachment_settings(
        deadline_client=deadline_client,
        farm_id=deadline_resources.farm.id,
        queue_id=deadline_resources.queue_a.id,
    )

    assert job_attachment_settings is not None

    job_output_downloader = download.OutputDownloader(
        s3_settings=job_attachment_settings,
        farm_id=deadline_resources.farm.id,
        queue_id=deadline_resources.queue_a.id,
        job_id=job.id,
        step_id=None,
        task_id=None,
    )
    output_paths_by_root = job_output_downloader.get_paths_by_root()
    LOG.info(f"Output paths by root: {job_output_downloader.paths_by_root}")

    # Download file and place it into the output_paths_by_root
    if output_root_path is not None:
        if len(output_paths_by_root) == 1:
            # Single root path
            job_output_downloader.set_root_path(
                list(output_paths_by_root.keys())[0], os.path.abspath(output_root_path)
            )
        else:
            # Multiple root paths - process each one with a subdirectory
            for i, root_path in enumerate(sorted(output_paths_by_root.keys())):
                # Create a subdirectory for each root path to avoid conflicts
                root_output_path = os.path.join(output_root_path, f"root_{i}")
                os.makedirs(root_output_path, exist_ok=True)
                job_output_downloader.set_root_path(root_path, os.path.abspath(root_output_path))

    download_stats = job_output_downloader.download()
    LOG.info(f"Download summary statistics: {dataclasses.asdict(download_stats)}")
    return job_output_downloader.get_paths_by_root()


def submit_sleep_job(
    job_name: str, deadline_client: DeadlineClient, farm: Farm, queue: Queue
) -> Job:
    job = Job.submit(
        client=deadline_client,
        farm=farm,
        queue=queue,
        priority=98,
        template={
            "specificationVersion": "jobtemplate-2023-09",
            "name": f"{job_name}",
            "steps": [
                {
                    "hostRequirements": {
                        "attributes": [
                            {
                                "name": "attr.worker.os.family",
                                "allOf": [os.environ["OPERATING_SYSTEM"]],
                            }
                        ]
                    },
                    "name": "Step0",
                    "script": {
                        "actions": {
                            "onRun": {
                                "command": (
                                    "/bin/sleep"
                                    if os.environ["OPERATING_SYSTEM"] != "windows"
                                    else "powershell"
                                ),
                                "args": (
                                    ["5"]
                                    if os.environ["OPERATING_SYSTEM"] != "windows"
                                    else ["ping", "localhost"]
                                ),
                            },
                        },
                    },
                },
            ],
        },
    )

    return job


def submit_job_from_bundle(
    deadline_client: DeadlineClient,
    farm: Farm,
    queue: Queue,
    bundle_path: str,
    template_file_name: str = "template",
    job_attachments_file_system: str = "COPIED",
    queue_parameter_definitions: List[dict] = [],
    max_retries_per_task: Optional[int] = None,
    job_parameters: list[dict[str, Any]] = [],
) -> Job:
    bundle_path = os.path.normpath(bundle_path)
    LOG.info(f"Submitting bundle {bundle_path} to farm {farm.id} and queue {queue.id}")
    yaml_path = f"{bundle_path}/{template_file_name}.yaml"
    json_path = f"{bundle_path}/{template_file_name}.json"
    if os.path.isfile(yaml_path):
        with open(yaml_path) as f:
            job_template = yaml.safe_load(f.read())
    elif os.path.isfile(json_path):
        with open(json_path) as f:
            job_template = json.loads(f.read())
    else:
        LOG.error(
            f"Was expecting to find either {template_file_name}.yaml or {template_file_name}.json in directory {bundle_path} but found none."
        )
        raise FileNotFoundError

    config_dict = {
        "defaults": {
            "aws_profile_name": "default",
        },
        "profile-default settings": {
            "user_identities": "False",
        },
        "profile-default defaults": {
            "farm_id": farm.id,
        },
        f"profile-default {farm.id} defaults": {"queue_id": queue.id},
    }

    config = ConfigParser()
    config.read_dict(config_dict)

    create_job_args = {
        "job_bundle_dir": bundle_path,
        "queue_parameter_definitions": queue_parameter_definitions,
        "job_attachments_file_system": job_attachments_file_system,
        "config": config,
    }

    if max_retries_per_task is not None:
        create_job_args["max_retries_per_task"] = max_retries_per_task  # type: ignore

    if job_parameters is not None:
        create_job_args["job_parameters"] = job_parameters  # type: ignore

    job_id = create_job_from_job_bundle(**create_job_args)  # type: ignore
    assert job_id is not None

    LOG.info(f"Bundle successfully submitted {job_id} to farm {farm.id} {queue.id}")

    job_details = Job.get_job_details(
        client=deadline_client,
        farm=farm,
        queue=queue,
        job_id=job_id,
    )
    LOG.info(f"Job details: {job_details}")
    LOG.info(f"Job template: {job_template}")

    return Job(farm=farm, queue=queue, template=job_template, **job_details)


def verify_output_dir_matches(
    reference_dir_path: str, output_dir_path: str, convert_line_endings=True
):
    LOG.info(
        f"Comparing output files in reference directory {reference_dir_path} to the output in {output_dir_path}"
    )
    reference_files = recursively_list_files_as_relative_paths(reference_dir_path)
    output_files = recursively_list_files_as_relative_paths(output_dir_path)

    if convert_line_endings:
        # replacement strings
        WINDOWS_LINE_ENDING = b"\r\n"
        UNIX_LINE_ENDING = b"\n"

        # relative or absolute file path, e.g.:
        for file in output_files:
            file_path = os.path.join(output_dir_path, file)
            with open(file_path, "rb") as open_file:
                content = open_file.read()
            # Windows ➡ Unix
            content = content.replace(WINDOWS_LINE_ENDING, UNIX_LINE_ENDING)
            with open(file_path, "wb") as open_file:
                open_file.write(content)

    # len check confirms there are no extra files in output
    assert len(reference_files) == len(output_files)

    # match: list of equivalent files, mismatch: list of files with different content,
    # errors: list of files that couldn't be compared (e.g. missing in one of the comparison directories)
    match, mismatch, errors = filecmp.cmpfiles(
        reference_dir_path, output_dir_path, reference_files, False
    )
    LOG.info(f"Matches: {match}, Mismatches: {mismatch}, Errors: {errors}")

    assert len(match) == len(reference_files), (
        f"Reference files ({len(reference_files)}) did not equal match ({len(match)})"
    )
    assert len(mismatch) == 0, "Number of mismatched files is non-zero"
    assert len(errors) == 0, "Number of errors is non-zero"


def recursively_list_files_as_relative_paths(dir_path: str) -> List[str]:
    """
    Get all files in a directory and its subdirectories, returning paths relative to dir_path.
    Uses glob for more efficient file discovery.

    Args:
        dir_path: The directory path to search

    Returns:
        A list of file paths relative to dir_path
    """

    rel_files = [
        # Convert absolute paths to relative paths
        os.path.relpath(f, dir_path)
        # Use ** pattern for recursive search through all subdirectories
        # The recursive=True parameter enables ** to match directories at any level
        for f in glob(os.path.join(dir_path, "**"), recursive=True)
        # Filter out directories, keep only files
        if os.path.isfile(f)
    ]

    return rel_files


def submit_custom_job(
    job_name: str,
    deadline_client: DeadlineClient,
    farm: Farm,
    queue: Queue,
    run_script: str,
    max_retries_per_task: int = 5,
    description: str = "to-be-filled-in",
) -> Job:
    template: dict = {
        "specificationVersion": "jobtemplate-2023-09",
        "name": f"{job_name}",
        "description": description,
        "steps": [
            {
                "hostRequirements": {
                    "attributes": [
                        {
                            "name": "attr.worker.os.family",
                            "allOf": [os.environ["OPERATING_SYSTEM"]],
                        }
                    ]
                },
                "name": "Step0",
                "script": {
                    "actions": {
                        "onRun": (
                            {"command": "{{ Task.File.runScript }}"}
                            if os.environ["OPERATING_SYSTEM"] != "windows"
                            else {
                                "command": "powershell",
                                "args": ["{{ Task.File.runScript }}"],  # type: ignore[dict-item]
                            }
                        ),
                    },
                    "embeddedFiles": [
                        {
                            "name": "runScript",
                            "type": "TEXT",
                            "runnable": True,
                            "data": run_script,
                            **(
                                {"filename": "runScript.ps1"}
                                if os.environ["OPERATING_SYSTEM"] == "windows"
                                else {}
                            ),
                        }
                    ],
                },
            },
        ],
    }
    job = Job.submit(
        client=deadline_client,
        farm=farm,
        queue=queue,
        max_retries_per_task=max_retries_per_task,
        priority=98,
        template=template,
    )

    return job


@backoff.on_predicate(
    wait_gen=backoff.constant,
    max_time=60,
    interval=10,
)
def is_worker_started(
    deadline_client: DeadlineClient, farm_id: str, fleet_id: str, worker_id: str
) -> bool:
    get_worker_response: Dict[str, Any] = deadline_client.get_worker(
        farmId=farm_id,
        fleetId=fleet_id,
        workerId=worker_id,
    )
    worker_status = get_worker_response["status"]
    if worker_status in ["STARTED", "IDLE"]:
        # Worker should eventually be in either STARTED or IDLE.
        return True
    elif worker_status == "CREATED":
        # This is an acceptable status meaning that the worker is created state has not been updated
        return False
    # Any other status is unexpected, so we should fail
    raise Exception(f"Status {worker_status} is unexpected after worker has just started")


@backoff.on_predicate(
    wait_gen=backoff.constant,
    max_time=180,
    interval=10,
)
def is_worker_stopped(
    deadline_client: DeadlineClient, farm_id: str, fleet_id: str, worker_id: str
) -> bool:
    get_worker_response: Dict[str, Any] = deadline_client.get_worker(
        farmId=farm_id,
        fleetId=fleet_id,
        workerId=worker_id,
    )
    worker_status = get_worker_response["status"]
    return worker_status == "STOPPED"


def get_shutdown_on_stop_status_from_toml(
    worker: EC2InstanceWorker,
) -> str:
    if os.environ["OPERATING_SYSTEM"] == "windows":
        cmd_result = worker.send_command(
            command="""
$content = Get-Content "C:\\ProgramData\\Amazon\\Deadline\\Config\\worker.toml"
$content | Select-String -Pattern "^# shutdown_on_stop =|^shutdown_on_stop ="
"""
        )
        assert cmd_result.exit_code == 0, "Failed to execute Windows command on .toml"
        assert "shutdown_on_stop" in cmd_result.stdout, "shutdown_on_stop not found in .toml"
        return cmd_result.stdout.strip()
    else:
        # POSIX (linux and macos): the worker.toml path is identical on both.
        cmd_result = worker.send_command(
            command="""
grep -E \
  '^(# shutdown_on_stop =|shutdown_on_stop =)' \
  /etc/amazon/deadline/worker.toml
"""
        )
        assert cmd_result.exit_code == 0, "Failed to execute POSIX command on .toml"
        assert "shutdown_on_stop" in cmd_result.stdout, "shutdown_on_stop not found in .toml"
        return cmd_result.stdout.strip()


def submit_job_from_create_job_API(
    deadline_client: DeadlineClient,
    deadline_resources: DeadlineResources,
    farm: Farm,
    queue: Queue,
    debug_snapshot_dir: str,
    storage_profile: bool = False,
    job_name: Optional[str] = None,
) -> Job:
    """Submit a job using the Deadline create job API.

    Args:
        deadline_client: The Deadline client instance for making API calls
        deadline_resources: Deadline resources containing configuration details
        farm: The farm where the job will be submitted
        queue: The queue where the job will be submitted
        debug_snapshot_dir: Directory path containing job data, manifests, and parameter files
        storage_profile: Whether to enable storage profile for the job (defaults to False)

    Returns:
        Job: The created job object with details from the submission
    """
    debug_snapshot_dir = os.path.normpath(debug_snapshot_dir)
    LOG.info(f"Submitting bundle {debug_snapshot_dir} to farm {farm.id} and queue {queue.id}")

    job_attachment_settings = _get_queue_attachment_settings(
        deadline_client=deadline_client,
        farm_id=farm.id,
        queue_id=queue.id,
    )

    if not job_attachment_settings:
        raise ValueError("Queue does not have job attachment settings configured")

    s3_bucket = job_attachment_settings.s3BucketName
    rootPrefix = job_attachment_settings.rootPrefix

    s3_client = get_s3_client()

    upload_directory_to_s3(
        s3_client=s3_client,
        local_dir=f"{debug_snapshot_dir}/Data",
        bucket=s3_bucket,
        s3_prefix=rootPrefix,
    )
    upload_directory_to_s3(
        s3_client=s3_client,
        local_dir=f"{debug_snapshot_dir}/Manifests",
        bucket=s3_bucket,
        s3_prefix=rootPrefix,
    )

    # Load template file
    with open(f"{debug_snapshot_dir}/template_param.data", "r") as f:
        template_content = f.read()

    # Load attachments file
    with open(f"{debug_snapshot_dir}/attachments_param.json", "r") as f:
        attachments = json.load(f)

    # Load parameters file
    with open(f"{debug_snapshot_dir}/parameters_param.json", "r") as f:
        parameters = json.load(f)

    # Override job name if provided
    if job_name is not None:
        parameters["JobName"] = {"string": job_name}

    create_job_kwargs = {
        "farmId": farm.id,
        "queueId": queue.id,
        "template": template_content,
        "templateType": "YAML",
        "priority": 50,
        "attachments": attachments,
        "parameters": parameters,
        "maxRetriesPerTask": 0,
    }

    if storage_profile:
        create_job_kwargs["storageProfileId"] = deadline_resources.queue_a_job_storage_profile_id

    response = deadline_client.create_job(**create_job_kwargs)
    assert response["jobId"] is not None
    job_id = response["jobId"]

    LOG.info(f"Bundle successfully submitted {job_id} to farm {farm.id} {queue.id}")

    job_details = Job.get_job_details(
        client=deadline_client,
        farm=farm,
        queue=queue,
        job_id=job_id,
    )
    LOG.info(f"Job details: {job_details}")
    LOG.info(f"Job template: {template_content}")

    return Job(
        farm=farm,
        queue=queue,
        template=yaml.safe_load(template_content),
        **job_details,
    )


def upload_directory_to_s3(s3_client: Any, local_dir: str, bucket: str, s3_prefix: str) -> None:
    """Upload a directory recursively to S3"""
    local_path = Path(local_dir)
    if not local_path.exists():
        LOG.info(f"Warning: {local_dir} does not exist, skipping upload")
        return

    for file_path in local_path.rglob("*"):
        if file_path.is_file():
            # Calculate relative path for S3 key, including the top folder name
            relative_path = file_path.relative_to(local_path.parent)
            s3_key = f"{s3_prefix}/{relative_path}".replace("\\", "/")

            LOG.info(f"Uploading {file_path} to s3://{bucket}/{s3_key}")
            s3_client.upload_file(str(file_path), bucket, s3_key)


def windows_replace_and_verify(
    worker: EC2InstanceWorker,
    file_path: str,
    old_pattern: str,
    new_pattern: str,
) -> None:
    """
    Performs a PowerShell string replacement in a file and verifies it succeeded.

    PowerShell's -replace operator returns exit code 0 even when no replacement occurs,
    so this function adds verification to ensure the replacement actually happened.

    Args:
        worker: The EC2 worker instance to execute commands on
        file_path: Windows path to the file to modify (e.g., "C:\\ProgramData\\Amazon\\Deadline\\Config\\worker.toml")
        old_pattern: Pattern to replace (regex pattern used in PowerShell -replace)
        new_pattern: Replacement string

    Raises:
        AssertionError: If replacement command or verification fails
    """
    # Perform replacement
    cmd_result = worker.send_command(
        f"(Get-Content -Path {file_path} -Raw) -replace '{old_pattern}', '{new_pattern}' | Set-Content -Path {file_path}"
    )
    assert cmd_result.exit_code == 0, f"Replacement command failed: {cmd_result}"

    # Verify the replacement actually happened - PowerShell's -replace silently
    # returns the original string if the pattern isn't found
    verify_result = worker.send_command(f"Get-Content {file_path}")
    assert verify_result.exit_code == 0, f"Failed to read config file: {verify_result}"
    assert new_pattern in verify_result.stdout, (
        f"Config replacement failed - expected pattern not found after replacement. "
        f"Expected: {new_pattern}\n"
        f"Config contents:\n{verify_result.stdout}"
    )


def job_failure_message(
    job: Job,
    deadline_client: DeadlineClient,
    queue: Queue,
    deadline_resources: DeadlineResources,
) -> str:
    """Build a detailed failure message with job identifiers, console links, and logs."""
    region = deadline_client._real_client.meta.region_name
    console_url = (
        f"https://{region}.console.aws.amazon.com/deadline/home?region={region}"
        f"#/farms/{job.farm.id}/queues/{queue.id}/jobs/{job.id}"
    )
    worker_log_url = (
        f"https://{region}.console.aws.amazon.com/cloudwatch/home?region={region}"
        f"#logsV2:log-groups/log-group/$252Faws$252Fdeadline$252F{job.farm.id}$252F{deadline_resources.fleet.id}"
    )

    logs_client = boto3.client("logs")
    try:
        job_logs = job.get_logs(deadline_client=deadline_client, logs_client=logs_client)
        session_logs = (
            "\n".join(f"  {log.session_id}: {log.log.messages}" for log in job_logs.sessions)
            if hasattr(job_logs, "sessions")
            else str(job_logs)
        )
    except Exception as e:
        session_logs = f"(failed to fetch logs: {e})"

    return (
        f"Job did not succeed (status={job.task_run_status}).\n"
        f"  farm={job.farm.id}\n"
        f"  queue={queue.id}\n"
        f"  job={job.id}\n"
        f"  job console: {console_url}\n"
        f"  worker logs: {worker_log_url}\n"
        f"  session logs:\n{session_logs}"
    )
