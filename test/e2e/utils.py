# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
import os
import logging
import filecmp
import json
import yaml

from glob import glob
from typing import Any, Dict, Optional, List
from configparser import ConfigParser
from deadline.job_attachments._aws.deadline import get_queue
from deadline.job_attachments import download
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


def wait_for_job_output(
    job: Job,
    deadline_client: DeadlineClient,
    deadline_resources: DeadlineResources,
    output_root_path: Optional[str] = None,
) -> dict[str, list[str]]:
    job.wait_until_complete(client=deadline_client, max_retries=20)

    job_attachment_settings = get_queue(
        farm_id=deadline_resources.farm.id,
        queue_id=deadline_resources.queue_a.id,
    ).jobAttachmentSettings

    assert job_attachment_settings is not None

    job_output_downloader = download.OutputDownloader(
        s3_settings=job_attachment_settings,
        farm_id=deadline_resources.farm.id,
        queue_id=deadline_resources.queue_a.id,
        job_id=job.id,
        step_id=None,
        task_id=None,
    )
    output_paths_by_root = job_output_downloader.get_output_paths_by_root()
    LOG.info(f"Output paths by root: {job_output_downloader.outputs_by_root}")

    # Download file and place it into the output_paths_by_root
    if output_root_path is not None:
        if len(output_paths_by_root) != 1:
            raise NotImplementedError(
                f"Currently do not support more than one root paths, provided {output_paths_by_root.keys()}"
            )

        job_output_downloader.set_root_path(
            list(output_paths_by_root.keys())[0], os.path.abspath(output_root_path)
        )

    job_output_downloader.download_job_output()
    return output_paths_by_root


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
                                    if os.environ["OPERATING_SYSTEM"] == "linux"
                                    else "powershell"
                                ),
                                "args": (
                                    ["5"]
                                    if os.environ["OPERATING_SYSTEM"] == "linux"
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
) -> Job:
    job = Job.submit(
        client=deadline_client,
        farm=farm,
        queue=queue,
        max_retries_per_task=max_retries_per_task,
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
                            "onRun": (
                                {"command": "{{ Task.File.runScript }}"}
                                if os.environ["OPERATING_SYSTEM"] == "linux"
                                else {
                                    "command": "powershell",
                                    "args": ["{{ Task.File.runScript }}"],
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
        },
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
    if os.environ["OPERATING_SYSTEM"] == "linux":
        cmd_result = worker.send_command(
            command="""
grep -E \
  '^(# shutdown_on_stop =|shutdown_on_stop =)' \
  /etc/amazon/deadline/worker.toml
"""
        )
        assert cmd_result.exit_code == 0, "Failed to execute Linux command on .toml"
        assert "shutdown_on_stop" in cmd_result.stdout, "shutdown_on_stop not found in .toml"
        return cmd_result.stdout.strip()
    elif os.environ["OPERATING_SYSTEM"] == "windows":
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
        raise Exception(f"Unsupported operating system: {os.environ['OPERATING_SYSTEM']}")
