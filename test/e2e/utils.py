# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
import os
from typing import Any, Dict

from deadline.job_attachments._aws.deadline import get_queue
from deadline.job_attachments import download
from deadline_test_fixtures import (
    DeadlineClient,
    Job,
    Farm,
    Queue,
)
import backoff
from e2e.conftest import DeadlineResources


def wait_for_job_output(
    job: Job, deadline_client: DeadlineClient, deadline_resources: DeadlineResources
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
    # Download file and place it into the output_paths_by_root
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
