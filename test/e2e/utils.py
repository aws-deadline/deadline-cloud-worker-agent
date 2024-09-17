# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
import os

from deadline.job_attachments._aws.deadline import get_queue
from deadline.job_attachments import download
from deadline_test_fixtures import (
    DeadlineClient,
    Job,
    Farm,
    Queue,
)

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
