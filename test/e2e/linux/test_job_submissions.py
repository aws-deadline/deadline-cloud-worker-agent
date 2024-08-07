# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
This test module contains tests that verify the Worker agent's behavior by submitting jobs to the
Deadline Cloud service and checking that the result/output of the jobs is as we expect it.
"""
import tempfile
import boto3
import botocore.client
import configparser
import os
import botocore.config
import botocore.exceptions
import pytest
import logging
from deadline.job_attachments._aws.deadline import get_queue
from deadline.job_attachments import download
from e2e.conftest import DeadlineResources
from deadline.client.config import set_setting
from deadline.client import api
from typing import Dict, List, Optional
import uuid

from deadline_test_fixtures import Job, TaskStatus, PosixSessionUser, DeadlineClient

LOG = logging.getLogger(__name__)


@pytest.mark.usefixtures("worker")
@pytest.mark.parametrize("operating_system", ["linux"], indirect=True)
class TestJobSubmission:
    def test_success(
        self,
        deadline_resources,
        deadline_client: DeadlineClient,
    ) -> None:
        # WHEN
        job = Job.submit(
            client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            priority=98,
            template={
                "specificationVersion": "jobtemplate-2023-09",
                "name": "Sleep Job",
                "steps": [
                    {
                        "name": "Step0",
                        "script": {"actions": {"onRun": {"command": "/bin/sleep", "args": ["5"]}}},
                    },
                ],
            },
        )

        # THEN
        LOG.info(f"Waiting for job {job.id} to complete")
        job.wait_until_complete(client=deadline_client, max_retries=20)
        LOG.info(f"Job result: {job}")

        assert job.task_run_status == TaskStatus.SUCCEEDED

    def test_job_run_as_user(
        self,
        deadline_resources,
        deadline_client: DeadlineClient,
        job_run_as_user: PosixSessionUser,
    ) -> None:
        # WHEN
        job = Job.submit(
            client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            priority=98,
            template={
                "specificationVersion": "jobtemplate-2023-09",
                "name": "whoami",
                "steps": [
                    {
                        "name": "Step0",
                        "script": {
                            "embeddedFiles": [
                                {
                                    "name": "whoami",
                                    "type": "TEXT",
                                    "runnable": True,
                                    "data": "\n".join(
                                        [
                                            "#!/bin/bash",
                                            'echo "I am: $(whoami)"',
                                        ]
                                    ),
                                },
                            ],
                            "actions": {
                                "onRun": {
                                    "command": "{{ Task.File.whoami }}",
                                },
                            },
                        },
                    },
                ],
            },
        )

        # THEN
        job.wait_until_complete(client=deadline_client, max_retries=20)

        # Retrieve job output and verify whoami printed the queue's jobsRunAsUser
        job_logs = job.get_logs(
            deadline_client=deadline_client,
            logs_client=boto3.client(
                "logs",
                config=botocore.config.Config(retries={"max_attempts": 10, "mode": "adaptive"}),
            ),
        )
        full_log = "\n".join(
            [le.message for _, log_events in job_logs.logs.items() for le in log_events]
        )
        assert (
            f"I am: {job_run_as_user.user}" in full_log
        ), f"Expected message not found in Job logs. Logs are in CloudWatch log group: {job_logs.log_group_name}"
        assert job.task_run_status == TaskStatus.SUCCEEDED

    def test_worker_uses_job_attachment_configuration(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
    ) -> None:
        # Verify that the worker uses the correct job attachment configuration, and writes the output to the correct location

        test_run_uuid: str = str(uuid.uuid4())

        job_bundle_path: str = os.path.join(
            os.path.dirname(__file__),
            "job_attachment_bundle",
        )
        job_parameters: List[Dict[str, str]] = [
            {"name": "StringToAppend", "value": test_run_uuid},
            {"name": "DataDir", "value": job_bundle_path},
        ]
        config = configparser.ConfigParser()

        set_setting("defaults.farm_id", deadline_resources.farm.id, config)
        set_setting("defaults.queue_id", deadline_resources.queue_a.id, config)

        job_id: Optional[str] = api.create_job_from_job_bundle(
            job_bundle_path,
            job_parameters,
            priority=99,
            config=config,
            queue_parameter_definitions=[],
        )
        assert job_id is not None

        job_details = Job.get_job_details(
            client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            job_id=job_id,
        )
        job = Job(
            farm=deadline_resources.farm,
            queue=deadline_resources.queue_a,
            template={},
            **job_details,
        )
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
        with tempfile.TemporaryDirectory() as tmp_dir_name:

            # Set root path output will be downloaded to to output_root_path. Assumes there is only one root path.
            job_output_downloader.set_root_path(
                list(output_paths_by_root.keys())[0],
                tmp_dir_name,
            )
            job_output_downloader.download_job_output()

            with (
                open(os.path.join(job_bundle_path, "files", "test_input_file"), "r") as input_file,
                open(os.path.join(tmp_dir_name, "output_file.txt"), "r") as output_file,
            ):
                input_file_content: str = input_file.read()
                output_file_content = output_file.read()

                assert output_file_content == (input_file_content + test_run_uuid)
