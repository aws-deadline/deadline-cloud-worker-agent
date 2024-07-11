# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
This test module contains tests that verify the Worker agent's behavior by submitting jobs to the
Deadline Cloud service and checking that the result/output of the jobs is as we expect it.
"""

import boto3
import botocore.client
import botocore.config
import botocore.exceptions
import pytest

import logging

from deadline_test_fixtures import (
    Job,
    TaskStatus,
    DeadlineClient,
    PosixSessionUser,
)


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
        job.wait_until_complete(client=deadline_client)
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
        job.wait_until_complete(client=deadline_client)

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
