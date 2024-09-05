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
import os
import re

from deadline_test_fixtures import Job, TaskStatus, PosixSessionUser, DeadlineClient


LOG = logging.getLogger(__name__)


@pytest.mark.usefixtures("session_worker")
@pytest.mark.usefixtures("operating_system")
@pytest.mark.skipif(
    os.environ["OPERATING_SYSTEM"] == "windows",
    reason="Linux specific test",
)
@pytest.mark.parametrize("operating_system", ["linux"], indirect=True)
class TestJobUser:
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
                        "hostRequirements": {
                            "attributes": [{"name": "attr.worker.os.family", "allOf": ["linux"]}]
                        },
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

        job.assert_single_task_log_contains(
            deadline_client=deadline_client,
            logs_client=boto3.client(
                "logs",
                config=botocore.config.Config(retries={"max_attempts": 10, "mode": "adaptive"}),
            ),
            expected_pattern=rf"I am: {re.escape(job_run_as_user.user)}",
        )

        assert job.task_run_status == TaskStatus.SUCCEEDED
