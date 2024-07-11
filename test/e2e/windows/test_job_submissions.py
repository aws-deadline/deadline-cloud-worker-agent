# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
This test module contains tests that verify the Worker agent's behavior by submitting jobs to the
Deadline Cloud service and checking that the result/output of the jobs is as we expect it.
"""

import pytest

import logging

from deadline_test_fixtures import (
    Job,
    TaskStatus,
    DeadlineClient,
    EC2InstanceWorker,
)


LOG = logging.getLogger(__name__)


@pytest.mark.parametrize("operating_system", ["windows"], indirect=True)
class TestJobSubmission:
    def test_success(
        self,
        deadline_resources,
        class_worker: EC2InstanceWorker,
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
                        "script": {
                            "actions": {
                                "onRun": {"command": "powershell", "args": ["ping", "localhost"]}
                            }
                        },
                    },
                ],
            },
        )

        # THEN
        LOG.info(f"Waiting for job {job.id} to complete")
        job.wait_until_complete(client=deadline_client)
        LOG.info(f"Job result: {job}")

        assert job.task_run_status == TaskStatus.SUCCEEDED
