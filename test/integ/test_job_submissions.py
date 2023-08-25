# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
This test module contains tests that verify the Worker agent's behavior by submitting jobs to the
Deadline Cloud service and checking that the result/output of the jobs is as we expect it.
"""

import pytest  # noqa: F401

import logging

from deadline_test_fixtures import (
    DeadlineClient,
    DeadlineResources,
    Farm,
    Job,
    Queue,
    TaskStatus,
)

LOG = logging.getLogger(__name__)


@pytest.mark.usefixtures("worker")
class TestJobSubmissions:
    @pytest.fixture
    def farm(self, deadline_resources: DeadlineResources) -> Farm:
        return deadline_resources.farm

    @pytest.fixture
    def queue(self, deadline_resources: DeadlineResources) -> Queue:
        return deadline_resources.queue

    def test_success(
        self,
        deadline_client: DeadlineClient,
        farm: Farm,
        queue: Queue,
    ) -> None:
        # WHEN
        job = Job.submit(
            client=deadline_client,
            farm=farm,
            queue=queue,
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
