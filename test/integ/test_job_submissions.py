# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
This test module contains tests that verify the Worker agent's behavior by submitting jobs to the
Deadline Cloud service and checking that the result/output of the jobs is as we expect it.
"""

import boto3
import botocore.client
import botocore.config
import botocore.exceptions
import dataclasses

import pytest  # noqa: F401

import logging

from typing import Generator

from deadline_test_fixtures import (
    DeadlineClient,
    DeadlineResources,
    DeadlineWorkerConfiguration,
    Farm,
    Fleet,
    Job,
    JobRunAsUser,
    PosixSessionUser,
    Queue,
    QueueFleetAssociation,
    TaskStatus,
)

LOG = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def farm(deadline_resources: DeadlineResources) -> Farm:
    return deadline_resources.farm


@pytest.fixture(scope="session")
def queue(deadline_resources: DeadlineResources) -> Queue:
    return deadline_resources.queue


@pytest.fixture(scope="session")
def fleet(deadline_resources: DeadlineResources) -> Fleet:
    return deadline_resources.fleet


@pytest.fixture(scope="session")
def job_run_as_user() -> PosixSessionUser:
    return PosixSessionUser(
        user="job-run-as-user",
        group="job-run-as-user-group",
    )


@pytest.fixture(scope="session")
def worker_config(
    worker_config: DeadlineWorkerConfiguration,
    job_run_as_user: PosixSessionUser,
) -> DeadlineWorkerConfiguration:
    return dataclasses.replace(
        worker_config,
        job_users=[
            *worker_config.job_users,
            job_run_as_user,
        ],
    )


@pytest.fixture(scope="session")
def queue_with_job_run_as_user(
    farm: Farm,
    fleet: Fleet,
    deadline_client: DeadlineClient,
    job_run_as_user: PosixSessionUser,
) -> Generator[Queue, None, None]:
    queue = Queue.create(
        client=deadline_client,
        display_name=f"Queue with jobsRunAsUser {job_run_as_user.user}",
        farm=farm,
        job_run_as_user=JobRunAsUser(runAs="QUEUE_CONFIGURED_USER", posix=job_run_as_user),
    )

    qfa = QueueFleetAssociation.create(
        client=deadline_client,
        farm=farm,
        queue=queue,
        fleet=fleet,
    )

    yield queue

    qfa.delete(
        client=deadline_client,
        stop_mode="STOP_SCHEDULING_AND_CANCEL_TASKS",
    )
    queue.delete(client=deadline_client)


@pytest.mark.usefixtures("worker")
class TestJobSubmission:
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

    def test_jobs_run_as_user(
        self,
        deadline_client: DeadlineClient,
        farm: Farm,
        queue_with_job_run_as_user: Queue,
        job_run_as_user: PosixSessionUser,
    ) -> None:
        # WHEN
        job = Job.submit(
            client=deadline_client,
            farm=farm,
            queue=queue_with_job_run_as_user,
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
