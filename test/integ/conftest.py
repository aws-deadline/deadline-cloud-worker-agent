# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import dataclasses
from typing import Generator

from deadline_test_fixtures import (
    DeadlineClient,
    DeadlineResources,
    DeadlineWorkerConfiguration,
    Farm,
    Fleet,
    JobRunAsUser,
    PosixSessionUser,
    Queue,
    QueueFleetAssociation,
)
import pytest


pytest_plugins = ["deadline_test_fixtures.pytest_hooks"]


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
        job_run_as_user=JobRunAsUser(posix=job_run_as_user),
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
