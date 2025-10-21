# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
Utility script for cleaning up the test environment before running
Deadline Cloud Worker Agent E2E tests. Will terminate all instances
tagged with InstanceIdentification=DeadlineScaffoldingWorker and
TestCategory=<Value of TEST_CATEGORY environment variable> by default
test instances are tagged with "dev"

You must first follow the E2E test setup instructions in DEVELOPMENT.md and source
the environment files before running this script.

This script will automatically run before the tests, if the tests are run with
hatch run e2e-test
"""

from typing import Any
import time
import boto3
import boto3.session

import logging
import os
import sys
from dataclasses import dataclass
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from botocore.config import Config

from deadline.client.api import _list_jobs_by_filter_expression
from deadline_test_fixtures.deadline.resources import COMPLETE_TASK_STATUSES, TaskStatus


LOG = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.INFO)


@dataclass
class QueueJob:
    job_id: str
    queue_id: str


def terminate_instances_and_wait(ec2_client: BaseClient, instances: list[str]) -> None:
    if not instances:
        LOG.info("No instances to terminate")
        return
    instances_to_wait_on: list[str] = []

    # Delete instances one by one instead of terminating them
    # in a batch call, otherwise the error handling can get messy.
    # If there's at least one instance that isn't valid in the
    # InstanceIds list then the whole call fails.
    LOG.info("Terminating %s leftover instances", len(instances))
    for instance in instances:
        try:
            LOG.info("Terminating %s", instance)
            ec2_client.terminate_instances(
                InstanceIds=[instance],
                Force=True,
                SkipOsShutdown=True,
            )
            # We only want to wait on instances to terminate
            # that exist
            instances_to_wait_on.append(instance)
            LOG.info("Succesfully terminated %s", instance)
        except ClientError as e:
            # If an instance id isn't valid anymore we can assume
            # it's already been deleted, so we can ignore it.
            if e.response["Error"]["Code"].startswith("InvalidInstanceID"):
                LOG.warning(
                    "Instance %s failed to be terminated, doesn't exist: %s", instance, exc_info=e
                )
                continue
            LOG.error("Error encountered cleaning up instance %s", instance, exc_info=e)
            raise e

    waiter = ec2_client.get_waiter("instance_terminated")

    LOG.info("Waiting for instances to terminate")
    waiter.wait(InstanceIds=instances_to_wait_on)
    LOG.info("All instances have been terminated")


def get_all_test_tagged_instances(ec2_client: BaseClient) -> list[str]:
    paginator = ec2_client.get_paginator("describe_instances")
    tagged_instances: list[str] = []

    test_category = os.getenv("TEST_CATEGORY", "dev")

    response_iterator = paginator.paginate(
        Filters=[
            {
                "Name": "tag:InstanceIdentification",
                "Values": [
                    "DeadlineScaffoldingWorker",
                ],
            },
            {"Name": "tag:TestCategory", "Values": [test_category]},
        ],
    )

    LOG.info("Collecting all tests tagged with TestCategory: %s", test_category)

    for response in response_iterator:
        for reservation in response["Reservations"]:
            for instance in reservation["Instances"]:
                tagged_instances.append(instance["InstanceId"])

    return tagged_instances


def cleanup_ec2_instances(ec2_client: BaseClient) -> None:
    test_tagged_instances = get_all_test_tagged_instances(ec2_client)
    terminate_instances_and_wait(ec2_client, test_tagged_instances)


def get_incomplete_jobs(
    boto3_session: boto3.Session, farm_id: str, queues: list[str]
) -> list[QueueJob]:
    filters: list[dict[str, dict[str, str]]] = []
    jobs: list[QueueJob] = []

    for status in COMPLETE_TASK_STATUSES:
        filters.append(
            {
                "stringFilter": {
                    "name": "TASK_RUN_STATUS",
                    "operator": "NOT_EQUAL",
                    "value": status,
                }
            }
        )

    list_jobs: list[dict[str, Any]] = []

    LOG.info("Fetching jobs that aren't yet completed")
    for queue in queues:
        list_jobs.extend(
            _list_jobs_by_filter_expression._list_jobs_by_filter_expression(
                boto3_session,
                farm_id,
                queue,
                filter_expression={
                    "filters": filters,
                    "operator": "AND",
                },
            )
        )

    for job in list_jobs:
        jobs.append(QueueJob(job_id=job["jobId"], queue_id=job["queueId"]))

    return jobs


def cancel_job(deadline_client: BaseClient, farm_id: str, job: QueueJob) -> None:
    LOG.info("Cancelling job: %s", job.job_id)
    deadline_client.update_job(
        farmId=farm_id,
        queueId=job.queue_id,
        jobId=job.job_id,
        targetTaskRunStatus=TaskStatus.CANCELED,
    )


def wait_for_jobs_to_be_cancelled(
    deadline_client: BaseClient, farm_id: str, jobs: list[QueueJob]
) -> None:
    max_retries_seconds = 30

    LOG.info("Waiting for jobs to be cancelled")
    for job in jobs:
        tries = 0
        while True:
            response = deadline_client.get_job(
                farmId=farm_id, queueId=job.queue_id, jobId=job.job_id
            )

            if response["taskRunStatus"] == TaskStatus.CANCELED:
                break

            tries += 1

            if tries >= max_retries_seconds:
                raise RuntimeError(f"Job {job.job_id} failed to cancel in {max_retries_seconds}")

            time.sleep(1)


def cleanup_queues(
    session: boto3.Session, deadline_client: BaseClient, farm_id: str, queues: list[str]
) -> None:
    jobs = get_incomplete_jobs(session, farm_id, queues)

    if not jobs:
        LOG.info("No jobs to cleanup")
        return

    LOG.info("Attempting to cancel %s jobs", len(jobs))
    for job in jobs:
        cancel_job(deadline_client, farm_id, job)

    wait_for_jobs_to_be_cancelled(deadline_client, farm_id, jobs)


def get_queues() -> list[str]:
    try:
        return [
            os.environ["QUEUE_A_ID"],
            os.environ["QUEUE_B_ID"],
            os.environ["JOBS_RUN_AS_AGENT_USER_QUEUE_ID"],
            os.environ["NON_VALID_ROLE_QUEUE_ID"],
            os.environ["SCALING_QUEUE_ID"],
        ]
    except KeyError as e:
        LOG.error("Missing needed queue environment variables", exc_info=e)
        raise e


def get_farm() -> str:
    try:
        return os.environ["FARM_ID"]
    except KeyError as e:
        LOG.error("Missing needed farm environment variable", exc_info=e)
        raise e


def cleanup_test_environment() -> None:
    config = Config(retries={"mode": "adaptive"})

    if os.getenv("KEEP_WORKER_AFTER_FAILURE", "").lower() != "true":
        ec2_client = boto3.client("ec2", config=config)
        cleanup_ec2_instances(ec2_client)

    queues = get_queues()
    farm_id = get_farm()

    deadline_client = boto3.client("deadline", config=config)
    session = boto3.session.Session()
    cleanup_queues(session, deadline_client, farm_id, queues)


if __name__ == "__main__":
    cleanup_test_environment()
