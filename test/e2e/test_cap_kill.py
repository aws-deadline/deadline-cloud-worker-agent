# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
This test module contains tests that verify the Worker agent removes CAP_KILL from its inheritable
capability set
"""

import os
import re
from typing import Generator

import boto3
import botocore
import pytest

from deadline_test_fixtures import (
    DeadlineClient,
    EC2InstanceWorker,
    Job,
    TaskStatus,
)
from e2e.conftest import DeadlineResources


@pytest.fixture
def sleep_job_in_bg_pid(session_worker: EC2InstanceWorker) -> Generator[int, None, None]:
    """Context manager that runs a sleep command in the background and yields the process ID of the
    sleep process. The context-manager will do a best-effort to kill the sleep job when exiting the
    context"""

    # Send SSM command to write and run a bash script
    # The script creates a detached sleep process and outputs that process' PID
    # This sleep process will run as the ssm-user which is different from the job user
    result = session_worker.send_command(
        " ; ".join(
            [
                "echo '#!/bin/bash' > script.sh",
                "echo 'set -euo pipefail' >> script.sh",
                "echo 'nohup sleep 240 < /dev/null 2> /dev/null > /dev/null &' >> script.sh",
                "echo 'echo $!' >> script.sh",
                "chmod +x script.sh",
                "./script.sh",
                "rm script.sh",
            ]
        )
    )

    # Capture the PID from the SSM command output
    sleep_pid = int(result.stdout)
    yield sleep_pid

    # Clean up the background sleep job if needed
    try:
        session_worker.send_command(f"kill -9 {sleep_pid} || true")
    except Exception as e:
        print(f"Failed to cleanup background sleep job {sleep_pid}: {e}")


@pytest.mark.skipif(
    os.environ["OPERATING_SYSTEM"] == "windows",
    reason="Linux specific test",
)
@pytest.mark.usefixtures("session_worker")
def test_cap_kill_not_inherited_by_running_jobs(
    deadline_client: DeadlineClient,
    deadline_resources: DeadlineResources,
    sleep_job_in_bg_pid: int,
) -> None:
    """Tests that the worker agent drops CAP_KILL from its inheritable capability set and that
    session actions are not able to signal processes belonging to different OS users"""

    # WHEN
    # Submit a job that tries to send a SIGTERM to the process owned by another user
    job: Job = Job.submit(
        client=deadline_client,
        farm=deadline_resources.farm,
        queue=deadline_resources.queue_a,
        priority=98,
        max_retries_per_task=1,
        template={
            "specificationVersion": "jobtemplate-2023-09",
            "name": "Try to send cross-user Linux signals",
            "description": "Tests that CAP_KILL is not inherited from the worker agent",
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
                                "command": "kill",
                                "args": [
                                    "-s",
                                    "term",
                                    str(sleep_job_in_bg_pid),
                                ],
                                "timeout": 1,  # Times out in 1 second
                                "cancelation": {
                                    "mode": "NOTIFY_THEN_TERMINATE",
                                    "notifyPeriodInSeconds": 1,
                                },
                            },
                        },
                    },
                },
            ],
        },
    )
    job.wait_until_complete(client=deadline_client)

    # THEN
    job.refresh_job_info(client=deadline_client)
    assert job.task_run_status == TaskStatus.FAILED
    logs_client = boto3.client(
        "logs",
        config=botocore.config.Config(retries={"max_attempts": 10, "mode": "adaptive"}),
    )
    possible_error_messages: list[str] = [
        # this is the output format from the "kill" program which will be the format in
        # openjd-sessions versions after 0.9.0
        # (see https://github.com/OpenJobDescription/openjd-sessions-for-python/commit/84008be79e80cdd9b06095933ea0c58baee89c92#diff-7cf6bc1778d45b770b1736b74b151f26d01d1cd26611f36df4c689e892aefbc6R379)
        f"kill: sending signal to {sleep_job_in_bg_pid} failed: Operation not permitted",
        # this is the output format in openjd-sessions 0.9.0 and earlier (used "kill" bash built-in
        # and not exec which uses the kill program)
        f"kill: ({sleep_job_in_bg_pid}) - Operation not permitted",
    ]
    possible_error_message_re_pattern = (
        "("
        + "|".join(re.escape(possible_error_msg) for possible_error_msg in possible_error_messages)
        + ")"
    )
    job.assert_single_task_log_contains(
        deadline_client=deadline_client,
        logs_client=logs_client,
        expected_pattern=possible_error_message_re_pattern,
    )
