# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
This test module contains tests that verify the Worker agent removes CAP_KILL from its inheritable
capability set
"""

import os
from typing import Generator

import pytest

from deadline_test_fixtures import (
    DeadlineClient,
    EC2InstanceWorker,
    Job,
    TaskStatus,
)
from e2e.conftest import DeadlineResources
from e2e.utils import job_failure_message


@pytest.fixture
def sleep_job_in_bg_pid(
    session_worker: EC2InstanceWorker,
) -> Generator[int, None, None]:
    """Context manager that runs a sleep command in the background and yields the process ID of the
    sleep process. The context-manager will do a best-effort to kill the sleep job when exiting the
    context"""

    # Start a detached sleep process (as ssm-user, different from the job user) and capture its PID
    result = session_worker.send_command(
        "\n".join(
            [
                "nohup sleep 240 < /dev/null 2> /dev/null > /dev/null &",
                "echo $!",
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
    os.environ["OPERATING_SYSTEM"] != "linux",
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
    # Submit a job that tries to send a SIGTERM to the process owned by another user.
    # The script attempts the kill and asserts it fails with "Operation not permitted".
    job: Job = Job.submit(
        client=deadline_client,
        farm=deadline_resources.farm,
        queue=deadline_resources.queue_a,
        priority=98,
        max_retries_per_task=1,
        template={
            "specificationVersion": "jobtemplate-2023-09",
            "name": "Try to send cross-user Linux signals",
            "description": (
                "Verifies that CAP_KILL is not inherited by jobs. "
                "The job attempts to kill a process owned by another user. "
                "SUCCESS means the kill was denied with 'Operation not permitted'."
            ),
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
                                "command": "bash",
                                "args": ["{{Task.File.Script}}"],
                                "timeout": 5,
                                "cancelation": {
                                    "mode": "NOTIFY_THEN_TERMINATE",
                                    "notifyPeriodInSeconds": 1,
                                },
                            },
                        },
                        "embeddedFiles": [
                            {
                                "name": "Script",
                                "type": "TEXT",
                                "runnable": True,
                                "data": "\n".join(
                                    [
                                        "#!/bin/bash",
                                        "set -euo pipefail",
                                        'echo "=== Testing: trying to kill PID owned by another user ==="',
                                        f'echo "Target PID: {sleep_job_in_bg_pid}"',
                                        f"OUTPUT=$(kill -s term {sleep_job_in_bg_pid} 2>&1) && echo 'ERROR: kill succeeded unexpectedly' && exit 1",
                                        'echo "Kill output: $OUTPUT"',
                                        'echo "$OUTPUT" | grep -q "Operation not permitted" || { echo "FAIL: expected Operation not permitted, got: $OUTPUT"; exit 1; }',
                                        'echo "PASS: got Operation not permitted as expected"',
                                        'echo "=== All checks passed ==="',
                                    ]
                                ),
                            },
                        ],
                    },
                },
            ],
        },
    )
    job.wait_until_complete(client=deadline_client)

    # THEN
    job.refresh_job_info(client=deadline_client)
    assert job.task_run_status == TaskStatus.SUCCEEDED, job_failure_message(
        job, deadline_client, deadline_resources.queue_a, deadline_resources
    )


@pytest.mark.skipif(
    os.environ["OPERATING_SYSTEM"] != "linux",
    reason="Linux specific test",
)
@pytest.mark.usefixtures("session_worker")
def test_worker_subprocesses_have_no_capabilities(
    deadline_client: DeadlineClient,
    deadline_resources: DeadlineResources,
) -> None:
    """Tests to make sure that subprocesses of the worker agent have no capabilities"""
    job: Job = Job.submit(
        client=deadline_client,
        farm=deadline_resources.farm,
        queue=deadline_resources.queue_a,
        priority=98,
        max_retries_per_task=1,
        template={
            "specificationVersion": "jobtemplate-2023-09",
            "name": "Check process Linux capabilities",
            "description": (
                "Verifies that worker subprocesses inherit no capabilities. "
                "SUCCESS means both Current and Ambient capability sets are empty."
            ),
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
                                "command": "bash",
                                "args": ["{{Task.File.CheckCaps}}"],
                            },
                        },
                        "embeddedFiles": [
                            {
                                "name": "CheckCaps",
                                "type": "TEXT",
                                "runnable": True,
                                "data": "\n".join(
                                    [
                                        "#!/bin/bash",
                                        "set -euo pipefail",
                                        'echo "=== Testing: checking subprocess capabilities (Current and Ambient) ==="',
                                        "OUTPUT=$(capsh --print)",
                                        'echo "Full capsh output:"',
                                        'echo "$OUTPUT"',
                                        'echo "$OUTPUT" | grep -Pq "^Current: =\\s*$" || { echo "FAIL: Current capabilities not empty"; exit 1; }',
                                        'echo "PASS: Current capabilities are empty"',
                                        'echo "$OUTPUT" | grep -Pq "^Ambient set =\\s*$" || { echo "FAIL: Ambient set not empty"; exit 1; }',
                                        'echo "PASS: Ambient set is empty"',
                                        'echo "=== All checks passed ==="',
                                    ]
                                ),
                            },
                        ],
                    },
                },
            ],
        },
    )
    job.wait_until_complete(client=deadline_client)

    # THEN
    job.refresh_job_info(client=deadline_client)
    assert job.task_run_status == TaskStatus.SUCCEEDED, job_failure_message(
        job, deadline_client, deadline_resources.queue_a, deadline_resources
    )


@pytest.mark.skipif(
    os.environ["OPERATING_SYSTEM"] != "linux",
    reason="Linux specific test",
)
@pytest.mark.usefixtures("session_worker")
def test_worker_only_has_cap_kill(
    deadline_client: DeadlineClient,
    deadline_resources: DeadlineResources,
) -> None:
    """Tests to make sure that the worker agent has no capabilities besides cap_kill, and that it is not inheritable"""
    job: Job = Job.submit(
        client=deadline_client,
        farm=deadline_resources.farm,
        queue=deadline_resources.queue_a,
        priority=98,
        max_retries_per_task=1,
        template={
            "specificationVersion": "jobtemplate-2023-09",
            "name": "Check worker process Linux capabilities",
            "description": (
                "Verifies that the worker agent process only has cap_kill=ep. "
                "SUCCESS means the worker has cap_kill effective+permitted but not inheritable."
            ),
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
                                "command": "bash",
                                "args": ["{{Task.File.CheckCaps}}"],
                            },
                        },
                        "embeddedFiles": [
                            {
                                "name": "CheckCaps",
                                "type": "TEXT",
                                "runnable": True,
                                # Resolve the worker agent PID via systemd rather than walking the
                                # process tree. This is runtime-agnostic: the Rust session runtime's
                                # persistent helper binary adds an extra process layer that changes
                                # the tree depth, making grandparent-based resolution unreliable.
                                "data": "\n".join(
                                    [
                                        "#!/bin/bash",
                                        "set -euo pipefail",
                                        'echo "=== Testing: verifying worker process only has cap_kill=ep ==="',
                                        "WORKER_PID=$(systemctl show --property=MainPID --value deadline-worker)",
                                        '[ -n "$WORKER_PID" ] && [ "$WORKER_PID" -gt 0 ] || { echo "FAIL: could not resolve worker agent PID from systemd"; exit 1; }',
                                        'echo "Worker PID: $WORKER_PID"',
                                        "OUTPUT=$(getpcaps $WORKER_PID)",
                                        'echo "Capabilities output: $OUTPUT"',
                                        'echo "$OUTPUT" | grep -Pq "\\d+: cap_kill=ep$" || { echo "FAIL: expected only cap_kill=ep, got: $OUTPUT"; exit 1; }',
                                        'echo "PASS: worker only has cap_kill=ep"',
                                        'echo "=== All checks passed ==="',
                                    ]
                                ),
                            },
                        ],
                    },
                },
            ],
        },
    )
    job.wait_until_complete(client=deadline_client)

    # THEN
    job.refresh_job_info(client=deadline_client)
    assert job.task_run_status == TaskStatus.SUCCEEDED, job_failure_message(
        job, deadline_client, deadline_resources.queue_a, deadline_resources
    )
