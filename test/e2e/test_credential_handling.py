# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
This test module contains tests that verify the Worker agent's credential handling behavior.

Once the worker is online, the tests run SSM commands that attempt to access credentials from an
attacker position in a supposed different security boundary.
"""

import logging
import boto3
import botocore
import pytest
import os

from deadline_test_fixtures import (
    CommandResult,
    DeadlineClient,
    DeadlineWorkerConfiguration,
    EC2InstanceWorker,
    Job,
    TaskStatus,
)

from .conftest import DeadlineResources


@pytest.mark.skipif(
    os.environ["OPERATING_SYSTEM"] == "windows",
    reason="Linux specific test",
)
def test_access_worker_credential_file_from_job_linux(
    session_worker: EC2InstanceWorker,
    worker_config: DeadlineWorkerConfiguration,
) -> None:
    """Tests that the worker agent credentials file cannot be read by a job user"""
    # GIVEN
    job_users = worker_config.job_users
    assert len(job_users) >= 1
    job_user = job_users[0]

    ########################################################################################
    # We first ensure that the worker agent user can read the agent's IAM credential files
    # to ensure that the file exists and our test is valid
    ########################################################################################
    # WHEN
    result = session_worker.send_command(
        f'sudo -u "{worker_config.agent_user}" cat /var/lib/deadline/credentials/{session_worker.worker_id}.json > /dev/null'
    )

    # THEN
    expect_ssm_success(
        result,
        failure_msg="Worker credentials file existence check SSM command failed",
    )

    ########################################################################################
    # Next we try to access the same credential file(s) as the job user and assert that the
    # command fails.
    ########################################################################################
    # WHEN
    result = session_worker.send_command(
        f'sudo -u "{job_user.user}" cat /var/lib/deadline/credentials/{session_worker.worker_id}.json > /dev/null'
    )

    # THEN
    assert result.exit_code != 0


@pytest.mark.skipif(
    os.environ["OPERATING_SYSTEM"] != "windows",
    reason="Windows specific test",
)
def test_access_worker_credential_file_from_job_windows(
    session_worker: EC2InstanceWorker,
    deadline_resources: DeadlineResources,
    deadline_client: DeadlineClient,
) -> None:
    # GIVEN
    powershell_script = """
Write-Host "Current user: $(whoami)"
Write-Host "Attempting to read worker credentials from cache directory..."

# Attempt to read worker credential files from the cache directory
try {
    # Look for JSON files in the credentials directory
    $credFiles = Get-ChildItem -Path "$env:ProgramData\\Amazon\\Deadline\\Cache\\credentials" -Filter *.json -ErrorAction Stop
    
    if ($credFiles) {
        foreach ($file in $credFiles) {
            Write-Host "Found credential file: $($file.FullName)"
            # Attempt to read the file contents - will throw if access is denied
            $content = Get-Content $file.FullName -ErrorAction Stop
            Write-Host "Successfully read file contents"
        }
    } else {
        Write-Host "No credential files found in the cache directory"
        exit 1
    }
} catch {
    Write-Host "Error accessing credential files: $_"
    exit 1
}

# Additional environment information
Write-Host "Environment variables:"
Get-ChildItem env: | Format-Table -AutoSize

Write-Host "Worker ID from environment: $env:DEADLINE_WORKER_ID"
"""
    logs_client = boto3.client(
        "logs",
        config=botocore.config.Config(retries={"max_attempts": 10, "mode": "adaptive"}),
    )

    # WHEN
    job: Job = Job.submit(
        client=deadline_client,
        farm=deadline_resources.farm,
        queue=deadline_resources.queue_a,
        priority=98,
        max_retries_per_task=0,
        template={
            "specificationVersion": "jobtemplate-2023-09",
            "name": "Windows Worker Credentials Read Test",
            "steps": [
                {
                    "name": "Read Windows Worker Credentials",
                    "script": {
                        "embeddedFiles": [
                            {
                                "name": "read_credentials",
                                "type": "TEXT",
                                "filename": "read_credentials.ps1",
                                "data": powershell_script,
                            },
                        ],
                        "actions": {
                            "onRun": {
                                "command": "powershell",
                                "args": ["-File", "{{Task.File.read_credentials}}"],
                            },
                        },
                    },
                },
            ],
        },
    )
    # Wait until the job is completed
    job.wait_until_complete(client=deadline_client)

    # THEN
    assert job.task_run_status == TaskStatus.FAILED
    job.assert_single_task_log_contains(
        deadline_client=deadline_client,
        expected_pattern="Error accessing credential files: ",
        logs_client=logs_client,
    )


def expect_ssm_success(
    result: CommandResult,
    *,
    failure_msg: str,
) -> None:
    """Expects an SSM command to succeed or raises an AssertionError"""
    if result.exit_code != 0:
        logging.info(failure_msg)
        logging.info("")
        logging.info("    [STDOUT]")
        logging.info("")
        logging.info(result.stdout)
        logging.info("")
        logging.info("    [STDERR]")
        logging.info("")
        logging.info(result.stderr)
        assert False, failure_msg
