# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
This test module contains tests that verify the Worker agent's credential handling behavior.

Once the worker is online, the tests run SSM commands that attempt to access credentials from an
attacker position in a supposed different security boundary.
"""

import logging
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
from .utils import job_failure_message


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
    # This script attempts to read worker credential files.
    # EXPECTED: access is denied → script exits 1 → job FAILS.
    # If access is NOT denied, the script exits 0 → job SUCCEEDS → test assertion catches the bug.
    powershell_script = """
Write-Host "--- Attempting to read worker credential files as job user ---"
Write-Host "Current user: $(whoami)"

try {
    $credFiles = Get-ChildItem -Path "$env:ProgramData\\Amazon\\Deadline\\Cache\\credentials" -Filter *.json -ErrorAction Stop
    if ($credFiles) {
        foreach ($file in $credFiles) {
            $content = Get-Content $file.FullName -ErrorAction Stop
            Write-Host "Read credential file: $($file.FullName) — access was NOT denied"
        }
    } else {
        Write-Host "No credential files found"
        exit 1
    }
} catch {
    Write-Host "PASS: Access denied as expected — $_"
    exit 1
}

# If we reach here, the job user could read credentials — this is a security bug.
Write-Host "FAIL: Job user was able to read worker credentials"
"""
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
            "description": "Verifies job user cannot read worker credential files. Expected status: FAILED",
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
    assert job.task_run_status == TaskStatus.FAILED, (
        "Job should have failed when trying to access worker credentials.\n"
        + job_failure_message(job, deadline_client, deadline_resources.queue_a, deadline_resources)
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
