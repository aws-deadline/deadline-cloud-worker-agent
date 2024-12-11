# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
This test module contains tests that verify the Worker agent's credential handling behavior.

Once the worker is online, the tests run SSM commands that attempt to access credentials from an
attacker position in a supposed different security boundary.
"""

import logging
import pytest
import os

from deadline_test_fixtures import CommandResult, DeadlineWorkerConfiguration, EC2InstanceWorker


@pytest.mark.skipif(
    os.environ["OPERATING_SYSTEM"] == "windows",
    reason="Linux specific test",
)
def test_access_worker_credential_file_from_job(
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
