# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
This test module contains tests that verify the Worker agent's behavior by submitting jobs to the
Deadline Cloud service and checking that the result/output of the jobs is as we expect it.
"""

import boto3
import botocore
import pytest

import logging

from deadline_test_fixtures import (
    Job,
    Farm,
    Queue,
    TaskStatus,
    DeadlineClient,
    EC2InstanceWorker,
)
from deadline_test_fixtures.deadline.resources import JobLogs

LOG = logging.getLogger(__name__)


def get_log_from_job(job: Job, deadline_client: DeadlineClient) -> tuple[str, JobLogs]:
    """
    Waits for the job to complete, then gets all the logs from the job and returns it as a string
    """
    LOG.info(f"Waiting for job {job.id} to complete")
    job.wait_until_complete(client=deadline_client)
    LOG.info(f"Job result: {job}")

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
    return full_log, job_logs


@pytest.mark.parametrize("operating_system", ["windows"], indirect=True)
class TestJobUserOverride:
    @staticmethod
    def submit_whoami_job(
        test_name: str, deadline_client: DeadlineClient, farm: Farm, queue: Queue
    ) -> Job:
        job = Job.submit(
            client=deadline_client,
            farm=farm,
            queue=queue,
            priority=98,
            template={
                "specificationVersion": "jobtemplate-2023-09",
                "name": f"whoami {test_name}",
                "steps": [
                    {
                        "hostRequirements": {
                            "attributes": [
                                {
                                    "name": "attr.worker.os.family",
                                    "allOf": ["windows"],
                                }
                            ]
                        },
                        "name": "Step0",
                        "script": {
                            "actions": {
                                "onRun": {
                                    "command": "powershell",
                                    "args": ["echo", '"I am: $((whoami).split("\\")[1])"'],
                                }
                            }
                        },
                    },
                ],
            },
        )
        return job

    def test_no_user_override(
        self,
        deadline_resources,
        class_worker: EC2InstanceWorker,
        deadline_client: DeadlineClient,
    ) -> None:

        job = self.submit_whoami_job(
            "no user override", deadline_client, deadline_resources.farm, deadline_resources.queue_a
        )

        full_log, job_logs = get_log_from_job(job, deadline_client)
        assert (
            "I am: job-user" in full_log
        ), f"Expected message not found in Job logs. Logs are in CloudWatch log group: {job_logs.log_group_name}"
        assert job.task_run_status == TaskStatus.SUCCEEDED

    def test_config_file_user_override(
        self,
        deadline_resources,
        class_worker: EC2InstanceWorker,
        deadline_client: DeadlineClient,
    ) -> None:

        class_worker.stop_worker_service()

        cmd_result = class_worker.send_command(
            "(Get-Content -Path C:\ProgramData\Amazon\Deadline\Config\worker.toml -Raw) -replace '# windows_job_user = \"job-user\"', 'windows_job_user = \"config-override\"' | Set-Content -Path C:\ProgramData\Amazon\Deadline\Config\worker.toml"
        )

        class_worker.start_worker_service()

        assert (
            cmd_result.exit_code == 0
        ), f"Setting the job user override via CLI failed: {cmd_result}"

        job = self.submit_whoami_job(
            "config user override",
            deadline_client,
            deadline_resources.farm,
            deadline_resources.queue_a,
        )

        full_log, job_logs = get_log_from_job(job, deadline_client)
        assert (
            "I am: config-override" in full_log
        ), f"Expected message not found in Job logs. Logs are in CloudWatch log group: {job_logs.log_group_name}"
        assert job.task_run_status == TaskStatus.SUCCEEDED

        # reset config file
        cmd_result = class_worker.send_command(
            "(Get-Content -Path C:\ProgramData\Amazon\Deadline\Config\worker.toml -Raw) -replace 'windows_job_user = \"config-override\"', '# windows_job_user = \"job-user\"' | Set-Content -Path C:\ProgramData\Amazon\Deadline\Config\worker.toml"
        )

        assert cmd_result.exit_code == 0, f"Failed to reset config file: {cmd_result}"

    def test_installer_user_override(
        self,
        deadline_resources,
        class_worker: EC2InstanceWorker,
        deadline_client: DeadlineClient,
    ) -> None:

        class_worker.stop_worker_service()

        cmd_result = class_worker.send_command(
            "install-deadline-worker "
            + "-y "
            + f"--farm-id {deadline_resources.farm.id} "
            + f"--fleet-id {deadline_resources.fleet.id} "
            + "--user ssm-user "
            + "--windows-job-user install-override"
        )

        assert (
            cmd_result.exit_code == 0
        ), f"Failed to install worker with job user override: {cmd_result}"

        class_worker.start_worker_service()

        job = self.submit_whoami_job(
            "installer user override",
            deadline_client,
            deadline_resources.farm,
            deadline_resources.queue_a,
        )

        full_log, job_logs = get_log_from_job(job, deadline_client)
        assert (
            "I am: install-override" in full_log
        ), f"Expected message not found in Job logs. Logs are in CloudWatch log group: {job_logs.log_group_name}"
        assert job.task_run_status == TaskStatus.SUCCEEDED

        # reset config file
        cmd_result = class_worker.send_command(
            "(Get-Content -Path C:\ProgramData\Amazon\Deadline\Config\worker.toml -Raw) -replace 'windows_job_user = \"installer-override\"', '# windows_job_user = \"job-user\"' | Set-Content -Path C:\ProgramData\Amazon\Deadline\Config\worker.toml"
        )

        assert cmd_result.exit_code == 0, f"Failed to reset config file: {cmd_result}"

    def test_env_var_user_override(
        self,
        deadline_resources,
        class_worker: EC2InstanceWorker,
        deadline_client: DeadlineClient,
    ) -> None:

        class_worker.stop_worker_service()

        cmd_result = class_worker.send_command(
            "[System.Environment]::SetEnvironmentVariable('DEADLINE_WORKER_WINDOWS_JOB_USER', 'env-override', [System.EnvironmentVariableTarget]::Machine)",
        )

        assert (
            cmd_result.exit_code == 0
        ), f"Failed to set DEADLINE_WORKER_WINDOWS_JOB_USER: {cmd_result}"

        class_worker.start_worker_service()

        job = self.submit_whoami_job(
            "environment override",
            deadline_client,
            deadline_resources.farm,
            deadline_resources.queue_a,
        )

        full_log, job_logs = get_log_from_job(job, deadline_client)
        assert (
            "I am: env-override" in full_log
        ), f"Expected message not found in Job logs. Logs are in CloudWatch log group: {job_logs.log_group_name}"
        assert job.task_run_status == TaskStatus.SUCCEEDED

        cmd_result = class_worker.send_command(
            "[System.Environment]::SetEnvironmentVariable('DEADLINE_WORKER_WINDOWS_JOB_USER', '', [System.EnvironmentVariableTarget]::Machine)",
        )

        assert (
            cmd_result.exit_code == 0
        ), f"Failed to unset DEADLINE_WORKER_WINDOWS_JOB_USER: {cmd_result}"
