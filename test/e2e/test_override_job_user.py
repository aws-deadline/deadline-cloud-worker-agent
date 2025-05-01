# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
This test module contains tests that verify the Worker agent's behavior by submitting jobs to the
Deadline Cloud service and checking that the result/output of the jobs is as we expect it.
"""

import re
import backoff
import boto3
import botocore
import pytest
import os

import logging

from e2e.conftest import DeadlineResources
from deadline_test_fixtures import (
    Job,
    Farm,
    PosixSessionUser,
    Queue,
    TaskStatus,
    DeadlineClient,
    EC2InstanceWorker,
)

LOG = logging.getLogger(__name__)


@pytest.mark.skipif(
    os.environ["OPERATING_SYSTEM"] == "linux",
    reason="Windows Specific Job User Override Tests.",
)
class TestWindowsJobUserOverride:
    @staticmethod
    def submit_whoami_job(
        test_name: str,
        deadline_client: DeadlineClient,
        farm: Farm,
        queue: Queue,
        task_retries: int = 5,
    ) -> Job:
        job = Job.submit(
            client=deadline_client,
            farm=farm,
            queue=queue,
            priority=98,
            max_retries_per_task=task_retries,
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

        job.wait_until_complete(client=deadline_client, max_retries=20)

        job.assert_single_task_log_contains(
            deadline_client=deadline_client,
            logs_client=boto3.client(
                "logs",
                config=botocore.config.Config(retries={"max_attempts": 10, "mode": "adaptive"}),
            ),
            expected_pattern=r"I am: job-user",
        )

        assert job.task_run_status == TaskStatus.SUCCEEDED

    def test_no_jobs_run_as_windows_worker_agent(
        self,
        deadline_client: DeadlineClient,
        deadline_resources: DeadlineResources,
        class_worker: EC2InstanceWorker,
    ) -> None:
        job = self.submit_whoami_job(
            test_name="prevent job run as windows worker agent",
            deadline_client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.jobs_run_as_agent_user_queue,
            task_retries=0,
        )

        job.wait_until_complete(client=deadline_client)

        assert job.task_run_status == TaskStatus.FAILED, (
            "Job should not run as the Windows Worker Agent user."
        )

    def test_config_file_user_override(
        self,
        deadline_resources,
        class_worker: EC2InstanceWorker,
        deadline_client: DeadlineClient,
    ) -> None:
        class_worker.stop_worker_service()

        cmd_result = class_worker.send_command(
            "(Get-Content -Path C:\\ProgramData\\Amazon\\Deadline\\Config\\worker.toml -Raw) -replace '# windows_job_user = \"job-user\"', 'windows_job_user = \"config-override\"' | Set-Content -Path C:\\ProgramData\\Amazon\\Deadline\\Config\\worker.toml"
        )

        assert cmd_result.exit_code == 0, (
            f"Setting the job user override via CLI failed: {cmd_result}"
        )

        class_worker.start_worker_service()

        job = self.submit_whoami_job(
            "config user override",
            deadline_client,
            deadline_resources.farm,
            deadline_resources.queue_a,
        )

        job.wait_until_complete(client=deadline_client, max_retries=20)

        job.assert_single_task_log_contains(
            deadline_client=deadline_client,
            logs_client=boto3.client(
                "logs",
                config=botocore.config.Config(retries={"max_attempts": 10, "mode": "adaptive"}),
            ),
            expected_pattern=r"I am: config-override",
        )

        assert job.task_run_status == TaskStatus.SUCCEEDED

        # reset config file
        cmd_result = class_worker.send_command(
            "(Get-Content -Path C:\\ProgramData\Amazon\\Deadline\\Config\\worker.toml -Raw) -replace 'windows_job_user = \"config-override\"', '# windows_job_user = \"job-user\"' | Set-Content -Path C:\\ProgramData\\Amazon\\Deadline\\Config\\worker.toml"
        )

        assert cmd_result.exit_code == 0, f"Failed to reset config file: {cmd_result}"

    def test_installer_user_override(
        self,
        deadline_resources,
        class_worker: EC2InstanceWorker,
        deadline_client: DeadlineClient,
    ) -> None:
        WINDOWS_JOB_USER = "install-override"
        class_worker.stop_worker_service()

        cmd_result = class_worker.send_command(
            "install-deadline-worker "
            + "-y "
            + f"--farm-id {deadline_resources.farm.id} "
            + f"--fleet-id {deadline_resources.fleet.id} "
            + "--user ssm-user "
            + f"--windows-job-user {WINDOWS_JOB_USER}"
        )

        assert cmd_result.exit_code == 0, (
            f"Failed to install worker with job user override: {cmd_result}"
        )

        class_worker.start_worker_service()

        job = self.submit_whoami_job(
            "installer user override",
            deadline_client,
            deadline_resources.farm,
            deadline_resources.queue_a,
        )

        # This user should also take priority over jobs run as worker agent
        override_worker_agent_job = self.submit_whoami_job(
            test_name="override job run as worker agent",
            deadline_client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.jobs_run_as_agent_user_queue,
        )

        job.wait_until_complete(client=deadline_client, max_retries=20)
        job.assert_single_task_log_contains(
            deadline_client=deadline_client,
            logs_client=boto3.client(
                "logs",
                config=botocore.config.Config(retries={"max_attempts": 10, "mode": "adaptive"}),
            ),
            expected_pattern=rf"I am: {WINDOWS_JOB_USER}",
        )
        assert job.task_run_status == TaskStatus.SUCCEEDED

        override_worker_agent_job.wait_until_complete(client=deadline_client, max_retries=1)
        override_worker_agent_job.assert_single_task_log_contains(
            deadline_client=deadline_client,
            logs_client=boto3.client(
                "logs",
                config=botocore.config.Config(retries={"max_attempts": 10, "mode": "adaptive"}),
            ),
            expected_pattern=rf"I am: {WINDOWS_JOB_USER}",
        )
        assert override_worker_agent_job.task_run_status == TaskStatus.SUCCEEDED

        # reset config file
        cmd_result = class_worker.send_command(
            "(Get-Content -Path C:\\ProgramData\\Amazon\\Deadline\\Config\\worker.toml -Raw) -replace 'windows_job_user = \"installer-override\"', '# windows_job_user = \"job-user\"' | Set-Content -Path C:\\ProgramData\\Amazon\\Deadline\\Config\\worker.toml"
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

        assert cmd_result.exit_code == 0, (
            f"Failed to set DEADLINE_WORKER_WINDOWS_JOB_USER: {cmd_result}"
        )

        class_worker.start_worker_service()

        job = self.submit_whoami_job(
            "environment override",
            deadline_client,
            deadline_resources.farm,
            deadline_resources.queue_a,
        )

        job.wait_until_complete(client=deadline_client, max_retries=20)

        job.assert_single_task_log_contains(
            deadline_client=deadline_client,
            logs_client=boto3.client(
                "logs",
                config=botocore.config.Config(retries={"max_attempts": 10, "mode": "adaptive"}),
            ),
            expected_pattern=r"I am: env-override",
        )

        assert job.task_run_status == TaskStatus.SUCCEEDED

        cmd_result = class_worker.send_command(
            "[System.Environment]::SetEnvironmentVariable('DEADLINE_WORKER_WINDOWS_JOB_USER', '', [System.EnvironmentVariableTarget]::Machine)",
        )

        assert cmd_result.exit_code == 0, (
            f"Failed to unset DEADLINE_WORKER_WINDOWS_JOB_USER: {cmd_result}"
        )


@pytest.mark.skipif(
    os.environ["OPERATING_SYSTEM"] == "windows",
    reason="Linux specific Job User Override tests",
)
class TestLinuxJobUserOverride:
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
                        "name": "Step0",
                        "hostRequirements": {
                            "attributes": [{"name": "attr.worker.os.family", "allOf": ["linux"]}]
                        },
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
        return job

    def test_no_user_override(
        self,
        deadline_resources,
        deadline_client: DeadlineClient,
        class_worker: EC2InstanceWorker,
        posix_job_user: PosixSessionUser,
    ) -> None:
        # WHEN
        job = self.submit_whoami_job(
            "No user override",
            deadline_client,
            deadline_resources.farm,
            deadline_resources.queue_a,
        )

        # THEN
        job.wait_until_complete(client=deadline_client, max_retries=20)

        job.assert_single_task_log_contains(
            deadline_client=deadline_client,
            logs_client=boto3.client(
                "logs",
                config=botocore.config.Config(retries={"max_attempts": 10, "mode": "adaptive"}),
            ),
            expected_pattern=rf"I am: {re.escape(posix_job_user.user)}",
        )

        assert job.task_run_status == TaskStatus.SUCCEEDED

    # DeadlineWorkerConfiguration overwrites the default worker agent user
    # This test verifies that the job can run as the modified worker agent
    def test_job_is_run_as_custom_worker_agent_user(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        class_worker: EC2InstanceWorker,
    ) -> None:
        CUSTOM_AGENT_NAME = "deadline-worker"

        job = self.submit_whoami_job(
            test_name="override linux worker agent",
            deadline_client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.jobs_run_as_agent_user_queue,
        )
        job.wait_until_complete(client=deadline_client)

        job.assert_single_task_log_contains(
            deadline_client=deadline_client,
            logs_client=boto3.client(
                "logs",
                config=botocore.config.Config(retries={"max_attempts": 10, "mode": "adaptive"}),
            ),
            expected_pattern=rf"I am: {CUSTOM_AGENT_NAME}",
        )

    def test_config_file_user_override(
        self,
        deadline_resources,
        class_worker: EC2InstanceWorker,
        posix_config_override_job_user: PosixSessionUser,
        deadline_client: DeadlineClient,
    ) -> None:
        class_worker.stop_worker_service()

        @backoff.on_exception(
            backoff.constant,
            Exception,
            max_time=45,
            interval=5,
        )
        def check_worker_service_stopped() -> None:
            worker_status_cmd_response = class_worker.send_command(
                "systemctl is-active deadline-worker"
            )

            assert worker_status_cmd_response.exit_code != 0
            assert worker_status_cmd_response.stdout != "active"

        check_worker_service_stopped()

        cmd_result = class_worker.send_command(
            command=f'sed -i \'s/# posix_job_user = "user:group"/posix_job_user = "{posix_config_override_job_user.user}:{posix_config_override_job_user.group}"/g\' /etc/amazon/deadline/worker.toml'
        )
        assert cmd_result.exit_code == 0, (
            f"Setting the job user override via CLI failed: {cmd_result}"
        )

        try:
            class_worker.start_worker_service()

            job = self.submit_whoami_job(
                "config user override",
                deadline_client,
                deadline_resources.farm,
                deadline_resources.queue_a,
            )

            job.wait_until_complete(client=deadline_client, max_retries=20)

            job.assert_single_task_log_contains(
                deadline_client=deadline_client,
                logs_client=boto3.client(
                    "logs",
                    config=botocore.config.Config(retries={"max_attempts": 10, "mode": "adaptive"}),
                ),
                expected_pattern=f"I am: {posix_config_override_job_user.user}",
            )

            assert job.task_run_status == TaskStatus.SUCCEEDED
        finally:
            cmd_result = class_worker.send_command(
                command=f'sed -i \'s/posix_job_user = "{posix_config_override_job_user.user}:{posix_config_override_job_user.group}"/# posix_job_user = "user:group"/g\' /etc/amazon/deadline/worker.toml'
            )
            assert cmd_result.exit_code == 0, (
                f"Resetting the job user override via CLI failed: {cmd_result}"
            )

    def test_env_var_user_override(
        self,
        deadline_resources,
        class_worker: EC2InstanceWorker,
        posix_env_override_job_user: PosixSessionUser,
        deadline_client: DeadlineClient,
    ) -> None:
        class_worker.stop_worker_service()

        @backoff.on_exception(
            backoff.constant,
            Exception,
            max_time=45,
            interval=5,
        )
        def check_worker_service_stopped() -> None:
            worker_status_cmd_response = class_worker.send_command(
                "systemctl is-active deadline-worker"
            )

            assert worker_status_cmd_response.exit_code != 0
            assert worker_status_cmd_response.stdout != "active"

        check_worker_service_stopped()

        cmd_result = class_worker.send_command(
            f'echo "Environment=DEADLINE_WORKER_POSIX_JOB_USER={posix_env_override_job_user.user}:{posix_env_override_job_user.group}" >> /etc/systemd/system/deadline-worker.service.d/config.conf',
        )

        assert cmd_result.exit_code == 0, (
            f"Failed to set DEADLINE_WORKER_POSIX_JOB_USER: {cmd_result}"
        )

        class_worker.send_command("systemctl daemon-reload")

        try:
            class_worker.start_worker_service()

            job = self.submit_whoami_job(
                "environment override",
                deadline_client,
                deadline_resources.farm,
                deadline_resources.queue_a,
            )

            job.wait_until_complete(client=deadline_client, max_retries=20)

            job.assert_single_task_log_contains(
                deadline_client=deadline_client,
                logs_client=boto3.client(
                    "logs",
                    config=botocore.config.Config(retries={"max_attempts": 10, "mode": "adaptive"}),
                ),
                expected_pattern=f"I am: {posix_env_override_job_user.user}",
            )

            assert job.task_run_status == TaskStatus.SUCCEEDED
        finally:
            cmd_result = class_worker.send_command(
                f"sed -i '/Environment=DEADLINE_WORKER_POSIX_JOB_USER={posix_env_override_job_user.user}/d' /etc/systemd/system/deadline-worker.service.d/config.conf"
            )
            assert cmd_result.exit_code == 0, (
                f"Resetting the job user override via CLI failed: {cmd_result}"
            )
            class_worker.send_command("sudo systemctl daemon-reload")
