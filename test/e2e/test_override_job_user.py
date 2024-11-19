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
@pytest.mark.parametrize("operating_system", ["windows"], indirect=True)
class TestWindowsJobUserOverride:
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

        assert (
            cmd_result.exit_code == 0
        ), f"Setting the job user override via CLI failed: {cmd_result}"

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

        job.wait_until_complete(client=deadline_client, max_retries=20)

        job.assert_single_task_log_contains(
            deadline_client=deadline_client,
            logs_client=boto3.client(
                "logs",
                config=botocore.config.Config(retries={"max_attempts": 10, "mode": "adaptive"}),
            ),
            expected_pattern=r"I am: install-override",
        )

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

        assert (
            cmd_result.exit_code == 0
        ), f"Failed to unset DEADLINE_WORKER_WINDOWS_JOB_USER: {cmd_result}"


@pytest.mark.usefixtures("operating_system")
@pytest.mark.skipif(
    os.environ["OPERATING_SYSTEM"] == "windows",
    reason="Linux specific Job User Override tests",
)
@pytest.mark.parametrize("operating_system", ["linux"], indirect=True)
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
        assert (
            cmd_result.exit_code == 0
        ), f"Setting the job user override via CLI failed: {cmd_result}"

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
            assert (
                cmd_result.exit_code == 0
            ), f"Resetting the job user override via CLI failed: {cmd_result}"

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

        assert (
            cmd_result.exit_code == 0
        ), f"Failed to set DEADLINE_WORKER_POSIX_JOB_USER: {cmd_result}"

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
            assert (
                cmd_result.exit_code == 0
            ), f"Resetting the job user override via CLI failed: {cmd_result}"
            class_worker.send_command("sudo systemctl daemon-reload")
