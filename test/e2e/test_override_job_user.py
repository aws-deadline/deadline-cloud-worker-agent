# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
This test module contains tests that verify the Worker agent's behavior by submitting jobs to the
Deadline Cloud service and checking that the result/output of the jobs is as we expect it.
"""

import backoff
import pytest
import os
from flaky import flaky

import logging

from e2e.conftest import DeadlineResources
from e2e.utils import (
    is_worker_started,
    is_worker_stopped,
    job_failure_message,
    windows_replace_and_verify,
)
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
        expected_user: str,
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
                "description": f"Verifies job runs as '{expected_user}'. Expected status: SUCCEEDED",
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
                            "embeddedFiles": [
                                {
                                    "name": "runScript",
                                    "type": "TEXT",
                                    "filename": "runScript.ps1",
                                    "data": "\n".join(
                                        [
                                            f'Write-Output "=== Whoami Test: {test_name} ==="',
                                            f'Write-Output "Expected user: {expected_user}"',
                                            'Write-Output ""',
                                            'Write-Output "--- Step 1: Running whoami ---"',
                                            "$actual = (whoami).split('\\')[-1]",
                                            'Write-Output "Actual user: $actual"',
                                            'Write-Output ""',
                                            'Write-Output "--- Step 2: Validating user identity ---"',
                                            f"if ($actual -ne '{expected_user}') {{",
                                            f"  Write-Output \"FAIL: expected '{expected_user}' but got '$actual'\"",
                                            "  exit 1",
                                            "}",
                                            f"Write-Output \"PASS: Running as expected user '{expected_user}'\"",
                                            'Write-Output ""',
                                            'Write-Output "=== All checks passed ==="',
                                        ]
                                    ),
                                },
                            ],
                            "actions": {
                                "onRun": {
                                    "command": "powershell",
                                    "args": ["-File", "{{Task.File.runScript}}"],
                                }
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
        class_worker: EC2InstanceWorker,
        deadline_client: DeadlineClient,
    ) -> None:
        job = self.submit_whoami_job(
            "no user override",
            deadline_client,
            deadline_resources.farm,
            deadline_resources.queue_a,
            expected_user="job-user",
        )

        job.wait_until_complete(client=deadline_client, max_retries=20)
        assert job.task_run_status == TaskStatus.SUCCEEDED, job_failure_message(
            job, deadline_client, deadline_resources.queue_a, deadline_resources
        )

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
            expected_user="should-not-matter",
            task_retries=0,
        )

        job.wait_until_complete(client=deadline_client)

        assert job.task_run_status == TaskStatus.FAILED, (
            "Job should not run as the Windows Worker Agent user.\n"
            + job_failure_message(
                job,
                deadline_client,
                deadline_resources.jobs_run_as_agent_user_queue,
                deadline_resources,
            )
        )

    @flaky(max_runs=3, min_passes=1)
    def test_config_file_user_override(
        self,
        deadline_resources,
        class_worker: EC2InstanceWorker,
        deadline_client: DeadlineClient,
    ) -> None:
        # Wait for worker to reach STARTED/IDLE via Deadline API before stopping.
        # After tests 1/2, the worker needs time to finish session cleanup and
        # return to an idle state. Polling the API is deterministic and avoids
        # the race condition of stopping mid-transition.
        assert class_worker.worker_id is not None
        assert is_worker_started(
            deadline_client=deadline_client,
            farm_id=deadline_resources.farm.id,
            fleet_id=deadline_resources.fleet.id,
            worker_id=class_worker.worker_id,
        ), f"Worker {class_worker.worker_id} did not reach STARTED/IDLE before stop within 180s"

        class_worker.stop_worker_service()
        assert is_worker_stopped(
            deadline_client=deadline_client,
            farm_id=deadline_resources.farm.id,
            fleet_id=deadline_resources.fleet.id,
            worker_id=class_worker.worker_id,
        ), f"Worker {class_worker.worker_id} did not transition to STOPPED within 180s"

        windows_replace_and_verify(
            worker=class_worker,
            file_path="C:\\ProgramData\\Amazon\\Deadline\\Config\\worker.toml",
            old_pattern='# windows_job_user = "job-user"',
            new_pattern='windows_job_user = "config-override"',
        )

        class_worker.start_worker_service()

        job = self.submit_whoami_job(
            "config user override",
            deadline_client,
            deadline_resources.farm,
            deadline_resources.queue_a,
            expected_user="config-override",
        )

        job.wait_until_complete(client=deadline_client, max_retries=20)
        assert job.task_run_status == TaskStatus.SUCCEEDED, job_failure_message(
            job, deadline_client, deadline_resources.queue_a, deadline_resources
        )

        # reset config file
        windows_replace_and_verify(
            worker=class_worker,
            file_path="C:\\ProgramData\\Amazon\\Deadline\\Config\\worker.toml",
            old_pattern='windows_job_user = "config-override"',
            new_pattern='# windows_job_user = "job-user"',
        )

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
            expected_user=WINDOWS_JOB_USER,
        )

        job.wait_until_complete(client=deadline_client, max_retries=20)
        assert job.task_run_status == TaskStatus.SUCCEEDED, job_failure_message(
            job, deadline_client, deadline_resources.queue_a, deadline_resources
        )

        # This user should also take priority over jobs run as worker agent
        override_worker_agent_job = self.submit_whoami_job(
            test_name="override job run as worker agent",
            deadline_client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.jobs_run_as_agent_user_queue,
            expected_user=WINDOWS_JOB_USER,
        )

        override_worker_agent_job.wait_until_complete(client=deadline_client, max_retries=20)
        assert override_worker_agent_job.task_run_status == TaskStatus.SUCCEEDED, (
            job_failure_message(
                override_worker_agent_job,
                deadline_client,
                deadline_resources.jobs_run_as_agent_user_queue,
                deadline_resources,
            )
        )

        # reset config file
        windows_replace_and_verify(
            worker=class_worker,
            file_path="C:\\ProgramData\\Amazon\\Deadline\\Config\\worker.toml",
            old_pattern=f'windows_job_user = "{WINDOWS_JOB_USER}"',
            new_pattern='# windows_job_user = "job-user"',
        )

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
            expected_user="env-override",
        )

        job.wait_until_complete(client=deadline_client, max_retries=20)
        assert job.task_run_status == TaskStatus.SUCCEEDED, job_failure_message(
            job, deadline_client, deadline_resources.queue_a, deadline_resources
        )

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
        test_name: str,
        deadline_client: DeadlineClient,
        farm: Farm,
        queue: Queue,
        expected_user: str,
    ) -> Job:
        job = Job.submit(
            client=deadline_client,
            farm=farm,
            queue=queue,
            priority=98,
            template={
                "specificationVersion": "jobtemplate-2023-09",
                "name": f"whoami {test_name}",
                "description": f"Verifies job runs as '{expected_user}'. Expected status: SUCCEEDED",
                "steps": [
                    {
                        "name": "Step0",
                        "hostRequirements": {
                            "attributes": [{"name": "attr.worker.os.family", "allOf": ["linux"]}]
                        },
                        "script": {
                            "embeddedFiles": [
                                {
                                    "name": "runScript",
                                    "type": "TEXT",
                                    "runnable": True,
                                    "filename": "runScript.sh",
                                    "data": "\n".join(
                                        [
                                            "#!/bin/bash",
                                            "set -e",
                                            f'echo "=== Whoami Test: {test_name} ==="',
                                            f'echo "Expected user: {expected_user}"',
                                            'echo ""',
                                            'echo "--- Step 1: Running whoami ---"',
                                            "actual=$(whoami)",
                                            'echo "Actual user: $actual"',
                                            'echo ""',
                                            'echo "--- Step 2: Validating user identity ---"',
                                            f'if [ "$actual" != "{expected_user}" ]; then',
                                            f"  echo \"FAIL: expected '{expected_user}' but got '$actual'\"",
                                            "  exit 1",
                                            "fi",
                                            f"echo \"PASS: Running as expected user '{expected_user}'\"",
                                            'echo ""',
                                            'echo "=== All checks passed ==="',
                                        ]
                                    ),
                                },
                            ],
                            "actions": {
                                "onRun": {
                                    "command": "{{Task.File.runScript}}",
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
        job = self.submit_whoami_job(
            "No user override",
            deadline_client,
            deadline_resources.farm,
            deadline_resources.queue_a,
            expected_user=posix_job_user.user,
        )

        job.wait_until_complete(client=deadline_client, max_retries=20)
        assert job.task_run_status == TaskStatus.SUCCEEDED, job_failure_message(
            job, deadline_client, deadline_resources.queue_a, deadline_resources
        )

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
            expected_user=CUSTOM_AGENT_NAME,
        )
        job.wait_until_complete(client=deadline_client)
        assert job.task_run_status == TaskStatus.SUCCEEDED, job_failure_message(
            job,
            deadline_client,
            deadline_resources.jobs_run_as_agent_user_queue,
            deadline_resources,
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
                expected_user=posix_config_override_job_user.user,
            )

            job.wait_until_complete(client=deadline_client, max_retries=20)
            assert job.task_run_status == TaskStatus.SUCCEEDED, job_failure_message(
                job, deadline_client, deadline_resources.queue_a, deadline_resources
            )
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
                expected_user=posix_env_override_job_user.user,
            )

            job.wait_until_complete(client=deadline_client, max_retries=20)
            assert job.task_run_status == TaskStatus.SUCCEEDED, job_failure_message(
                job, deadline_client, deadline_resources.queue_a, deadline_resources
            )
        finally:
            cmd_result = class_worker.send_command(
                f"sed -i '/Environment=DEADLINE_WORKER_POSIX_JOB_USER={posix_env_override_job_user.user}/d' /etc/systemd/system/deadline-worker.service.d/config.conf"
            )
            assert cmd_result.exit_code == 0, (
                f"Resetting the job user override via CLI failed: {cmd_result}"
            )
            class_worker.send_command("sudo systemctl daemon-reload")
