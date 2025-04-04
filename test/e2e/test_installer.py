# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
This test module contains tests that verify the Installer's behavior by submitting commands to the
Deadline Cloud worker and checking that the result/output of the worker agent is as we expect it.
"""

import pytest
import boto3
import botocore
import dataclasses
import logging
import os


from typing import Callable
from e2e.utils import submit_custom_job
from e2e.conftest import DeadlineResources
from deadline_test_fixtures import (
    DeadlineClient,
    DeadlineWorkerConfiguration,
    EC2InstanceWorker,
    Job,
    TaskStatus,
)


LOG = logging.getLogger(__name__)


@pytest.mark.skipif(
    os.environ["OPERATING_SYSTEM"] == "windows",
    reason="Linux specific test",
)
class TestInstaller:
    def test_installer_shutdown_permission(
        self,
        session_worker: EC2InstanceWorker,
    ) -> None:
        cmd_result = session_worker.send_command(
            "egrep \
                '^deadline-worker ALL=\\(root\\) NOPASSWD: /usr/sbin/shutdown now$' \
                /etc/sudoers.d/deadline-worker-shutdown"
        )

        assert cmd_result.exit_code == 0, f"Shutdown WA permission do not exist: {cmd_result}"


@pytest.mark.skipif(os.environ["OPERATING_SYSTEM"] == "linux", reason="Windows specific tests")
class TestWindowsInstaller:
    # Names for tests
    CUSTOM_AGENT_NAME = "custom-agent-worker"
    DEFAULT_AGENT_NAME = "deadline-worker"
    DEFAULT_JOB_USER = "job-user"

    WHOAMI_COMMAND = '((whoami).split("\\")[1])'

    # Shared Class Variables
    deadline_client: DeadlineClient
    deadline_resources: DeadlineResources
    class_worker: EC2InstanceWorker

    @pytest.fixture(scope="class", autouse=True)
    def setup_custom_worker(
        self,
        request,
        deadline_client: DeadlineClient,
        deadline_resources: DeadlineResources,
        worker_config: DeadlineWorkerConfiguration,
        class_worker_factory: Callable[[DeadlineWorkerConfiguration], EC2InstanceWorker],
    ):
        cls = request.cls

        cls.deadline_client = deadline_client
        cls.deadline_resources = deadline_resources

        # Creating an EC2 instance with a custom worker agent
        cls.class_worker = class_worker_factory(
            dataclasses.replace(
                worker_config,
                agent_user=self.CUSTOM_AGENT_NAME,
            )
        )

    # Shared Class Methods
    @staticmethod
    def check_admin_permissions(
        session: EC2InstanceWorker,
        username: str,
    ) -> None:
        test_command = "net localgroup administrators"
        cmd_result = session.send_command(command=test_command)
        assert cmd_result.exit_code == 0, "Failed to execute {test_command} command"
        assert username in cmd_result.stdout, (
            f"User {username} should exist when using command {test_command}"
        )

    @staticmethod
    def check_security_permissions(
        session: EC2InstanceWorker,
        username: str,
        permissions: list[str],
        should_exist: bool,
    ) -> None:
        for permission in permissions:
            cmd_result = session.send_command(
                command=f"""
secedit /export /cfg "$env:TEMP\security.cfg" | Out-Null
Get-Content "$env:TEMP\security.cfg" | Select-String "{permission}"
"""
            )
            assert cmd_result.exit_code == 0, (
                f"Failed to execute 'Get-Content' for permissions: {permission}"
            )
            if should_exist:
                assert username in cmd_result.stdout, (
                    f"{username} does not have required permissions: {permission}"
                )
            else:
                assert username not in cmd_result.stdout, (
                    f"{username} has unexpected permissions: {permission}"
                )

    # Windows Installer Tests
    def test_custom_worker_agent_permissions(self) -> None:
        try:
            # Check administrator membership
            self.check_admin_permissions(self.class_worker, self.CUSTOM_AGENT_NAME)

            # Check for verified permissions
            self.check_security_permissions(
                session=self.class_worker,
                username=self.CUSTOM_AGENT_NAME,
                permissions=["SeServiceLogonRight", "SeAssignPrimary"],
                should_exist=True,
            )

            # Check permissions that should not be assigned
            self.check_security_permissions(
                session=self.class_worker,
                username=self.CUSTOM_AGENT_NAME,
                permissions=["SeShutdown", "SeIncreaseQuota"],
                should_exist=False,
            )
        finally:
            # Cleanup the temp directory
            cmd_result = self.class_worker.send_command(
                command='Remove-Item "$env:TEMP\security.cfg" -Force'
            )
            assert cmd_result.exit_code == 0, "Failed to cleanup security configuration file"

    def test_no_default_worker_agent_user(self) -> None:
        # Get all local users
        get_users_cmd_result = self.class_worker.send_command(
            command="""
Get-LocalUser | Select-Object Name, Enabled | Format-Table -AutoSize
"""
        )
        assert get_users_cmd_result.exit_code == 0, "Failed to get local users"
        assert self.DEFAULT_AGENT_NAME not in get_users_cmd_result.stdout, (
            f"Default worker agent user {self.DEFAULT_AGENT_NAME} should not exist"
        )

    def test_custom_agent_runs_job_as_user(self) -> None:
        # Submit a job that prints the job users username
        job_result: Job = submit_custom_job(
            job_name="Test Custom Worker Agent Runs Job as User",
            deadline_client=self.deadline_client,
            farm=self.deadline_resources.farm,
            queue=self.deadline_resources.queue_a,
            run_script=self.WHOAMI_COMMAND,
        )

        job_result.wait_until_complete(client=self.deadline_client)

        job_result.assert_single_task_log_contains(
            deadline_client=self.deadline_client,
            logs_client=boto3.client(
                "logs",
                config=botocore.config.Config(retries={"max_attempts": 10, "mode": "adaptive"}),
            ),
            expected_pattern=f"{self.DEFAULT_JOB_USER}",
        )

        assert job_result.task_run_status == TaskStatus.SUCCEEDED
