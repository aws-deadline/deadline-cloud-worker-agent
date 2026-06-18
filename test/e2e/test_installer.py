# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
This test module contains tests that verify the Installer's behavior by submitting commands to the
Deadline Cloud worker and checking that the result/output of the worker agent is as we expect it.
"""

import pytest
import backoff
import boto3
import dataclasses
import logging
import os

from e2e.utils import (
    get_shutdown_on_stop_status_from_toml,
    job_failure_message,
    submit_custom_job,
)
from e2e.conftest import DeadlineResources
from deadline_test_fixtures import (
    DeadlineClient,
    DeadlineWorkerConfiguration,
    EC2InstanceWorker,
    WindowsInstanceWorkerBase,
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
@pytest.mark.usefixtures("test_job")
class TestWindowsInstaller:
    # Names for tests
    CUSTOM_AGENT_NAME = "custom-agent-worker"
    DEFAULT_AGENT_NAME = "deadline-worker"
    WINDOWS_SECRET = "WindowsPasswordSecret"
    DEFAULT_JOB_USER = "job-user"
    ADMIN_SID = "S-1-5-32-544"

    WHOAMI_COMMAND = (
        f"Write-Host '=== Step: Verify job runs as expected user ==='\n"
        f"$actual = (whoami).split('\\')[1]\n"
        f'Write-Host "Expected: {DEFAULT_JOB_USER}"\n'
        f'Write-Host "Actual:   $actual"\n'
        f"if ($actual -ne '{DEFAULT_JOB_USER}') {{\n"
        f"  Write-Host 'FAIL: Job is not running as expected user'\n"
        f"  exit 1\n"
        f"}}\n"
        f"Write-Host 'PASS: Job is running as expected user'"
    )

    @pytest.fixture(scope="class")
    def worker_config(
        self,
        deadline_resources: DeadlineResources,
        worker_config: DeadlineWorkerConfiguration,
    ) -> DeadlineWorkerConfiguration:
        return dataclasses.replace(
            worker_config,
            agent_user=self.CUSTOM_AGENT_NAME,
            windows_user_secret=self.WINDOWS_SECRET,
            allow_shutdown=False,
            start_service=False,
            fleet=deadline_resources.scaling_fleet,
            # TODO: Temporary workaround due to AWS CLI v2 upgrade causing canary failures when copying over AWS models for deadline
            service_model_path=None,
        )

    @pytest.fixture(scope="class")
    def test_job(
        self,
        deadline_client: DeadlineClient,
        deadline_resources: DeadlineResources,
    ) -> Job:
        return submit_custom_job(
            job_name="Windows: Test Custom Worker Agent Runs Job as User",
            deadline_client=deadline_client,
            farm=deadline_resources.farm,
            queue=deadline_resources.scaling_queue,
            run_script=self.WHOAMI_COMMAND,
            description="Verifies the worker agent runs jobs as the expected job-user. Expected status: SUCCEEDED if whoami returns job-user.",
        )

    @pytest.fixture(scope="class")
    def completed_job(
        self,
        class_worker: EC2InstanceWorker,
        deadline_client: DeadlineClient,
        deadline_resources: DeadlineResources,
        test_job: Job,
    ) -> Job:
        """Fixture that ensures the test job is completed before running tests."""
        LOG.info("Ensuring job is completed before running tests")
        if test_job.task_run_status != TaskStatus.SUCCEEDED:
            LOG.info("Job hasn't been completed, starting the worker service")
            class_worker.start_worker_service()
            test_job.wait_until_complete(client=deadline_client)

        assert test_job.task_run_status == TaskStatus.SUCCEEDED, job_failure_message(
            test_job, deadline_client, deadline_resources.scaling_queue, deadline_resources
        )
        return test_job

    # Shared Class Methods
    @staticmethod
    def check_admin_permissions(
        worker: EC2InstanceWorker,
        username: str,
    ) -> None:
        test_command = "net localgroup administrators"
        cmd_result = worker.send_command(command=test_command)
        assert cmd_result.exit_code == 0, "Failed to execute {test_command} command"
        assert username in cmd_result.stdout, (
            f"User {username} should exist when using command {test_command}"
        )

    @staticmethod
    def check_security_permissions(
        worker: EC2InstanceWorker,
        username: str,
        permissions: list[str],
        should_exist: bool,
    ) -> None:
        for permission in permissions:
            cmd_result = worker.send_command(
                command=f"""
secedit /export /cfg "$env:TEMP\\security.cfg" | Out-Null
Get-Content "$env:TEMP\\security.cfg" | Select-String "{permission}"
"""
            )
            assert cmd_result.exit_code == 0, (
                f"Failed to execute 'Get-Content' for permissions: {permission}"
            )
            # LOG.info(f"Permissions Output: {cmd_result.stdout}")
            if should_exist:
                assert username in cmd_result.stdout, (
                    f"{username} does not have required permissions: {permission}"
                )
            else:
                assert username not in cmd_result.stdout, (
                    f"{username} has unexpected permissions: {permission}"
                )

    # Windows Installer Tests
    def test_custom_worker_agent_permissions(
        self,
        class_worker: EC2InstanceWorker,
    ) -> None:
        try:
            # Check administrator membership
            self.check_admin_permissions(
                worker=class_worker,
                username=self.CUSTOM_AGENT_NAME,
            )

            # Verify additional install service permissions are granted
            self.check_security_permissions(
                worker=class_worker,
                username=self.CUSTOM_AGENT_NAME,
                permissions=["SeServiceLogonRight", "SeAssignPrimary"],
                should_exist=True,
            )

            # Verify Admin have the Shutdown and Increase Quota permissions
            self.check_security_permissions(
                worker=class_worker,
                username=self.ADMIN_SID,
                permissions=["SeIncreaseQuota", "SeShutdownPrivilege"],
                should_exist=True,
            )

            # Verify shutdown permissions in worker.toml
            shutdown_status = get_shutdown_on_stop_status_from_toml(class_worker)
            assert shutdown_status != "shutdown_on_stop = true", (
                "Shutdown on stop should be disabled"
            )
        finally:
            # Cleanup the temp directory
            cmd_result = class_worker.send_command(
                command='Remove-Item "$env:TEMP\\security.cfg" -Force'
            )
            assert cmd_result.exit_code == 0, "Failed to cleanup security configuration file"

    def test_no_default_worker_agent_user(
        self,
        class_worker: EC2InstanceWorker,
    ) -> None:
        # Get all local users
        get_users_cmd_result = class_worker.send_command(
            command="""
Get-LocalUser | Select-Object Name, Enabled | Format-Table -AutoSize
"""
        )
        assert get_users_cmd_result.exit_code == 0, "Failed to get local users"
        assert self.DEFAULT_AGENT_NAME not in get_users_cmd_result.stdout, (
            f"Default worker agent user {self.DEFAULT_AGENT_NAME} should not exist"
        )

    def test_worker_agent_credentials(
        self,
        class_worker: WindowsInstanceWorkerBase,
    ) -> None:
        LOG.info("Verifying the worker agent credentials")

        verify_credentials_command = f"""
Add-Type -AssemblyName System.DirectoryServices.AccountManagement
$contextType = [System.DirectoryServices.AccountManagement.ContextType]::Machine
$principalContext = New-Object System.DirectoryServices.AccountManagement.PrincipalContext($contextType)

$username = "{self.CUSTOM_AGENT_NAME}"

$isValid = $principalContext.ValidateCredentials($username, "$({class_worker.get_windows_user_secret_cmd(secret_id=self.WINDOWS_SECRET)})")

if ($isValid) {{
    Write-Host "Credentials are valid."
}}
"""
        check_creds_result = class_worker.send_command(command=verify_credentials_command)
        assert "Credentials are valid." in check_creds_result.stdout, (
            "Worker agent credentials validation failed."
        )

    def test_custom_agent_runs_job_as_user(
        self,
        class_worker: EC2InstanceWorker,
        deadline_client: DeadlineClient,
        deadline_resources: DeadlineResources,
        test_job: Job,
    ) -> None:
        LOG.info("Start worker service and complete test job")
        class_worker.start_worker_service()
        test_job.wait_until_complete(client=deadline_client)

        assert test_job.task_run_status == TaskStatus.SUCCEEDED, job_failure_message(
            test_job, deadline_client, deadline_resources.scaling_queue, deadline_resources
        )

    def test_deny_shutdown_on_stop(
        self,
        class_worker: EC2InstanceWorker,
        completed_job: Job,
    ) -> None:
        LOG.info("Wait for Worker Service to begin Stopping")
        # This can take over 5 minutes
        class_worker.wait_until_desired_worker_status(
            seconds_between_checks=25, desired_status="STOPPING"
        )

        ec2_client = boto3.client("ec2")

        @backoff.on_exception(
            backoff.constant,
            Exception,
            max_time=30,
            interval=10,
        )
        def check_worker_agent_log() -> None:
            instance_status = ec2_client.describe_instance_status(
                InstanceIds=[class_worker.instance_id], IncludeAllInstances=True
            )["InstanceStatuses"][0]["InstanceState"]
            if instance_status["Name"] != "running":
                LOG.warning(f"Instance is not running, current state: {instance_status['Name']}")
                return  # Exit the function early

            cmd_result = class_worker.send_command(
                command="""
$content = Get-Content "C:\\ProgramData\\Amazon\\Deadline\\Logs\\worker-agent.log"
$pattern = "NOT shutting down the host"
$content | Select-String -Pattern $pattern
"""
            )
            assert cmd_result.exit_code == 0, "Failed to get shutdown status from worker-agent.log"
            assert "NOT shutting down the host" in cmd_result.stdout, (
                "Worker Agent should not be shutting down the host"
            )

        check_worker_agent_log()

        LOG.info("Assert EC2 Instance is still Running")
        instance_status = ec2_client.describe_instance_status(
            InstanceIds=[class_worker.instance_id], IncludeAllInstances=True
        )["InstanceStatuses"][0]["InstanceState"]
        assert instance_status["Name"] == "running"
