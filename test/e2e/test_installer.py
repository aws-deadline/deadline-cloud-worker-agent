# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
This test module contains tests that verify the Installer's behavior by submitting commands to the
Deadline Cloud worker and checking that the result/output of the worker agent is as we expect it.
"""

import pytest
import backoff
import boto3
import botocore
import dataclasses
import logging
import os

from e2e.utils import (
    get_shutdown_on_stop_status_from_toml,
    submit_custom_job,
)
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
@pytest.mark.usefixtures("test_job")
class TestWindowsInstaller:
    # Names for tests
    CUSTOM_AGENT_NAME = "custom-agent-worker"
    DEFAULT_AGENT_NAME = "deadline-worker"
    DEFAULT_JOB_USER = "job-user"
    ADMIN_SID = "S-1-5-32-544"

    WHOAMI_COMMAND = "Write-Output \"Jobs Run As: $((whoami).split('\\')[1])\""

    @pytest.fixture(scope="class")
    def worker_config(
        self,
        deadline_resources: DeadlineResources,
        worker_config: DeadlineWorkerConfiguration,
    ) -> DeadlineWorkerConfiguration:
        return dataclasses.replace(
            worker_config,
            agent_user=self.CUSTOM_AGENT_NAME,
            allow_shutdown=False,
            start_service=False,
            fleet=deadline_resources.scaling_fleet,
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
        )

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

    def test_custom_agent_runs_job_as_user(
        self,
        class_worker: EC2InstanceWorker,
        deadline_client: DeadlineClient,
        test_job: Job,
    ) -> None:
        LOG.info("Start worker service and complete test job")
        class_worker.start_worker_service()
        test_job.wait_until_complete(client=deadline_client)

        LOG.info("Assert the queue job user is correct")
        test_job.assert_single_task_log_contains(
            deadline_client=deadline_client,
            logs_client=boto3.client(
                "logs",
                config=botocore.config.Config(retries={"max_attempts": 10, "mode": "adaptive"}),
            ),
            expected_pattern=rf"Jobs Run As: {self.DEFAULT_JOB_USER}",
        )

        assert test_job.task_run_status == TaskStatus.SUCCEEDED

    def test_deny_shutdown_on_stop(
        self,
        class_worker: EC2InstanceWorker,
        test_job: Job,
    ) -> None:
        # Check if the job has run for the set of tests
        if test_job.task_run_status != TaskStatus.SUCCEEDED:
            LOG.info("Job hasn't been completed, start the worker service")
            class_worker.start_worker_service()

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
