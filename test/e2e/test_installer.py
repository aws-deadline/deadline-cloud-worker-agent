# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
This test module contains tests that verify the Installer's behavior by submitting commands to the
Deadline Cloud worker and checking that the result/output of the worker agent is as we expect it.
"""

import pytest
import logging
import os
import backoff

from typing import Any
from e2e.utils import submit_custom_job
from deadline_test_fixtures import (
    DeadlineClient,
    DeadlineResources,
    EC2InstanceWorker,
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
                '^deadline-worker ALL=\(root\) NOPASSWD: /usr/sbin/shutdown now$' \
                /etc/sudoers.d/deadline-worker-shutdown"
        )

        assert cmd_result.exit_code == 0, f"Shutdown WA permission do not exist: {cmd_result}"

@pytest.mark.skipif(
    os.environ["OPERATING_SYSTEM"] == "linux",
    reason="Windows specific tests"
)
class TestWindowsInstaller:
    def test_windows_installer_default_agent_user(
            self,
            deadline_resources: DeadlineResources,
            deadline_client: DeadlineClient,
            session_worker: EC2InstanceWorker,
    ) -> None:
        # Length of time for the job to run
        job_time = 5
        # Default Windows Worker Agent username
        defaultName = "deadline-worker"

        def countdown_script(seconds: int = 1):
            return f"""
for ($i = {seconds}; $i -gt 0; $i--) {{
  Write-Output "Seconds remaining $i"
  Start-Sleep -Seconds 1
}}
Write-Output "Done!"
"""

        def check_admin_permissions(
                session: EC2InstanceWorker,
                username: str,
        ) -> None:
            test_command = "net localgroup administrators"
            cmd_result = session.send_command(
                command = test_command
            )
            assert cmd_result.exit_code == 0, (
                "Failed to execute {test_command} command"
            )
            assert username in cmd_result.stdout, (
                f"User {username} should exist when using command {test_command}"
            )
        
        def check_security_permissions(
                session: EC2InstanceWorker,
                username: str,
                permissions: list[str],
                should_exist: bool = True,
        ) -> None:
            permissions_str = '", "'.join(permissions)
            cmd_result = session.send_command(
                command=f"""
secedit /export /cfg "$env:TEMP\security.cfg" | Out-Null
Get-Content "$env:TEMP\security.cfg" | Select-String "{permissions_str}"
"""
            )
            assert cmd_result.exit_code == 0, (
                f"Failed to execute 'Get-Content' for permissions: {permissions}"
            )
            if should_exist:
                assert username in cmd_result.stdout, f"{username} does not have required permissions: {permissions}"
            else:
                assert username not in cmd_result.stdout, f"{username} has unexpected permissions: {permissions}"

        # Job to ensure the instance is fully running
        submit_custom_job(
            "Test Job to start the instance",
            deadline_client,
            deadline_resources.farm,
            deadline_resources.queue_a,
            run_script=countdown_script(),
        ).wait_until_complete(client=deadline_client)

        retain_instance_job = submit_custom_job(
            "Test Job to retain the instance",
            deadline_client,
            deadline_resources.farm,
            deadline_resources.queue_a,
            run_script=countdown_script(job_time),
        )

        try:
            # Check administrator membership
            check_admin_permissions(session_worker, defaultName)

            # Check for verified permissions
            check_security_permissions(
                session=session_worker, 
                username=defaultName, 
                permissions=["SeServiceLogonRight"], 
                should_exist=True)
            
            check_security_permissions(
                session=session_worker, 
                username=defaultName, 
                permissions=["SeAssignPrimary"], 
                should_exist=True)
            
            # Check permissions that should not be assigned
            check_security_permissions(
                session=session_worker, 
                username=defaultName, 
                permissions=["SeShutdown", "SeIncreaseQuota"], 
                should_exist=False)
        finally:
            # Cleanup the temp directory
            cmd_result = session_worker.send_command(
                command='Remove-Item "$env:TEMP\security.cfg" -Force'
            )
            assert cmd_result.exit_code == 0, "Failed to cleanup security configuration file"

        retain_instance_job.wait_until_complete(client=deadline_client)    
