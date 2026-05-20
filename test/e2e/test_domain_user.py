# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
Tests that verify the Worker agent supports domain users for both the agent user
and the queue user (job run-as user).

These tests deploy a Windows Server instance, promote it to a domain controller,
create domain users, and verify jobs run as the expected domain user.

The agent user format (DDL vs UPN) is parameterized — each format gets its own
install cycle on the same instance.
"""

import boto3
import pytest
import os
import time
import logging
from typing import Generator

from e2e.conftest import DeadlineResources
from e2e.utils import job_failure_message
from deadline_test_fixtures import (
    Job,
    Farm,
    Queue,
    TaskStatus,
    DeadlineClient,
    EC2InstanceWorker,
)

LOG = logging.getLogger(__name__)

DOMAIN_NAME = "test.local"
DOMAIN_NETBIOS = "TEST"
DOMAIN_ADMIN_PASSWORD = "D0m@inP@ss!"
DOMAIN_AGENT_USER = "domain-agent"
DOMAIN_JOB_USER = "domain-job-user"
WINDOWS_PASSWORD_SECRET = "WindowsPasswordSecret"

AGENT_USER_DDL = f"{DOMAIN_NETBIOS}\\{DOMAIN_AGENT_USER}"
AGENT_USER_UPN = f"{DOMAIN_AGENT_USER}@{DOMAIN_NAME}"


def wait_for_ssm_online(ssm_client, instance_id: str, timeout: int = 600) -> None:
    """Wait for SSM agent to report the instance as online."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            response = ssm_client.describe_instance_information(
                Filters=[{"Key": "InstanceIds", "Values": [instance_id]}]
            )
            instances = response.get("InstanceInformationList", [])
            if instances and instances[0].get("PingStatus") == "Online":
                return
        except Exception:
            pass
        time.sleep(10)
    raise TimeoutError(f"Instance {instance_id} did not become SSM-online within {timeout}s")


def promote_to_domain_controller(worker: EC2InstanceWorker) -> None:
    """Promote the instance to a domain controller and wait for reboot."""
    LOG.info("Installing AD Domain Services feature...")
    cmd_result = worker.send_command(
        "Install-WindowsFeature AD-Domain-Services -IncludeManagementTools"
    )
    assert cmd_result.exit_code == 0, f"Failed to install AD DS: {cmd_result}"

    LOG.info("Promoting to domain controller (will trigger reboot)...")
    try:
        worker.send_command(
            "Install-ADDSForest "
            f"-DomainName '{DOMAIN_NAME}' "
            f"-DomainNetbiosName '{DOMAIN_NETBIOS}' "
            f"-SafeModeAdministratorPassword (ConvertTo-SecureString '{DOMAIN_ADMIN_PASSWORD}' -AsPlainText -Force) "
            "-InstallDns "
            "-Force"
        )
    except Exception as e:
        LOG.info(f"DC promotion command timed out as expected (reboot): {e}")


def create_domain_users(worker: EC2InstanceWorker, region: str) -> None:
    """Create domain users using the password from the existing WindowsPasswordSecret."""
    LOG.info("Creating domain users...")
    cmd_result = worker.send_command(
        f"$secret = (aws secretsmanager get-secret-value --secret-id {WINDOWS_PASSWORD_SECRET} --query SecretString --output text --region {region} | ConvertFrom-Json).password; "
        "Import-Module ActiveDirectory; "
        f"New-ADUser -Name '{DOMAIN_AGENT_USER}' "
        f"-SamAccountName '{DOMAIN_AGENT_USER}' "
        f"-UserPrincipalName '{DOMAIN_AGENT_USER}@{DOMAIN_NAME}' "
        "-AccountPassword (ConvertTo-SecureString $secret -AsPlainText -Force) "
        "-Enabled $true -PasswordNeverExpires $true; "
        f"New-ADUser -Name '{DOMAIN_JOB_USER}' "
        f"-SamAccountName '{DOMAIN_JOB_USER}' "
        f"-UserPrincipalName '{DOMAIN_JOB_USER}@{DOMAIN_NAME}' "
        "-AccountPassword (ConvertTo-SecureString $secret -AsPlainText -Force) "
        "-Enabled $true -PasswordNeverExpires $true; "
        f"Add-ADGroupMember -Identity 'Administrators' -Members '{DOMAIN_AGENT_USER}'"
    )
    assert cmd_result.exit_code == 0, f"Failed to create domain users: {cmd_result}"
    LOG.info("Domain users created successfully")


def grant_user_rights(worker: EC2InstanceWorker) -> None:
    """Grant all required user rights after DC promotion."""
    # Agent user: service logon, quota, assign primary token
    cmd_result = worker.send_command(
        "$account = New-Object System.Security.Principal.NTAccount("
        f"'{DOMAIN_NETBIOS}\\{DOMAIN_AGENT_USER}'); "
        "$sid = $account.Translate([System.Security.Principal.SecurityIdentifier]).Value; "
        "$tmp = [System.IO.Path]::GetTempFileName(); "
        "secedit /export /cfg $tmp /quiet; "
        "$cfg = Get-Content $tmp; "
        "$cfg = $cfg -replace '(SeServiceLogonRight = .*)', \"`$1,*$sid\"; "
        "$cfg = $cfg -replace '(SeIncreaseQuotaPrivilege = .*)', \"`$1,*$sid\"; "
        "$cfg = $cfg -replace '(SeAssignPrimaryTokenPrivilege = .*)', \"`$1,*$sid\"; "
        "$cfg | Set-Content $tmp; "
        "secedit /configure /db C:\\Windows\\security\\local.sdb /cfg $tmp /quiet; "
        "Remove-Item $tmp"
    )
    LOG.info(f"Grant agent user rights: exit_code={cmd_result.exit_code}")

    # Domain job user + local job-user: interactive and batch logon
    cmd_result = worker.send_command(
        "$domainJobSid = (New-Object System.Security.Principal.NTAccount("
        f"'{DOMAIN_NETBIOS}\\{DOMAIN_JOB_USER}')).Translate("
        "[System.Security.Principal.SecurityIdentifier]).Value; "
        "$localJobSid = (New-Object System.Security.Principal.NTAccount("
        "'job-user')).Translate([System.Security.Principal.SecurityIdentifier]).Value; "
        "$tmp = [System.IO.Path]::GetTempFileName(); "
        "secedit /export /cfg $tmp /quiet; "
        "$cfg = Get-Content $tmp; "
        "$cfg = $cfg -replace '(SeInteractiveLogonRight = .*)', \"`$1,*$domainJobSid,*$localJobSid\"; "
        "$cfg = $cfg -replace '(SeBatchLogonRight = .*)', \"`$1,*$domainJobSid,*$localJobSid\"; "
        "$cfg | Set-Content $tmp; "
        "secedit /configure /db C:\\Windows\\security\\local.sdb /cfg $tmp /quiet; "
        "Remove-Item $tmp"
    )
    LOG.info(f"Grant job user rights: exit_code={cmd_result.exit_code}")


def install_agent_as(
    worker: EC2InstanceWorker, deadline_resources: DeadlineResources, user: str, region: str
) -> None:
    """Install the worker agent as the specified user."""
    LOG.info(f"Installing worker agent as '{user}'...")
    worker.stop_worker_service()

    cmd_result = worker.send_command(
        f"$password = (aws secretsmanager get-secret-value --secret-id {WINDOWS_PASSWORD_SECRET} --query SecretString --output text --region {region} | ConvertFrom-Json).password; "
        "install-deadline-worker "
        "-y "
        f"--farm-id {deadline_resources.farm.id} "
        f"--fleet-id {deadline_resources.fleet.id} "
        f"--user '{user}' "
        "--password $password "
        "--grant-required-access "
        "--start"
    )
    assert cmd_result.exit_code == 0, f"Failed to install worker as '{user}': {cmd_result}"
    LOG.info(f"Worker agent installed as '{user}'")


@pytest.mark.skipif(
    os.environ.get("OPERATING_SYSTEM") != "windows",
    reason="Domain user tests are Windows-only",
)
@pytest.mark.parametrize(
    "agent_user_format",
    [AGENT_USER_DDL, AGENT_USER_UPN],
    ids=["ddl", "upn"],
    scope="class",
)
class TestDomainUser:
    """Tests that verify domain user support for both agent and queue users."""

    @pytest.fixture(scope="class")
    def domain_controller(
        self,
        deadline_resources: DeadlineResources,
        class_worker: EC2InstanceWorker,
        region: str,
    ) -> EC2InstanceWorker:
        """Promotes the instance to a DC and creates domain users. Shared across all tests."""
        worker = class_worker

        promote_to_domain_controller(worker)

        LOG.info("Waiting for instance to come back online after DC promotion...")
        time.sleep(120)
        wait_for_ssm_online(boto3.client("ssm"), worker.instance_id, timeout=600)

        LOG.info("Verifying AD Domain Services are ready...")
        cmd_result = worker.send_command("Import-Module ActiveDirectory; Get-ADDomain")
        assert cmd_result.exit_code == 0, f"AD not ready after promotion: {cmd_result}"

        # Install NVIDIA GRID drivers if on a GPU instance
        if os.environ.get("WORKER_INSTANCE_TYPE", "").startswith("g"):
            LOG.info("Installing NVIDIA GRID drivers...")
            cmd_result = worker.send_command(
                "aws s3 cp s3://ec2-windows-nvidia-drivers/latest/ C:\\Temp\\ --recursive --exclude '*' --include '*.exe' --region us-east-1; "
                "$installer = Get-ChildItem C:\\Temp\\*.exe | Select-Object -First 1; "
                "Start-Process $installer.FullName -ArgumentList '/s /noreboot' -Wait; "
                "nvidia-smi"
            )
            LOG.info(f"GPU driver install result: exit_code={cmd_result.exit_code}")

        create_domain_users(worker, region)
        grant_user_rights(worker)

        return worker

    _current_format = None

    @pytest.fixture(autouse=True)
    def installed_agent(
        self,
        agent_user_format: str,
        deadline_resources: DeadlineResources,
        domain_controller: EC2InstanceWorker,
        region: str,
    ) -> EC2InstanceWorker:
        """Installs the agent as the parameterized user format. Skips if already installed."""
        if TestDomainUser._current_format != agent_user_format:
            install_agent_as(domain_controller, deadline_resources, agent_user_format, region)
            TestDomainUser._current_format = agent_user_format
        return domain_controller

    @pytest.fixture(scope="class")
    def domain_job_queue(
        self,
        deadline_resources: DeadlineResources,
        domain_controller: EC2InstanceWorker,
        region: str,
    ) -> Generator[Queue, None, None]:
        """Create a queue configured to run jobs as the domain job user."""
        deadline_client = boto3.client("deadline", region_name=region)
        secretsmanager_client = boto3.client("secretsmanager", region_name=region)

        secret = secretsmanager_client.describe_secret(SecretId=WINDOWS_PASSWORD_SECRET)
        secret_arn = secret["ARN"]

        queue_role_arn = os.environ["SESSION_ROLE"]
        job_attachments_bucket = os.environ["JOB_ATTACHMENTS_BUCKET"]
        response = deadline_client.create_queue(
            farmId=deadline_resources.farm.id,
            displayName="DomainJobUserTestQueue",
            roleArn=queue_role_arn,
            jobAttachmentSettings={
                "s3BucketName": job_attachments_bucket,
                "rootPrefix": "Deadline",
            },
            allowedStorageProfileIds=[
                deadline_resources.windows_fleet_storage_profile_id,
            ],
            jobRunAsUser={
                "runAs": "QUEUE_CONFIGURED_USER",
                "windows": {
                    "user": f"{DOMAIN_NETBIOS}\\{DOMAIN_JOB_USER}",
                    "passwordArn": secret_arn,
                },
            },
        )
        queue_id = response["queueId"]
        LOG.info(f"Created domain job user queue: {queue_id}")

        deadline_client.create_queue_fleet_association(
            farmId=deadline_resources.farm.id,
            queueId=queue_id,
            fleetId=deadline_resources.fleet.id,
        )

        yield Queue(id=queue_id, farm=deadline_resources.farm)

        # Teardown: stop association, delete association, delete queue
        try:
            deadline_client.update_queue_fleet_association(
                farmId=deadline_resources.farm.id,
                queueId=queue_id,
                fleetId=deadline_resources.fleet.id,
                status="STOP_SCHEDULING_AND_CANCEL_TASKS",
            )
            for _ in range(30):
                resp = deadline_client.get_queue_fleet_association(
                    farmId=deadline_resources.farm.id,
                    queueId=queue_id,
                    fleetId=deadline_resources.fleet.id,
                )
                if resp.get("status") == "STOPPED":
                    break
                time.sleep(2)
            deadline_client.delete_queue_fleet_association(
                farmId=deadline_resources.farm.id,
                queueId=queue_id,
                fleetId=deadline_resources.fleet.id,
            )
        except Exception as e:
            LOG.warning(f"Failed to clean up queue-fleet association: {e}")
        try:
            deadline_client.delete_queue(farmId=deadline_resources.farm.id, queueId=queue_id)
        except Exception as e:
            LOG.warning(f"Failed to delete queue {queue_id}: {e}")

    @staticmethod
    def submit_whoami_job(
        test_name: str,
        deadline_client: DeadlineClient,
        farm: Farm,
        queue: Queue,
        expected_user: str,
    ) -> Job:
        from e2e.utils import submit_job_from_bundle

        bundle_path = os.path.join(
            os.path.dirname(__file__), "job_bundles", "domain_user_whoami"
        )
        return submit_job_from_bundle(
            deadline_client=deadline_client,
            farm=farm,
            queue=queue,
            bundle_path=bundle_path,
            job_parameters=[{"name": "ExpectedUser", "value": expected_user}],
            max_retries_per_task=0,
        )

    def test_job_runs_as_local_queue_user(
        self,
        deadline_resources: DeadlineResources,
        domain_controller: EC2InstanceWorker,
        deadline_client: DeadlineClient,
    ) -> None:
        """Agent (DDL or UPN) can run a job as the local queue-configured user."""
        job = self.submit_whoami_job(
            "local queue user",
            deadline_client,
            deadline_resources.farm,
            deadline_resources.queue_a,
            expected_user=r"*\job-user",
        )

        job.wait_until_complete(client=deadline_client, max_retries=20)
        assert job.task_run_status == TaskStatus.SUCCEEDED, job_failure_message(
            job, deadline_client, deadline_resources.queue_a, deadline_resources
        )

    def test_job_runs_as_domain_queue_user(
        self,
        deadline_resources: DeadlineResources,
        domain_controller: EC2InstanceWorker,
        domain_job_queue: Queue,
        deadline_client: DeadlineClient,
    ) -> None:
        """Agent (DDL or UPN) can run a job as a domain queue-configured user."""
        job = self.submit_whoami_job(
            "domain queue user",
            deadline_client,
            deadline_resources.farm,
            domain_job_queue,
            expected_user=f"{DOMAIN_NETBIOS}\\{DOMAIN_JOB_USER}",
        )

        job.wait_until_complete(client=deadline_client, max_retries=20)
        assert job.task_run_status == TaskStatus.SUCCEEDED, job_failure_message(
            job, deadline_client, domain_job_queue, deadline_resources
        )

    def test_service_identity(
        self,
        agent_user_format: str,
        domain_controller: EC2InstanceWorker,
    ) -> None:
        """Verify the service is configured to run as the domain agent user in the expected format."""
        cmd_result = domain_controller.send_command(
            "sc.exe qc DeadlineWorker | Select-String SERVICE_START_NAME"
        )
        assert cmd_result.exit_code == 0
        assert agent_user_format.lower() in cmd_result.stdout.lower(), (
            f"Expected service configured as '{agent_user_format}', got: {cmd_result.stdout}"
        )


