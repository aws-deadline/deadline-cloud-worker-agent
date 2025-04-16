# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
This test module contains tests that verify the Worker agent's behavior by creating the Worker with non-default configuration settings,
and making sure that the behavior and outputs of the Worker is that of what we expect.
"""

from time import sleep
from typing import Any, Callable, Optional
import backoff
import boto3
import botocore
import dataclasses
import logging
import os
import re

from deadline_test_fixtures import (
    DeadlineClient,
    DeadlineWorkerConfiguration,
    EC2InstanceWorker,
    OperatingSystem,
)

from e2e.utils import (
    get_shutdown_on_stop_status_from_toml,
    submit_custom_job,
    submit_sleep_job,
)
from e2e.conftest import DeadlineResources

LOG = logging.getLogger(__name__)


class TestWorkerConfiguration:
    def test_worker_requires_no_instance_profile(
        self,
        deadline_resources,
        deadline_client: DeadlineClient,
        worker_config: DeadlineWorkerConfiguration,
        function_worker_factory: Callable[[DeadlineWorkerConfiguration], EC2InstanceWorker],
    ) -> None:
        # Create a EC2 worker with disallow-instance-profiles option for the worker agent
        # Note that the EC2 instance is created with an instance profile, so no job will ever be picked up by this worker
        function_worker_factory(
            dataclasses.replace(
                worker_config,
                disallow_instance_profile="True",
            )
        )

        # Check that the worker agent will eventually shut down
        job = submit_sleep_job(
            "Test Job with worker instance profiles disallowed",
            deadline_client,
            deadline_resources.farm,
            deadline_resources.queue_a,
        )
        try:
            # Wait until the job is finished creation

            @backoff.on_exception(
                exception=Exception,
                wait_gen=backoff.constant,
                max_time=120,
                interval=10,
            )
            def check_job_created() -> None:
                job.refresh_job_info(client=deadline_client)
                assert job.lifecycle_status != "CREATE_IN_PROGRESS"

            check_job_created()

            # Sleep 30 seconds to allow the worker to pick up the job, the worker will not pick up the job due to the instance profile
            sleep(30)

            def check_job_not_picked_up() -> None:
                # Check that the job is never picked up, since the worker will not pick up any jobs due to the instance profile
                job.refresh_job_info(client=deadline_client)
                assert job.task_run_status in ["PENDING", "READY"]

            check_job_not_picked_up()

        finally:
            deadline_client.update_job(
                farmId=job.farm.id,
                queueId=job.queue.id,
                jobId=job.id,
                targetTaskRunStatus="CANCELED",
            )

    def test_worker_local_session_logs_can_be_turned_off(
        self,
        deadline_resources,
        deadline_client: DeadlineClient,
        worker_config: DeadlineWorkerConfiguration,
        function_worker_factory: Callable[[DeadlineWorkerConfiguration], EC2InstanceWorker],
    ) -> None:
        worker_with_local_session_logs_off: EC2InstanceWorker = function_worker_factory(
            dataclasses.replace(worker_config, no_local_session_logs="True")
        )

        # Submit a job and confirm that local session logs do not appear.

        job = submit_custom_job(
            "Test Job with worker local session logs off",
            deadline_client,
            deadline_resources.farm,
            deadline_resources.queue_a,
            run_script="whoami",
        )

        job.wait_until_complete(client=deadline_client)
        sessions: list[dict[str, Any]] = deadline_client.list_sessions(
            farmId=job.farm.id,
            queueId=job.queue.id,
            jobId=job.id,
        ).get("sessions")
        assert sessions

        for session in sessions:
            session_id: str = session["sessionId"]
            session_logs_file_path: str = (
                os.path.join("/var/log/amazon/deadline", job.queue.id, f"{session_id}.log")
                if os.environ["OPERATING_SYSTEM"] == "linux"
                else os.path.join(
                    "C:/ProgramData/Amazon/Deadline/Logs",
                    job.queue.id,
                    f"{session_id}.log",
                )
            )
            if os.environ["OPERATING_SYSTEM"] == "linux":
                # Linux worker
                check_log_exists_result = worker_with_local_session_logs_off.send_command(
                    command=f'[ -e "{session_logs_file_path}" ]'
                )

                assert (
                    check_log_exists_result.exit_code == 1
                )  # The -e command returns 1 on linux if the file does not exist
            else:
                # Windows worker
                check_log_exists_result = worker_with_local_session_logs_off.send_command(
                    command=f'Test-Path -Path "{session_logs_file_path}" -PathType leaf -Credential $Cred'
                )
                assert "false" in check_log_exists_result.stdout.lower(), (
                    f"Checking that local session logs do not exist returned unexpected response: {check_log_exists_result}"
                )

    def test_worker_shuts_down_host_machine_if_configured(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        worker_config: DeadlineWorkerConfiguration,
        function_worker_factory: Callable[[DeadlineWorkerConfiguration], EC2InstanceWorker],
    ) -> None:
        # Test that if worker in an autoscaling fleet is configured to shut down host machine, the host machine is shut down when there are no more jobs available for the fleet.

        # Submit a job
        job = submit_sleep_job(
            "Test Sleep Job with worker shut down host machine",
            deadline_client,
            deadline_resources.farm,
            deadline_resources.scaling_queue,
        )

        worker_in_autoscaling_fleet_with_shut_down: EC2InstanceWorker = function_worker_factory(
            dataclasses.replace(
                worker_config, allow_shutdown=True, fleet=deadline_resources.scaling_fleet
            )
        )
        instance_id: Optional[str] = worker_in_autoscaling_fleet_with_shut_down.instance_id
        assert instance_id

        ec2_client = boto3.client("ec2")
        instance_status = ec2_client.describe_instance_status(
            InstanceIds=[instance_id], IncludeAllInstances=True
        )["InstanceStatuses"][0]["InstanceState"]
        assert instance_status["Name"] == "running"

        assert (
            get_shutdown_on_stop_status_from_toml(worker=worker_in_autoscaling_fleet_with_shut_down)
            == "shutdown_on_stop = true"
        ), "Shutdown on stop should be enabled"

        job.wait_until_complete(client=deadline_client)

        # Check that the worker instance has been shut down
        # Should not take more than 10 minutes to stop
        @backoff.on_exception(
            backoff.constant,
            Exception,
            max_time=600,
            interval=30,
        )
        def check_instance_stopping() -> None:
            instance_status = ec2_client.describe_instance_status(
                InstanceIds=[instance_id], IncludeAllInstances=True
            )["InstanceStatuses"][0]["InstanceState"]

            assert instance_status["Name"] in ["stopped", "stopping"]

        check_instance_stopping()

    def test_session_root_dir(
        self,
        deadline_resources: DeadlineResources,
        deadline_client: DeadlineClient,
        worker_config: DeadlineWorkerConfiguration,
        function_worker_factory: Callable[[DeadlineWorkerConfiguration], EC2InstanceWorker],
        operating_system: OperatingSystem,
    ) -> None:
        """Tests that when a session root directory is configured, that session directories
        are created under this root directory."""

        # GIVEN
        logs_client = boto3.client(
            "logs",
            config=botocore.config.Config(retries={"max_attempts": 10, "mode": "adaptive"}),
        )
        session_root_dir: str
        run_script: str

        if operating_system.is_amazon_linux():
            run_script = """
#!/usr/bin/bash

set -euo pipefail

pwd
""".lstrip()
            session_root_dir = "/mysessionroot"
        elif operating_system.is_windows():
            run_script = """
$ErrorActionPreference = 'Stop'

(Get-Item .).FullName
"""
            session_root_dir = "C:\\Sessions"
        else:
            raise NotImplementedError(f"Test not implemented for {operating_system}")

        function_worker_factory(
            dataclasses.replace(worker_config, session_root_dir=session_root_dir)
        )

        # WHEN
        job = submit_custom_job(
            "Test Job with worker local session logs off",
            deadline_client,
            deadline_resources.farm,
            deadline_resources.queue_a,
            run_script=run_script,
        )

        # THEN
        job.wait_until_complete(client=deadline_client)
        assert job.task_run_status == "SUCCEEDED"
        job.assert_single_task_log_contains(
            deadline_client=deadline_client,
            logs_client=logs_client,
            expected_pattern=re.compile(
                f"^{re.escape(session_root_dir)}[\\\\/]session-[a-f0-9]{{32}}.*$", re.MULTILINE
            ),
            assert_fail_msg=f"Session root directory ({session_root_dir}) not applied",
        )
