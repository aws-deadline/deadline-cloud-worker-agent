# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
This test module contains tests that verify the Worker agent's behavior by creating the Worker with non-default configuration settings,
and making sure that the behavior and outputs of the Worker is that of what we expect.
"""
import logging
import os
from typing import Any, Callable, Optional
import backoff
import boto3
import pytest
from deadline_test_fixtures import DeadlineClient, EC2InstanceWorker, DeadlineWorkerConfiguration
import pytest
import dataclasses
from e2e.utils import submit_custom_job, submit_sleep_job
from e2e.conftest import DeadlineResources

LOG = logging.getLogger(__name__)


@pytest.mark.parametrize("operating_system", [os.environ["OPERATING_SYSTEM"]], indirect=True)
class TestWorkerConfiguration:

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
                assert (
                    "false" in check_log_exists_result.stdout.lower()
                ), f"Checking that local session logs do not exist returned unexpected response: {check_log_exists_result}"

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

        job.wait_until_complete(client=deadline_client)

        # Check that the worker instance has been shut down
        @backoff.on_exception(
            backoff.constant,
            Exception,
            max_time=800,
            interval=30,
        )
        def check_instance_stopping() -> None:
            instance_status = ec2_client.describe_instance_status(
                InstanceIds=[instance_id], IncludeAllInstances=True
            )["InstanceStatuses"][0]["InstanceState"]

            assert instance_status["Name"] in ["stopped", "stopping"]

        check_instance_stopping()
