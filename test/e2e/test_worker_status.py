# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
This test module contains tests that verify the Worker agent's behavior by starting/stopping the Worker,
and making sure that the status of the Worker is that of what we expect.
"""
from datetime import datetime, timezone
import logging
import os
from time import sleep
import pytest
from deadline_test_fixtures import DeadlineClient, EC2InstanceWorker
import pytest
from e2e.utils import is_worker_started, is_worker_stopped
import backoff

LOG = logging.getLogger(__name__)


class TestWorkerStatus:

    @pytest.mark.skipif(
        os.environ["OPERATING_SYSTEM"] == "windows",
        reason="Linux specific test",
    )
    def test_linux_worker_restarts_process(
        self,
        deadline_resources,
        deadline_client: DeadlineClient,
        class_worker: EC2InstanceWorker,
    ) -> None:
        # Verifies that Linux Worker service restarts the process when we start/stop worker process

        assert class_worker.worker_id is not None  # This fixes linter type mismatch

        assert is_worker_started(
            deadline_client=deadline_client,
            farm_id=deadline_resources.farm.id,
            fleet_id=deadline_resources.fleet.id,
            worker_id=class_worker.worker_id,
        )

        # First check that the worker service is running

        @backoff.on_exception(
            backoff.constant,
            Exception,
            max_time=30,
            interval=2,
        )
        def check_service_is_active() -> None:
            # The service should be active
            service_check_result = class_worker.send_command("systemctl is-active deadline-worker")
            assert (
                service_check_result.exit_code == 0
            ), "Unable to check whether deadline-worker is active"
            assert (
                "inactive" not in service_check_result.stdout
                and "active" in service_check_result.stdout
            ), f"deadline-worker is in unexpected status {service_check_result.stdout}"

        check_service_is_active()

        # Check that the worker process is running

        def check_worker_processes_exist() -> None:
            process_check_result = class_worker.send_command(
                f"pgrep --count --full -u {class_worker.configuration.agent_user} deadline-worker-agent"
            )

            assert (
                process_check_result.exit_code == 0
            ), "deadline-worker-agent process is not running"

        check_worker_processes_exist()

        # Sleep to make sure the worker process will be killed at least one second after the initial start
        sleep(1)

        # Floor to seconds, as service start time is precise to seconds
        time_that_worker_was_killed: datetime = datetime.now(timezone.utc).replace(microsecond=0)

        # Kill the worker process
        pkill_command_result = class_worker.send_command(
            f"sudo pkill -9 --full -u {class_worker.configuration.agent_user} deadline-worker-agent"
        )
        assert (
            pkill_command_result.exit_code == 0
        ), f"Failed to kill the worker agent process: {pkill_command_result}"

        # Wait for the process to be restarted by the service

        check_service_is_active()

        # Check that the service active time is after when we killed the process, since it should have restarted after the kill
        service_active_enter_timestamp_result = class_worker.send_command(
            "systemctl show --property=ActiveEnterTimestamp  deadline-worker"
        )
        assert service_active_enter_timestamp_result.exit_code == 0

        time_service_started: datetime = datetime.strptime(
            service_active_enter_timestamp_result.stdout.split("=")[1].strip(),
            "%a %Y-%m-%d %H:%M:%S %Z",
        ).replace(tzinfo=timezone.utc)

        assert (
            time_service_started >= time_that_worker_was_killed
        ), "Service has not restarted properly as service started before kill command"

        # Check that there are worker processes running
        check_worker_processes_exist()

    @pytest.mark.skipif(
        os.environ["OPERATING_SYSTEM"] == "linux",
        reason="Windows specific test",
    )
    def test_windows_worker_restarts_process(
        self,
        deadline_resources,
        deadline_client: DeadlineClient,
        class_worker: EC2InstanceWorker,
    ) -> None:
        # Verifies that Windows Worker service restarts the process when we start/stop worker process

        assert class_worker.worker_id is not None  # This fixes linter type mismatch

        assert is_worker_started(
            deadline_client=deadline_client,
            farm_id=deadline_resources.farm.id,
            fleet_id=deadline_resources.fleet.id,
            worker_id=class_worker.worker_id,
        )

        # First check that the worker service is running

        @backoff.on_exception(
            backoff.constant,
            Exception,
            max_time=30,
            interval=2,
        )
        def check_service_is_running() -> None:
            # The service should be running
            service_check_result = class_worker.send_command(
                '(Get-Service -Name "DeadlineWorker").Status'
            )
            assert (
                service_check_result.exit_code == 0
            ), "Unable to check whether DeadlineWorker service is running"
            assert (
                "Running" in service_check_result.stdout
            ), f"DeadlineWorker service is in unexpected status {service_check_result.stdout}"

        check_service_is_running()

        # Check that the worker process is running

        def check_worker_processes_exist() -> None:
            process_check_result = class_worker.send_command("Get-Process pythonservice")

            assert process_check_result.exit_code == 0, "Worker agent process is not running"

        check_worker_processes_exist()
        # Kill the worker process
        pkill_command_result = class_worker.send_command("Stop-Process pythonservice")
        assert (
            pkill_command_result.exit_code == 0
        ), f"Failed to kill the worker agent process: {pkill_command_result}"

        # Wait for the process to be restarted by the service

        check_service_is_running()

        check_worker_processes_exist()

    def test_worker_lifecycle_status_is_expected(
        self,
        deadline_resources,
        deadline_client: DeadlineClient,
        class_worker: EC2InstanceWorker,
    ) -> None:
        # Verifies that Worker Status returned by the GetWorker API is as expected when we start/stop workers

        assert class_worker.worker_id is not None  # To fix linter type mismatch

        assert is_worker_started(
            deadline_client=deadline_client,
            farm_id=deadline_resources.farm.id,
            fleet_id=deadline_resources.fleet.id,
            worker_id=class_worker.worker_id,
        )

        class_worker.stop_worker_service()

        assert is_worker_stopped(
            deadline_client=deadline_client,
            farm_id=deadline_resources.farm.id,
            fleet_id=deadline_resources.fleet.id,
            worker_id=class_worker.worker_id,
        )
