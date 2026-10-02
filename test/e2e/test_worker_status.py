# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
This test module contains tests that verify the Worker agent's behavior by starting/stopping the Worker,
and making sure that the status of the Worker is that of what we expect.
"""

from datetime import datetime, timezone
import logging
import os
import re
from time import sleep
import pytest
from deadline_test_fixtures import DeadlineClient, EC2InstanceWorker, LocalMacWorker
import pytest
from e2e.utils import is_worker_started, is_worker_stopped
import backoff

LOG = logging.getLogger(__name__)


class TestWorkerStatus:
    @pytest.mark.skipif(
        os.environ["OPERATING_SYSTEM"] != "linux",
        reason="Linux (systemd) specific test",
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
            assert service_check_result.exit_code == 0, (
                "Unable to check whether deadline-worker is active"
            )
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

            assert process_check_result.exit_code == 0, (
                "deadline-worker-agent process is not running"
            )

        check_worker_processes_exist()

        # Sleep to make sure the worker process will be killed at least one second after the initial start
        sleep(1)

        # Floor to seconds, as service start time is precise to seconds
        time_that_worker_was_killed: datetime = datetime.now(timezone.utc).replace(microsecond=0)

        # Kill the worker process
        pkill_command_result = class_worker.send_command(
            f"sudo pkill -9 --full -u {class_worker.configuration.agent_user} deadline-worker-agent"
        )
        assert pkill_command_result.exit_code == 0, (
            f"Failed to kill the worker agent process: {pkill_command_result}"
        )

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

        assert time_service_started >= time_that_worker_was_killed, (
            "Service has not restarted properly as service started before kill command"
        )

        # Check that there are worker processes running
        check_worker_processes_exist()

    @pytest.mark.skipif(
        os.environ["OPERATING_SYSTEM"] != "macos",
        reason="macOS (launchd) specific test",
    )
    def test_macos_worker_restarts_process(
        self,
        deadline_resources,
        deadline_client: DeadlineClient,
        class_worker: LocalMacWorker,
    ) -> None:
        # Verifies that launchd restarts the agent process when it is killed, the macOS
        # counterpart to the systemd and Windows-service tests above. The installer writes
        # KeepAlive { SuccessfulExit = false }, which is the Restart=on-failure analog, so a
        # SIGKILL is an unsuccessful exit and launchd respawns the job.

        assert class_worker.worker_id is not None  # This fixes linter type mismatch
        # Bound to a local so the nested closures below do not re-read an attribute that could
        # change, which is how the other tests in this package narrow it for mypy.
        worker_id = class_worker.worker_id
        label = f"system/{LocalMacWorker.LAUNCHD_LABEL}"

        assert is_worker_started(
            deadline_client=deadline_client,
            farm_id=deadline_resources.farm.id,
            fleet_id=deadline_resources.fleet.id,
            worker_id=class_worker.worker_id,
        )

        def running_pid() -> int:
            """Return the pid launchd reports for the daemon, asserting it is running.

            Anchored to a single leading tab: `launchctl print` repeats `state` and other keys
            for nested endpoints and services at deeper indentation, and the first unanchored
            match is not reliably the job's own.
            """
            result = class_worker.send_command(f"launchctl print {label}")
            assert result.exit_code == 0, f"{label} is not loaded: {result}"
            assert "\n\tstate = running" in result.stdout, (
                f"{label} is loaded but not running: {result.stdout}"
            )
            pids = re.findall(r"^\tpid = (\d+)$", result.stdout, re.MULTILINE)
            assert pids, f"launchctl print reported no pid for {label}: {result.stdout}"
            return int(pids[0])

        def check_worker_processes_exist() -> None:
            # BSD pgrep, so no --count/--full: macOS rejects the long options the Linux test
            # uses. -f matches the full command line, -u selects by effective uid, and a
            # zero exit means at least one match, which is all this needs to assert.
            process_check_result = class_worker.send_command(
                f"pgrep -f -u {class_worker.configuration.agent_user} deadline-worker-agent"
            )
            assert process_check_result.exit_code == 0, (
                f"deadline-worker-agent process is not running: {process_check_result}"
            )

        pid_before = running_pid()
        check_worker_processes_exist()

        kill_result = class_worker.send_command(f"kill -9 {pid_before}")
        assert kill_result.exit_code == 0, f"Failed to kill the worker agent process: {kill_result}"

        # A new pid is the evidence that launchd respawned the job rather than that it never
        # died; the Linux test uses the service's ActiveEnterTimestamp for the same purpose.
        #
        # 120s, well above the 30s the other two allow. launchd throttles respawns to one per
        # 10 seconds per job by default, and the agent then has to start and re-register, so a
        # shorter window would make this flaky rather than failing.
        @backoff.on_exception(
            backoff.constant,
            Exception,
            max_time=120,
            interval=5,
        )
        def check_restarted_with_a_new_pid() -> None:
            assert running_pid() != pid_before, (
                f"launchd has not respawned {label}; still pid {pid_before}"
            )

        check_restarted_with_a_new_pid()
        check_worker_processes_exist()

        # Wait for the respawned agent to re-register before leaving. class_worker is class-scoped
        # and the next test in this class stops the service and asserts the worker reports STOPPED;
        # a new pid and a live process are both true within seconds of the respawn, well before the
        # agent has finished registering. Returning at that point let the next test boot the service
        # out mid-registration, so the agent died without ever reporting STOPPED and that test
        # failed on a worker this one had left half-started.
        @backoff.on_exception(
            backoff.constant,
            Exception,
            max_time=120,
            interval=5,
        )
        def wait_until_registered_again() -> None:
            assert is_worker_started(
                deadline_client=deadline_client,
                farm_id=deadline_resources.farm.id,
                fleet_id=deadline_resources.fleet.id,
                worker_id=worker_id,
            )

        wait_until_registered_again()

    @pytest.mark.skipif(
        os.environ["OPERATING_SYSTEM"] != "windows",
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
            assert service_check_result.exit_code == 0, (
                "Unable to check whether DeadlineWorker service is running"
            )
            assert "Running" in service_check_result.stdout, (
                f"DeadlineWorker service is in unexpected status {service_check_result.stdout}"
            )

        check_service_is_running()

        # Check that the worker process is running

        def check_worker_processes_exist() -> None:
            process_check_result = class_worker.send_command("Get-Process pythonservice")

            assert process_check_result.exit_code == 0, "Worker agent process is not running"

        check_worker_processes_exist()
        # Kill the worker process
        pkill_command_result = class_worker.send_command("Stop-Process pythonservice")
        assert pkill_command_result.exit_code == 0, (
            f"Failed to kill the worker agent process: {pkill_command_result}"
        )

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
