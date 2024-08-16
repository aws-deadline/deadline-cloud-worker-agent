# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
This test module contains tests that verify the Worker agent's behavior by starting/stopping the Worker,
and making sure that the status of the Worker is that of what we expect.
"""
import logging
import os
import pytest
from typing import Any, Dict
import backoff
from deadline_test_fixtures import DeadlineClient, EC2InstanceWorker
import pytest

LOG = logging.getLogger(__name__)


@pytest.mark.parametrize("operating_system", [os.environ["OPERATING_SYSTEM"]], indirect=True)
class TestWorkerStatus:
    def test_worker_lifecycle_status_is_expected(
        self,
        deadline_resources,
        deadline_client: DeadlineClient,
        function_worker: EC2InstanceWorker,
    ) -> None:
        # Verifies that Worker Status returned by the GetWorker API is as expected when we start/stop workers

        @backoff.on_predicate(
            wait_gen=backoff.constant,
            max_time=60,
            interval=10,
        )
        def is_worker_started() -> bool:
            get_worker_response: Dict[str, Any] = deadline_client.get_worker(
                farmId=deadline_resources.farm.id,
                fleetId=deadline_resources.function_fleet.id,
                workerId=function_worker.worker_id,
            )
            worker_status = get_worker_response["status"]
            if worker_status in ["STARTED", "IDLE"]:
                # Worker should eventually be in either STARTED or IDLE.
                return True
            elif worker_status == "CREATED":
                # This is an acceptable status meaning that the worker is created state has not been updated
                return False
            # Any other status is unexpected, so we should fail
            raise Exception(f"Status {worker_status} is unexpected after worker has just started")

        assert is_worker_started()

        function_worker.stop_worker_service()

        @backoff.on_predicate(
            wait_gen=backoff.constant,
            max_time=180,
            interval=10,
        )
        def is_worker_stopped() -> bool:
            LOG.info(f"Checking whether {function_worker.worker_id} is stopped")
            get_worker_response: Dict[str, Any] = deadline_client.get_worker(
                farmId=deadline_resources.farm.id,
                fleetId=deadline_resources.function_fleet.id,
                workerId=function_worker.worker_id,
            )
            worker_status = get_worker_response["status"]
            return worker_status == "STOPPED"

        assert is_worker_stopped()
