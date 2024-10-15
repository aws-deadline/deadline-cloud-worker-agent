# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
This test module contains tests that verify the Worker agent's behavior by starting/stopping the Worker,
and making sure that the status of the Worker is that of what we expect.
"""
import logging
import os
import pytest
from deadline_test_fixtures import DeadlineClient, EC2InstanceWorker
import pytest
from e2e.utils import is_worker_started, is_worker_stopped

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

        assert function_worker.worker_id is not None  # To fix linter type mismatch

        assert is_worker_started(
            deadline_client=deadline_client,
            farm_id=deadline_resources.farm.id,
            fleet_id=deadline_resources.fleet.id,
            worker_id=function_worker.worker_id,
        )

        function_worker.stop_worker_service()

        assert is_worker_stopped(
            deadline_client=deadline_client,
            farm_id=deadline_resources.farm.id,
            fleet_id=deadline_resources.fleet.id,
            worker_id=function_worker.worker_id,
        )
