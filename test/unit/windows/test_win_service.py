# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
import sys

if sys.platform != "win32":
    pytest.skip("Windows-specific tests", allow_module_level=True)

from win32serviceutil import ServiceFramework

from deadline_worker_agent.windows.win_service import WorkerAgentWindowsService


def test_svc_name() -> None:
    """Tests that the service name (ID used for the service) is "DeadlineWorker" """
    # THEN
    assert WorkerAgentWindowsService._svc_name_ == "DeadlineWorker"


def test_svc_description() -> None:
    """Tests that the description of the service is correct"""
    # THEN
    assert WorkerAgentWindowsService._svc_description_ == (
        "Service hosting the AWS Deadline Cloud Worker Agent. Connects to AWS "
        "Deadline Cloud and runs jobs as a worker in a fleet."
    )


def test_display_name() -> None:
    """Tests that the display name of the service is "AWS Deadline Cloud Worker Agent" """
    # THEN
    assert WorkerAgentWindowsService._svc_display_name_ == "AWS Deadline Cloud Worker"


def test_parent_class() -> None:
    """Tests that the WorkerAgentWindowsService subclasses win32serviceutil.ServiceFramework"""

    # THEN
    assert issubclass(WorkerAgentWindowsService, ServiceFramework)
