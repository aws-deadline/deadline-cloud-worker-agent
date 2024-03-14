# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from unittest.mock import patch

from win32serviceutil import ServiceFramework
import pytest

from deadline_worker_agent.windows.win_service import WorkerAgentWindowsService
from deadline_worker_agent.windows import win_service


def test_get_current_process_session() -> None:
    """Tests that the _get_current_process_session() function uses the expected pywin32 API calls"""

    # GIVEN
    with (
        patch.object(
            win_service.win32process, "GetCurrentProcessId"
        ) as mock_get_current_process_id,
        patch.object(win_service.win32ts, "ProcessIdToSessionId") as mock_process_id_to_session_id,
    ):
        # WHEN
        result = win_service._get_current_process_session()

    # THEN
    mock_get_current_process_id.assert_called_once_with()
    mock_process_id_to_session_id.assert_called_once_with(mock_get_current_process_id.return_value)
    assert result == mock_process_id_to_session_id.return_value


@pytest.mark.parametrize(
    argnames="session_id,expected_result",
    argvalues=(
        pytest.param(0, True, id="session-zero"),
        pytest.param(1, False, id="session-non-zero"),
    ),
)
def test_is_service(session_id: int, expected_result: bool) -> None:
    """Tests that the _is_service() function returns true iff the return value of
    _get_current_process_session is 0"""

    # GIVEN
    # clear the cache decorator to ensure the function result is not cached between tests
    win_service.is_service.cache_clear()
    with patch.object(win_service, "_get_current_process_session", return_value=session_id):
        # WHEN
        result = win_service.is_service()

    # THEN
    assert result == expected_result


def test_is_service_cached() -> None:
    """Tests that the _is_service() function caches the result between calls"""

    # GIVEN
    # clear the cache decorator to ensure the function result is not cached on first run
    win_service.is_service.cache_clear()
    with patch.object(
        win_service, "_get_current_process_session"
    ) as mock_get_current_process_session:
        # We make our mocked _get_current_process_session return different session IDs between calls
        mock_get_current_process_session.side_effect = [0, 1]
        first_result = win_service.is_service()
        # WHEN
        second_result = win_service.is_service()

    # THEN
    assert first_result is True
    assert second_result == first_result
    mock_get_current_process_session.assert_called_once_with()


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
