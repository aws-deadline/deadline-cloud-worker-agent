# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from unittest.mock import patch

import pytest
import sys

if sys.platform != "win32":
    pytest.skip("Windows-specific tests", allow_module_level=True)

from deadline_worker_agent.windows import win_session


def test_get_current_process_session() -> None:
    """Tests that the _get_current_process_session() function uses the expected pywin32 API calls"""

    # GIVEN
    with (
        patch.object(
            win_session.win32process, "GetCurrentProcessId"
        ) as mock_get_current_process_id,
        patch.object(win_session.win32ts, "ProcessIdToSessionId") as mock_process_id_to_session_id,
    ):
        # WHEN
        result = win_session._get_current_process_session()

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
def test_is_windows_session_zero(session_id: int, expected_result: bool) -> None:
    """Tests that the is_windows_session_zero() function returns true iff the return value of
    _get_current_process_session is 0"""

    # GIVEN
    # clear the cache decorator to ensure the function result is not cached between tests
    win_session.is_windows_session_zero.cache_clear()
    with patch.object(win_session, "_get_current_process_session", return_value=session_id):
        # WHEN
        result = win_session.is_windows_session_zero()

    # THEN
    assert result == expected_result


def test_is_windows_session_zero_cached() -> None:
    """Tests that the is_windows_session_zero() function caches the result between calls"""

    # GIVEN
    # clear the cache decorator to ensure the function result is not cached on first run
    win_session.is_windows_session_zero.cache_clear()
    with patch.object(
        win_session, "_get_current_process_session"
    ) as mock_get_current_process_session:
        # We make our mocked _get_current_process_session return different session IDs between calls
        mock_get_current_process_session.side_effect = [0, 1]
        first_result = win_session.is_windows_session_zero()
        # WHEN
        second_result = win_session.is_windows_session_zero()

    # THEN
    assert first_result is True
    assert second_result == first_result
    mock_get_current_process_session.assert_called_once_with()
