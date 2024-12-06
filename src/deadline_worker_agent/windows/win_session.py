# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from functools import cache

import win32process
import win32ts


def _get_current_process_session() -> int:
    """Returns the Windows session ID number for the current process

    Returns
    -------
    int
        The session ID of the current process
    """
    process_id = win32process.GetCurrentProcessId()
    return win32ts.ProcessIdToSessionId(process_id)


@cache
def is_windows_session_zero() -> bool:
    """Returns whether the current Python process is running in Windows session 0.

    Returns
    -------
    bool
        True if the current process is running in Windows session 0
    """
    return _get_current_process_session() == 0
