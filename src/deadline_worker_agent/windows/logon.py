# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

# This assertion short-circuits mypy from type checking this module on platforms other than Windows
# https://mypy.readthedocs.io/en/stable/common_issues.html#python-version-and-system-platform-checks
import sys

assert sys.platform == "win32"

from contextlib import contextmanager
from ctypes import (
    byref,
    sizeof,
    WinError,
)
from ctypes.wintypes import HANDLE
from enum import Enum
from typing import Generator

from .win_api import (
    AdjustTokenPrivileges,
    CloseHandle,
    GetCurrentProcess,
    LoadUserProfileW,
    LogonUserW,
    LOGON32_PROVIDER_DEFAULT,
    LOGON32_LOGON_INTERACTIVE,
    LOGON32_LOGON_NETWORK,
    LOGON32_LOGON_BATCH,
    LOGON32_LOGON_SERVICE,
    LOGON32_LOGON_NETWORK_CLEARTEXT,
    LookupPrivilegeValueW,
    OpenProcessToken,
    PI_NOUI,
    PROFILEINFO,
    SE_BACKUP_NAME,
    SE_RESTORE_NAME,
    SE_PRIVILEGE_ENABLED,
    SE_PRIVILEGE_REMOVED,
    TOKEN_ADJUST_PRIVILEGES,
    TOKEN_PRIVILEGES,
)


def adjust_privileges(
    *,
    privilege_constants: list[str],
    enable: bool,
) -> None:
    """
    Adjusts the privileges of THIS PROCESS.

    Args:
        privilege_constants: List of the privilege constants to enable/disable.
            See: https://learn.microsoft.com/en-us/windows/win32/secauthz/privilege-constants
        enable: True if we are to enable the privileges, False if we're to disable them

    Raises:
        OSError - If there is an error modifying the privileges.
    """
    proc_token = HANDLE(0)
    if not OpenProcessToken(GetCurrentProcess(), TOKEN_ADJUST_PRIVILEGES, byref(proc_token)):
        raise WinError()

    token_privileges = TOKEN_PRIVILEGES.allocate(len(privilege_constants))
    privs_array = token_privileges.privileges_array()
    for i, name in enumerate(privilege_constants):
        if not LookupPrivilegeValueW(None, name, byref(privs_array[i].Luid)):
            CloseHandle(proc_token)
            raise WinError()
        privs_array[i].Attributes = SE_PRIVILEGE_ENABLED if enable else SE_PRIVILEGE_REMOVED

    if not AdjustTokenPrivileges(
        proc_token, False, byref(token_privileges), sizeof(token_privileges), None, None
    ):
        CloseHandle(proc_token)
        raise WinError()

    CloseHandle(proc_token)


@contextmanager
def grant_privilege_context(privilege_constants: list[str]) -> Generator[None, None, None]:
    """
    A context wrapper around adjust_privileges().
    This will enable the given privileges when entered, and disable them when exited.
    """
    try:
        adjust_privileges(privilege_constants=privilege_constants, enable=True)
        yield
    finally:
        adjust_privileges(privilege_constants=privilege_constants, enable=False)


def load_user_profile(
    *,
    user: str,
    logon_token: HANDLE,
) -> PROFILEINFO:
    """
    Loads the profile for the given user.

    Args:
        user: The username of the user whose profile we're loading
        logon_token: "Token for the user, which is returned by the LogonUser,
            CreateRestrictedToken, DuplicateToken, OpenProcessToken, or OpenThreadToken
            function. The token must have TOKEN_QUERY, TOKEN_IMPERSONATE, and TOKEN_DUPLICATE access."
            Reference: https://learn.microsoft.com/en-us/windows/win32/api/userenv/nf-userenv-loaduserprofilew

    Returns:
        The PROFILEINFO for the loaded profile

    Note:
        The caller MUST UnloadUserProfile the return.hProfile when done with the logon_token, and before
        closing the token.
    """
    # TODO - Handle Roaming Profiles
    # As per https://learn.microsoft.com/en-us/windows/win32/api/userenv/nf-userenv-loaduserprofilew#remarks
    # "Services and applications that call LoadUserProfile should check to see if the user has a roaming profile. ..."

    # "The calling process must have the SE_RESTORE_NAME and SE_BACKUP_NAME privileges"
    with grant_privilege_context([SE_BACKUP_NAME, SE_RESTORE_NAME]):
        # Note: As per https://learn.microsoft.com/en-us/windows/win32/api/userenv/nf-userenv-loaduserprofilew#remarks
        # the caller must *be* an Administrator or the LocalSystem account.
        pi = PROFILEINFO()
        pi.dwSize = sizeof(PROFILEINFO)
        pi.lpUserName = user
        pi.dwFlags = PI_NOUI  # Prevents displaying of messages

        if not LoadUserProfileW(logon_token, byref(pi)):
            raise WinError()

        return pi


class LogonType(Enum):
    INTERACTIVE = LOGON32_LOGON_INTERACTIVE
    NETWORK = LOGON32_LOGON_NETWORK
    BATCH = LOGON32_LOGON_BATCH
    SERVICE = LOGON32_LOGON_SERVICE
    NETWORK_CLEARTEXT = LOGON32_LOGON_NETWORK_CLEARTEXT


def logon_user(
    *,
    username: str,
    password: str,
    logon_type: LogonType = LogonType.NETWORK_CLEARTEXT,
) -> HANDLE:
    """
    Attempt to logon as the given username & password.
    Return a HANDLE to a logon_token.

    Note:
      The caller *MUST* call CloseHandle on the returned value when done with it.
      Handles are not automatically garbage collected.

    Raises:
        OSError - If an error is encountered.
    """
    hToken = HANDLE(0)
    if not LogonUserW(
        username,
        None,  # TODO - domain handling??
        password,
        logon_type.value,
        LOGON32_PROVIDER_DEFAULT,
        byref(hToken),
    ):
        raise WinError()

    return hToken
