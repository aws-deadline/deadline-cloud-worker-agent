# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

# This assertion short-circuits mypy from type checking this module on platforms other than Windows
# https://mypy.readthedocs.io/en/stable/common_issues.html#python-version-and-system-platform-checks
import sys

assert sys.platform == "win32"

from ctypes import (
    byref,
    sizeof,
    WinError,
)
from ctypes.wintypes import HANDLE
from enum import Enum

from win32profile import PI_NOUI
from win32security import (
    LOGON32_LOGON_BATCH,
    LOGON32_LOGON_INTERACTIVE,
    LOGON32_LOGON_NETWORK,
    LOGON32_LOGON_NETWORK_CLEARTEXT,
    LOGON32_LOGON_SERVICE,
    LOGON32_PROVIDER_DEFAULT,
)

from .win_api import (
    LoadUserProfileW,
    LogonUserW,
    PROFILEINFO,
)


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
