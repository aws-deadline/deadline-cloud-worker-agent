# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

# This assertion short-circuits mypy from type checking this module on platforms other than Windows
# https://mypy.readthedocs.io/en/stable/common_issues.html#python-version-and-system-platform-checks
import sys

assert sys.platform == "win32"
from ctypes.wintypes import HANDLE as cHANDLE
from pywintypes import error as PyWinError
from win32security import (
    LogonUser,
    LOGON32_LOGON_INTERACTIVE,
    LOGON32_PROVIDER_DEFAULT,
)
import win32net
import win32security
from win32profile import LoadUserProfile, PI_NOUI, UnloadUserProfile
from typing import Any, Optional, TYPE_CHECKING
from logging import getLogger
from datetime import datetime, timezone

import secrets
import string

from .win_session import is_windows_session_zero

if TYPE_CHECKING:
    from _win32typing import PyHKEY, PyHANDLE
else:
    PyHKEY = Any
    PyHANDLE = Any

from openjd.sessions import WindowsSessionUser, BadCredentialsException

logger = getLogger(__name__)


class PasswordResetException(Exception):
    pass


class _WindowsCredentialsCacheEntry:
    def __init__(
        self,
        windows_session_user: Optional[WindowsSessionUser],
        last_fetched_at: Optional[datetime] = None,
        last_accessed: Optional[datetime] = None,
        user_profile: Optional[PyHKEY] = None,
        logon_token: Optional[PyHANDLE] = None,
    ):
        self.windows_session_user = windows_session_user
        self.last_fetched_at = (
            last_fetched_at if last_fetched_at is not None else datetime.now(tz=timezone.utc)
        )
        self.last_accessed = (
            last_accessed if last_accessed is not None else datetime.now(tz=timezone.utc)
        )
        self.user_profile = user_profile
        self.logon_token = logon_token


def get_windows_credentials(username: str, password: str) -> _WindowsCredentialsCacheEntry:
    """
    Returns a WindowsSessionUser object for the given username and password.

    When running in session zero this acquires a logon token and loads the user profile.
    Otherwise, returns a bare WindowsSessionUser object

    Raises:
        BadCredentialsException: If the username and/or password are incorrect
        OSError: If the UserProfile fails to load in session zero
    """
    if not is_windows_session_zero():
        # raises: BadCredentialsException
        return _WindowsCredentialsCacheEntry(
            windows_session_user=WindowsSessionUser(user=username, password=password)
        )
    try:
        # https://timgolden.me.uk/pywin32-docs/win32profile__LoadUserProfile_meth.html
        logon_token = LogonUser(
            Username=username,
            LogonType=LOGON32_LOGON_INTERACTIVE,
            LogonProvider=LOGON32_PROVIDER_DEFAULT,
            Password=password,
            Domain=None,
        )
    except OSError as e:
        raise BadCredentialsException(f'Error logging on as "{username}": {e}')
    else:
        # https://timgolden.me.uk/pywin32-docs/win32profile__LoadUserProfile_meth.html
        # raises: OSError
        user_profile = LoadUserProfile(
            logon_token,
            {
                "UserName": username,
                "Flags": PI_NOUI,
                "ProfilePath": None,
            },
        )
        try:
            windows_session_user = WindowsSessionUser(
                user=username,
                logon_token=cHANDLE(int(logon_token)),
            )
        except OSError as e:
            raise BadCredentialsException(f'Error logging on as "{username}": {e}')
        else:
            return _WindowsCredentialsCacheEntry(
                windows_session_user=windows_session_user,
                user_profile=user_profile,
                logon_token=logon_token,
            )


def reset_user_password(username: str) -> _WindowsCredentialsCacheEntry:
    """
    Change the password of a Windows OS user without requiring the old one.

    :param username: The username of the account.
    :param new_password: The new password for the account.

    Raises:
        PasswordResetException: If the password reset and logon was not successful.
    """
    try:
        user_info = win32net.NetUserGetInfo(None, username, 1)
        user_info["password"] = generate_password(username)
        win32net.NetUserSetInfo(None, username, 1, user_info)
    except PyWinError as e:
        raise PasswordResetException(
            f'Failed to reset password for "{username}". Error: {e.strerror}'
        ) from e
    except Exception as e:
        raise PasswordResetException(
            f'Failed to reset password for "{username}". Error: {e}'
        ) from e
    else:
        logger.info(f'Password for user "{username}" successfully reset')

    try:
        return get_windows_credentials(username, user_info["password"])
    except OSError as e:
        raise PasswordResetException(
            f'Failed to load the user profile for "{username}". Error: {e}'
        ) from e
    except BadCredentialsException as e:
        raise PasswordResetException(f'Failed to logon as "{username}". Error: {e}') from e


def unload_and_close(user_profile: Optional[PyHKEY], logon_token: PyHANDLE):
    """Unloads the user profile and closes the logon token handle"""
    # https://timgolden.me.uk/pywin32-docs/win32profile__UnloadUserProfile_meth.html
    if user_profile is not None:
        UnloadUserProfile(logon_token, user_profile)
    logon_token.Close()


def generate_password(username: str, length: int = 256):
    """
    Generate a password of the given length that:
        - Does not contain any two consecutive characcters from the user's account or full name
        - Contains characters from three of the following four categories:
            - uppercase alphabet character
            - lowercase alphabet characters
            - digits 0-9
            - punctuation characters
    Returns
        str: password
    """
    alphabet = string.ascii_letters + string.digits + string.punctuation
    account_name = username
    full_name = ""
    try:
        user_info = win32net.NetUserGetInfo(None, username, 2)
        account_name = user_info["name"]
        full_name = user_info.get("full_name", "")
    except PyWinError:
        # The user may not exist yet. Just use the username
        pass

    lower_name_pairs = (
        set()
    )  # contains pairs of consecutive characters which will fail windows password validation
    for name in [account_name, full_name]:
        for i in range(len(name) - 1):
            lower_name_pairs.add(name[i : i + 2].lower())

    for i in range(100):
        password = secrets.choice(alphabet)
        while len(password) < length:
            # Use secrets.choice to ensure a secure random selection of characters
            # https://docs.python.org/3/library/secrets.html#recipes-and-best-practices
            choice = secrets.choice(alphabet)
            if (password[-1] + choice).lower() in lower_name_pairs:
                continue
            password += choice
        if _validate_password_chars(password):
            return password
    else:
        raise PasswordResetException(
            "Failed to generate a password which meets security requirements."
        )


def _validate_password_chars(password: str) -> bool:
    """
    Windows requires that three of the following four categories of characters be present in a password:
        - uppercase alphabet character
        - lowercase alphabet characters
        - digits 0-9
        - punctuation characters

    This function returns a boolean indicating whether the given password contains at least three of
    the four above categories of characters.
    """
    upper_in_password = any(char.isupper() for char in password)
    lower_in_password = any(char.islower() for char in password)
    digit_in_password = any(char.isdigit() for char in password)
    special_in_password = any(not char.isalnum() for char in password)
    return sum([upper_in_password, lower_in_password, digit_in_password, special_in_password]) >= 3


def users_equal(user1: str, user2: str) -> bool:
    """
    Returns a boolean indicating whether the two users are the same.

    If both accounts do not exist this falls back to a simple string comparison.
    """
    lookup_failed = False
    try:
        acc_sid1, _, _ = win32security.LookupAccountName(None, user1)
    except PyWinError:
        lookup_failed = True

    try:
        acc_sid2, _, _ = win32security.LookupAccountName(None, user2)
    except PyWinError:
        lookup_failed = True

    if lookup_failed:
        return user1 == user2  # one or both accounts do not exist, fall back to string comparison

    return acc_sid1 == acc_sid2
