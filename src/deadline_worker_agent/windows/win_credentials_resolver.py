# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

# This assertion short-circuits mypy from type checking this module on platforms other than Windows
# https://mypy.readthedocs.io/en/stable/common_issues.html#python-version-and-system-platform-checks
import sys

assert sys.platform == "win32"


import json
import os
from ctypes.wintypes import HANDLE as cHANDLE
from datetime import datetime, timedelta, timezone
from logging import getLogger
from typing import Any, Dict, Optional, TYPE_CHECKING

from botocore.client import BaseClient
from botocore.exceptions import ClientError
from botocore.retries.standard import RetryContext
from openjd.sessions import WindowsSessionUser, BadCredentialsException
from pywintypes import HANDLE as PyHANDLE
from win32security import (
    LogonUser,
    LOGON32_LOGON_INTERACTIVE,
    LOGON32_PROVIDER_DEFAULT,
)
from win32profile import LoadUserProfile, PI_NOUI, UnloadUserProfile

if TYPE_CHECKING:
    from _win32typing import PyHKEY
else:
    PyHKEY = Any

from ..boto import (
    OTHER_BOTOCORE_CONFIG,
    NoOverflowExponentialBackoff as Backoff,
    Session as BotoSession,
)
from . import win_service

logger = getLogger(__name__)


class _WindowsCredentialsCacheEntry:
    def __init__(
        self,
        windows_session_user: Optional[WindowsSessionUser],
        last_fetched_at: datetime,
        last_accessed: datetime,
        user_profile: Optional[PyHKEY] = None,
        logon_token: Optional[PyHANDLE] = None,
    ):
        self.windows_session_user = windows_session_user
        self.last_fetched_at = last_fetched_at
        self.last_accessed = last_accessed
        self.user_profile = user_profile
        self.logon_token = logon_token


class WindowsCredentialsResolver:
    """Class for obtaining Windows job user credentials"""

    _boto_session: BotoSession
    CACHE_EXPIRATION = timedelta(hours=12)
    RETRY_AFTER = timedelta(minutes=5)

    def __init__(
        self,
        boto_session: BotoSession,
    ) -> None:
        if os.name != "nt":  # pragma: no cover
            raise RuntimeError("Windows credentials resolver can only be used on Windows")
        self._boto_session = boto_session
        self._user_cache: Dict[str, _WindowsCredentialsCacheEntry] = {}

    def _get_secrets_manager_client(self) -> BaseClient:
        secrets_manager_client = self._boto_session.client(
            "secretsmanager", config=OTHER_BOTOCORE_CONFIG
        )
        return secrets_manager_client

    def _fetch_secret_from_secrets_manager(self, secretArn: str) -> dict:
        backoff = Backoff(max_backoff=10)
        retry = 0
        while True:
            try:
                secrets_manager_client = self._get_secrets_manager_client()
                logger.info(
                    f"Fetching the secret with secretArn: {secretArn} from Secrets Manager as it's not cached or is too old"
                )
                response = secrets_manager_client.get_secret_value(SecretId=secretArn)  # type: ignore
                break
            # Possible client error exceptions that could happen here are
            # Can be retried: InternalServiceError, ThrottlingException
            # Can't be retried: ResourceNotFoundException, InvalidRequestException, DecryptionFailure, AccessDeniedException
            except ClientError as e:
                delay = backoff.delay_amount(RetryContext(retry))
                code = e.response.get("Error", {}).get("Code", None)
                if code in ["InternalServiceError", "ThrottlingException"] and retry <= 10:
                    logger.warning(
                        f"GetSecretValue received {code} ({str(e)}). Retrying in {delay} seconds..."
                    )
                else:
                    if code in ["AccessDeniedException"]:
                        logger.error(
                            f"Access to secret was denied. Please ensure the resource policy for {secretArn} allows access to the fleet role, and the fleet role is correctly configured"
                        )
                    raise RuntimeError(e)
                retry += 1
            except Exception as e:
                # General catch-all for the unexpected, so that the agent can try to handle it gracefully.
                logger.error(
                    "Unexpected exception calling GetSecretValue. Please report this to the service team.",
                    exc_info=True,
                )
                raise RuntimeError(e)
        try:
            return json.loads(response.get("SecretString"))
        except Exception:
            raise ValueError(f"Contents of secret {secretArn} is not valid JSON.")

    def prune_cache(self):
        # If we are running as a Windows Service, we maintain a logon token for the user and
        # do not need to persist the password nor rotate it.
        if win_service.is_windows_session_zero():
            return

        # Filter out entries that haven't been accessed in the last CACHE_EXPIRATION hours
        now = datetime.now(tz=timezone.utc)
        self._user_cache = {
            key: value
            for key, value in self._user_cache.items()
            if now - value.last_accessed < self.CACHE_EXPIRATION
        }

    def clear(self):
        """Clears all users from the cache and cleans up any open resources"""
        if win_service.is_windows_session_zero():
            for user in self._user_cache.values():
                if user.windows_session_user:
                    logger.info(
                        f"Removing user {user.windows_session_user.user} from the windows credentials resolver cache"
                    )
                    if user.user_profile:
                        # https://timgolden.me.uk/pywin32-docs/win32profile__UnloadUserProfile_meth.html
                        UnloadUserProfile(user.logon_token, user.user_profile)
                    assert user.logon_token is not None
                    user.logon_token.Close()
        self._user_cache.clear()

    @staticmethod
    def _user_cache_key(*, user_name: str, password_arn: str) -> str:
        """Returns the cache key for a given user and password ARN

        This behavior differs in a Windows Service. Through experimentation, we can use the
        LogonUserW and CreateProcessAsUserW win32 APIs. We can cache the Windows logon token
        handle from LogonUserW indefinitely which should remain valid after password rotations.

        Outside a Windows Service, we must use the CreateProcessWithLogonW API which requires
        a username and password. For this reason, our cache key should use the password secret
        ARN since a change of secret may imply a change of password.
        """
        if win_service.is_windows_session_zero():
            return user_name
        else:
            # Create a composite key using user and arn
            return f"{user_name}_{password_arn}"

    def get_windows_session_user(self, user: str, passwordArn: str) -> WindowsSessionUser:
        # Raises ValueError on problems so that the scheduler can cleanly fail the associated jobs
        # Any failure here should be cached so that we wait self.RETRY_AFTER minutes before fetching
        # again

        # Create a composite key using user and arn
        should_fetch = True
        user_key = self._user_cache_key(user_name=user, password_arn=passwordArn)
        windows_session_user: Optional[WindowsSessionUser] = None
        logon_token: Optional[PyHANDLE] = None
        user_profile: Optional[PyHKEY] = None

        # Prune the cache before fetching or returning the user
        self.prune_cache()

        # Check if the user is already in the cache (with either good or bad credentials)
        if user_key in self._user_cache:
            cached_data = self._user_cache[user_key]

            if cached_data.windows_session_user:
                # We have valid credentials for this user
                # Update last accessed time
                self._user_cache[user_key].last_accessed = datetime.now(tz=timezone.utc)

                logger.info("Using cached WindowsSessionUser for %s", user)
                return cached_data.windows_session_user
            else:
                # Only refetch if the last fetch was more than RETRY_AFTER minutes ago
                now = datetime.now(tz=timezone.utc)
                if now - self._user_cache[user_key].last_fetched_at < self.RETRY_AFTER:
                    should_fetch = False

        if should_fetch:
            # Fetch the secret from Secrets Manager
            try:
                secret = self._fetch_secret_from_secrets_manager(passwordArn)
            except Exception as e:
                logger.error(
                    f"Contents of secret {passwordArn} could not be fetched or were not valid: {str(e)}"
                )
            else:
                password = secret.get("password")
                if not password:
                    logger.error(
                        f'Contents of secret {passwordArn} did not match the expected format: {"password":"value"}'
                    )
                else:
                    if win_service.is_windows_session_zero():
                        try:
                            # https://timgolden.me.uk/pywin32-docs/win32profile__LoadUserProfile_meth.html
                            logon_token = LogonUser(
                                Username=user,
                                LogonType=LOGON32_LOGON_INTERACTIVE,
                                LogonProvider=LOGON32_PROVIDER_DEFAULT,
                                Password=password,
                                Domain=None,
                            )
                        except OSError as e:
                            logger.error(
                                f'Error logging on as "{user}", please check that your password within {passwordArn} is correct: {e}'
                            )
                        else:
                            try:
                                # https://timgolden.me.uk/pywin32-docs/win32profile__LoadUserProfile_meth.html
                                user_profile = LoadUserProfile(
                                    logon_token,
                                    {
                                        "UserName": user,
                                        "Flags": PI_NOUI,
                                        "ProfilePath": None,
                                    },
                                )
                            except OSError as e:
                                logger.error(
                                    (
                                        f'Error loading profile for "{user}": {e}\n'
                                        "Please ensure that the Worker Agent is running as a user that is an Administrator, and has user rights to both backup and restore files and directories."
                                    )
                                )
                            else:
                                try:
                                    windows_session_user = WindowsSessionUser(
                                        user=user,
                                        logon_token=cHANDLE(int(logon_token)),
                                    )
                                except OSError as e:
                                    logger.error(f'Error logging on as "{user}": {e}')
                    else:
                        try:
                            # OpenJD will test the ultimate validity of the credentials when creating a WindowsSessionUser
                            windows_session_user = WindowsSessionUser(user=user, password=password)
                        except BadCredentialsException:
                            logger.error(
                                f"Username and/or password within {passwordArn} were not correct"
                            )

        # Cache the WindowsSessionUser object, last fetched at, and last accessed time for future use
        # If the credentials were not valid cache that too to prevent repeated calls to SecretsManager
        self._user_cache[user_key] = _WindowsCredentialsCacheEntry(
            windows_session_user=windows_session_user,
            last_fetched_at=datetime.now(tz=timezone.utc),
            last_accessed=datetime.now(tz=timezone.utc),
            logon_token=logon_token,
            user_profile=user_profile,
        )

        if not windows_session_user:
            raise ValueError(
                f"No valid credentials for {user} available. Credentials will be fetched again {self.RETRY_AFTER.total_seconds()//60} minutes after last fetch"
            )

        return windows_session_user
