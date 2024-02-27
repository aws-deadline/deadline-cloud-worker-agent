# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import json
import os
from datetime import datetime, timedelta, timezone
from logging import getLogger
from typing import Dict, Optional

from botocore.client import BaseClient
from botocore.exceptions import ClientError
from botocore.retries.standard import RetryContext

from openjd.sessions import WindowsSessionUser, BadCredentialsException

from .boto import (
    OTHER_BOTOCORE_CONFIG,
    NoOverflowExponentialBackoff as Backoff,
    Session as BotoSession,
)

logger = getLogger(__name__)


class _WindowsCredentialsCacheEntry:
    def __init__(
        self,
        windows_session_user: Optional[WindowsSessionUser],
        last_fetched_at: datetime,
        last_accessed: datetime,
    ):
        self.windows_session_user = windows_session_user
        self.last_fetched_at = last_fetched_at
        self.last_accessed = last_accessed


class WindowsCredentialsResolver:
    """Class for obtaining Windows job user credentials"""

    _boto_session: BotoSession
    CACHE_EXPIRATION = timedelta(hours=12)
    RETRY_AFTER = timedelta(minutes=5)

    def __init__(
        self,
        boto_session: BotoSession,
    ) -> None:
        if os.name != "nt":
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
            # Can't be retired: ResourceNotFoundException, InvalidRequestException, DecryptionFailure
            except ClientError as e:
                delay = backoff.delay_amount(RetryContext(retry))
                code = e.response.get("Error", {}).get("Code", None)
                if code in ["InternalServiceError", "ThrottlingException"] and retry <= 10:
                    logger.warning(
                        f"GetSecretValue received {code} ({str(e)}). Retrying in {delay} seconds..."
                    )
                else:
                    raise RuntimeError(e) from None
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
        now = datetime.now(tz=timezone.utc)
        # Filter out entries that haven't been accessed in the last CACHE_EXPIRATION hours
        self._user_cache = {
            key: value
            for key, value in self._user_cache.items()
            if now - value.last_accessed < self.CACHE_EXPIRATION
        }

    def get_windows_session_user(
        self, user: str, group: Optional[str], passwordArn: str
    ) -> WindowsSessionUser:
        # Raises ValueError on problems so that the scheduler can cleanly fail the associated jobs
        # Any failure here should be cached so that we wait self.RETRY_AFTER minutes before fetching
        # again

        # Create a composite key using user and arn
        should_fetch = True
        user_key = f"{user}_{passwordArn}"

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
            except Exception:
                logger.error(
                    f"Contents of secret {passwordArn} could not be fetched or were not valid"
                )
            else:
                password = secret.get("password")
                if not password:
                    windows_session_user = None
                    logger.error(
                        f'Contents of secret {passwordArn} did not match the expected format: {"password":"value"}'
                    )
                else:
                    try:
                        # OpenJD will test the ultimate validity of the credentials when creating a WindowsSessionUser
                        windows_session_user = WindowsSessionUser(
                            user=user, group=group, password=password
                        )
                    except BadCredentialsException:
                        windows_session_user = None
                        logger.error(
                            f"Username and/or password within {passwordArn} were not correct"
                        )

        # Cache the WindowsSessionUser object, last fetched at, and last accessed time for future use
        # If the credentials were not valid cache that too to prevent repeated calls to SecretsManager
        self._user_cache[user_key] = _WindowsCredentialsCacheEntry(
            windows_session_user=windows_session_user,
            last_fetched_at=datetime.now(tz=timezone.utc),
            last_accessed=datetime.now(tz=timezone.utc),
        )

        if not windows_session_user:
            raise ValueError(
                f"No valid credentials for {user} available. Credentials will be fetched again {self.RETRY_AFTER.total_seconds()//60} minutes after last fetch"
            )

        return windows_session_user
