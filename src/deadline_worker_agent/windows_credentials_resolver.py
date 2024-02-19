# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from botocore.client import BaseClient
from botocore.exceptions import ClientError
from botocore.retries.standard import RetryContext
from typing import Optional, Dict
from .aws.deadline import DeadlineRequestUnrecoverableError
from .boto import (
    OTHER_BOTOCORE_CONFIG,
    NoOverflowExponentialBackoff as Backoff,
    Session as BotoSession,
)
from openjd.sessions import WindowsSessionUser
from logging import getLogger
import json
import os
from datetime import datetime, timedelta

logger = getLogger(__name__)


class _WindowsCredentialsCacheEntry:
    def __init__(
        self,
        windows_session_user: WindowsSessionUser,
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
                    f"Fetching the secret with secretArn: {secretArn} from Secrets Manager if not in the cache or if it's too old"
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
                    raise DeadlineRequestUnrecoverableError(e) from None
                retry += 1
            except Exception as e:
                # General catch-all for the unexpected, so that the agent can try to handle it gracefully.
                logger.critical(
                    "Unexpected exception calling GetSecretValue. Please report this to the service team.",
                    exc_info=True,
                )
                raise DeadlineRequestUnrecoverableError(e)
        return json.loads(response.get("SecretString"))

    def prune_cache(self):
        now = datetime.utcnow()
        # Filter out entries that haven't been accessed in the last CACHE_EXPIRATION hours
        self._user_cache = {
            key: value
            for key, value in self._user_cache.items()
            if now - value.last_accessed < self.CACHE_EXPIRATION
        }

    def get_windows_session_user(
        self, user: str, group: Optional[str], passwordArn: str
    ) -> WindowsSessionUser:
        # Create a composite key using user and arn
        user_key = f"{user}_{passwordArn}"

        # Prune the cache before fetching or returning the user
        self.prune_cache()

        # Check if the user is already in the cache and if it's less than CACHE_EXPIRATION hours old
        if user_key in self._user_cache:
            cached_data = self._user_cache[user_key]
            windows_session_user = cached_data.windows_session_user

            # Update last accessed time
            self._user_cache[user_key].last_accessed = datetime.utcnow()

            logger.info("Using cached WindowsSessionUser for %s", user)
            return windows_session_user

        # Fetch the secret from Secrets Manager if not in the cache or if it's too old
        secret = self._fetch_secret_from_secrets_manager(passwordArn)

        # Create WindowsSessionUser object
        password = secret.get("password")
        windows_session_user = WindowsSessionUser(user=user, group=group, password=password)

        # Cache the WindowsSessionUser object, last fetched at, and last accessed time for future use
        self._user_cache[user_key] = _WindowsCredentialsCacheEntry(
            windows_session_user=windows_session_user,
            last_fetched_at=datetime.utcnow(),
            last_accessed=datetime.utcnow(),
        )

        return windows_session_user
