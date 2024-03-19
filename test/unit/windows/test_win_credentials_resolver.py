# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.


from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
from typing import Generator
import botocore
import sys

from openjd.sessions import WindowsSessionUser, BadCredentialsException
from pytest import fixture
import pytest

# This if is required for two purposes:
# 1.  It short-circuits mypy from type checking this module on platforms other than Windows
#     https://mypy.readthedocs.io/en/stable/common_issues.html#python-version-and-system-platform-checks
# 2.  It causes the tests to not be discovered/ran on non-Windows platforms
if sys.platform == "win32":
    import deadline_worker_agent.windows.win_credentials_resolver as credentials_mod

    class TestWindowsCredentialsResolver:
        @fixture(autouse=True)
        def now(self) -> datetime:
            return datetime(2000, 1, 1)

        @fixture(autouse=True)
        def datetime_mock(self, now: datetime) -> Generator[MagicMock, None, None]:
            with patch.object(credentials_mod, "datetime") as mock:
                mock.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
                mock.fromtimestamp.side_effect = lambda *args, **kwargs: datetime.fromtimestamp(
                    *args, **kwargs
                )
                mock.now.return_value = now
                yield mock

        def test_prune_cache(self, datetime_mock: MagicMock):
            # GIVEN
            mock_boto_session = MagicMock()
            now = datetime(2023, 1, 1, 12, 0, 0)
            datetime_mock.now.return_value = now
            resolver = credentials_mod.WindowsCredentialsResolver(mock_boto_session)

            # Add a user to the cache that should be pruned
            expired_user = WindowsSessionUser(user="expired_user", password="fake_password")
            expired_entry = credentials_mod._WindowsCredentialsCacheEntry(
                windows_session_user=expired_user,
                last_fetched_at=now - timedelta(hours=13),
                last_accessed=now - timedelta(hours=13),
            )
            resolver._user_cache["expired_user_arn"] = expired_entry

            # Add a user to the cache that should be kept
            valid_user = WindowsSessionUser(user="valid_user", password="fake_password")
            valid_entry = credentials_mod._WindowsCredentialsCacheEntry(
                windows_session_user=valid_user,
                last_fetched_at=now - timedelta(hours=11),
                last_accessed=now - timedelta(hours=11),
            )
            resolver._user_cache["valid_user_arn"] = valid_entry

            # WHEN
            resolver.prune_cache()

            # THEN
            assert len(resolver._user_cache) == 1
            assert "valid_user_arn" in resolver._user_cache
            assert "expired_user_arn" not in resolver._user_cache

        @patch(
            "deadline_worker_agent.windows.win_credentials_resolver.WindowsCredentialsResolver._fetch_secret_from_secrets_manager"
        )
        def test_get_windows_session_user_non_cached(self, fetch_secret_mock, datetime_mock):
            # GIVEN
            mock_boto_session = MagicMock()
            now = datetime(2023, 1, 1, 12, 0, 0)
            datetime_mock.now.return_value = now
            resolver = credentials_mod.WindowsCredentialsResolver(mock_boto_session)
            secret_data = {"password": "fake_password"}
            fetch_secret_mock.return_value = secret_data
            user = "new_user"
            password_arn = "new_password_arn"

            # WHEN
            result = resolver.get_windows_session_user(user, password_arn)

            # THEN
            fetch_secret_mock.assert_called_once_with(password_arn)
            assert isinstance(result, WindowsSessionUser)
            assert result.user == user
            assert result.password == secret_data["password"]

        @patch(
            "deadline_worker_agent.windows.win_credentials_resolver.WindowsCredentialsResolver._fetch_secret_from_secrets_manager"
        )
        def test_get_windows_session_user_no_password_in_secret(
            self, fetch_secret_mock, datetime_mock
        ):
            # GIVEN
            mock_boto_session = MagicMock()
            now = datetime(2023, 1, 1, 12, 0, 0)
            datetime_mock.now.return_value = now
            resolver = credentials_mod.WindowsCredentialsResolver(mock_boto_session)
            secret_data = {"something-other-than-password": "fake_password"}
            fetch_secret_mock.return_value = secret_data
            user = "new_user"
            password_arn = "new_password_arn"

            # WHEN
            with pytest.raises(ValueError):
                resolver.get_windows_session_user(user, password_arn)

            # THEN
            fetch_secret_mock.assert_called_once_with(password_arn)

        @patch(
            "deadline_worker_agent.windows.win_credentials_resolver.WindowsCredentialsResolver._fetch_secret_from_secrets_manager"
        )
        def test_get_windows_session_user_cached(self, fetch_secret_mock, datetime_mock):
            # GIVEN
            mock_boto_session = MagicMock()
            now = datetime(2023, 1, 1, 12, 0, 0)
            datetime_mock.now.return_value = now
            resolver = credentials_mod.WindowsCredentialsResolver(mock_boto_session)
            password_arn = "password_arn"
            user = "user"
            user_obj = WindowsSessionUser(user=user, password="fake_cached_password")
            cached_entry = credentials_mod._WindowsCredentialsCacheEntry(
                windows_session_user=user_obj,
                last_fetched_at=now - timedelta(hours=11),
                last_accessed=now - timedelta(hours=11),
            )
            resolver._user_cache[f"{user}_{password_arn}"] = cached_entry
            secret_data = {"password": "fake_new_password"}
            fetch_secret_mock.return_value = secret_data

            # WHEN
            result = resolver.get_windows_session_user(user, password_arn)

            # THEN
            fetch_secret_mock.assert_not_called()
            assert isinstance(result, WindowsSessionUser)
            assert result.user == user
            assert result.password == "fake_cached_password"

        @patch(
            "deadline_worker_agent.windows.win_credentials_resolver.WindowsCredentialsResolver._fetch_secret_from_secrets_manager"
        )
        def test_get_windows_session_user_invalid_credentials(
            self, fetch_secret_mock, datetime_mock
        ):
            # GIVEN
            mock_boto_session = MagicMock()
            now = datetime(2023, 1, 1, 12, 0, 0)
            datetime_mock.now.return_value = now
            resolver = credentials_mod.WindowsCredentialsResolver(mock_boto_session)
            secret_data = {"password": "fake_password"}
            fetch_secret_mock.return_value = secret_data
            user = "new_user"
            password_arn = "new_password_arn"

            with patch(
                "deadline_worker_agent.windows.win_credentials_resolver.WindowsSessionUser",
                side_effect=BadCredentialsException("Invalid credentials"),
            ):
                # WHEN
                with pytest.raises(ValueError):
                    resolver.get_windows_session_user(user, password_arn)
                    assert (
                        resolver._user_cache[f"{user}_{password_arn}"].windows_session_user is None
                    )

        @pytest.mark.parametrize(
            "exception_code",
            [
                "ResourceNotFoundException",
                "InvalidRequestException",
                "DecryptionFailure",
                "AccessDeniedException",
            ],
        )
        @patch(
            "deadline_worker_agent.windows.win_credentials_resolver.WindowsCredentialsResolver._get_secrets_manager_client"
        )
        def test_fetch_secrets_manager_non_retriable_exception(
            self, secrets_manager_client_mock: MagicMock, exception_code: str
        ):
            # GIVEN
            mock_boto_session = MagicMock()
            resolver = credentials_mod.WindowsCredentialsResolver(mock_boto_session)
            password_arn = "password_arn"
            exc = botocore.exceptions.ClientError(
                {"Error": {"Code": exception_code, "Message": "A message"}}, "GetSecretValue"
            )
            secrets_manager_client_mock.side_effect = exc

            # THEN
            with pytest.raises(RuntimeError):
                resolver._fetch_secret_from_secrets_manager(password_arn)

        @pytest.mark.parametrize(
            "exception_code",
            [
                "InternalServiceError",
                "ThrottlingException",
            ],
        )
        @patch(
            "deadline_worker_agent.windows.win_credentials_resolver.WindowsCredentialsResolver._get_secrets_manager_client"
        )
        def test_fetch_secrets_manager_retriable_exception(
            self, secrets_manager_client_mock: MagicMock, exception_code: str
        ):
            # GIVEN
            mock_boto_session = MagicMock()
            resolver = credentials_mod.WindowsCredentialsResolver(mock_boto_session)
            password_arn = "password_arn"
            exc = botocore.exceptions.ClientError(
                {"Error": {"Code": exception_code, "Message": "A message"}}, "GetSecretValue"
            )
            secrets_manager_client_mock.side_effect = exc

            # THEN
            # Assert raising DeadlineRequestUnrecoverableError after 10 retries
            with pytest.raises(RuntimeError):
                resolver._fetch_secret_from_secrets_manager(password_arn)
                assert secrets_manager_client_mock.call_count == 10

        @patch(
            "deadline_worker_agent.windows.win_credentials_resolver.WindowsCredentialsResolver._get_secrets_manager_client"
        )
        def test_fetch_secrets_manager_non_json_secret_exception(
            self,
            secrets_manager_client_mock: MagicMock,
        ):
            # GIVEN
            mock_boto_session = MagicMock()
            resolver = credentials_mod.WindowsCredentialsResolver(mock_boto_session)
            password_arn = "password_arn"
            secrets_manager_client_mock.get_secret_value.return_value = {
                "SecretString": "_a string_"
            }

            # THEN
            with pytest.raises(ValueError):
                resolver._fetch_secret_from_secrets_manager(password_arn)
