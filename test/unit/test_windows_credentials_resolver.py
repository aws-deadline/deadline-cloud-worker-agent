# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from unittest.mock import patch
from datetime import datetime, timedelta
from openjd.sessions import WindowsSessionUser
from unittest.mock import patch, MagicMock
from typing import Generator
from pytest import fixture, mark
import os

import deadline_worker_agent.windows_credentials_resolver as credentials_mod


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
            mock.utcnow.return_value = now
            yield mock

    @mark.skipif(os.name != "nt", reason="Windows-only test.")
    def test_prune_cache(self, datetime_mock: MagicMock):
        # GIVEN
        mock_boto_session = MagicMock()
        now = datetime(2023, 1, 1, 12, 0, 0)
        datetime_mock.utcnow.return_value = now
        resolver = credentials_mod.WindowsCredentialsResolver(mock_boto_session)

        # Add a user to the cache that should be pruned
        expired_user = WindowsSessionUser(
            user="expired_user", group="fake_group", password="fake_password"
        )
        expired_entry = credentials_mod._WindowsCredentialsCacheEntry(
            windows_session_user=expired_user,
            last_fetched_at=now - timedelta(hours=13),
            last_accessed=now - timedelta(hours=13),
        )
        resolver._user_cache["expired_user_arn"] = expired_entry

        # Add a user to the cache that should be kept
        valid_user = WindowsSessionUser(
            user="valid_user", group="fake_group", password="fake_password"
        )
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

    @mark.skipif(os.name != "nt", reason="Windows-only test.")
    @patch(
        "deadline_worker_agent.windows_credentials_resolver.WindowsCredentialsResolver._fetch_secret_from_secrets_manager"
    )
    def test_get_windows_session_user_non_cached(self, fetch_secret_mock, datetime_mock):
        # GIVEN
        mock_boto_session = MagicMock()
        now = datetime(2023, 1, 1, 12, 0, 0)
        datetime_mock.utcnow.return_value = now
        resolver = credentials_mod.WindowsCredentialsResolver(mock_boto_session)
        secret_data = {"password": "fake_password"}
        fetch_secret_mock.return_value = secret_data
        user = "new_user"
        group = "new_group"
        password_arn = "new_password_arn"

        # WHEN
        result = resolver.get_windows_session_user(user, group, password_arn)

        # THEN
        fetch_secret_mock.assert_called_once_with(password_arn)
        assert isinstance(result, WindowsSessionUser)
        assert result.user == user
        assert result.group == group
        assert result.password == secret_data["password"]

    @patch(
        "deadline_worker_agent.windows_credentials_resolver.WindowsCredentialsResolver._fetch_secret_from_secrets_manager"
    )
    def test_get_windows_session_user_cached(self, fetch_secret_mock, datetime_mock):
        # GIVEN
        mock_boto_session = MagicMock()
        now = datetime(2023, 1, 1, 12, 0, 0)
        datetime_mock.utcnow.return_value = now
        resolver = credentials_mod.WindowsCredentialsResolver(mock_boto_session)
        password_arn = "password_arn"
        user = "user"
        group = "group"
        user_obj = WindowsSessionUser(user=user, group=group, password="fake_cached_password")
        cached_entry = credentials_mod._WindowsCredentialsCacheEntry(
            windows_session_user=user_obj,
            last_fetched_at=now - timedelta(hours=11),
            last_accessed=now - timedelta(hours=11),
        )
        resolver._user_cache[f"{user}_{password_arn}"] = cached_entry
        secret_data = {"password": "fake_new_password"}
        fetch_secret_mock.return_value = secret_data

        # WHEN
        result = resolver.get_windows_session_user(user, group, password_arn)

        # THEN
        fetch_secret_mock.assert_not_called()
        assert isinstance(result, WindowsSessionUser)
        assert result.user == user
        assert result.group == group
        assert result.password == "fake_cached_password"
