# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

from unittest.mock import patch, MagicMock, call
from typing import Generator
import string
import sys

from openjd.sessions import BadCredentialsException
from pytest import fixture, param
import pytest

# This if is required for two purposes:
# 1.  It short-circuits mypy from type checking this module on platforms other than Windows
#     https://mypy.readthedocs.io/en/stable/common_issues.html#python-version-and-system-platform-checks
# 2.  It causes the tests to not be discovered/ran on non-Windows platforms
if sys.platform == "win32":
    import deadline_worker_agent.windows.win_logon as win_logon_mod
    from deadline_worker_agent.windows.win_logon import (
        get_windows_credentials,
        reset_user_password,
        unload_and_close,
        generate_password,
        PI_NOUI,
        LOGON32_LOGON_INTERACTIVE,
        LOGON32_PROVIDER_DEFAULT,
        PasswordResetException,
        PyWinError,
        users_equal,
    )

    class TestGetWindowsCredentialsOutsideSession0:
        @pytest.fixture(
            params=[("some-user", "some-password"), ("another-user", "another-password")]
        )
        def username_password(self, request: pytest.FixtureRequest) -> str:
            return request.param

        @fixture(autouse=True)
        def windows_session_user(self) -> Generator[MagicMock, None, None]:
            with patch.object(win_logon_mod, "WindowsSessionUser") as mock:
                yield mock

        @fixture(autouse=True)
        def outside_session_0(self) -> Generator[MagicMock, None, None]:
            with patch.object(win_logon_mod, "is_windows_session_zero") as mock:
                mock.return_value = False
                yield mock

        def test_logon_outside_session_0(
            self,
            windows_session_user: MagicMock,
            username_password: tuple[str, str],
        ):
            # GIVEN
            username, password = username_password

            # WHEN
            cache_entry = get_windows_credentials(username, password)

            # THEN
            windows_session_user.assert_called_once_with(user=username, password=password)
            assert cache_entry.windows_session_user is windows_session_user.return_value

        def test_wrong_password_raises_outside_session_0(
            self,
            windows_session_user: MagicMock,
            username_password: tuple[str, str],
        ):
            # GIVEN
            username, password = username_password
            windows_session_user.side_effect = BadCredentialsException("password bad!")

            # THEN
            with pytest.raises(BadCredentialsException) as exc_info:
                # WHEN
                get_windows_credentials(username, password)

            # THEN
            assert exc_info.value is windows_session_user.side_effect

    class TestGetWindowsCredentialsInSession0:
        @pytest.fixture(
            params=[("some-user", "some-password"), ("another-user", "another-password")]
        )
        def username_password(self, request: pytest.FixtureRequest) -> str:
            return request.param

        @fixture(autouse=True)
        def logon_user(self) -> Generator[MagicMock, None, None]:
            with patch.object(win_logon_mod, "LogonUser") as mock:
                mock.return_value.__int__.return_value = 12983791  # default is 1, mix it up a bit
                yield mock

        @fixture(autouse=True)
        def load_user_profile(self) -> Generator[MagicMock, None, None]:
            with patch.object(win_logon_mod, "LoadUserProfile") as mock:
                yield mock

        @fixture(autouse=True)
        def windows_session_user(self) -> Generator[MagicMock, None, None]:
            with patch.object(win_logon_mod, "WindowsSessionUser") as mock:
                yield mock

        @fixture(autouse=True)
        def chandle(self) -> Generator[MagicMock, None, None]:
            with patch.object(win_logon_mod, "cHANDLE") as mock:
                yield mock

        @fixture(autouse=True)
        def is_session_0(self) -> Generator[MagicMock, None, None]:
            with patch.object(win_logon_mod, "is_windows_session_zero") as mock:
                mock.return_value = True
                yield mock

        def test_logon_in_session_0(
            self,
            windows_session_user: MagicMock,
            logon_user: MagicMock,
            load_user_profile: MagicMock,
            chandle: MagicMock,
            username_password: tuple[str, str],
        ):
            # GIVEN
            username, password = username_password

            # WHEN
            cache_entry = get_windows_credentials(username, password)

            # THEN

            logon_user.assert_called_once_with(
                Username=username,
                LogonType=LOGON32_LOGON_INTERACTIVE,
                LogonProvider=LOGON32_PROVIDER_DEFAULT,
                Password=password,
                Domain=None,
            )
            load_user_profile.assert_called_once_with(
                logon_user.return_value,
                {
                    "UserName": username,
                    "Flags": PI_NOUI,
                    "ProfilePath": None,
                },
            )
            chandle.assert_called_once_with(logon_user.return_value.__int__.return_value)
            windows_session_user.assert_called_once_with(
                user=username, logon_token=chandle.return_value
            )

            assert cache_entry.windows_session_user is windows_session_user.return_value
            assert cache_entry.logon_token is logon_user.return_value
            assert cache_entry.user_profile is load_user_profile.return_value

        def test_wrong_password_raises_in_session_0(
            self,
            logon_user: MagicMock,
            username_password: tuple[str, str],
        ):
            # GIVEN
            username, password = username_password
            message = "Password bad!"
            logon_user.side_effect = OSError(message)

            # THEN
            with pytest.raises(BadCredentialsException) as exc_info:
                # WHEN
                get_windows_credentials(username, password)

            # THEN
            assert str(exc_info.value) == f'Error logging on as "{username}": {message}'

        def test_user_profile_load_fail_raises_in_session_0(
            self,
            load_user_profile: MagicMock,
            username_password: tuple[str, str],
        ):
            # GIVEN
            username, password = username_password
            load_user_profile.side_effect = OSError("Profile bad!")

            # THEN
            with pytest.raises(OSError) as exc_info:
                # WHEN
                get_windows_credentials(username, password)

            assert exc_info.value is load_user_profile.side_effect

        def test_logon_token_is_bad(
            self,
            windows_session_user: MagicMock,
            username_password: tuple[str, str],
        ):
            # GIVEN
            username, password = username_password
            message = "Token bad!"
            windows_session_user.side_effect = OSError(message)

            # THEN
            with pytest.raises(BadCredentialsException) as exc_info:
                # WHEN
                get_windows_credentials(username, password)

            # THEN
            assert str(exc_info.value) == f'Error logging on as "{username}": {message}'

    class TestResetUserPassword:
        @fixture
        def username(self) -> str:
            return "some-user"

        @fixture(autouse=True)
        def get_windows_credentials(self) -> Generator[MagicMock, None, None]:
            with patch.object(win_logon_mod, "get_windows_credentials") as mock:
                yield mock

        @fixture(autouse=True)
        def win32net(self) -> Generator[MagicMock, None, None]:
            with patch.object(win_logon_mod, "win32net") as mock:
                yield mock

        @fixture(autouse=True)
        def generate_password(self) -> Generator[MagicMock, None, None]:
            with patch.object(win_logon_mod, "generate_password") as mock:
                mock.return_value = "new-password"
                yield mock

        def test_get_user_info_fails(
            self,
            username: str,
            win32net: MagicMock,
        ):
            # GIVEN
            win32net.NetUserGetInfo.side_effect = PyWinError(
                2221, "NetUserGetInfo", "The user name could not be found."
            )

            # WHEN
            with pytest.raises(PasswordResetException) as exc_info:
                reset_user_password(username)

            # THEN
            assert (
                str(exc_info.value)
                == f'Failed to reset password for "{username}". Error: {win32net.NetUserGetInfo.side_effect.strerror}'
            )

        def test_set_user_info_fails(
            self,
            username: str,
            win32net: MagicMock,
        ):
            # GIVEN
            win32net.NetUserSetInfo.side_effect = PyWinError(
                2246, "NetUserGetInfo", "The password of this user is too recent to change"
            )

            # WHEN
            with pytest.raises(PasswordResetException) as exc_info:
                reset_user_password(username)

            # THEN
            assert (
                str(exc_info.value)
                == f'Failed to reset password for "{username}". Error: {win32net.NetUserSetInfo.side_effect.strerror}'
            )

        @pytest.mark.parametrize(
            "error",
            [
                OSError("User Profile does not exist"),
                BadCredentialsException("Password is not correct"),
            ],
        )
        def test_get_windows_credentials_fails(
            self,
            username: str,
            get_windows_credentials: MagicMock,
            error: Exception,
        ):
            # GIVEN
            get_windows_credentials.side_effect = error

            # WHEN
            with pytest.raises(PasswordResetException) as exc_info:
                reset_user_password(username)

            # THEN
            if isinstance(error, OSError):
                assert (
                    str(exc_info.value)
                    == f'Failed to load the user profile for "{username}". Error: {str(error)}'
                )
            else:
                assert (
                    str(exc_info.value) == f'Failed to logon as "{username}". Error: {str(error)}'
                )

        def test_reset_user_password(
            self,
            username: str,
            get_windows_credentials: MagicMock,
            generate_password: MagicMock,
        ):
            cache_entry = reset_user_password(username)

            # THEN
            generate_password.assert_called_once()
            assert cache_entry is get_windows_credentials.return_value

    class TestUnloadAndClose:
        @fixture
        def unload_user_profile(self) -> Generator[MagicMock, None, None]:
            with patch.object(win_logon_mod, "UnloadUserProfile") as mock:
                yield mock

        @pytest.mark.parametrize(
            "logon_token,user_profile",
            [
                (MagicMock(), MagicMock()),
                (MagicMock(), None),
            ],
        )
        def test_unload_and_close(
            self,
            unload_user_profile: MagicMock,
            logon_token: MagicMock,
            user_profile: MagicMock | None,
        ):
            # WHEN
            unload_and_close(user_profile, logon_token)

            # THEN
            if user_profile is not None:
                unload_user_profile.assert_called_once_with(logon_token, user_profile)
            logon_token.Close.assert_called_once()

    class TestGeneratePassword:
        @fixture
        def secrets_choice(self) -> Generator[MagicMock, None, None]:
            with patch.object(
                win_logon_mod.secrets, "choice", side_effect=["a", "A", "1", "*"] * 100
            ) as mock:
                yield mock

        @fixture(autouse=True)
        def net_user_get_info(self):
            with patch.object(
                win_logon_mod.win32net,
                "NetUserGetInfo",
                return_value={"name": "test-user", "full_name": "Test User"},
            ) as mock:
                yield mock

        @fixture
        def alphabet(self) -> str:
            return string.ascii_letters + string.digits + string.punctuation

        def test_password_is_not_reused(self, net_user_get_info: MagicMock):
            # WHEN
            password = generate_password("username")

            # THEN
            assert generate_password("username") != password

        def test_password_uses_secrets(self, secrets_choice: MagicMock, alphabet: str):
            # WHEN
            generate_password("username")

            # THEN
            assert secrets_choice.call_count >= 256
            secrets_choice.assert_has_calls(
                [call(alphabet) for _ in range(secrets_choice.call_count)]
            )

        def test_password_composition(self, net_user_get_info: MagicMock, alphabet: str):
            # WHEN
            password = generate_password("username")

            # THEN
            assert len(password) == 256
            assert any(char.isupper() for char in password)
            assert any(char.islower() for char in password)
            assert any(char.isdigit() for char in password)
            assert any(not char.isalnum() for char in password)

        def test_cannot_generate_valid_password(self):
            # GIVEN
            with pytest.raises(PasswordResetException) as exc_info:
                # WHEN
                generate_password("username", length=1)

            assert (
                str(exc_info.value)
                == "Failed to generate a password which meets security requirements."
            )

    class TestUsersEqual:
        @pytest.mark.parametrize(
            "side_effect, should_be_equal, usernames",
            [
                param(
                    [(1, None, None), (1, None, None)],
                    True,
                    ("user", "domain\\user"),
                    id="same sid",
                ),
                param(
                    [(1, None, None), (2, None, None)],
                    False,
                    ("user", "domain\\user"),
                    id="diff sid",
                ),
                param(
                    [
                        (1, None, None),
                        PyWinError(2221, "NetUserGetInfo", "The user name could not be found."),
                    ],
                    False,
                    ("user", "domain\\user"),
                    id="one doesn't exist",
                ),
                param(
                    [
                        PyWinError(2221, "NetUserGetInfo", "The user name could not be found."),
                        PyWinError(2221, "NetUserGetInfo", "The user name could not be found."),
                    ],
                    True,
                    ("user", "user"),
                    id="both don't exist string comparison success",
                ),
                param(
                    [
                        PyWinError(2221, "NetUserGetInfo", "The user name could not be found."),
                        PyWinError(2221, "NetUserGetInfo", "The user name could not be found."),
                    ],
                    False,
                    ("user", "domain\\user"),
                    id="both don't exist string comparison failure",
                ),
            ],
        )
        @patch.object(win_logon_mod.win32security, "LookupAccountName")
        def test_users_equal(
            self,
            lookup_account_name: MagicMock,
            side_effect: list,
            should_be_equal: bool,
            usernames: tuple[str, str],
        ):
            # GIVEN
            lookup_account_name.side_effect = side_effect
            user_1, user_2 = usernames

            # WHEN
            is_equal = users_equal(user_1, user_2)

            # THEN
            lookup_account_name.assert_has_calls([call(None, user_1), call(None, user_2)])
            assert is_equal == should_be_equal
