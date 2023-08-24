# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from datetime import datetime, timezone
from dateutil.tz import tzlocal
from typing import Any, cast
from unittest.mock import MagicMock, patch

from pytest import fixture, mark, param, raises

from deadline_worker_agent.aws_credentials.temporary_credentials import (
    FileCredentials,
    TemporaryCredentials,
    _DATETIME_FORMAT,
)
import deadline_worker_agent.aws_credentials.temporary_credentials as temporary_credentials_mod
from deadline_worker_agent.api_models import AwsCredentials


@fixture
def access_key_id() -> str:
    return "access_key_id"


@fixture
def secret_access_key() -> str:
    return "secret_access_key"


@fixture
def expiry_time() -> datetime:
    return datetime.now(tz=tzlocal())


@fixture
def session_token() -> str:
    return "session_token"


@fixture
def temporary_credentials(
    access_key_id: str,
    secret_access_key: str,
    expiry_time: datetime,
    session_token: str,
) -> TemporaryCredentials:
    return TemporaryCredentials(
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        expiry_time=expiry_time,
        session_token=session_token,
    )


@fixture
def deadline_credentials(
    access_key_id: str,
    secret_access_key: str,
    expiry_time: datetime,
    session_token: str,
) -> AwsCredentials:
    return AwsCredentials(
        accessKeyId=access_key_id,
        secretAccessKey=secret_access_key,
        expiration=expiry_time,
        sessionToken=session_token,
    )


class TestTemporaryCredentials:
    def test_access_key_id(
        self,
        access_key_id: str,
        temporary_credentials: TemporaryCredentials,
    ) -> None:
        # THEN
        assert temporary_credentials.access_key_id == access_key_id

    def test_secret_access_key(
        self,
        secret_access_key: str,
        temporary_credentials: TemporaryCredentials,
    ) -> None:
        # THEN
        assert temporary_credentials.secret_access_key == secret_access_key

    def test_expiry_time(
        self,
        expiry_time: str,
        temporary_credentials: TemporaryCredentials,
    ) -> None:
        # THEN
        assert temporary_credentials.expiry_time == expiry_time

    def test_session_token(
        self,
        session_token: str,
        temporary_credentials: TemporaryCredentials,
    ) -> None:
        # THEN
        assert temporary_credentials.session_token == session_token

    def test_cache(
        self,
        temporary_credentials: TemporaryCredentials,
    ) -> None:
        # GIVEN
        cache = MagicMock()
        cache_key = "cache key"

        with patch.object(temporary_credentials, "to_file_format") as mock_to_file_format:
            # WHEN
            temporary_credentials.cache(
                cache=cache,
                cache_key=cache_key,
            )

            # THEN
            mock_to_file_format.assert_called_once_with()
            cache.__setitem__.assert_called_once_with(cache_key, mock_to_file_format.return_value)

    @mark.parametrize(
        argnames=("cache_hit",),
        argvalues=(
            param(True, id="cache-hit"),
            param(False, id="cache-miss"),
        ),
    )
    def test_from_cache(
        self,
        cache_hit: bool,
    ) -> None:
        # GIVEN
        cache = MagicMock()
        cache_key = "cache key"
        cache.__contains__.return_value = cache_hit
        cached_credentials = cache.__getitem__.return_value

        with (
            patch.object(TemporaryCredentials, "validate_file_credentials") as mock_validate,
            patch.object(TemporaryCredentials, "from_file_format") as mock_from_file_format,
        ):
            # WHEN
            result = TemporaryCredentials.from_cache(
                cache=cache,
                cache_key=cache_key,
            )

        # THEN
        if cache_hit:
            mock_validate.assert_called_once_with(cached_credentials)
            mock_from_file_format.assert_called_once_with(mock_validate.return_value)
            assert result is mock_from_file_format.return_value
        else:
            mock_validate.assert_not_called()
            mock_from_file_format.assert_not_called()
            assert result is None

    def test_to_deadline(
        self,
        temporary_credentials: TemporaryCredentials,
    ) -> None:
        # GIVEN
        expected_keys = {"accessKeyId", "expiration", "secretAccessKey", "sessionToken"}
        # WHEN
        result = temporary_credentials.to_deadline()

        # THEN
        assert isinstance(result, dict)
        assert set(result.keys()) == expected_keys
        assert result["accessKeyId"] == temporary_credentials.access_key_id
        assert result["secretAccessKey"] == temporary_credentials.secret_access_key
        assert result["sessionToken"] == temporary_credentials.session_token
        assert result["expiration"] == temporary_credentials.expiry_time

    def test_to_file_format(self, temporary_credentials: TemporaryCredentials) -> None:
        # GIVEN
        expected_keys = {"Version", "AccessKeyId", "Expiration", "SecretAccessKey", "SessionToken"}
        # WHEN
        result = temporary_credentials.to_file_format()

        # THEN
        assert isinstance(result, dict)
        assert set(result.keys()) == expected_keys
        assert result["Version"] == 1
        assert result["AccessKeyId"] == temporary_credentials.access_key_id
        assert result["SecretAccessKey"] == temporary_credentials.secret_access_key
        assert result["SessionToken"] == temporary_credentials.session_token
        assert result["Expiration"] == temporary_credentials.expiry_time.astimezone(
            timezone.utc
        ).strftime("%Y-%m-%dT%H:%M:%SZ")

    def test_from_file_format(
        self,
        access_key_id: str,
        secret_access_key: str,
        expiry_time: datetime,
        session_token: str,
    ) -> None:
        # GIVEN
        data = FileCredentials(
            Version=1,
            AccessKeyId=access_key_id,
            SecretAccessKey=secret_access_key,
            SessionToken=session_token,
            Expiration=expiry_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )

        # WHEN
        temporary_credentials = TemporaryCredentials.from_file_format(data)

        # THEN
        assert temporary_credentials is not None
        assert temporary_credentials.access_key_id == data["AccessKeyId"] == access_key_id
        assert (
            temporary_credentials.secret_access_key == data["SecretAccessKey"] == secret_access_key
        )
        assert temporary_credentials.session_token == data["SessionToken"] == session_token
        assert (
            temporary_credentials.expiry_time.replace(microsecond=0)
            == datetime.strptime(data["Expiration"], _DATETIME_FORMAT).replace(tzinfo=timezone.utc)
            == expiry_time.replace(microsecond=0, tzinfo=timezone.utc)
        )

    def test_validate_file_credentials_success(
        self,
        access_key_id: str,
        secret_access_key: str,
        expiry_time: datetime,
        session_token: str,
    ) -> None:
        # GIVEN
        data = {
            "Version": 1,
            "AccessKeyId": access_key_id,
            "SecretAccessKey": secret_access_key,
            "SessionToken": session_token,
            "Expiration": expiry_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

        # WHEN
        file_credentials = TemporaryCredentials.validate_file_credentials(data)

        # THEN
        assert file_credentials["Version"] == 1
        assert file_credentials["AccessKeyId"] == data["AccessKeyId"] == access_key_id
        assert file_credentials["SecretAccessKey"] == data["SecretAccessKey"] == secret_access_key
        assert file_credentials["SessionToken"] == data["SessionToken"] == session_token
        assert (
            file_credentials["Expiration"]
            == data["Expiration"]
            == expiry_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        )

    @mark.parametrize(
        "version", [param(None, id="absent"), param(2, id="not 1"), param(1, id="okay")]
    )
    @mark.parametrize(
        "access_key",
        [
            param(None, id="absent"),
            param(12, id="not string"),
            param("", id="empty string"),
            param("access_key", id="okay"),
        ],
    )
    @mark.parametrize(
        "secret_key",
        [
            param(None, id="absent"),
            param(12, id="not string"),
            param("", id="empty string"),
            param("secret_key", id="okay"),
        ],
    )
    @mark.parametrize(
        "token",
        [
            param(None, id="absent"),
            param(12, id="not string"),
            param("", id="empty string"),
            param("token", id="okay"),
        ],
    )
    @mark.parametrize(
        "expiration",
        [
            param(None, id="absent"),
            param(12, id="not valid ISO string"),
            param(datetime.now(), id="not a string"),
            param("2023-07-28T17:39", id="valid"),
        ],
    )
    def test_validate_file_credentials_error(
        self, version: Any, access_key: Any, secret_key: Any, token: Any, expiration: Any
    ) -> None:
        # GIVEN
        is_valid_data = version == 1
        is_valid_data = is_valid_data and isinstance(access_key, str) and len(access_key) > 0
        is_valid_data = is_valid_data and isinstance(secret_key, str) and len(secret_key) > 0
        is_valid_data = is_valid_data and isinstance(token, str) and len(token) > 0
        is_valid_data = is_valid_data and isinstance(expiration, str)
        if is_valid_data:
            try:
                datetime.strptime(expiration, _DATETIME_FORMAT)
            except ValueError:
                is_valid_data = False

        if is_valid_data:
            # The one valid case. Skip it; we're testing for errors.
            return
        data = dict[str, Any]()
        if version is not None:
            data["Version"] = version
        if access_key is not None:
            data["AccessKeyId"] = access_key
        if secret_key is not None:
            data["SecretAccessKey"] = secret_key
        if token is not None:
            data["SessionToken"] = token
        if expiration is not None:
            data["Expiration"] = expiration

        # THEN
        with raises(Exception):
            TemporaryCredentials.validate_file_credentials(data)

    def test_from_deadline(
        self,
        access_key_id: str,
        secret_access_key: str,
        expiry_time: datetime,
        session_token: str,
    ) -> None:
        # GIVEN
        deadline_credentials = AwsCredentials(
            accessKeyId=access_key_id,
            secretAccessKey=secret_access_key,
            sessionToken=session_token,
            expiration=expiry_time,
        )

        # WHEN
        temporary_credentials = TemporaryCredentials.from_deadline(deadline_credentials)

        # THEN
        assert temporary_credentials is not None
        assert temporary_credentials.access_key_id == deadline_credentials["accessKeyId"]
        assert temporary_credentials.secret_access_key == deadline_credentials["secretAccessKey"]
        assert temporary_credentials.session_token == deadline_credentials["sessionToken"]
        assert temporary_credentials.expiry_time == deadline_credentials["expiration"]

    def test_validate_deadline_credentials_success(
        self,
        access_key_id: str,
        secret_access_key: str,
        expiry_time: datetime,
        session_token: str,
    ) -> None:
        # GIVEN
        data = {
            "accessKeyId": access_key_id,
            "secretAccessKey": secret_access_key,
            "sessionToken": session_token,
            "expiration": expiry_time,
        }

        # WHEN
        result = TemporaryCredentials.validate_deadline_credentials(data)

        # THEN
        assert result["accessKeyId"] == data["accessKeyId"]
        assert result["secretAccessKey"] == data["secretAccessKey"]
        assert result["sessionToken"] == data["sessionToken"]
        assert result["expiration"] == data["expiration"]

    @mark.parametrize(
        "access_key",
        [
            param(None, id="absent"),
            param(12, id="not string"),
            param("", id="empty string"),
            param("access_key", id="okay"),
        ],
    )
    @mark.parametrize(
        "secret_key",
        [
            param(None, id="absent"),
            param(12, id="not string"),
            param("", id="empty string"),
            param("secret_key", id="okay"),
        ],
    )
    @mark.parametrize(
        "token",
        [
            param(None, id="absent"),
            param(12, id="not string"),
            param("", id="empty string"),
            param("token", id="okay"),
        ],
    )
    @mark.parametrize(
        "expiration",
        [param(None, id="absent"), param(12, id="not datetime"), param(datetime.now(), id="okay")],
    )
    def test_validate_deadline_credentials_error(
        self, access_key: Any, secret_key: Any, token: Any, expiration: Any
    ) -> None:
        # GIVEN
        is_valid_data = isinstance(access_key, str) and len(access_key) > 0
        is_valid_data = is_valid_data and isinstance(secret_key, str) and len(secret_key) > 0
        is_valid_data = is_valid_data and isinstance(token, str) and len(token) > 0
        is_valid_data = is_valid_data and isinstance(expiration, datetime)
        if is_valid_data:
            # The one valid case. Skip it; we're testing for errors.
            return
        data = dict[str, Any]()
        if access_key is not None:
            data["accessKeyId"] = access_key
        if secret_key is not None:
            data["secretAccessKey"] = secret_key
        if token is not None:
            data["sessionToken"] = token
        if expiration is not None:
            data["expiration"] = expiration

        # THEN
        with raises(Exception):
            TemporaryCredentials.validate_deadline_credentials(data)

    def test_from_deadline_assume_role_success(
        self,
        deadline_credentials: AwsCredentials,
    ) -> None:
        # GIVEN
        data: dict[str, Any] = {"credentials": deadline_credentials}

        # WHEN
        temporary_credentials = TemporaryCredentials.from_deadline_assume_role_response(
            response=data, credentials_required=True, api_name="Testing"
        )

        # THEN
        assert temporary_credentials is not None
        assert temporary_credentials.access_key_id == deadline_credentials["accessKeyId"]
        assert temporary_credentials.secret_access_key == deadline_credentials["secretAccessKey"]
        assert temporary_credentials.session_token == deadline_credentials["sessionToken"]
        assert temporary_credentials.expiry_time == deadline_credentials["expiration"]

    def test_from_deadline_assume_role_success_not_required(
        self,
        deadline_credentials: AwsCredentials,
    ) -> None:
        # GIVEN
        data: dict[str, Any] = {}

        # WHEN
        temporary_credentials = TemporaryCredentials.from_deadline_assume_role_response(
            response=data, credentials_required=False, api_name="Testing"
        )

        # THEN
        assert temporary_credentials is None

    @mark.parametrize(
        "data",
        [
            param(12, id="not dict"),
            param({"foo": 12}, id="no credentials key"),
            param({"credentials": 12}, id="not a dict"),
        ],
    )
    def test_from_deadline_assume_role_failure(self, data: Any) -> None:
        # WHEN
        with raises(Exception):
            TemporaryCredentials.from_deadline_assume_role_response(
                response=cast(dict[str, Any], data), credentials_required=True, api_name="Testing"
            )

    @mark.parametrize(
        argnames=("expiry_time", "now", "expected_result"),
        argvalues=(
            param(
                datetime(2022, 1, 1, 2, 0, 0),
                datetime(2022, 1, 1, 1, 0, 0),
                False,
                id="not-expired",
            ),
            param(
                datetime(2022, 1, 1, 2, 0, 0), datetime(2022, 1, 1, 3, 0, 0), True, id="are-expired"
            ),
        ),
    )
    def test_are_expired(
        self,
        temporary_credentials: TemporaryCredentials,
        now: datetime,
        expected_result: bool,
    ) -> None:
        # GIVEN
        with patch.object(temporary_credentials_mod, "datetime", return_value=now) as mock_datetime:
            mock_datetime.now.return_value = now

            # WHEN
            result = temporary_credentials.are_expired()

        # THEN
        mock_datetime.now.assert_called_once()
        assert result == expected_result
