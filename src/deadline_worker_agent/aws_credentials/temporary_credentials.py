# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional, Type
import logging
import json

from typing_extensions import TypedDict

from botocore.credentials import JSONFileCache

from ..api_models import AwsCredentials
from ..log_messages import AwsCredentialsLogEvent, AwsCredentialsLogEventOp

_logger = logging.getLogger(__name__)

_DATETIME_FORMAT = r"%Y-%m-%dT%H:%M:%SZ"


class FileCredentials(TypedDict):
    """The credentials dictionary structure as expected from a credential process.
    We also use this format for storing the Worker Agent's credentials to disk.
    See:
    https://docs.aws.amazon.com/sdkref/latest/guide/feature-process-credentials.html#feature-process-credentials-output
    """

    Version: int
    AccessKeyId: str
    SecretAccessKey: str
    SessionToken: str
    Expiration: str  # ISO 8601


@dataclass
class TemporaryCredentials:
    """
    A model class representing temporary AWS credentials and accompanying logic for:

    - converting to/from AWS Deadline Cloud and botocore credential formats
    - checking whether the credentials are expired
    """

    access_key_id: str
    secret_access_key: str
    session_token: str
    expiry_time: datetime

    def cache(
        self,
        *,
        cache: JSONFileCache,
        cache_key: str,
    ) -> None:
        """Caches the temporary credentials to a botocore JSONFileCache"""
        cache[cache_key] = self.to_file_format()

    @classmethod
    def from_cache(
        cls: Type[TemporaryCredentials],
        *,
        cache: JSONFileCache,
        cache_key: str,
    ) -> Optional[TemporaryCredentials]:
        """Obtains temporary credentials from a botocore JSONFileCache"""
        try:
            if cache_key not in cache:
                return None

            result = cache[cache_key]
        except (json.JSONDecodeError, KeyError) as e:
            # Handle corrupted cache file
            full_filename = cache._convert_cache_key(cache_key)
            _logger.error(
                AwsCredentialsLogEvent(
                    op=AwsCredentialsLogEventOp.LOAD,
                    resource=full_filename,
                    message=f"Error reading AWS Credentials from cache file: {str(e)}",
                )
            )
            # Delete the corrupted cache entry to allow fresh credentials to be obtained
            del cache[cache_key]
            _logger.info(
                AwsCredentialsLogEvent(
                    op=AwsCredentialsLogEventOp.DELETE,
                    resource=full_filename,
                    message="Deleted corrupted AWS Credentials cache file",
                )
            )
            return None

        try:
            credentials = cls.validate_file_credentials(result)
        except Exception as e:
            full_filename = cache._convert_cache_key(cache_key)
            _logger.error(
                AwsCredentialsLogEvent(
                    op=AwsCredentialsLogEventOp.LOAD,
                    resource=full_filename,
                    message=f"Error reading AWS Credentials from cache file: {str(e)}",
                )
            )
            return None

        return cls.from_file_format(credentials)

    def to_deadline(self) -> AwsCredentials:
        """Converts the temporary credentials to a dictionary as returned from AWS Deadline Cloud APIs"""
        return AwsCredentials(
            accessKeyId=self.access_key_id,
            secretAccessKey=self.secret_access_key,
            sessionToken=self.session_token,
            expiration=self.expiry_time,
        )

    def to_file_format(self) -> FileCredentials:
        """
        Converts the temporary credentials to a dictionary as expected from a credential process
        """
        return FileCredentials(
            Version=1,
            AccessKeyId=self.access_key_id,
            SecretAccessKey=self.secret_access_key,
            SessionToken=self.session_token,
            Expiration=self.expiry_time.astimezone(timezone.utc).strftime(_DATETIME_FORMAT),
        )

    @classmethod
    def from_file_format(
        cls: Type[TemporaryCredentials], data: FileCredentials
    ) -> TemporaryCredentials:
        return cls(
            access_key_id=data["AccessKeyId"],
            secret_access_key=data["SecretAccessKey"],
            session_token=data["SessionToken"],
            expiry_time=datetime.strptime(data["Expiration"], _DATETIME_FORMAT).replace(
                tzinfo=timezone.utc
            ),
        )

    @classmethod
    def validate_file_credentials(
        cls,
        data: dict[str, Any],
    ) -> FileCredentials:
        """
        Validates that a dictionary conforms to the structure expected to be read from our cache file.
        """

        # Validation
        def _get_required_str(key: str) -> str:
            value = data.get(key, None)
            if value is None:
                raise KeyError(f'Expected key in "{key}" not found')
            elif not isinstance(value, str):
                raise TypeError(f'Expected key "{key}" was not a string')
            elif value == "":
                raise ValueError(f'Expected key in "{key}" was an empty string')
            return value

        if not (expiration := data.get("Expiration", None)):
            raise KeyError('Expected key "Expiration" not found')
        elif not isinstance(expiration, str):
            raise TypeError('Expected key "Expiration" was not a ISO 8601 format string')
        else:
            try:
                expiration = datetime.strptime(expiration, _DATETIME_FORMAT).replace(
                    tzinfo=timezone.utc
                )
            except ValueError:
                raise ValueError('Expected key "Expiration" was not a valid ISO 8601 format string')
        if not (version := data.get("Version", None)):
            raise KeyError('Expected key "Version" not found')
        elif version != 1:
            raise ValueError('Key "Version" must equal 1')

        return FileCredentials(
            Version=1,
            AccessKeyId=_get_required_str("AccessKeyId"),
            SecretAccessKey=_get_required_str("SecretAccessKey"),
            SessionToken=_get_required_str("SessionToken"),
            Expiration=expiration.strftime(_DATETIME_FORMAT),
        )

    @classmethod
    def from_deadline(
        cls: Type[TemporaryCredentials],
        data: AwsCredentials,
    ) -> TemporaryCredentials:
        """
        Creates a TemporaryCredentials instance from a credentials dictionary as returned by AWS Deadline Cloud
        APIs
        """
        return cls(
            access_key_id=data["accessKeyId"],
            secret_access_key=data["secretAccessKey"],
            session_token=data["sessionToken"],
            expiry_time=data["expiration"],
        )

    @classmethod
    def validate_deadline_credentials(
        cls,
        data: dict[str, Any],
    ) -> AwsCredentials:
        """
        Validates that a dictionary conforms to the structure expected to be returned by AWS Deadline Cloud
        APIs via botocore
        """

        # Validation
        def _get_required_str(key: str) -> str:
            value = data.get(key, None)
            if value is None:
                raise KeyError(f'Expected key in "{key}" not found')
            elif not isinstance(value, str):
                raise TypeError(f'Expected key "{key}" was not a string, was {type(value)}')
            elif value == "":
                raise ValueError(f'Expected key in "{key}" was an empty string')
            return value

        if not (expiration := data.get("expiration", None)):
            raise KeyError('Expected key "expiration" not found')
        elif not isinstance(expiration, datetime):
            raise TypeError(f'Expected key "expiration" was not a datetime, was {type(expiration)}')

        return AwsCredentials(
            accessKeyId=_get_required_str("accessKeyId"),
            secretAccessKey=_get_required_str("secretAccessKey"),
            sessionToken=_get_required_str("sessionToken"),
            expiration=expiration,
        )

    @classmethod
    def from_deadline_assume_role_response(
        cls, *, response: dict[str, Any], credentials_required: bool, api_name: str
    ) -> Optional[TemporaryCredentials]:
        """
        Converts the response as returned by a boto3 method for AWS Deadline Cloud's Assume*RoleForWorker
        methods into a TemporaryCredentials instance.

        This performs basic key and type validation on the response before returning the result.

        Arguments:
            response (dict): The response dictionary as returned by boto3
            credentials_required (bool): Whether the credentials are required in the response. If
                False and the credentials key is not in the response, then the function returns
                None. Otherwise, the function raises a ValueError.

        Returns:
            TemporaryCredentials: The TemporaryCredentials instance created from the response.

        Raises:
            TypeError: If the response is malformed
            ValueError: If there are no credentials in the response
        """
        credentials_key = "credentials"

        if not isinstance(response, dict):
            raise TypeError(f"Expected dict for response but got {type(response)}")

        if not (data := response.get(credentials_key, None)):
            if credentials_required:
                raise KeyError(
                    f'Expected key "{credentials_key}" not found in {api_name} API response'
                )
            return None
        elif not isinstance(data, dict):
            raise TypeError(f'Expected dict for "{credentials_key}" but got {type(data)}')

        temporary_creds = cls.validate_deadline_credentials(data)
        return cls.from_deadline(temporary_creds)

    def are_expired(self) -> bool:
        return datetime.now(timezone.utc) > self.expiry_time
