# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock
from deadline_worker_agent.aws_credentials.boto3_sessions import (
    BaseBoto3Session,
    SettableCredentials,
)
from deadline_worker_agent.api_models import AwsCredentials


class TestBaseBoto3Session:
    def test_construction(self) -> None:
        # The BaseBoto3Session must always be constructed such that the credentials
        # resolve to a SettableCredentials object.

        # WHEN
        session = BaseBoto3Session()

        # THEN
        assert isinstance(session.get_credentials(), SettableCredentials)

    def test_unique_credentials(self) -> None:
        # Separate constructions of a BaseBoto3Session must have separate
        # Credentials objects.

        # GIVEN
        session1 = BaseBoto3Session()
        session2 = BaseBoto3Session()

        # WHEN
        creds1 = session1.get_credentials()
        creds2 = session2.get_credentials()

        # THEN
        assert creds1 is not creds2


class TestSettableCredentials:
    def test_construction(self) -> None:
        # Make sure that the object is constructed correctly:
        #  1) with expired credentials
        #  2) with a threading.Lock

        # WHEN
        creds = SettableCredentials()

        # THEN
        assert creds._expiry < datetime.now(timezone.utc)
        assert creds._lock is not None

    def test_set_credentials(self) -> None:
        # Test that we can set the credentials, and that all
        # getters get the same credentials back.

        # GIVEN
        expiry = datetime.now(timezone.utc) + timedelta(minutes=60)
        given: AwsCredentials = {
            "accessKeyId": "access-key",
            "secretAccessKey": "secret-key",
            "sessionToken": "token",
            "expiration": expiry,
        }
        credentials = SettableCredentials()
        credentials._lock = MagicMock()

        # WHEN
        credentials.set_credentials(given)
        frozen = credentials.get_frozen_credentials()

        # THEN
        assert credentials._lock.__enter__.call_count == 2
        assert not credentials.are_expired()

        assert credentials.access_key == given["accessKeyId"]
        assert credentials.secret_key == given["secretAccessKey"]
        assert credentials.token == given["sessionToken"]
        assert credentials.expiry is expiry

        assert frozen.access_key == given["accessKeyId"]
        assert frozen.secret_key == given["secretAccessKey"]
        assert frozen.token == given["sessionToken"]
