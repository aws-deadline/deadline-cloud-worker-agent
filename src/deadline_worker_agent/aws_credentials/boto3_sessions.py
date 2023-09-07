# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from typing import Optional
from datetime import datetime, timezone
from threading import Lock

from botocore.session import Session as BotocoreSession, get_session as get_botocore_session
from botocore.credentials import (
    CredentialProvider,
    Credentials as BotocoreCredentials,
    ReadOnlyCredentials,
)
from ..session_events import configure_session_events


from ..boto import Session
from ..api_models import AwsCredentials


class PassThroughCredentialProvider(CredentialProvider):
    """A super basic botocore CredentialProvider that just
    passes a Credential object that it's given through as
    the credentials for the requesting botocore.Session
    """

    # The following two constants are part of botocore's CredentialProvider interface

    # A short name to identify the provider within botocore.
    METHOD = "pass-through"
    # A name to identify the provider for use in cross-sdk features. The AWS SDKs
    # require that providers outside of botocore are prefixed with "custom"
    CANONICAL_NAME = "custom-pass-through"

    def __init__(
        self, *, session: Optional[BotocoreSession] = None, credentials: BotocoreCredentials
    ) -> None:
        super().__init__(session)
        self._credentials = credentials

    def load(self):
        return self._credentials


class SettableCredentials(BotocoreCredentials):
    """Botocore AWS Credentials that can be modified to a new
    set of credentials as desired.
    """

    _access_key: str
    _secret_key: str
    _token: str
    _expiry: datetime
    _lock: Lock
    _frozen_credentials: ReadOnlyCredentials

    def __init__(self) -> None:
        super().__init__(access_key="", secret_key="", token="", method="custom-settable")
        self._access_key = ""
        self._secret_key = ""
        self._token = ""
        self._expiry = datetime.fromtimestamp(0, tz=timezone.utc)
        self._lock = Lock()

    def set_credentials(self, credentials: AwsCredentials) -> None:
        """Change the credentials stored to the given ones."""

        # Note: We need to be able to update the credentials stored in a thread-safe manner -- the updater
        # thread is separate from the user thread(s). So, this update needs to be effectively atomic in
        # that all three values (access, secret, & token) need to update at once; else you could have
        # a situation where a user-thread gets, say, an old secret key but a new token. The result would
        # be an API request with a bad signature.

        with self._lock:
            self.access_key = credentials["accessKeyId"]
            self.secret_key = credentials["secretAccessKey"]
            self.token = credentials["sessionToken"]
            self.expiry = credentials["expiration"]
            self._frozen_credentials = ReadOnlyCredentials(
                self.access_key, self.secret_key, self.token
            )

    # Override the base class's to avoid data races, since the base
    # class constructs a fresh ReadOnlyCredentials object on each
    # call from the access_key, secret_key, and token properties.
    def get_frozen_credentials(self) -> ReadOnlyCredentials:
        with self._lock:
            return self._frozen_credentials

    def are_expired(self) -> bool:
        """Determine whether or not the stored credentials are expired.
        Returns: True if so, False otherwise.
        """
        return datetime.now(timezone.utc) > self.expiry

    @property
    def expiry(self) -> datetime:
        return self._expiry

    @expiry.setter
    def expiry(self, v: datetime) -> None:
        self._expiry = v

    # We need the access_key, secret_key, and token properties
    # for compatibility with the base class.
    # Using these to access credentials for signing will result
    # in a data race if there are separate threads accessing credentials
    # and calling set_credentials(). We have that situation in this codebase,
    # but botocore's request signer only obtains credentials from this
    # via its get_frozen_credentials() method.
    # See: RequestSigner.get_auth_instance() in
    #   https://github.com/boto/botocore/blob/develop/botocore/signers.py
    @property
    def access_key(self) -> str:
        return self._access_key

    @access_key.setter
    def access_key(self, v: str) -> None:
        self._access_key = v

    @property
    def secret_key(self) -> str:
        return self._secret_key

    @secret_key.setter
    def secret_key(self, v: str) -> None:
        self._secret_key = v

    @property
    def token(self) -> str:
        return self._token

    @token.setter
    def token(self, v: str) -> None:
        self._token = v


class BaseBoto3Session(Session):
    """Base class for our WorkerBoto3Session and QueueBoto3Session classes.
    This is providing an interface for the AwsCredentialRefresher to
    use (BaseBoto3Session.refresh_credentials()), and setting up the
    Boto3 Session with an empty expired SettableCredentials for its
    AWS Credentials.
    """

    def __init__(self) -> None:
        # Create a botocore Session for this Boto3 Session, and set it
        # so that the AWS Credentials are always resolved with a
        # PassThroughCredentialProvider to a SettableCredentials object.
        botocore_session = get_botocore_session()
        configure_session_events(botocore_session=botocore_session)
        creds_resolver = botocore_session.get_component("credential_provider")
        # Must insert at the start of the provider chain.
        # See 'create_credential_resolver()' in
        #   https://github.com/boto/botocore/blob/develop/botocore/credentials.py
        creds_resolver.insert_before(
            "env", PassThroughCredentialProvider(credentials=SettableCredentials())
        )

        super().__init__(botocore_session=botocore_session)

    def refresh_credentials(self) -> None:  # pragma: no cover
        """Calling this function will cause the derived class to run a workflow
        to refresh the credentials that are stored in the Session.

        Expect this to raise exceptions if the refresh fails.
        """
        raise NotImplementedError("Base class. Not intended for direct-use.")
