# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

import logging
from typing import Any, cast

from botocore.utils import JSONFileCache

from ..boto import DEADLINE_BOTOCORE_CONFIG, Session
from ..config import Configuration
from ..aws.deadline import (
    DeadlineRequestUnrecoverableError,
    assume_fleet_role_for_worker,
)
from .boto3_sessions import BaseBoto3Session, SettableCredentials
from .temporary_credentials import TemporaryCredentials
from ..log_messages import (
    FilesystemLogEvent,
    FilesystemLogEventOp,
    AwsCredentialsLogEvent,
    AwsCredentialsLogEventOp,
)

_logger = logging.getLogger(__name__)


class WorkerBoto3Session(BaseBoto3Session):
    """A Boto3 Session that contains Fleet Role AWS Credentials for use by the Worker
    Agent when performing actions on its own behalf (e.g. querying for work, writing logs, etc).

    Calling WorkerBoto3Session.refresh_credentials() will cause a service call to AssumeFleetRoleForWorker
    and this Session's AWS Credentials will be updated with the result.
    """

    def __init__(
        self,
        *,
        bootstrap_session: Session,
        config: Configuration,
        worker_id: str,
    ) -> None:
        super().__init__()

        self._bootstrap_session = bootstrap_session
        self._file_cache = JSONFileCache(working_dir=config.worker_credentials_dir)
        self._creds_filename = f"{worker_id}"  # note: .json extension added by JSONFileCache

        self._farm_id = config.farm_id
        self._fleet_id = config.fleet_id
        self._worker_id = worker_id

        # Worker Agent credentials may be retained on disk for resilience to unexpected
        # application exits (e.g. crashes). Load them if we have them; they may be expired
        # so check for expiry and force a refresh if they are.
        credentials_object = cast(SettableCredentials, self.get_credentials())
        if initial_credentials := TemporaryCredentials.from_cache(
            cache=self._file_cache, cache_key=self._creds_filename
        ):
            credentials_object.set_credentials(initial_credentials.to_deadline())

        if credentials_object.are_expired():
            # May raise an exception
            self.refresh_credentials()

    def refresh_credentials(self) -> None:
        """Attempt a refresh of the AWS Credentials stored in this Session by
        calling the AssumeFleetRoleForWorker API.

        If successful, then:
        1. Update the credentials stored in this Session; and
        2. Persist the retrieved credentials to disk.

        Raises:
           DeadlineRequestUnrecoverableError -- When we could not obtain new credentials
           for any reason.
        """

        credentials_object = cast(SettableCredentials, self.get_credentials())
        if credentials_object.are_expired():
            session = self._bootstrap_session
        else:
            session = self

        _logger.info(
            AwsCredentialsLogEvent(
                op=AwsCredentialsLogEventOp.QUERY,
                resource=self._worker_id,
                message="Requesting AWS Credentials",
            )
        )

        deadline_client = session.client("deadline", config=DEADLINE_BOTOCORE_CONFIG)
        try:
            response = assume_fleet_role_for_worker(
                deadline_client=deadline_client,
                farm_id=self._farm_id,
                fleet_id=self._fleet_id,
                worker_id=self._worker_id,
            )
        except DeadlineRequestUnrecoverableError as e:
            # Re-raise to let the caller know, and handle the exception.
            raise e from None

        try:
            temporary_creds = TemporaryCredentials.from_deadline_assume_role_response(
                response=cast(dict[str, Any], response),
                credentials_required=True,
                api_name="AssumeFleetRoleForWorker",
            )
        except (KeyError, TypeError, ValueError) as e:
            # Something was bad with the response. That's unrecoverable.
            raise DeadlineRequestUnrecoverableError(e)

        assert temporary_creds is not None  # For type checker
        temporary_creds.cache(cache=self._file_cache, cache_key=self._creds_filename)
        credentials_object.set_credentials(temporary_creds.to_deadline())

        _logger.info(
            AwsCredentialsLogEvent(
                op=AwsCredentialsLogEventOp.QUERY,
                resource=self._worker_id,
                message="Obtained temporary Worker AWS Credentials.",
                expiry=str(temporary_creds.expiry_time),
            )
        )
        _logger.info(
            FilesystemLogEvent(
                op=FilesystemLogEventOp.WRITE,
                filepath=str(self._file_cache._convert_cache_key(self._creds_filename)),
                message="Worker AWS Credentials cached.",
            )
        )
