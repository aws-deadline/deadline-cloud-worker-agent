# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from typing import Generator
from datetime import datetime, timedelta
from dateutil.tz import tzutc
from unittest.mock import MagicMock, patch
import pytest
from botocore.exceptions import ClientError

from deadline_worker_agent.aws.deadline import (
    assume_fleet_role_for_worker,
    DeadlineRequestUnrecoverableError,
)

import deadline_worker_agent.aws.deadline as deadline_mod
from deadline_worker_agent.api_models import AssumeFleetRoleForWorkerResponse, AwsCredentials


@pytest.fixture
def deadline_credentials() -> AwsCredentials:
    return AwsCredentials(
        accessKeyId="accessKeyId",
        expiration=datetime.utcnow().astimezone(tzutc()) + timedelta(hours=1),
        secretAccessKey="secretAccessKey",
        sessionToken="sessionToken",
    )


@pytest.fixture
def mock_assume_fleet_role_for_worker_response(
    deadline_credentials: AwsCredentials,
) -> AssumeFleetRoleForWorkerResponse:
    return {
        "credentials": deadline_credentials,
    }


@pytest.fixture
def sleep_mock() -> Generator[MagicMock, None, None]:
    with patch.object(deadline_mod, "sleep") as sleep_mock:
        yield sleep_mock


def test_success(
    client: MagicMock,
    farm_id: str,
    fleet_id: str,
    worker_id: str,
    mock_assume_fleet_role_for_worker_response: AssumeFleetRoleForWorkerResponse,
) -> None:
    # Simple success-case that we return the response of the API request.

    # GIVEN
    client.assume_fleet_role_for_worker.return_value = mock_assume_fleet_role_for_worker_response

    # WHEN
    response = assume_fleet_role_for_worker(
        deadline_client=client, farm_id=farm_id, fleet_id=fleet_id, worker_id=worker_id
    )

    # THEN
    assert response == mock_assume_fleet_role_for_worker_response
    client.assume_fleet_role_for_worker.assert_called_once_with(
        farmId=farm_id, fleetId=fleet_id, workerId=worker_id
    )


@pytest.mark.parametrize("exception_code", ["ThrottlingException", "InternalServerException"])
def test_retries_when_throttled(
    client: MagicMock,
    farm_id: str,
    fleet_id: str,
    worker_id: str,
    mock_assume_fleet_role_for_worker_response: AssumeFleetRoleForWorkerResponse,
    exception_code: str,
    sleep_mock: MagicMock,
):
    # A test that the assume_fleet_role_for_worker() function will
    # retry calls to the API when throttled.

    # GIVEN
    exc = ClientError(
        {"Error": {"Code": exception_code, "Message": "A message"}}, "AssumeFleetRoleForWorker"
    )
    client.assume_fleet_role_for_worker.side_effect = [
        exc,
        mock_assume_fleet_role_for_worker_response,
    ]

    # WHEN
    response = assume_fleet_role_for_worker(
        deadline_client=client, farm_id=farm_id, fleet_id=fleet_id, worker_id=worker_id
    )

    # THEN
    assert response == mock_assume_fleet_role_for_worker_response
    assert client.assume_fleet_role_for_worker.call_count == 2
    sleep_mock.assert_called_once()


@pytest.mark.parametrize(
    "exception_code",
    [
        "NotARealException",
        "AccessDeniedException",
        "ValidationException",
        "ResourceNotFoundException",
    ],
)
def test_raises_clienterror(
    client: MagicMock,
    farm_id: str,
    fleet_id: str,
    worker_id: str,
    exception_code: str,
    sleep_mock: MagicMock,
):
    # Test that when assume_fleet_role_for_worker() gets a non-throttle
    # exception from calls to the API that it raises those as an exception

    # GIVEN
    exc = ClientError(
        {"Error": {"Code": exception_code, "Message": "A message"}}, "AssumeFleetRoleForWorker"
    )
    client.assume_fleet_role_for_worker.side_effect = exc

    with pytest.raises(DeadlineRequestUnrecoverableError) as exc_context:
        # WHEN
        assume_fleet_role_for_worker(
            deadline_client=client, farm_id=farm_id, fleet_id=fleet_id, worker_id=worker_id
        )

    # THEN
    assert exc_context.value.inner_exc is exc
    sleep_mock.assert_not_called()


def test_raises_unexpected_exception(
    client: MagicMock, farm_id: str, fleet_id: str, worker_id: str, sleep_mock: MagicMock
):
    # Test that when assume_fleet_role_for_worker() gets a non-ClientError
    # exception from calls to the API that it raises those as an exception

    # GIVEN
    exc = Exception("Surprise!")
    client.assume_fleet_role_for_worker.side_effect = exc

    with pytest.raises(DeadlineRequestUnrecoverableError) as exc_context:
        # WHEN
        assume_fleet_role_for_worker(
            deadline_client=client, farm_id=farm_id, fleet_id=fleet_id, worker_id=worker_id
        )

    # THEN
    assert exc_context.value.inner_exc is exc
    sleep_mock.assert_not_called()
