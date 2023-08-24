# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from typing import Any, Generator
from unittest.mock import MagicMock, patch
import pytest
from botocore.exceptions import ClientError

from deadline_worker_agent.aws.deadline import (
    DeadlineRequestUnrecoverableError,
    DeadlineRequestWorkerNotFound,
    batch_get_job_entity,
)

import deadline_worker_agent.aws.deadline as deadline_mod
from deadline_worker_agent.api_models import BatchGetJobEntityResponse, EntityIdentifier

SAMPLE_IDENTITIES: list[EntityIdentifier] = [
    {"environmentDetails": {"jobId": "job-1234", "environmentId": "env:1234"}}
]
SAMPLE_RESPONSE: BatchGetJobEntityResponse = {
    "entities": [
        {
            "environmentDetails": {
                "jobId": "job-1234",
                "environmentId": "env:1234",
                "schemaVersion": "2022-09-01",
                "template": {},
            }
        }
    ],
    "errors": [],
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
) -> None:
    # Simple success-case that we return the response of the API request.

    # GIVEN
    client.batch_get_job_entity.return_value = SAMPLE_RESPONSE

    # WHEN
    response = batch_get_job_entity(
        deadline_client=client,
        farm_id=farm_id,
        fleet_id=fleet_id,
        worker_id=worker_id,
        identifiers=SAMPLE_IDENTITIES,
    )

    # THEN
    assert response is SAMPLE_RESPONSE
    client.batch_get_job_entity.assert_called_once_with(
        farmId=farm_id, fleetId=fleet_id, workerId=worker_id, identifiers=SAMPLE_IDENTITIES
    )


@pytest.mark.parametrize(
    "exception",
    [
        pytest.param(
            ClientError(
                {"Error": {"Code": "ThrottlingException", "Message": "A message"}},
                "BatchGetJobEntity",
            ),
            id="Throttling",
        ),
        pytest.param(
            ClientError(
                {"Error": {"Code": "InternalServerException", "Message": "A message"}},
                "BatchGetJobEntity",
            ),
            id="InternalServer",
        ),
    ],
)
def test_retries_when_appropriate(
    client: MagicMock,
    farm_id: str,
    fleet_id: str,
    worker_id: str,
    exception: ClientError,
    sleep_mock: MagicMock,
):
    # A test that the batch_get_job_entity() function will retry calls to the API when:
    # 1. Throttled
    # 2. InternalServerException

    # GIVEN
    client.batch_get_job_entity.side_effect = [exception, dict[str, Any]()]

    # WHEN
    batch_get_job_entity(
        deadline_client=client,
        farm_id=farm_id,
        fleet_id=fleet_id,
        worker_id=worker_id,
        identifiers=SAMPLE_IDENTITIES,
    )

    # THEN
    assert client.batch_get_job_entity.call_count == 2
    sleep_mock.assert_called_once()


@pytest.mark.parametrize(
    "exception",
    [
        pytest.param(
            ClientError(
                {"Error": {"Code": "AccessDeniedException", "Message": "A message"}},
                "BatchGetJobEntity",
            ),
            id="AccessDenied",
        ),
        pytest.param(
            ClientError(
                {"Error": {"Code": "ValidationException", "Message": "A message"}},
                "BatchGetJobEntity",
            ),
            id="Validation",
        ),
        pytest.param(Exception("Surprise!"), id="Arbitrary Exception"),
    ],
)
def test_raises_unrecoverable(
    client: MagicMock,
    farm_id: str,
    fleet_id: str,
    worker_id: str,
    exception: Exception,
    sleep_mock: MagicMock,
):
    # A test that the delete_worker() function will re-raise a ClientError
    # as unrecoverable whenever it gets one that shouldn't lead to a retry.

    # GIVEN
    client.batch_get_job_entity.side_effect = exception

    with pytest.raises(DeadlineRequestUnrecoverableError) as exc_context:
        # WHEN
        batch_get_job_entity(
            deadline_client=client,
            farm_id=farm_id,
            fleet_id=fleet_id,
            worker_id=worker_id,
            identifiers=SAMPLE_IDENTITIES,
        )

    # THEN
    assert exc_context.value.inner_exc is exception
    sleep_mock.assert_not_called()


@pytest.mark.parametrize(
    "exception",
    [
        pytest.param(
            ClientError(
                {"Error": {"Code": "ResourceNotFoundException", "Message": "A message"}},
                "BatchGetJobEntity",
            ),
            id="NotFound",
        ),
    ],
)
def test_raises_not_found(
    client: MagicMock,
    farm_id: str,
    fleet_id: str,
    worker_id: str,
    exception: Exception,
    sleep_mock: MagicMock,
):
    # A test that the delete_worker() function will re-raise a ClientError
    # as unrecoverable whenever it gets one that shouldn't lead to a retry.

    # GIVEN
    client.batch_get_job_entity.side_effect = exception

    with pytest.raises(DeadlineRequestWorkerNotFound) as exc_context:
        # WHEN
        batch_get_job_entity(
            deadline_client=client,
            farm_id=farm_id,
            fleet_id=fleet_id,
            worker_id=worker_id,
            identifiers=SAMPLE_IDENTITIES,
        )

    # THEN
    assert exc_context.value.inner_exc is exception
    sleep_mock.assert_not_called()
