# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from typing import Generator
from datetime import datetime, timedelta
from dateutil.tz import tzutc
from unittest.mock import MagicMock, patch
import pytest
from botocore.exceptions import ClientError
from time import monotonic

from deadline_worker_agent.aws.deadline import (
    assume_queue_role_for_worker,
    DeadlineRequestConditionallyRecoverableError,
    DeadlineRequestInterrupted,
    DeadlineRequestUnrecoverableError,
    DeadlineRequestWorkerOfflineError,
)

import deadline_worker_agent.aws.deadline as deadline_mod
from deadline_worker_agent.api_models import AssumeQueueRoleForWorkerResponse, AwsCredentials


@pytest.fixture
def deadline_credentials() -> AwsCredentials:
    return AwsCredentials(
        accessKeyId="accessKeyId",
        expiration=datetime.utcnow().astimezone(tzutc()) + timedelta(hours=1),
        secretAccessKey="secretAccessKey",
        sessionToken="sessionToken",
    )


@pytest.fixture
def mock_assume_queue_role_for_worker_response(
    deadline_credentials: AwsCredentials,
) -> AssumeQueueRoleForWorkerResponse:
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
    queue_id: str,
    mock_assume_queue_role_for_worker_response: AssumeQueueRoleForWorkerResponse,
) -> None:
    # Simple success-case that we return the response of the API request.

    # GIVEN
    client.assume_queue_role_for_worker.return_value = mock_assume_queue_role_for_worker_response

    # WHEN
    response = assume_queue_role_for_worker(
        deadline_client=client,
        farm_id=farm_id,
        fleet_id=fleet_id,
        worker_id=worker_id,
        queue_id=queue_id,
    )

    # THEN
    assert response == mock_assume_queue_role_for_worker_response
    client.assume_queue_role_for_worker.assert_called_once_with(
        farmId=farm_id, fleetId=fleet_id, workerId=worker_id, queueId=queue_id
    )


def test_can_interrupt(
    client: MagicMock,
    farm_id: str,
    fleet_id: str,
    worker_id: str,
    queue_id: str,
    mock_assume_queue_role_for_worker_response: AssumeQueueRoleForWorkerResponse,
    sleep_mock: MagicMock,
):
    # A test that the assume_queue_role_for_worker() function will
    # retry calls to the API when throttled.

    # GIVEN
    event = MagicMock()
    event.is_set.side_effect = [False, True]
    throttle_exc = ClientError(
        {"Error": {"Code": "ThrottlingException", "Message": "A message"}},
        "AssumeQueueRoleForWorker",
    )
    client.assume_queue_role_for_worker.side_effect = [
        throttle_exc,
        throttle_exc,
        mock_assume_queue_role_for_worker_response,
    ]

    # WHEN
    with pytest.raises(DeadlineRequestInterrupted):
        assume_queue_role_for_worker(
            deadline_client=client,
            farm_id=farm_id,
            fleet_id=fleet_id,
            worker_id=worker_id,
            queue_id=queue_id,
            interrupt_event=event,
        )

    # THEN
    assert client.assume_queue_role_for_worker.call_count == 1
    event.wait.assert_called_once()
    sleep_mock.assert_not_called()


@pytest.mark.parametrize("exception_code", ["ThrottlingException", "InternalServerException"])
def test_retries_when_throttled(
    client: MagicMock,
    farm_id: str,
    fleet_id: str,
    worker_id: str,
    queue_id: str,
    mock_assume_queue_role_for_worker_response: AssumeQueueRoleForWorkerResponse,
    exception_code: str,
    sleep_mock: MagicMock,
):
    # A test that the assume_queue_role_for_worker() function will
    # retry calls to the API when throttled.

    # GIVEN
    exc = ClientError(
        {"Error": {"Code": exception_code, "Message": "A message"}}, "AssumeQueueRoleForWorker"
    )
    client.assume_queue_role_for_worker.side_effect = [
        exc,
        mock_assume_queue_role_for_worker_response,
    ]

    # WHEN
    response = assume_queue_role_for_worker(
        deadline_client=client,
        farm_id=farm_id,
        fleet_id=fleet_id,
        worker_id=worker_id,
        queue_id=queue_id,
    )

    # THEN
    assert response == mock_assume_queue_role_for_worker_response
    assert client.assume_queue_role_for_worker.call_count == 2
    sleep_mock.assert_called_once()


def test_limited_retries_when_queue_in_conflict(
    client: MagicMock,
    farm_id: str,
    fleet_id: str,
    worker_id: str,
    queue_id: str,
    sleep_mock: MagicMock,
):
    # A test that when we recieve a ConflictException[STATUS_CONFLICT] for the Queue's status,
    # then we retry some limited number of times up to a defined time threshold.

    # GIVEN
    exc = ClientError(
        {
            "Error": {"Code": "ConflictException", "Message": "A message"},
            "reason": "STATUS_CONFLICT",
            "resourceId": queue_id,
        },
        "AssumeQueueRoleForWorker",
    )
    # We want to see the exceptions thrice -- once to test that we retry, and the second to test that
    # we still retry, and the third to test that we bail when we've hit the timeout
    client.assume_queue_role_for_worker.side_effect = [exc, exc, exc]

    with patch.object(
        deadline_mod,
        "_assume_queue_role_for_worker_eventual_consistency_time_elapsed",
        MagicMock(side_effect=[False, False, True]),
    ):
        # WHEN
        with pytest.raises(DeadlineRequestConditionallyRecoverableError) as exc_context:
            assume_queue_role_for_worker(
                deadline_client=client,
                farm_id=farm_id,
                fleet_id=fleet_id,
                worker_id=worker_id,
                queue_id=queue_id,
            )

    # THEN
    assert exc_context.value.inner_exc is exc
    assert client.assume_queue_role_for_worker.call_count == 3
    assert sleep_mock.call_count == 2


@pytest.mark.parametrize(
    "exception",
    [
        pytest.param(
            ClientError(
                {"Error": {"Code": "AccessDeniedException", "Message": "A message"}},
                "AssumeQueueRoleForWorker",
            ),
            id="AccessDenied",
        ),
        pytest.param(
            ClientError(
                {"Error": {"Code": "ValidationException", "Message": "A message"}},
                "AssumeQueueRoleForWorker",
            ),
            id="Validation",
        ),
        pytest.param(
            ClientError(
                {
                    "Error": {"Code": "ConflictException", "Message": "A message"},
                },
                "AssumeQueueRoleForWorker",
            ),
            id="Conflict",
        ),
        pytest.param(
            ClientError(
                {
                    "Error": {"Code": "ConflictException", "Message": "A message"},
                    "reason": "STATUS_CONFLICT",
                    "resourceId": "not-worker-or-queue",
                },
                "AssumeQueueRoleForWorker",
            ),
            id="UnknownSTATUS_CONFLICT",
        ),
        pytest.param(
            ClientError(
                {
                    "Error": {"Code": "ConflictException", "Message": "A message"},
                    "reason": "UnknownReason",
                },
                "AssumeQueueRoleForWorker",
            ),
            id="UnknownConflict",
        ),
    ],
)
def test_raises_conditionally_recoverable(
    client: MagicMock,
    farm_id: str,
    fleet_id: str,
    worker_id: str,
    queue_id: str,
    exception: ClientError,
    sleep_mock: MagicMock,
):
    # Test that the expected exceptions raise a ConditionallyRecoverable exception.
    # These are the cases that lead to "When Failing to Obtain Session AWS Credentials"
    # in the Worker API Contract that are not covered by other tests.

    # GIVEN
    client.assume_queue_role_for_worker.side_effect = exception

    # WHEN
    with pytest.raises(DeadlineRequestConditionallyRecoverableError) as exc_context:
        assume_queue_role_for_worker(
            deadline_client=client,
            farm_id=farm_id,
            fleet_id=fleet_id,
            worker_id=worker_id,
            queue_id=queue_id,
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
                "AssumeQueueRoleForWorker",
            ),
            id="NotFound",
        ),
        pytest.param(
            # This one's actually impossible, but is the only broad category of ClientError that
            # isn't handled. Tests the fall-through case.
            ClientError(
                {"Error": {"Code": "ServiceQuotaExceededException", "Message": "A message"}},
                "AssumeQueueRoleForWorker",
            ),
            id="ServiceQuota",
        ),
        pytest.param(
            # Make sure that we expect the Spanish Inquisition
            Exception("The Spanish Inquisition"),
            id="GenericException",
        ),
    ],
)
def test_raises_unrecoverable(
    client: MagicMock,
    farm_id: str,
    fleet_id: str,
    worker_id: str,
    queue_id: str,
    exception: Exception,
    sleep_mock: MagicMock,
):
    # Test that the expected exceptions raise an Unrecoverable exception.
    # These are the terminal cases where we need to fail corresponding session actions.

    # GIVEN
    client.assume_queue_role_for_worker.side_effect = exception

    # WHEN
    with pytest.raises(DeadlineRequestUnrecoverableError) as exc_context:
        assume_queue_role_for_worker(
            deadline_client=client,
            farm_id=farm_id,
            fleet_id=fleet_id,
            worker_id=worker_id,
            queue_id=queue_id,
        )

    # THEN
    assert exc_context.value.inner_exc is exception
    sleep_mock.assert_not_called()


def test_raises_worker_offline(
    client: MagicMock,
    farm_id: str,
    fleet_id: str,
    worker_id: str,
    queue_id: str,
    sleep_mock: MagicMock,
):
    # Test that when a ConflictException[STATUS_CONFLICT] indicates that the Worker is no longer
    # considered online, then we raise a WorkerOfflineError.

    # GIVEN
    exc = ClientError(
        {
            "Error": {"Code": "ConflictException", "Message": "A message"},
            "reason": "STATUS_CONFLICT",
            "resourceId": worker_id,
        },
        "AssumeQueueRoleForWorker",
    )
    client.assume_queue_role_for_worker.side_effect = exc

    # WHEN
    with pytest.raises(DeadlineRequestWorkerOfflineError) as exc_context:
        assume_queue_role_for_worker(
            deadline_client=client,
            farm_id=farm_id,
            fleet_id=fleet_id,
            worker_id=worker_id,
            queue_id=queue_id,
        )

    # THEN
    assert exc_context.value.inner_exc is exc
    sleep_mock.assert_not_called()


def test_time_elapsed():
    # A test that _assume_queue_role_for_worker_eventual_consistency_time_elapsed is implemented correctly.

    # GIVEN
    start_time = monotonic()
    epsilon = 1e6
    ten_seconds = start_time + 10 - epsilon
    over_ten_seconds = start_time + 10.01

    # WHEN
    boundary_return = deadline_mod._assume_queue_role_for_worker_eventual_consistency_time_elapsed(
        start_time, ten_seconds
    )
    over_boundary_return = (
        deadline_mod._assume_queue_role_for_worker_eventual_consistency_time_elapsed(
            start_time, over_ten_seconds
        )
    )

    # THEN
    assert not boundary_return
    assert over_boundary_return
