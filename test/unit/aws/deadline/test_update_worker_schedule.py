# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from typing import Generator, Optional
from unittest.mock import MagicMock, patch
import pytest
from botocore.exceptions import (
    ClientError,
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    ReadTimeoutError,
    ProxyConnectionError,
    ResponseStreamingError,
    SSLError,
)


from deadline_worker_agent.api_models import UpdateWorkerScheduleResponse, UpdatedSessionActionInfo
from deadline_worker_agent.aws.deadline import (
    update_worker_schedule,
    DeadlineRequestInterrupted,
    DeadlineRequestUnrecoverableError,
    DeadlineRequestWorkerNotFound,
    DeadlineRequestWorkerOfflineError,
    _MAX_TRANSIENT_NETWORK_RETRIES,
)
import deadline_worker_agent.aws.deadline as deadline_mod


UPDATED_SESSION_ACTIONS: dict[str, UpdatedSessionActionInfo] = {
    "sessionaction-1234": {"completedStatus": "SUCCEEDED"}
}

SAMPLE_UPDATE_WORKER_SCHEDULE_RESPONSE: UpdateWorkerScheduleResponse = {
    "assignedSessions": {},
    "cancelSessionActions": {},
    "updateIntervalSeconds": 15,
}


@pytest.fixture
def sleep_mock() -> Generator[MagicMock, None, None]:
    with patch.object(deadline_mod, "sleep") as sleep_mock:
        yield sleep_mock


@pytest.mark.parametrize("updated_session_actions", [UPDATED_SESSION_ACTIONS, None])
def test_success(
    client: MagicMock,
    farm_id: str,
    fleet_id: str,
    worker_id: str,
    updated_session_actions: Optional[dict[str, UpdatedSessionActionInfo]],
) -> None:
    # Test the happy-path of the update_worker_schedule function.

    # GIVEN
    client.update_worker_schedule.return_value = SAMPLE_UPDATE_WORKER_SCHEDULE_RESPONSE

    # WHEN
    response = update_worker_schedule(
        deadline_client=client,
        farm_id=farm_id,
        fleet_id=fleet_id,
        worker_id=worker_id,
        updated_session_actions=updated_session_actions,
    )

    # THEN
    if updated_session_actions:
        client.update_worker_schedule.assert_called_once_with(
            farmId=farm_id,
            fleetId=fleet_id,
            workerId=worker_id,
            updatedSessionActions=updated_session_actions,
        )
    else:
        client.update_worker_schedule.assert_called_once_with(
            farmId=farm_id, fleetId=fleet_id, workerId=worker_id, updatedSessionActions=dict()
        )
    assert response == SAMPLE_UPDATE_WORKER_SCHEDULE_RESPONSE


def test_can_interrupt(
    client: MagicMock,
    farm_id: str,
    fleet_id: str,
    worker_id: str,
    sleep_mock: MagicMock,
):
    # A test that the update_worker_schedule() function will cease retries when the interrupt
    # event it set.

    # GIVEN
    event = MagicMock()
    event.is_set.side_effect = [False, True]
    throttle_exc = ClientError(
        {"Error": {"Code": "ThrottlingException", "Message": "A message"}},
        "UpdateWorkerSchedule",
    )
    client.update_worker_schedule.side_effect = [
        throttle_exc,
        throttle_exc,
        SAMPLE_UPDATE_WORKER_SCHEDULE_RESPONSE,
    ]

    # WHEN
    with pytest.raises(DeadlineRequestInterrupted):
        update_worker_schedule(
            deadline_client=client,
            farm_id=farm_id,
            fleet_id=fleet_id,
            worker_id=worker_id,
            interrupt_event=event,
        )

    # THEN
    assert client.update_worker_schedule.call_count == 1
    event.wait.assert_called_once()
    sleep_mock.assert_not_called()


@pytest.mark.parametrize(
    "exception,min_retry",
    [
        pytest.param(
            ClientError(
                {
                    "Error": {"Code": "ConflictException", "Message": "A message"},
                    "reason": "CONCURRENT_MODIFICATION",
                },
                "UpdateWorkerSchedule",
            ),
            None,
            id="ConcurrentMod",
        ),
        pytest.param(
            ClientError(
                {"Error": {"Code": "ThrottlingException", "Message": "A message"}},
                "UpdateWorkerSchedule",
            ),
            None,
            id="Throttling",
        ),
        pytest.param(
            ClientError(
                {"Error": {"Code": "InternalServerException", "Message": "A message"}},
                "UpdateWorkerSchedule",
            ),
            None,
            id="InternalServer",
        ),
        pytest.param(
            ClientError(
                {
                    "Error": {"Code": "ConflictException", "Message": "A message"},
                    "reason": "CONCURRENT_MODIFICATION",
                    "retryAfterSeconds": 30,
                },
                "UpdateWorkerSchedule",
            ),
            30,
            id="ConcurrentMod-minretry",
        ),
        pytest.param(
            ClientError(
                {
                    "Error": {"Code": "ThrottlingException", "Message": "A message"},
                    "retryAfterSeconds": 30,
                },
                "UpdateWorkerSchedule",
            ),
            30,
            id="Throttling-minretry",
        ),
        pytest.param(
            ClientError(
                {
                    "Error": {"Code": "InternalServerException", "Message": "A message"},
                    "retryAfterSeconds": 30,
                },
                "UpdateWorkerSchedule",
            ),
            30,
            id="InternalServer-minretry",
        ),
    ],
)
def test_retries_when_appropriate(
    client: MagicMock,
    farm_id: str,
    fleet_id: str,
    worker_id: str,
    exception: ClientError,
    min_retry: Optional[float],
    sleep_mock: MagicMock,
) -> None:
    # A test that the update_worker_schedule() function will retry calls to the API when:
    # 1. Throttled
    # 2. InternalServerException

    # GIVEN
    client.update_worker_schedule.side_effect = [exception, SAMPLE_UPDATE_WORKER_SCHEDULE_RESPONSE]

    # WHEN
    response = update_worker_schedule(
        deadline_client=client, farm_id=farm_id, fleet_id=fleet_id, worker_id=worker_id
    )

    # THEN
    assert response == SAMPLE_UPDATE_WORKER_SCHEDULE_RESPONSE
    assert client.update_worker_schedule.call_count == 2
    sleep_mock.assert_called_once()
    if min_retry is not None:
        assert min_retry <= sleep_mock.call_args.args[0] <= (min_retry + 0.2 * min_retry)


@pytest.mark.parametrize(
    "exception",
    [
        pytest.param(
            ClientError(
                {"Error": {"Code": "AccessDeniedException", "Message": "A message"}},
                "UpdateWorkerSchedule",
            ),
            id="AccessDenied",
        ),
        pytest.param(
            ClientError(
                {"Error": {"Code": "ValidationException", "Message": "A message"}},
                "UpdateWorkerSchedule",
            ),
            id="Validation",
        ),
        pytest.param(
            ClientError(
                {"Error": {"Code": "ConflictException", "Message": "A message"}, "code": "Unknown"},
                "UpdateWorkerSchedule",
            ),
            id="UnknownConflict",
        ),
        pytest.param(
            ClientError(
                {
                    "Error": {"Code": "ConflictException", "Message": "A message"},
                    "reason": "STATUS_CONFLICT",
                    "resourceId": "not-worker",
                },
                "UpdateWorkerSchedule",
            ),
            id="Conflict-NotWorker",
        ),
        pytest.param(
            Exception("Surprise!"),
            id="Arbitrary exception",
        ),
    ],
)
def test_raises_unrecoverable_error(
    client: MagicMock,
    farm_id: str,
    fleet_id: str,
    worker_id: str,
    exception: Exception,
    sleep_mock: MagicMock,
) -> None:
    # A test that update_worker_schedule() raises an UnrecoverableError when it's
    # appropriate.

    # GIVEN
    client.update_worker_schedule.side_effect = exception

    # WHEN
    with pytest.raises(DeadlineRequestUnrecoverableError) as exc_context:
        update_worker_schedule(
            deadline_client=client, farm_id=farm_id, fleet_id=fleet_id, worker_id=worker_id
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
                "UpdateWorkerSchedule",
            ),
            id="ResourceNotFound",
        ),
    ],
)
def test_raises_workernotfound_error(
    client: MagicMock,
    farm_id: str,
    fleet_id: str,
    worker_id: str,
    exception: Exception,
    sleep_mock: MagicMock,
) -> None:
    # A test that update_worker_schedule() raises a WorkerNotFound when it's
    # appropriate.

    # GIVEN
    client.update_worker_schedule.side_effect = exception

    # WHEN
    with pytest.raises(DeadlineRequestWorkerNotFound) as exc_context:
        update_worker_schedule(
            deadline_client=client, farm_id=farm_id, fleet_id=fleet_id, worker_id=worker_id
        )

    # THEN
    assert exc_context.value.inner_exc is exception
    sleep_mock.assert_not_called()


@pytest.fixture
def status_conflict(
    worker_id: str,
) -> ClientError:
    return ClientError(
        {
            "Error": {"Code": "ConflictException", "Message": "A message"},
            "reason": "STATUS_CONFLICT",
            "resourceId": worker_id,
        },
        "UpdateWorkerSchedule",
    )


def test_raises_worker_offline(
    client: MagicMock,
    farm_id: str,
    fleet_id: str,
    worker_id: str,
    status_conflict: ClientError,
    sleep_mock: MagicMock,
) -> None:
    # A test that update_worker_schedule() raises an WorkerOffline when it's
    # appropriate.

    # GIVEN
    client.update_worker_schedule.side_effect = status_conflict

    # WHEN
    with pytest.raises(DeadlineRequestWorkerOfflineError) as exc_context:
        update_worker_schedule(
            deadline_client=client, farm_id=farm_id, fleet_id=fleet_id, worker_id=worker_id
        )

    # THEN
    assert exc_context.value.inner_exc is status_conflict
    sleep_mock.assert_not_called()


@pytest.mark.parametrize(
    "exception",
    [
        pytest.param(
            ConnectionClosedError(
                endpoint_url="https://scheduling.deadline.us-west-2.amazonaws.com"
            ),
            id="ConnectionClosed",
        ),
        pytest.param(
            ConnectTimeoutError(endpoint_url="https://scheduling.deadline.us-west-2.amazonaws.com"),
            id="ConnectTimeout",
        ),
        pytest.param(
            ReadTimeoutError(endpoint_url="https://scheduling.deadline.us-west-2.amazonaws.com"),
            id="ReadTimeout",
        ),
        pytest.param(
            EndpointConnectionError(
                endpoint_url="https://scheduling.deadline.us-west-2.amazonaws.com"
            ),
            id="EndpointConnection",
        ),
        # The following are covered by catching botocore's transport base classes
        # (HTTPClientError / ConnectionError) rather than enumerated leaf types.
        pytest.param(
            SSLError(
                endpoint_url="https://scheduling.deadline.us-west-2.amazonaws.com",
                error="handshake failure",
            ),
            id="SSLError",
        ),
        pytest.param(
            ProxyConnectionError(proxy_url="https://proxy.internal:8080"),
            id="ProxyConnection",
        ),
        pytest.param(
            ResponseStreamingError(error="connection reset mid-stream"),
            id="ResponseStreaming",
        ),
    ],
)
def test_retries_transient_network_errors(
    client: MagicMock,
    farm_id: str,
    fleet_id: str,
    worker_id: str,
    exception: Exception,
    sleep_mock: MagicMock,
) -> None:
    # GIVEN
    client.update_worker_schedule.side_effect = [exception, SAMPLE_UPDATE_WORKER_SCHEDULE_RESPONSE]

    # WHEN
    response = update_worker_schedule(
        deadline_client=client, farm_id=farm_id, fleet_id=fleet_id, worker_id=worker_id
    )

    # THEN
    assert response == SAMPLE_UPDATE_WORKER_SCHEDULE_RESPONSE
    assert client.update_worker_schedule.call_count == 2
    sleep_mock.assert_called_once()


def test_transient_network_error_raises_after_max_retries(
    client: MagicMock,
    farm_id: str,
    fleet_id: str,
    worker_id: str,
    sleep_mock: MagicMock,
) -> None:
    # GIVEN - network error persists beyond max retries
    client.update_worker_schedule.side_effect = ConnectionClosedError(
        endpoint_url="https://scheduling.deadline.us-west-2.amazonaws.com"
    )

    # WHEN
    with pytest.raises(DeadlineRequestUnrecoverableError):
        update_worker_schedule(
            deadline_client=client, farm_id=farm_id, fleet_id=fleet_id, worker_id=worker_id
        )

    # THEN - retried max times then gave up
    assert client.update_worker_schedule.call_count == _MAX_TRANSIENT_NETWORK_RETRIES + 1
    assert sleep_mock.call_count == _MAX_TRANSIENT_NETWORK_RETRIES


def test_transient_network_error_wait_is_interruptible(
    client: MagicMock,
    farm_id: str,
    fleet_id: str,
    worker_id: str,
    sleep_mock: MagicMock,
) -> None:
    # Test that a transient network error backoff uses the interruptible wait
    # (interrupt_event.wait) rather than an uninterruptible sleep, and that a set
    # interrupt_event breaks out of the retry loop with DeadlineRequestInterrupted.

    # GIVEN - a persistent network error, and an interrupt_event that becomes set
    # after the first backoff wait (simulating a shutdown/drain during the wait).
    client.update_worker_schedule.side_effect = ConnectionClosedError(
        endpoint_url="https://scheduling.deadline.us-west-2.amazonaws.com"
    )
    interrupt_event = MagicMock()
    # Not set on the initial loop check, then set after the first wait() returns.
    interrupt_event.is_set.side_effect = [False, True]

    # WHEN
    with pytest.raises(DeadlineRequestInterrupted):
        update_worker_schedule(
            deadline_client=client,
            farm_id=farm_id,
            fleet_id=fleet_id,
            worker_id=worker_id,
            interrupt_event=interrupt_event,
        )

    # THEN - the interruptible wait was used (not the plain sleep), and we bailed out.
    interrupt_event.wait.assert_called_once()
    sleep_mock.assert_not_called()
    assert client.update_worker_schedule.call_count == 1
