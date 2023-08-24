# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from typing import Generator, Optional
from unittest.mock import MagicMock, patch
import pytest
from botocore.exceptions import ClientError


from deadline_worker_agent.api_models import UpdateWorkerScheduleResponse, UpdatedSessionActionInfo
from deadline_worker_agent.aws.deadline import (
    update_worker_schedule,
    DeadlineRequestInterrupted,
    DeadlineRequestUnrecoverableError,
    DeadlineRequestWorkerNotFound,
    DeadlineRequestWorkerOfflineError,
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
    # A test that the update_worker_schedule() function will
    # retry calls to the API when throttled.

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
    "exception",
    [
        pytest.param(
            ClientError(
                {"Error": {"Code": "ThrottlingException", "Message": "A message"}},
                "UpdateWorkerSchedule",
            ),
            id="Throttling",
        ),
        pytest.param(
            ClientError(
                {"Error": {"Code": "InternalServerException", "Message": "A message"}},
                "UpdateWorkerSchedule",
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
