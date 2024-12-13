# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from typing import Generator, Optional
from unittest.mock import MagicMock, patch, call
import pytest
from botocore.exceptions import ClientError

from deadline_worker_agent.aws.deadline import (
    update_worker,
    DeadlineRequestInterrupted,
    DeadlineRequestUnrecoverableError,
    DeadlineRequestConditionallyRecoverableError,
)

import deadline_worker_agent.aws.deadline as deadline_mod
from deadline_worker_agent.config import Configuration
from deadline_worker_agent.api_models import (
    LogConfiguration,
    HostProperties,
    IpAddresses,
    WorkerStatus,
)
from deadline_worker_agent.config.cli_args import ParsedCommandLineArguments
from deadline_worker_agent.log_sync.cloudwatch import (
    LOG_CONFIG_OPTION_GROUP_NAME_KEY,
    LOG_CONFIG_OPTION_STREAM_NAME_KEY,
)

CLOUDWATCH_LOG_GROUP = "log-group"
CLOUDWATCH_LOG_STREAM = "log-stream"
AWSLOGS_LOG_CONFIGURATION = LogConfiguration(
    logDriver="awslogs",
    options={
        LOG_CONFIG_OPTION_GROUP_NAME_KEY: CLOUDWATCH_LOG_GROUP,
        LOG_CONFIG_OPTION_STREAM_NAME_KEY: CLOUDWATCH_LOG_STREAM,
    },
)
HOST_PROPERTIES = HostProperties(
    ipAddresses=IpAddresses(
        ipV4Addresses=["127.0.0.1", "192.168.1.100"],
        ipV6Addresses=["::1", "fe80:0000:0000:0000:c685:08ff:fe45:0641"],
    ),
)


@pytest.fixture
def config(
    farm_id: str,
    fleet_id: str,
    # Specified to avoid any impact from an existing worker agent config file in the development
    # environment
    mock_config_file_not_found: MagicMock,
) -> Generator[Configuration, None, None]:
    cli_args = ParsedCommandLineArguments()
    cli_args.farm_id = farm_id
    cli_args.fleet_id = fleet_id
    cli_args.run_jobs_as_agent_user = True
    cli_args.no_shutdown = True
    cli_args.profile = "profilename"
    cli_args.verbose = False
    cli_args.allow_instance_profile = True
    config = Configuration(parsed_cli_args=cli_args)

    # We patch the Path attributes to prevent real file-system operations when testing
    with (
        patch.object(config, "worker_persistence_dir"),
        patch.object(config, "worker_credentials_dir"),
        patch.object(config, "worker_state_file"),
    ):
        yield config


@pytest.fixture
def sleep_mock() -> Generator[MagicMock, None, None]:
    with patch.object(deadline_mod, "sleep") as sleep_mock:
        yield sleep_mock


@pytest.mark.parametrize(
    "status, host_properties",
    [
        pytest.param(status, host_properties)
        for status in (
            WorkerStatus.STARTED,
            WorkerStatus.STOPPED,
            WorkerStatus.STOPPING,
        )
        for host_properties in (HOST_PROPERTIES, None)
    ],
)
def test_success(
    client: MagicMock,
    config: Configuration,
    worker_id: str,
    status: WorkerStatus,
    host_properties: Optional[HostProperties],
) -> None:
    # Test the happy-path of the update_worker function.

    # GIVEN
    expected_response = {"log": AWSLOGS_LOG_CONFIGURATION}
    client.update_worker.return_value = expected_response

    # WHEN
    response = update_worker(
        deadline_client=client,
        farm_id=config.farm_id,
        fleet_id=config.fleet_id,
        worker_id=worker_id,
        status=status,
        capabilities=config.capabilities,
        host_properties=host_properties,
    )

    # THEN
    assert response is expected_response
    if host_properties:
        client.update_worker.assert_called_once_with(
            farmId=config.farm_id,
            fleetId=config.fleet_id,
            workerId=worker_id,
            capabilities=config.capabilities.for_update_worker(),
            status=status.value,
            hostProperties=host_properties,
        )
    else:
        client.update_worker.assert_called_once_with(
            farmId=config.farm_id,
            fleetId=config.fleet_id,
            workerId=worker_id,
            capabilities=config.capabilities.for_update_worker(),
            status=status.value,
        )


def test_can_interrupt(
    client: MagicMock,
    config: Configuration,
    worker_id: str,
    sleep_mock: MagicMock,
) -> None:
    # A test that the update_worker() function will cease retries when the interrupt
    # event it set.

    # GIVEN
    event = MagicMock()
    event.is_set.side_effect = [False, True]
    dummy_response = {"log": AWSLOGS_LOG_CONFIGURATION}
    throttle_exc = ClientError(
        {"Error": {"Code": "ThrottlingException", "Message": "A message"}},
        "UpdateWorker",
    )
    client.update_worker.side_effect = [
        throttle_exc,
        throttle_exc,
        dummy_response,
    ]

    # WHEN
    with pytest.raises(DeadlineRequestInterrupted):
        update_worker(
            deadline_client=client,
            farm_id=config.farm_id,
            fleet_id=config.fleet_id,
            worker_id=worker_id,
            status=WorkerStatus.STOPPING,
            interrupt_event=event,
        )

    # THEN
    assert client.update_worker.call_count == 1
    event.wait.assert_called_once()
    sleep_mock.assert_not_called()


@pytest.mark.parametrize("conflict_status", ["STOPPING", "NOT_COMPATIBLE"])
def test_updates_to_stopped_if_required(
    client: MagicMock,
    config: Configuration,
    worker_id: str,
    sleep_mock: MagicMock,
    conflict_status: str,
):
    # If we're trying to update to STARTED, but are already STOPPING or NOT_COMPATIBLE then
    # the API contract dictates that an exception will be raised and we should first transition
    # to STOPPED before proceeding to STARTED.
    # This is because a status transition from those statuses to STARTED is not allowed.

    # GIVEN
    stopped_response = dict[str, str]()
    expected_started_response = {"log": AWSLOGS_LOG_CONFIGURATION}
    # The exception that signals that we should go to STOPPED
    conflict_exception = ClientError(
        {
            "Error": {"Code": "ConflictException", "Message": "A message"},
            "reason": "STATUS_CONFLICT",
            "resourceId": worker_id,
            "context": {"status": conflict_status},
        },
        "UpdateWorker",
    )
    client.update_worker.side_effect = [
        conflict_exception,
        stopped_response,
        expected_started_response,
    ]

    # WHEN
    response = update_worker(
        deadline_client=client,
        farm_id=config.farm_id,
        fleet_id=config.fleet_id,
        worker_id=worker_id,
        status=WorkerStatus.STARTED,
    )

    # THEN
    assert response is expected_started_response
    client.update_worker.assert_has_calls(
        (
            call(
                farmId=config.farm_id,
                fleetId=config.fleet_id,
                workerId=worker_id,
                status=WorkerStatus.STARTED.value,
            ),
            call(
                farmId=config.farm_id,
                fleetId=config.fleet_id,
                workerId=worker_id,
                status=WorkerStatus.STOPPED.value,
            ),
            call(
                farmId=config.farm_id,
                fleetId=config.fleet_id,
                workerId=worker_id,
                status=WorkerStatus.STARTED.value,
            ),
        )
    )
    sleep_mock.assert_not_called()


@pytest.mark.parametrize(
    "target_status, conflict_status",
    [
        pytest.param(
            target_status,
            conflict_status,
        )
        for target_status in (WorkerStatus.STOPPED, WorkerStatus.STOPPING)
        for conflict_status in ("STOPPING", "NOT_COMPATIBLE")
    ],
)
def test_does_not_recurse_if_not_started(
    client: MagicMock,
    config: Configuration,
    worker_id: str,
    sleep_mock: MagicMock,
    target_status: WorkerStatus,
    conflict_status: str,
):
    # This is the negative test to complement test_updates_to_stopped_if_required()
    # It makes sure that we only do the recursive call if we're initially trying to
    # go to STARTED, rather than some other status, but still get the same exception
    # somehow. Specifically, that we raise an unrecoverable error if we hit this.
    # Won't ever happen, but it ensures that our logic is sound.

    # GIVEN
    conflict_exception = ClientError(
        {
            "Error": {"Code": "ConflictException", "Message": "A message"},
            "reason": "STATUS_CONFLICT",
            "resourceId": worker_id,
            "context": {"status": conflict_status},
        },
        "UpdateWorker",
    )
    client.update_worker.side_effect = (conflict_exception,)

    # WHEN
    with pytest.raises(DeadlineRequestUnrecoverableError) as exc_context:
        update_worker(
            deadline_client=client,
            farm_id=config.farm_id,
            fleet_id=config.fleet_id,
            worker_id=worker_id,
            status=target_status,
        )

    # THEN
    assert exc_context.value.inner_exc is conflict_exception
    sleep_mock.assert_not_called()


def test_reraises_when_updates_to_stopped(
    client: MagicMock,
    config: Configuration,
    worker_id: str,
    sleep_mock: MagicMock,
):
    # Another complement to test_updates_to_stopped_if_required() that ensures that if the recursive call
    # raises an exception then we reraise that as unrecoverable.

    # GIVEN
    conflict_exception = ClientError(
        {
            "Error": {"Code": "ConflictException", "Message": "A message"},
            "reason": "STATUS_CONFLICT",
            "resourceId": worker_id,
            "context": {"status": "STOPPING"},
        },
        "UpdateWorker",
    )
    recursive_exception = Exception("Inner exception")
    client.update_worker.side_effect = [
        conflict_exception,
        recursive_exception,
    ]

    # WHEN
    with pytest.raises(DeadlineRequestUnrecoverableError) as exc_context:
        update_worker(
            deadline_client=client,
            farm_id=config.farm_id,
            fleet_id=config.fleet_id,
            worker_id=worker_id,
            status=WorkerStatus.STARTED,
        )

    # THEN
    assert exc_context.value.inner_exc is recursive_exception
    client.update_worker.assert_has_calls(
        (
            call(
                farmId=config.farm_id,
                fleetId=config.fleet_id,
                workerId=worker_id,
                status=WorkerStatus.STARTED.value,
            ),
            call(
                farmId=config.farm_id,
                fleetId=config.fleet_id,
                workerId=worker_id,
                status=WorkerStatus.STOPPED.value,
            ),
        )
    )
    sleep_mock.assert_not_called()


@pytest.mark.parametrize(
    "exception,min_retry",
    [
        pytest.param(
            ClientError(
                {"Error": {"Code": "ThrottlingException", "Message": "A message"}}, "UpdateWorker"
            ),
            None,
            id="Throttling",
        ),
        pytest.param(
            ClientError(
                {"Error": {"Code": "InternalServerException", "Message": "A message"}},
                "UpdateWorker",
            ),
            None,
            id="InternalServer",
        ),
        pytest.param(
            ClientError(
                {
                    "Error": {"Code": "ThrottlingException", "Message": "A message"},
                    "retryAfterSeconds": 30,
                },
                "UpdateWorker",
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
                "UpdateWorker",
            ),
            30,
            id="InternalServer-minretry",
        ),
        pytest.param(
            ClientError(
                {
                    "Error": {"Code": "ConflictException", "Message": "A message"},
                    "reason": "CONCURRENT_MODIFICATION",
                },
                "UpdateWorker",
            ),
            None,
            id="Conflict-CONCURRENT_MODIFICATION",
        ),
        pytest.param(
            ClientError(
                {
                    "Error": {"Code": "ConflictException", "Message": "A message"},
                    "reason": "STATUS_CONFLICT",
                    # This must match the value of the worker_id
                    # TODO: find a way to use the fixture to avoid this becoming out of sync
                    "resourceId": "worker-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "context": {"status": "ASSOCIATED"},
                },
                "UpdateWorker",
            ),
            None,
            id="Conflict-STATUS_CONFLICT-worker-ASSOCIATED",
        ),
    ],
)
def test_retries_when_appropriate(
    client: MagicMock,
    config: Configuration,
    worker_id: str,
    sleep_mock: MagicMock,
    exception: ClientError,
    min_retry: Optional[float],
):
    # A test that the update_worker() function will retry calls to the API when:
    # 1. Throttled
    # 2. InternalServerException
    # 3. ConflictException with CONCURRENT_MODIFICATION
    # 4. ConflictException with STATUS_CONFLICT & ASSOCIATED status on the worker_id

    # GIVEN
    expected_response = {"log": AWSLOGS_LOG_CONFIGURATION}
    client.update_worker.side_effect = [exception, expected_response]

    # WHEN
    response = update_worker(
        deadline_client=client,
        farm_id=config.farm_id,
        fleet_id=config.fleet_id,
        worker_id=worker_id,
        status=WorkerStatus.STARTED,
    )

    # THEN
    assert response is expected_response
    assert client.update_worker.call_count == 2
    sleep_mock.assert_called_once()
    if min_retry is not None:
        assert min_retry <= sleep_mock.call_args.args[0] <= (min_retry + 0.2 * min_retry)


def test_not_found_raises_conditionally_recoverable(
    client: MagicMock,
    config: Configuration,
    worker_id: str,
    sleep_mock: MagicMock,
):
    # A test that the update_worker() function will re-raise a ResourceNotFoundException
    # ClientError as conditionally recoverable.
    # If the Agent is trying to transition to STARTED during bootstrapping and gets a
    # ResourceNotFoundException, then it can recover by creating a new Worker.

    # GIVEN
    exception = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "A message"}}, "UpdateWorker"
    )
    client.update_worker.side_effect = exception

    with pytest.raises(DeadlineRequestConditionallyRecoverableError) as exc_context:
        # WHEN
        update_worker(
            deadline_client=client,
            farm_id=config.farm_id,
            fleet_id=config.fleet_id,
            worker_id=worker_id,
            status=WorkerStatus.STARTED,
        )

    # THEN
    assert exc_context.value.inner_exc == exception
    sleep_mock.assert_not_called()


@pytest.mark.parametrize(
    "exception",
    [
        pytest.param(
            # To make sure that we handle unknown exceptions that aren't currently in the model.
            ClientError(
                {"Error": {"Code": "NotARealException", "Message": "A message"}}, "UpdateWorker"
            ),
            id="NotReal",
        ),
        pytest.param(
            ClientError(
                {"Error": {"Code": "AccessDeniedException", "Message": "A message"}}, "UpdateWorker"
            ),
            id="AccessDenied",
        ),
        pytest.param(
            ClientError(
                {"Error": {"Code": "ValidationException", "Message": "A message"}}, "UpdateWorker"
            ),
            id="Validation",
        ),
        pytest.param(
            ClientError(
                {
                    "Error": {"Code": "ConflictException", "Message": "A message"},
                    "reason": "UnhandledReason",
                },
                "UpdateWorker",
            ),
            id="Conflict-Unhandled",
        ),
        pytest.param(
            ClientError(
                {
                    "Error": {"Code": "ConflictException", "Message": "A message"},
                    "reason": "STATUS_CONFLICT",
                    # This must match the value of config.fleet_id
                    # TODO: find a way to use the config fixture to avoid this becoming out of sync
                    "resourceId": "fleet-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "context": {"status": "DELETE_IN_PROGRESS"},
                },
                "UpdateWorker",
            ),
            id="Conflict-STATUS_CONFLICT-not-worker",
        ),
    ]
    + [
        # None of these status's are actually possible as STATUS_CONFLICT exceptions, but we're testing the unexpected.
        pytest.param(
            ClientError(
                {
                    "Error": {"Code": "ConflictException", "Message": "A message"},
                    "reason": "STATUS_CONFLICT",
                    # This must match the value of the worker_id
                    # TODO: find a way to use the fixture to avoid this becoming out of sync
                    "resourceId": "worker-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "context": {"status": status},
                },
                "UpdateWorker",
            ),
            id=f"Conflict-STATUS_CONFLICT-{status}",
        )
        for status in (
            "CREATED",
            "STARTED",
            "STALLED",
            "DELETED",
        )
    ],
)
def test_raises_unrecoverable_error(
    client: MagicMock,
    config: Configuration,
    worker_id: str,
    exception: ClientError,
    sleep_mock: MagicMock,
):
    # A test that the update_worker() function will re-raise a ClientError
    # whenever it gets one that shouldn't lead to a retry.

    # GIVEN
    client.update_worker.side_effect = exception

    with pytest.raises(DeadlineRequestUnrecoverableError) as exc_context:
        # WHEN
        update_worker(
            deadline_client=client,
            farm_id=config.farm_id,
            fleet_id=config.fleet_id,
            worker_id=worker_id,
            status=WorkerStatus.STARTED,
        )

    # THEN
    assert exc_context.value.inner_exc == exception
    sleep_mock.assert_not_called()


def test_raises_unexpected_exception(
    client: MagicMock,
    config: Configuration,
    worker_id: str,
    sleep_mock: MagicMock,
):
    # A test that the update_worker() function will re-raise a ClientError
    # whenever it gets one that shouldn't lead to a retry.

    # GIVEN
    exception = Exception("Surprise!")
    client.update_worker.side_effect = exception

    with pytest.raises(DeadlineRequestUnrecoverableError) as exc_context:
        # WHEN
        update_worker(
            deadline_client=client,
            farm_id=config.farm_id,
            fleet_id=config.fleet_id,
            worker_id=worker_id,
            status=WorkerStatus.STARTED,
        )

    # THEN
    assert exc_context.value.inner_exc == exception
    sleep_mock.assert_not_called()
