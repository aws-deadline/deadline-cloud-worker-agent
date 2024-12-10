# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from typing import Generator, Optional
from unittest.mock import MagicMock, patch
import pytest
from botocore.exceptions import ClientError


from deadline_worker_agent.api_models import CreateWorkerResponse, HostProperties
from deadline_worker_agent.aws.deadline import (
    create_worker,
    DeadlineRequestUnrecoverableError,
)
from deadline_worker_agent.config import Configuration
from deadline_worker_agent.config.cli_args import ParsedCommandLineArguments
import deadline_worker_agent.aws.deadline as deadline_mod


@pytest.fixture
def mock_create_worker_response(worker_id: str) -> CreateWorkerResponse:
    return {
        "workerId": worker_id,
    }


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


def test_success(
    client: MagicMock,
    config: Configuration,
    mock_create_worker_response: CreateWorkerResponse,
    host_properties: HostProperties,
) -> None:
    # Test the happy-path of the create_worker function.

    # GIVEN
    client.create_worker.return_value = mock_create_worker_response

    # WHEN
    response = create_worker(deadline_client=client, config=config, host_properties=host_properties)

    # THEN
    client.create_worker.assert_called_once_with(
        farmId=config.farm_id, fleetId=config.fleet_id, hostProperties=host_properties
    )
    assert response == mock_create_worker_response


@pytest.mark.parametrize(
    "exception,min_retry",
    [
        pytest.param(
            ClientError(
                {"Error": {"Code": "ThrottlingException", "Message": "A message"}}, "CreateWorker"
            ),
            None,
            id="Throttling",
        ),
        pytest.param(
            ClientError(
                {"Error": {"Code": "InternalServerException", "Message": "A message"}},
                "CreateWorker",
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
                "CreateWorker",
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
                "CreateWorker",
            ),
            30,
            id="InternalServer-minretry",
        ),
        pytest.param(
            ClientError(
                {
                    "Error": {"Code": "ConflictException", "Message": "A message"},
                    "reason": "STATUS_CONFLICT",
                    # This must match the value of config.fleet_id
                    # TODO: find a way to use the config fixture to avoid this becoming out of sync
                    "resourceId": "fleet-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "context": {"status": "CREATE_IN_PROGRESS"},
                },
                "CreateWorker",
            ),
            None,
            id="Fleet-CREATE_IN_PROGRESS",
        ),
    ],
)
def test_retries_when_appropriate(
    client: MagicMock,
    config: Configuration,
    mock_create_worker_response: CreateWorkerResponse,
    host_properties: HostProperties,
    exception: ClientError,
    min_retry: Optional[float],
    sleep_mock: MagicMock,
):
    # A test that the create_worker() function will retry calls to the API when:
    # 1. Throttled
    # 2. InternalServerException
    # 3. The Fleet is still CREATE_IN_PROGRESS

    # GIVEN
    client.create_worker.side_effect = [exception, mock_create_worker_response]

    # WHEN
    response = create_worker(deadline_client=client, config=config, host_properties=host_properties)

    # THEN
    assert response == mock_create_worker_response
    assert client.create_worker.call_count == 2
    sleep_mock.assert_called_once()
    if min_retry is not None:
        assert min_retry <= sleep_mock.call_args.args[0] <= (min_retry + 0.2 * min_retry)


@pytest.mark.parametrize(
    "exception",
    [
        pytest.param(
            # To make sure that we handle unknown exceptions that aren't currently in the model.
            ClientError(
                {"Error": {"Code": "NotARealException", "Message": "A message"}}, "CreateWorker"
            ),
            id="NotReal",
        ),
        pytest.param(
            ClientError(
                {"Error": {"Code": "AccessDeniedException", "Message": "A message"}}, "CreateWorker"
            ),
            id="AccessDenied",
        ),
        pytest.param(
            ClientError(
                {"Error": {"Code": "ValidationException", "Message": "A message"}}, "CreateWorker"
            ),
            id="Validation",
        ),
        pytest.param(
            ClientError(
                {"Error": {"Code": "ResourceNotFoundException", "Message": "A message"}},
                "CreateWorker",
            ),
            id="ResourceNotFound",
        ),
        pytest.param(
            ClientError(
                {
                    "Error": {"Code": "ConflictException", "Message": "A message"},
                    "reason": "RESOURCE_ALREADY_EXISTS",
                },
                "CreateWorker",
            ),
            id="AlreadyExists",
        ),
        pytest.param(
            ClientError(
                {
                    "Error": {"Code": "ConflictException", "Message": "A message"},
                    "reason": "UnknownReason",
                },
                "CreateWorker",
            ),
            id="UnknownConflict",
        ),
    ]
    + [
        pytest.param(
            ClientError(
                {
                    "Error": {"Code": "ConflictException", "Message": "A message"},
                    "reason": "STATUS_CONFLICT",
                    # This must match the value of config.fleet_id
                    # TODO: find a way to use the config fixture to avoid this becoming out of sync
                    "resourceId": "fleet-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "context": {"status": fleet_status},
                },
                "CreateWorker",
            ),
            id=f"Fleet-{fleet_status}",
        )
        for fleet_status in (
            "DELETE_IN_PROGRESS",
            "CREATE_FAILED",
            "UPDATE_IN_PROGRESS",  # May never happen, but no harm in testing it
            "UPDATE_FAILED",  # May never happen, but no harm in testing it
            "ACTIVE",  # Should never happen, but no harm in testing it
            "DELETED",  # Should never happen, but no harm in testing it
        )
    ],
)
def test_raises_clienterror(
    client: MagicMock,
    config: Configuration,
    host_properties: HostProperties,
    exception: ClientError,
    sleep_mock: MagicMock,
):
    # A test that the create_worker() function will re-raise a ClientError
    # whenever it gets one that shouldn't lead to a retry.

    # GIVEN
    client.create_worker.side_effect = exception

    with pytest.raises(DeadlineRequestUnrecoverableError) as exc_context:
        # WHEN
        create_worker(deadline_client=client, config=config, host_properties=host_properties)

    # THEN
    assert exc_context.value.inner_exc == exception
    sleep_mock.assert_not_called()


def test_raises_unexpected_exception(
    client: MagicMock,
    config: Configuration,
    mock_create_worker_response: CreateWorkerResponse,
    host_properties: HostProperties,
    sleep_mock: MagicMock,
):
    # A test that the create_worker() function will re-raise a ClientError
    # whenever it gets one that shouldn't lead to a retry.

    # GIVEN
    exception = Exception("Surprise!")
    client.create_worker.side_effect = exception

    with pytest.raises(DeadlineRequestUnrecoverableError) as exc_context:
        # WHEN
        create_worker(deadline_client=client, config=config, host_properties=host_properties)

    # THEN
    assert exc_context.value.inner_exc == exception
    sleep_mock.assert_not_called()
