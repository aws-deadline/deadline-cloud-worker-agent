# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from typing import Generator, Any
from unittest.mock import MagicMock, patch
import pytest
from botocore.exceptions import ClientError

from deadline_worker_agent.aws.deadline import (
    delete_worker,
    DeadlineRequestRecoverableError,
    DeadlineRequestUnrecoverableError,
)

import deadline_worker_agent.aws.deadline as deadline_mod
from deadline_worker_agent.startup.config import Configuration
from deadline_worker_agent.startup.cli_args import ParsedCommandLineArguments


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
    cli_args.impersonation = False
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


def test_success(client: MagicMock, config: Configuration, worker_id: str) -> None:
    # Test the happy-path of the delete_worker function.

    # GIVEN
    client.delete_worker.return_value = dict[str, Any]()

    # WHEN
    delete_worker(deadline_client=client, config=config, worker_id=worker_id)

    # THEN
    client.delete_worker.assert_called_once_with(
        farmId=config.farm_id, fleetId=config.fleet_id, workerId=worker_id
    )


@pytest.mark.parametrize(
    "exception",
    [
        pytest.param(
            ClientError(
                {"Error": {"Code": "ThrottlingException", "Message": "A message"}}, "DeleteWorker"
            ),
            id="Throttling",
        ),
        pytest.param(
            ClientError(
                {"Error": {"Code": "InternalServerException", "Message": "A message"}},
                "DeleteWorker",
            ),
            id="InternalServer",
        ),
    ],
)
def test_retries_when_appropriate(
    client: MagicMock,
    config: Configuration,
    worker_id: str,
    exception: ClientError,
    sleep_mock: MagicMock,
):
    # A test that the delete_worker() function will retry calls to the API when:
    # 1. Throttled
    # 2. InternalServerException

    # GIVEN
    client.delete_worker.side_effect = [exception, dict[str, Any]()]

    # WHEN
    delete_worker(deadline_client=client, config=config, worker_id=worker_id)

    # THEN
    assert client.delete_worker.call_count == 2
    sleep_mock.assert_called_once()


@pytest.mark.parametrize(
    "exception",
    [
        pytest.param(
            ClientError(
                {"Error": {"Code": "AccessDeniedException", "Message": "A message"}}, "DeleteWorker"
            ),
            id="AccessDenied",
        ),
        pytest.param(
            ClientError(
                {"Error": {"Code": "ValidationException", "Message": "A message"}}, "DeleteWorker"
            ),
            id="Validation",
        ),
        pytest.param(
            ClientError(
                {"Error": {"Code": "ResourceNotFoundException", "Message": "A message"}},
                "DeleteWorker",
            ),
            id="ResourceNotFound",
        ),
        pytest.param(
            ClientError(
                {
                    "Error": {"Code": "ConflictException", "Message": "A message"},
                },
                "DeleteWorker",
            ),
            id="Generic-Conflict",
        ),
        pytest.param(
            ClientError(
                {
                    "Error": {"Code": "ConflictException", "Message": "A message"},
                    "reason": "STATUS_CONFLICT",
                    "resourceId": "not-the-worker-id",
                    "context": {"status": "STOPPED"},
                },
                "DeleteWorker",
            ),
            id="STATUS_CONFLICT-different-worker",
        ),
    ]
    + [
        # These will never happen practice, but we use them to check out handling logic
        pytest.param(
            ClientError(
                {
                    "Error": {"Code": "ConflictException", "Message": "A message"},
                    "reason": "STATUS_CONFLICT",
                    # This must match the value of config.fleet_id
                    # TODO: find a way to use the config fixture to avoid this becoming out of sync
                    "resourceId": "worker-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "context": {"status": status},
                },
                "DeleteWorker",
            ),
            id=f"STATUS_CONFLICT-worker-{status}",
        )
        for status in ("CREATED", "STOPPED")
    ],
)
def test_raises_unrecoverable_clienterror(
    client: MagicMock,
    config: Configuration,
    worker_id: str,
    exception: ClientError,
    sleep_mock: MagicMock,
):
    # A test that the delete_worker() function will re-raise a ClientError
    # as unrecoverable whenever it gets one that shouldn't lead to a retry.

    # GIVEN
    client.delete_worker.side_effect = exception

    with pytest.raises(DeadlineRequestUnrecoverableError) as exc_context:
        # WHEN
        delete_worker(deadline_client=client, config=config, worker_id=worker_id)

    # THEN
    assert exc_context.value.inner_exc == exception
    sleep_mock.assert_not_called()


class TestRaisesRecoverableClientError:
    @pytest.fixture(
        params=("STARTED", "STOPPING", "NOT_RESPONDING", "NOT_COMPATIBLE", "RUNNING", "IDLE"),
    )
    def status(self, request: pytest.FixtureRequest) -> str:
        return request.param

    @pytest.fixture
    def exception(
        self,
        status: str,
        worker_id: str,
    ) -> ClientError:
        return ClientError(
            {
                "Error": {"Code": "ConflictException", "Message": "A message"},
                "reason": "STATUS_CONFLICT",
                "resourceId": worker_id,
                "context": {"status": status},
            },
            "DeleteWorker",
        )

    def test_raises_recoverable_clienterror(
        self,
        client: MagicMock,
        config: Configuration,
        worker_id: str,
        exception: ClientError,
        sleep_mock: MagicMock,
    ):
        # A test that the delete_worker() function will re-raise a ClientError
        # as unrecoverable whenever it gets one that shouldn't lead to a retry.

        # GIVEN
        client.delete_worker.side_effect = exception

        with pytest.raises(DeadlineRequestRecoverableError) as exc_context:
            # WHEN
            delete_worker(deadline_client=client, config=config, worker_id=worker_id)

        # THEN
        assert exc_context.value.inner_exc == exception
        sleep_mock.assert_not_called()


def test_raises_unexpected_exception(
    client: MagicMock,
    config: Configuration,
    worker_id: str,
    sleep_mock: MagicMock,
):
    # A test that the delete_worker() function will re-raise a ClientError
    # whenever it gets one that shouldn't lead to a retry.

    # GIVEN
    exception = Exception("Surprise!")
    client.delete_worker.side_effect = exception

    with pytest.raises(DeadlineRequestUnrecoverableError) as exc_context:
        # WHEN
        delete_worker(deadline_client=client, config=config, worker_id=worker_id)

    # THEN
    assert exc_context.value.inner_exc == exception
    sleep_mock.assert_not_called()
