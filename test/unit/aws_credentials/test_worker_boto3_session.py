# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from typing import Optional, Generator
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone, timedelta

import pytest

from deadline_worker_agent.aws.deadline import DeadlineRequestUnrecoverableError
import deadline_worker_agent.aws_credentials.worker_boto3_session as worker_boto3_session_mod
from deadline_worker_agent.aws_credentials.worker_boto3_session import WorkerBoto3Session
from deadline_worker_agent.boto import DEADLINE_BOTOCORE_CONFIG
from deadline_worker_agent.config import Configuration
from deadline_worker_agent.config.cli_args import ParsedCommandLineArguments


@pytest.fixture
def no_shutdown() -> bool:
    return False


@pytest.fixture
def profile() -> Optional[str]:
    return None


@pytest.fixture
def run_jobs_as_agent_user() -> bool:
    return False


@pytest.fixture
def posix_job_user() -> str:
    return "some-user:some-group"


@pytest.fixture
def verbose() -> bool:
    return False


@pytest.fixture(autouse=True)
def worker_persistence_dir() -> MagicMock:
    return MagicMock()


@pytest.fixture(autouse=True)
def worker_credentials_dir() -> MagicMock:
    return MagicMock()


@pytest.fixture
def config(
    farm_id: str,
    fleet_id: str,
    run_jobs_as_agent_user: bool,
    posix_job_user: str,
    no_shutdown: bool,
    profile: Optional[str],
    verbose: bool,
    worker_persistence_dir: MagicMock,
    worker_credentials_dir: MagicMock,
    # Specified to avoid any impact from an existing worker agent config file in the development
    # environment
    mock_config_file_not_found: MagicMock,
) -> Generator[Configuration, None, None]:
    cli_args = ParsedCommandLineArguments()
    cli_args.farm_id = farm_id
    cli_args.fleet_id = fleet_id
    cli_args.run_jobs_as_agent_user = run_jobs_as_agent_user
    cli_args.posix_job_user = posix_job_user
    cli_args.no_shutdown = no_shutdown
    cli_args.profile = profile
    cli_args.verbose = verbose
    config = Configuration(parsed_cli_args=cli_args)

    # We patch the Path attributes to prevent real file-system operations when testing
    with (
        patch.object(config, "worker_persistence_dir", new=worker_persistence_dir),
        patch.object(config, "worker_credentials_dir", new=worker_credentials_dir),
    ):
        yield config


@pytest.fixture
def bootstrap_session() -> MagicMock:
    return MagicMock()


@pytest.fixture
def file_cache_cls_mock() -> Generator[MagicMock, None, None]:
    with patch.object(worker_boto3_session_mod, "JSONFileCache") as mock:
        yield mock


@pytest.fixture
def temporary_credentials_cls_mock() -> Generator[MagicMock, None, None]:
    with patch.object(worker_boto3_session_mod, "TemporaryCredentials") as mock:
        yield mock


class TestInit:
    def test_without_loading(
        self,
        bootstrap_session: MagicMock,
        config: Configuration,
        worker_id: str,
        file_cache_cls_mock: MagicMock,
        temporary_credentials_cls_mock: MagicMock,
    ) -> None:
        # GIVEN
        temporary_credentials_cls_mock.from_cache.return_value = None

        with patch.object(WorkerBoto3Session, "refresh_credentials") as mock_refresh:
            # WHEN
            session = WorkerBoto3Session(
                bootstrap_session=bootstrap_session, config=config, worker_id=worker_id
            )

            # THEN
            assert session._farm_id == config.farm_id
            assert session._fleet_id == config.fleet_id
            assert session._worker_id == worker_id
            temporary_credentials_cls_mock.from_cache.assert_called_once_with(
                cache=session._file_cache, cache_key=session._creds_filename
            )
            file_cache_cls_mock.assert_called_once_with(working_dir=config.worker_credentials_dir)
            mock_refresh.assert_called_once()

    def test_with_loading(
        self,
        bootstrap_session: MagicMock,
        config: Configuration,
        worker_id: str,
        file_cache_cls_mock: MagicMock,
        temporary_credentials_cls_mock: MagicMock,
    ) -> None:
        # GIVEN
        mock_initial_creds = MagicMock()
        temporary_credentials_cls_mock.from_cache.return_value = mock_initial_creds

        with (
            patch.object(WorkerBoto3Session, "refresh_credentials", MagicMock()) as mock_refresh,
            patch.object(WorkerBoto3Session, "get_credentials", MagicMock()) as mock_get_creds,
        ):
            mock_creds_object = MagicMock()
            mock_creds_object.are_expired.return_value = False
            mock_get_creds.return_value = mock_creds_object

            # WHEN
            WorkerBoto3Session(
                bootstrap_session=bootstrap_session, config=config, worker_id=worker_id
            )

            # THEN
            mock_creds_object.set_credentials.assert_called_once_with(
                mock_initial_creds.to_deadline.return_value
            )
            file_cache_cls_mock.assert_called_once_with(working_dir=config.worker_credentials_dir)
            mock_refresh.assert_not_called()


SAMPLE_DEADLINE_CREDENTIALS = {
    "accessKeyId": "access-key",
    "secretAccessKey": "secret-key",
    "sessionToken": "token",
    "expiration": datetime.now(timezone.utc) + timedelta(hours=1),
}
SAMPLE_ASSUME_ROLE_RESPONSE = {"credentials": SAMPLE_DEADLINE_CREDENTIALS}


class TestRefreshCredentials:
    def test_uses_own_credentials(
        self,
        bootstrap_session: MagicMock,
        config: Configuration,
        worker_id: str,
        file_cache_cls_mock: MagicMock,
        temporary_credentials_cls_mock: MagicMock,
    ) -> None:
        # Test that if the Session contains credentials that aren't expired yet,
        # then it will use those to perform the refresh.

        # GIVEN
        with (
            patch.object(WorkerBoto3Session, "get_credentials", MagicMock()) as mock_get_creds,
            patch.object(WorkerBoto3Session, "client", MagicMock()) as mock_client,
            patch.object(
                worker_boto3_session_mod, "assume_fleet_role_for_worker"
            ) as assume_role_mock,
        ):
            # Mocks to get through WorkerBoto3Session.__init__
            mock_creds_object = MagicMock()
            mock_creds_object.are_expired.return_value = False
            mock_get_creds.return_value = mock_creds_object
            temporary_credentials_cls_mock.from_cache.return_value = None

            # Mocks to get to where we want in refresh_credentials()
            assume_role_mock.return_value = SAMPLE_ASSUME_ROLE_RESPONSE
            mock_temporary_credentials = MagicMock()
            temporary_credentials_cls_mock.from_deadline_assume_role_response.return_value = (
                mock_temporary_credentials
            )

            session = WorkerBoto3Session(
                bootstrap_session=bootstrap_session, config=config, worker_id=worker_id
            )

            # WHEN
            session.refresh_credentials()

            # THEN
            mock_client.assert_called_once_with("deadline", config=DEADLINE_BOTOCORE_CONFIG)
            assume_role_mock.assert_called_once_with(
                deadline_client=mock_client.return_value,
                farm_id=config.farm_id,
                fleet_id=config.fleet_id,
                worker_id=worker_id,
            )
            temporary_credentials_cls_mock.from_deadline_assume_role_response.assert_called_once_with(
                response=SAMPLE_ASSUME_ROLE_RESPONSE,
                credentials_required=True,
                api_name="AssumeFleetRoleForWorker",
            )
            mock_temporary_credentials.cache.assert_called_once_with(
                cache=file_cache_cls_mock.return_value, cache_key=session._creds_filename
            )
            mock_creds_object.set_credentials.assert_called_once_with(
                mock_temporary_credentials.to_deadline.return_value
            )

    def test_uses_bootstrap_credentials(
        self,
        bootstrap_session: MagicMock,
        config: Configuration,
        worker_id: str,
        file_cache_cls_mock: MagicMock,
        temporary_credentials_cls_mock: MagicMock,
    ) -> None:
        # Test that if the Session contains credentials that ARE expired,
        # then it will use the given bootstrap_session credentials to do the refresh.

        # GIVEN
        with (
            patch.object(WorkerBoto3Session, "get_credentials", MagicMock()) as mock_get_creds,
            patch.object(WorkerBoto3Session, "client", MagicMock()),
            patch.object(
                worker_boto3_session_mod, "assume_fleet_role_for_worker"
            ) as assume_role_mock,
        ):
            # Mocks to get through WorkerBoto3Session.__init__
            mock_creds_object = MagicMock()
            mock_creds_object.are_expired.return_value = False
            mock_get_creds.return_value = mock_creds_object
            temporary_credentials_cls_mock.from_cache.return_value = None

            # Mocks to get to where we want in refresh_credentials()
            mock_client = MagicMock()
            bootstrap_session.client.return_value = mock_client
            assume_role_mock.return_value = SAMPLE_ASSUME_ROLE_RESPONSE
            mock_temporary_credentials = MagicMock()
            temporary_credentials_cls_mock.from_deadline_assume_role_response.return_value = (
                mock_temporary_credentials
            )

            session = WorkerBoto3Session(
                bootstrap_session=bootstrap_session, config=config, worker_id=worker_id
            )

            mock_creds_object.are_expired.return_value = True

            # WHEN
            session.refresh_credentials()

            # THEN
            bootstrap_session.client.assert_called_once_with(
                "deadline", config=DEADLINE_BOTOCORE_CONFIG
            )
            assume_role_mock.assert_called_once_with(
                deadline_client=mock_client,
                farm_id=config.farm_id,
                fleet_id=config.fleet_id,
                worker_id=worker_id,
            )
            temporary_credentials_cls_mock.from_deadline_assume_role_response.assert_called_once_with(
                response=SAMPLE_ASSUME_ROLE_RESPONSE,
                credentials_required=True,
                api_name="AssumeFleetRoleForWorker",
            )
            mock_temporary_credentials.cache.assert_called_once_with(
                cache=file_cache_cls_mock.return_value, cache_key=session._creds_filename
            )
            mock_creds_object.set_credentials.assert_called_once_with(
                mock_temporary_credentials.to_deadline.return_value
            )

    def test_reraises_from_assume(
        self,
        bootstrap_session: MagicMock,
        config: Configuration,
        worker_id: str,
        file_cache_cls_mock: MagicMock,
        temporary_credentials_cls_mock: MagicMock,
    ) -> None:
        # Test that if the assume-role raises an exception that we re-raise it..

        # GIVEN
        with (
            patch.object(WorkerBoto3Session, "get_credentials", MagicMock()) as mock_get_creds,
            patch.object(WorkerBoto3Session, "client", MagicMock()),
            patch.object(
                worker_boto3_session_mod, "assume_fleet_role_for_worker"
            ) as assume_role_mock,
        ):
            # Mocks to get through WorkerBoto3Session.__init__
            mock_creds_object = MagicMock()
            mock_creds_object.are_expired.return_value = False
            mock_get_creds.return_value = mock_creds_object
            temporary_credentials_cls_mock.from_cache.return_value = None

            # Mocks to get to where we want in refresh_credentials()
            assume_role_mock.side_effect = DeadlineRequestUnrecoverableError(Exception("Boo"))

            session = WorkerBoto3Session(
                bootstrap_session=bootstrap_session, config=config, worker_id=worker_id
            )

            # WHEN
            with pytest.raises(DeadlineRequestUnrecoverableError) as exc_context:
                session.refresh_credentials()

            # THEN
            assert exc_context.value is assume_role_mock.side_effect

    @pytest.mark.parametrize("exception", [KeyError("key"), TypeError("type"), ValueError("value")])
    def test_reraises_from_parse(
        self,
        bootstrap_session: MagicMock,
        config: Configuration,
        worker_id: str,
        file_cache_cls_mock: MagicMock,
        temporary_credentials_cls_mock: MagicMock,
        exception: Exception,
    ) -> None:
        # Test that if the parsing of the API response fails then we raise an exception.

        # GIVEN
        with (
            patch.object(WorkerBoto3Session, "get_credentials", MagicMock()) as mock_get_creds,
            patch.object(WorkerBoto3Session, "client", MagicMock()),
            patch.object(
                worker_boto3_session_mod, "assume_fleet_role_for_worker"
            ) as assume_role_mock,
        ):
            # Mocks to get through WorkerBoto3Session.__init__
            mock_creds_object = MagicMock()
            mock_creds_object.are_expired.return_value = False
            mock_get_creds.return_value = mock_creds_object
            temporary_credentials_cls_mock.from_cache.return_value = None

            # Mocks to get to where we want in refresh_credentials()
            assume_role_mock.return_value = SAMPLE_ASSUME_ROLE_RESPONSE
            temporary_credentials_cls_mock.from_deadline_assume_role_response.side_effect = (
                exception
            )

            session = WorkerBoto3Session(
                bootstrap_session=bootstrap_session, config=config, worker_id=worker_id
            )

            # WHEN
            with pytest.raises(DeadlineRequestUnrecoverableError) as exc_context:
                session.refresh_credentials()

            # THEN
            assert exc_context.value.inner_exc is exception
