# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from typing import Any, Generator, Optional
from unittest.mock import ANY, MagicMock, call, patch
import stat
from tempfile import TemporaryDirectory
from pathlib import Path
import json

from botocore.exceptions import ClientError
from pytest import fixture, mark, param, raises

from deadline_worker_agent.api_models import (
    HostProperties,
    LogConfiguration,
    UpdateWorkerResponse,
    WorkerStatus,
)
from deadline_worker_agent.boto import DEADLINE_BOTOCORE_CONFIG
from deadline_worker_agent.log_sync.cloudwatch import (
    LOG_CONFIG_OPTION_GROUP_NAME_KEY,
    LOG_CONFIG_OPTION_STREAM_NAME_KEY,
)
from deadline_worker_agent.config import Configuration
from deadline_worker_agent.config.cli_args import ParsedCommandLineArguments
from deadline_worker_agent.startup.bootstrap import WorkerPersistenceInfo
from deadline_worker_agent.startup import bootstrap as bootstrap_mod
from deadline_worker_agent.aws.deadline import (
    DeadlineRequestConditionallyRecoverableError,
    DeadlineRequestUnrecoverableError,
    WorkerLogConfig,
    construct_worker_log_config,
)
from deadline_worker_agent.log_sync.cloudwatch import (
    LOG_CONFIG_OPTION_GROUP_NAME_KEY,
    LOG_CONFIG_OPTION_STREAM_NAME_KEY,
)
from deadline_worker_agent.api_models import WorkerStatus

EMPTY_CAPABILITY_DICT: dict[str, Any] = {"amounts": [], "attributes": []}


CLOUDWATCH_LOG_GROUP = "log-group"
CLOUDWATCH_LOG_STREAM = "log-stream"
AWSLOGS_LOG_CONFIGURATION = LogConfiguration(
    logDriver="awslogs",
    options={
        LOG_CONFIG_OPTION_GROUP_NAME_KEY: CLOUDWATCH_LOG_GROUP,
        LOG_CONFIG_OPTION_STREAM_NAME_KEY: CLOUDWATCH_LOG_STREAM,
    },
)


@fixture
def run_jobs_as_agent_user() -> bool:
    return False


@fixture
def posix_job_user() -> str:
    return "some-user:some-group"


@fixture
def no_shutdown() -> bool:
    return False


@fixture
def profile() -> Optional[str]:
    return None


@fixture
def verbose() -> bool:
    return False


@fixture
def worker_persistence_dir() -> MagicMock:
    return MagicMock()


@fixture
def worker_credentials_dir() -> MagicMock:
    return MagicMock()


@fixture
def worker_state_file() -> MagicMock:
    return MagicMock()


@fixture
def allow_instance_profile() -> bool:
    return True


@fixture
def config(
    farm_id: str,
    fleet_id: str,
    run_jobs_as_agent_user: bool,
    posix_job_user: str,
    no_shutdown: bool,
    profile: Optional[str],
    verbose: bool,
    allow_instance_profile: bool,
    # Specified to avoid any impact from an existing worker agent config file in the development
    # environment
    mock_config_file_not_found: MagicMock,
) -> Generator[Configuration, None, None]:
    with TemporaryDirectory() as tempdir:
        cli_args = ParsedCommandLineArguments()
        cli_args.farm_id = farm_id
        cli_args.fleet_id = fleet_id
        cli_args.run_jobs_as_agent_user = run_jobs_as_agent_user
        cli_args.posix_job_user = posix_job_user
        cli_args.no_shutdown = no_shutdown
        cli_args.profile = profile
        cli_args.verbose = verbose
        cli_args.disallow_instance_profile = not allow_instance_profile
        # Direct the logs and persistence state into a temporary directory
        cli_args.logs_dir = Path(tempdir) / "temp-logs-dir"
        cli_args.persistence_dir = Path(tempdir) / "temp-persist-dir"
        config = Configuration(parsed_cli_args=cli_args)

        # These directories need to exist
        cli_args.logs_dir.mkdir()
        cli_args.persistence_dir.mkdir()

        yield config


@fixture
def cloudwatch_log_group() -> str:
    return "log-group"


@fixture
def cloudwatch_log_stream() -> str:
    return "log-stream"


@fixture
def update_worker_started_success_response(
    cloudwatch_log_group: str,
    cloudwatch_log_stream: str,
) -> UpdateWorkerResponse:
    return UpdateWorkerResponse(
        log=LogConfiguration(
            logDriver="awslogs",
            options={
                LOG_CONFIG_OPTION_GROUP_NAME_KEY: cloudwatch_log_group,
                LOG_CONFIG_OPTION_STREAM_NAME_KEY: cloudwatch_log_stream,
            },
            parameters={
                "interval": "15",
            },
        ),
    )


@fixture
def worker_info(
    worker_id: str,
) -> WorkerPersistenceInfo:
    return WorkerPersistenceInfo(
        worker_id=worker_id,
    )


@fixture
def mod_logger_mock() -> Generator[MagicMock, None, None]:
    with patch.object(bootstrap_mod, "_logger") as mod_logger_mock:
        yield mod_logger_mock


@fixture
def get_metadata_mock() -> Generator[MagicMock, None, None]:
    with patch.object(bootstrap_mod, "_get_metadata") as get_metadata_mock:
        yield get_metadata_mock


@fixture(autouse=True)
def sleep_mock() -> Generator[MagicMock, None, None]:
    with patch.object(bootstrap_mod, "sleep") as sleep_mock:
        yield sleep_mock


@fixture
def test_worker_deregistered_error() -> None:
    # GIVEN
    worker_id = "worker-a"
    exception = bootstrap_mod.WorkerDeregisteredError(worker_id=worker_id)

    # WHEN
    msg = str(exception)

    # THEN
    assert msg == f"Worker {worker_id} is DEREGISTERED"


@fixture(autouse=True)
def mock_get_host_properties(
    host_properties: HostProperties,
) -> Generator[MagicMock, None, None]:
    with patch.object(
        bootstrap_mod, "_get_host_properties", return_value=host_properties
    ) as mock_get_host_properties:
        yield mock_get_host_properties


class TestWorkerInfo:
    """Tests for WorkerInfo class"""

    def test_save(
        self,
        worker_info: WorkerPersistenceInfo,
        config: Configuration,
    ) -> None:
        # GIVEN
        config.worker_state_file = worker_state_file = MagicMock()
        worker_state_file.is_absolute.return_value = True
        with (patch.object(bootstrap_mod.json, "dump") as dump_mock,):
            state_file_open_mock: MagicMock = worker_state_file.open
            state_file_touch_mock: MagicMock = worker_state_file.touch
            state_file_open_mock_enter: MagicMock = state_file_open_mock.return_value.__enter__

            # WHEN
            worker_info.save(config=config)

            # THEN
            state_file_touch_mock.assert_called_once_with(
                mode=stat.S_IWUSR | stat.S_IRUSR, exist_ok=True
            )
            state_file_open_mock.assert_called_once_with("w", encoding="utf8")
            state_file_open_mock_enter.assert_called_once_with()
            dump_mock.assert_called_once_with(
                {
                    "worker_id": worker_info.worker_id,
                },
                state_file_open_mock_enter.return_value,
            )

    def test_load_file_does_not_exist(
        self,
        config: Configuration,
    ) -> None:
        """Tests that when the Worker persistence file does not exist, that WorkerInfo.load()
        returns None and does not try to open the file."""
        # GIVEN
        config.worker_state_file = worker_state_file = MagicMock()
        worker_state_file_is_file: MagicMock = worker_state_file.is_file
        worker_state_file_open: MagicMock = worker_state_file.open
        worker_state_file_is_file.return_value = False

        # WHEN
        result = WorkerPersistenceInfo.load(config=config)

        # THEN
        assert result is None
        worker_state_file_open.assert_not_called()

    @mark.parametrize(
        argnames=(
            "worker_id",
            "cloudwatch_log_group",
            "cloudwatch_log_stream",
            "cloudwatch_log_stream_sequence_token",
        ),
        argvalues=(
            param(
                "worker_id",
                "cloudwatch_log_group",
                "cloudwatch_log_stream",
                "cloudwatch_log_stream_sequence_token",
                id="with-log-stream-token",
            ),
            param(
                "worker_id",
                "cloudwatch_log_group",
                "cloudwatch_log_stream",
                None,
                id="no-log-stream-token",
            ),
        ),
    )
    def test_load_file_exists(
        self,
        config: Configuration,
        worker_id: str,
        cloudwatch_log_group: str,
        cloudwatch_log_stream: str,
        cloudwatch_log_stream_sequence_token: Optional[str],
    ) -> None:
        """Tests that when the Worker persistence file exists, that WorkerInfo.load() returns the
        values loaded from the file."""
        # GIVEN
        json_load_result = {
            "worker_id": worker_id,
            # These are added to ensure backwards-compatibility when the CWL destination was
            # previously persisted to the JSON file
            "cloudwatch_log_group": cloudwatch_log_group,
            "cloudwatch_log_stream": cloudwatch_log_stream,
        }
        if cloudwatch_log_stream_sequence_token:
            json_load_result["cloudwatch_log_stream_sequence_token"] = (
                cloudwatch_log_stream_sequence_token
            )
        with config.worker_state_file.open("w") as fh:
            json.dump(json_load_result, fh)

        # WHEN
        result = WorkerPersistenceInfo.load(config=config)

        # THEN
        assert isinstance(result, WorkerPersistenceInfo)
        assert result.worker_id == worker_id


class TestBootstrapWorker:
    """Tests for bootstrap_worker function"""

    @fixture
    def load_or_create_worker_mock(self) -> Generator[MagicMock, None, None]:
        with patch.object(bootstrap_mod, "_load_or_create_worker") as mock:
            yield mock

    @fixture
    def get_boto3_session_for_fleet_role_mock(self) -> Generator[MagicMock, None, None]:
        with patch.object(bootstrap_mod, "_get_boto3_session_for_fleet_role") as mock:

            def client_implementation(service: str, config: Any) -> MagicMock:
                if service == "deadline":
                    return MagicMock()
                raise NotImplementedError(f'No mock for service "{service}"')

            session_mock = MagicMock()
            mock.return_value = session_mock
            session_mock.client.side_effect = client_implementation
            yield mock

    @fixture
    def start_worker_mock(self) -> Generator[MagicMock, None, None]:
        with patch.object(bootstrap_mod, "_start_worker") as start_worker_mock:
            yield start_worker_mock

    @fixture
    def enforce_no_instance_profile_or_stop_worker_mock(self) -> Generator[MagicMock, None, None]:
        with patch.object(
            bootstrap_mod, "_enforce_no_instance_profile_or_stop_worker"
        ) as enforce_no_instance_profile_mock:
            yield enforce_no_instance_profile_mock

    @fixture
    def worker_log_config(
        self, cloudwatch_log_group: str, cloudwatch_log_stream: str
    ) -> WorkerLogConfig:
        return WorkerLogConfig(
            cloudwatch_log_group=cloudwatch_log_group,
            cloudwatch_log_stream=cloudwatch_log_stream,
        )

    def test_success(
        self,
        config: Configuration,
        worker_info: WorkerPersistenceInfo,
        load_or_create_worker_mock: MagicMock,
        get_boto3_session_for_fleet_role_mock: MagicMock,
        start_worker_mock: MagicMock,
        worker_log_config: WorkerLogConfig,
        enforce_no_instance_profile_or_stop_worker_mock: MagicMock,
    ) -> None:
        """Test of the happy-path of bootstrap_worker()."""
        # GIVEN
        load_or_create_worker_mock.return_value = (worker_info, False)
        start_worker_mock.return_value = worker_log_config

        # WHEN
        worker_bootstrap = bootstrap_mod.bootstrap_worker(config=config)

        # THEN
        assert worker_bootstrap.log_config is worker_log_config
        assert worker_bootstrap.session is get_boto3_session_for_fleet_role_mock.return_value
        assert worker_bootstrap.worker_info is worker_info
        enforce_no_instance_profile_or_stop_worker_mock.assert_called_once_with(
            config=config,
            deadline_client=ANY,  # The client instance mock is dynamicly created, so just assert ANY.
            worker_id=worker_info.worker_id,
        )

    def test_start_worker_generic_exception(
        self,
        config: Configuration,
        worker_info: WorkerPersistenceInfo,
        load_or_create_worker_mock: MagicMock,
        get_boto3_session_for_fleet_role_mock: MagicMock,
        start_worker_mock: MagicMock,
    ) -> None:
        """
        Test that when _start_worker() raises an unexpected exception, then bootstrap also raises
        the same exception.
        """
        # GIVEN
        load_or_create_worker_mock.return_value = (worker_info, False)
        start_worker_exception = Exception("error message")
        start_worker_mock.side_effect = start_worker_exception

        with raises(Exception) as raise_ctx:
            # WHEN
            bootstrap_mod.bootstrap_worker(config=config)

        # THEN
        assert raise_ctx.value is start_worker_exception

    def test_start_worker_deleted_and_not_loaded(
        self,
        config: Configuration,
        worker_info: WorkerPersistenceInfo,
        load_or_create_worker_mock: MagicMock,
        get_boto3_session_for_fleet_role_mock: MagicMock,
        start_worker_mock: MagicMock,
    ) -> None:
        """
        Test that when _start_worker() raises an error that indicates that the
        Worker has already been deleted, and we haven't loaded an existing Worker then
        we re-raise the same exception
        """
        # GIVEN
        load_or_create_worker_mock.return_value = (worker_info, False)
        start_worker_exception = bootstrap_mod.WorkerDeregisteredError(
            worker_id=worker_info.worker_id
        )
        start_worker_mock.side_effect = start_worker_exception

        with raises(bootstrap_mod.WorkerDeregisteredError) as raise_ctx:
            # WHEN
            bootstrap_mod.bootstrap_worker(config=config)

        # THEN
        assert raise_ctx.value is start_worker_exception

    def test_start_worker_deleted_and_loaded(
        self,
        config: Configuration,
        worker_info: WorkerPersistenceInfo,
        load_or_create_worker_mock: MagicMock,
        get_boto3_session_for_fleet_role_mock: MagicMock,
        start_worker_mock: MagicMock,
        worker_log_config: WorkerLogConfig,
        enforce_no_instance_profile_or_stop_worker_mock: MagicMock,
    ) -> None:
        """
        Test that when _start_worker() raises an error that indicates that the
        Worker has already been deleted, and we have loaded an existing Worker then
        we recover and create a new worker.
        """
        # GIVEN
        load_or_create_worker_mock.side_effect = [(worker_info, True), (worker_info, False)]
        start_worker_exception = bootstrap_mod.BootstrapWithoutWorkerLoad()
        start_worker_mock.side_effect = [start_worker_exception, worker_log_config]

        # WHEN
        worker_bootstrap = bootstrap_mod.bootstrap_worker(config=config)

        # THEN
        assert worker_bootstrap.log_config is worker_log_config
        assert worker_bootstrap.session is get_boto3_session_for_fleet_role_mock.return_value
        assert worker_bootstrap.worker_info is worker_info
        assert load_or_create_worker_mock.call_count == 2
        assert get_boto3_session_for_fleet_role_mock.call_count == 2
        assert start_worker_mock.call_count == 2
        enforce_no_instance_profile_or_stop_worker_mock.assert_called_once_with(
            config=config,
            deadline_client=ANY,  # The client instance mock is dynamicly created, so just assert ANY.
            worker_id=worker_info.worker_id,
        )


class TestLoadOrCreateWorker:
    """Tests for _load_or_create_worker()"""

    @fixture
    def worker_persistence_info_mock(self) -> Generator[MagicMock, None, None]:
        with patch.object(bootstrap_mod, "WorkerPersistenceInfo") as mock:
            yield mock

    @fixture
    def session_mock(self) -> MagicMock:
        return MagicMock()

    @fixture
    def deadline_client_mock(self, session_mock: MagicMock) -> MagicMock:
        deadline_client = MagicMock()

        def client_impl(service: str, config: Any) -> MagicMock:
            if service == "deadline":
                return deadline_client
            raise NotImplementedError(f'No mock for service "{service}"')

        session_mock.client.side_effect = client_impl

        return deadline_client

    @fixture
    def create_worker_mock(self, worker_id: str) -> Generator[MagicMock, None, None]:
        with patch.object(bootstrap_mod, "create_worker") as create_worker_mock:
            create_worker_mock.return_value = {"workerId": worker_id}
            yield create_worker_mock

    def test_existing_worker_successful_restore(
        self,
        worker_persistence_info_mock: MagicMock,
        session_mock: MagicMock,
        config: Configuration,
        create_worker_mock: MagicMock,
    ) -> None:
        """
        Test that we return a previously saved Worker when there is one.
        """

        # GIVEN
        worker_info_mock = MagicMock()
        worker_persistence_info_mock.load.return_value = worker_info_mock
        worker_persistence_info_mock.save = MagicMock()

        # WHEN
        worker_info_result, has_existing_result = bootstrap_mod._load_or_create_worker(
            session=session_mock, config=config, use_existing_worker=True
        )

        # THEN
        assert worker_info_result is worker_info_mock
        assert has_existing_result
        worker_persistence_info_mock.load.assert_called_once_with(config=config)
        worker_persistence_info_mock.save.assert_not_called()
        create_worker_mock.assert_not_called()
        worker_info_mock.save.assert_not_called()

    def test_creates_worker_when_no_existing(
        self,
        worker_id: str,
        worker_persistence_info_mock: MagicMock,
        session_mock: MagicMock,
        deadline_client_mock: MagicMock,
        config: Configuration,
        create_worker_mock: MagicMock,
        host_properties: HostProperties,
    ):
        """Test that we create and persist a new worker when there is no previously saved Worker to load."""

        # GIVEN
        worker_info_mock = MagicMock()
        worker_persistence_info_mock.return_value = worker_info_mock
        worker_persistence_info_mock.load.return_value = None

        # WHEN
        worker_info_result, has_existing_result = bootstrap_mod._load_or_create_worker(
            session=session_mock, config=config, use_existing_worker=True
        )

        # THEN
        assert worker_info_result is worker_info_mock
        assert not has_existing_result
        create_worker_mock.assert_called_once_with(
            deadline_client=deadline_client_mock, config=config, host_properties=host_properties
        )
        worker_persistence_info_mock.assert_called_once_with(worker_id=worker_id)
        worker_info_mock.save.assert_called_once_with(config=config)
        session_mock.client.assert_called_once_with("deadline", config=DEADLINE_BOTOCORE_CONFIG)

    def test_raises_system_exit(
        self,
        session_mock: MagicMock,
        config: Configuration,
        create_worker_mock: MagicMock,
    ):
        """Test that we raise SystemExit when the call to create_worker raises a DeadlineRequestUnrecoverableError."""

        # GIVEN
        create_worker_mock.side_effect = DeadlineRequestUnrecoverableError(Exception("Inner exc"))

        # THEN
        with raises(SystemExit):
            bootstrap_mod._load_or_create_worker(
                session=session_mock, config=config, use_existing_worker=False
            )


class TestGetBoto3SessionForFleetRole:
    """Tests of _get_boto3_session_for_fleet_role()"""

    @fixture
    def worker_boto3_session_cls_mock(
        self,
    ) -> Generator[MagicMock, None, None]:
        with patch.object(bootstrap_mod, "WorkerBoto3Session") as mock:
            yield mock

    def test_success(
        self,
        config: Configuration,
        worker_id: str,
        worker_boto3_session_cls_mock: MagicMock,
    ):
        """Test of the direct-line success path."""

        # GIVEN
        session_mock = MagicMock()
        return_session_mock = MagicMock()
        worker_boto3_session_cls_mock.return_value = return_session_mock

        # WHEN
        session = bootstrap_mod._get_boto3_session_for_fleet_role(
            session=session_mock, config=config, worker_id=worker_id, has_existing_worker=True
        )

        # THEN
        assert session is return_session_mock
        worker_boto3_session_cls_mock.assert_called_once_with(
            bootstrap_session=session_mock, config=config, worker_id=worker_id
        )

    def test_existing_worker_deleted_at_assume_credentials(
        self,
        config: Configuration,
        worker_id: str,
        worker_boto3_session_cls_mock: MagicMock,
    ):
        """Test that we raise BootstrapWithoutWorkerLoad with use_existing_worker=False
        if attempting to obtain Fleet AWS Credentials raises a ResourceNotFoundException."""

        # GIVEN
        session_mock = MagicMock()
        inner_exc = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "A dummy message"}},
            "AssumeFleetRoleForWorker",
        )
        get_credentials_exc = DeadlineRequestUnrecoverableError(inner_exc)
        worker_boto3_session_cls_mock.side_effect = get_credentials_exc

        # WHEN
        with raises(bootstrap_mod.BootstrapWithoutWorkerLoad):
            bootstrap_mod._get_boto3_session_for_fleet_role(
                session=session_mock, config=config, worker_id=worker_id, has_existing_worker=True
            )

    def test_existing_worker_assume_credentials_raises_terminal_clienterror(
        self,
        config: Configuration,
        worker_id: str,
        worker_boto3_session_cls_mock: MagicMock,
    ):
        """Test we raise SystemExit when the attempt to obtain AWS Credentials raises
        a terminal ClientError exception."""

        # GIVEN
        session_mock = MagicMock()
        inner_exc = ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "A dummy message"}},
            "AssumeFleetRoleForWorker",
        )
        get_credentials_exc = DeadlineRequestUnrecoverableError(inner_exc)
        worker_boto3_session_cls_mock.side_effect = get_credentials_exc

        # WHEN
        with raises(SystemExit):
            bootstrap_mod._get_boto3_session_for_fleet_role(
                session=session_mock, config=config, worker_id=worker_id, has_existing_worker=True
            )

    def test_existing_worker_assume_credentials_raises_terminal(
        self,
        config: Configuration,
        worker_id: str,
        worker_boto3_session_cls_mock: MagicMock,
    ):
        """Test we raise SystemExit when the attempt to obtain AWS Credentials raises
        a terminal non-ClientError exception."""

        # GIVEN
        session_mock = MagicMock()
        get_credentials_exc = Exception("I'm no ClientError!")
        worker_boto3_session_cls_mock.side_effect = get_credentials_exc

        # WHEN
        with raises(SystemExit):
            bootstrap_mod._get_boto3_session_for_fleet_role(
                session=session_mock, config=config, worker_id=worker_id, has_existing_worker=True
            )


class TestStartWorker:
    """Tests for the _start_worker() function"""

    @fixture
    def deadline_client(self) -> MagicMock:
        return MagicMock()

    @fixture
    def update_worker_mock(self) -> Generator[MagicMock, None, None]:
        with patch.object(bootstrap_mod, "update_worker") as mock:
            yield mock

    @fixture
    def mock_get_host_properties(
        self, host_properties: HostProperties
    ) -> Generator[MagicMock, None, None]:
        with patch.object(
            bootstrap_mod, "_get_host_properties", return_value=host_properties
        ) as mock:
            yield mock

    @mark.parametrize(
        "has_existing_worker, log_config",
        [
            param(True, AWSLOGS_LOG_CONFIGURATION, id="has-existing-with-logs"),
            param(False, AWSLOGS_LOG_CONFIGURATION, id="no-existing-with-logs"),
            param(True, None, id="has-existing-no-logs"),
            param(False, None, id="no-existing-no-logs"),
        ],
    )
    def test_success(
        self,
        config: Configuration,
        worker_id: str,
        has_existing_worker: bool,
        log_config: Optional[LogConfiguration],
        deadline_client: MagicMock,
        update_worker_mock: MagicMock,
        mock_get_host_properties: MagicMock,
        host_properties: HostProperties,
    ) -> None:
        # Tests the happy-path for _start_worker

        # GIVEN
        update_worker_response = dict[str, Any]()
        if log_config:
            update_worker_response["log"] = log_config
        update_worker_mock.return_value = update_worker_response

        # WHEN
        result = bootstrap_mod._start_worker(
            deadline_client=deadline_client,
            config=config,
            worker_id=worker_id,
            has_existing_worker=has_existing_worker,
        )

        # THEN
        mock_get_host_properties.assert_called_once()
        update_worker_mock.assert_called_once_with(
            deadline_client=deadline_client,
            farm_id=config.farm_id,
            fleet_id=config.fleet_id,
            worker_id=worker_id,
            status=WorkerStatus.STARTED,
            capabilities=config.capabilities,
            host_properties=host_properties,
        )
        if not log_config:
            assert result is None
        else:
            assert result == construct_worker_log_config(log_config=log_config)

    @mark.parametrize(
        "has_existing_worker, exception",
        [
            param(
                True,
                DeadlineRequestUnrecoverableError(ClientError({}, "UpdateWorker")),
                id="has-existing-unrecoverable",
            ),
            param(
                False,
                DeadlineRequestUnrecoverableError(ClientError({}, "UpdateWorker")),
                id="no-existing-unrecoverable",
            ),
            param(
                False,
                DeadlineRequestConditionallyRecoverableError(ClientError({}, "UpdateWorker")),
                id="no-existing-recoverable",
            ),
        ],
    )
    def test_raises_exit(
        self,
        config: Configuration,
        worker_id: str,
        has_existing_worker: bool,
        exception: Exception,
        deadline_client: MagicMock,
        update_worker_mock: MagicMock,
        mock_get_host_properties: MagicMock,
    ) -> None:
        # Tests the conditions under which _start_worker should raise an exception
        # to exit the application. These cases are when update_worker raises:
        # 1. Any unrecoverable exception
        # 2. A conditionally recoverable exception, but we don't have an existing worker

        # GIVEN
        update_worker_mock.side_effect = exception

        # WHEN
        with raises(SystemExit):
            bootstrap_mod._start_worker(
                deadline_client=deadline_client,
                config=config,
                worker_id=worker_id,
                has_existing_worker=has_existing_worker,
            )

    def test_raises_re_bootstrap(
        self,
        config: Configuration,
        worker_id: str,
        deadline_client: MagicMock,
        update_worker_mock: MagicMock,
        mock_get_host_properties: MagicMock,
    ) -> None:
        # Tests the conditions under which _start_worker should raise an exception
        # that signals that the caller (bootstrap_worker) should redo the bootstrap
        # with ignoring the locally-loaded WorkerId.

        # GIVEN
        update_worker_mock.side_effect = DeadlineRequestConditionallyRecoverableError(
            ClientError({}, "UpdateWorker")
        )

        # WHEN
        with raises(SystemExit):
            bootstrap_mod._start_worker(
                deadline_client=deadline_client,
                config=config,
                worker_id=worker_id,
                has_existing_worker=False,
            )


@mark.usefixtures("get_metadata_mock")
class TestEnforceNoInstanceProfile:
    def test_success(
        self,
        get_metadata_mock: MagicMock,
        mod_logger_mock: MagicMock,
    ) -> None:
        # GIVEN
        get_metadata_mock.return_value.status_code = 404
        get_metadata_mock.return_value.text = "some data"
        logger_info: MagicMock = mod_logger_mock.info

        # WHEN/THEN (no error, returns)
        bootstrap_mod._enforce_no_instance_profile()

        # THEN
        logger_info.mock_call_list == [
            call("IMDS /iam/info response 404"),
            call("Instance profile disassociated, proceeding to run tasks."),
        ]

    def test_instance_profile_attached_max_attempts_raises(
        self,
        get_metadata_mock: MagicMock,
    ) -> None:
        # GIVEN
        get_metadata_mock.return_value.status_code = 200
        get_metadata_mock.return_value.text = "some data"

        # THEN
        with raises(bootstrap_mod.InstanceProfileAttachedError):
            # WHEN
            bootstrap_mod._enforce_no_instance_profile()

    def test_imds_unexpected_error(
        self,
        get_metadata_mock: MagicMock,
        mod_logger_mock: MagicMock,
    ) -> None:
        # GIVEN
        unexpected_status_code = 500
        unexpected_error_response = MagicMock()
        unexpected_error_response.status_code = unexpected_status_code
        unexpected_error_response.text = "some data"
        profile_disassociated_response = MagicMock()
        profile_disassociated_response.status_code = 404
        profile_disassociated_response.text = "some data"

        get_metadata_mock.side_effect = [
            unexpected_error_response,
            profile_disassociated_response,
        ]
        logger_warning: MagicMock = mod_logger_mock.warning

        # WHEN
        bootstrap_mod._enforce_no_instance_profile()

        # THEN
        logger_warning.assert_called_once_with(
            "Unexpected HTTP status code (%d) from /iam/info IMDS response",
            unexpected_status_code,
        )


class TestEnforceNoInstanceProfileOrStopWorker:
    @fixture(autouse=True)
    def mock_enforce_no_instance_profile(self) -> Generator[MagicMock, None, None]:
        with patch.object(
            bootstrap_mod, "_enforce_no_instance_profile"
        ) as mock_enforce_no_instance_profile:
            yield mock_enforce_no_instance_profile

    @fixture(autouse=True)
    def update_worker_mock(self) -> Generator[MagicMock, None, None]:
        with patch.object(bootstrap_mod, "update_worker") as mock:
            yield mock

    @fixture(autouse=True)
    def allow_instance_profile(self) -> bool:
        return False

    @mark.parametrize(argnames="allow_instance_profile", argvalues=(True,))
    def test_allow_instance_profile_is_noop(
        self,
        mock_enforce_no_instance_profile: MagicMock,
        update_worker_mock: MagicMock,
        config: Configuration,
        client: MagicMock,
        worker_id: str,
    ) -> None:
        # WHEN
        bootstrap_mod._enforce_no_instance_profile_or_stop_worker(
            config=config,
            worker_id=worker_id,
            deadline_client=client,
        )

        # THEN
        mock_enforce_no_instance_profile.assert_not_called()
        update_worker_mock.assert_not_called()

    def test_success(
        self,
        mock_enforce_no_instance_profile: MagicMock,
        update_worker_mock: MagicMock,
        config: Configuration,
        worker_id: str,
        client: MagicMock,
    ) -> None:
        # WHEN
        bootstrap_mod._enforce_no_instance_profile_or_stop_worker(
            config=config,
            worker_id=worker_id,
            deadline_client=client,
        )

        # THEN
        mock_enforce_no_instance_profile.assert_called_once_with()
        update_worker_mock.assert_not_called()

    def test_instance_profile_attached_stops_worker(
        self,
        mock_enforce_no_instance_profile: MagicMock,
        update_worker_mock: MagicMock,
        config: Configuration,
        worker_id: str,
        client: MagicMock,
    ) -> None:
        # GIVEN
        exception = bootstrap_mod.InstanceProfileAttachedError()
        mock_enforce_no_instance_profile.side_effect = exception

        # THEN
        with raises(bootstrap_mod.InstanceProfileAttachedError) as raise_ctx:
            # WHEN
            bootstrap_mod._enforce_no_instance_profile_or_stop_worker(
                config=config,
                worker_id=worker_id,
                deadline_client=client,
            )

        # THEN
        assert raise_ctx.value is exception
        mock_enforce_no_instance_profile.assert_called_once_with()
        update_worker_mock.assert_called_once_with(
            deadline_client=client,
            farm_id=config.farm_id,
            fleet_id=config.fleet_id,
            worker_id=worker_id,
            status=WorkerStatus.STOPPED,
        )
