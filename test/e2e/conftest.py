# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import dataclasses
import logging
import os
import sys
import threading
import traceback
from collections.abc import Generator
from configparser import ConfigParser
from contextlib import contextmanager
from dataclasses import InitVar, dataclass, field
from typing import Callable, Optional, Type, cast

import backoff
import boto3
import pytest
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from deadline.client.api import (
    get_boto3_client,
    get_queue_user_boto3_session,
)
from deadline.client.config import set_setting as set_deadline_setting
from deadline.job_attachments.download import get_s3_client, get_s3_transfer_manager
from deadline_test_fixtures import (
    BootstrapResources,
    DeadlineWorker,
    DeadlineWorkerConfiguration,
    DockerContainerWorker,
    EC2InstanceWorker,
    Ec2Tag,
    Farm,
    Fleet,
    LocalMacWorker,
    OperatingSystem,
    PosixSessionUser,
    Queue,
)

LOG = logging.getLogger(__name__)

pytest_plugins = ["deadline_test_fixtures.pytest_hooks"]

_VALID_SESSION_RUNTIMES = ("python", "rust", "service-selected")


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--session-runtime",
        choices=_VALID_SESSION_RUNTIMES,
        default=None,
        help=(
            "Pin the session_runtime worker config setting for every worker in the run. "
            "Accepts: python, rust, service-selected. "
            "Falls back to the WORKER_AGENT_SESSION_RUNTIME env var when not given."
        ),
    )


@pytest.fixture(scope="session")
def session_runtime_option(request: pytest.FixtureRequest) -> Optional[str]:
    """Resolve the session runtime from the CLI option or environment variable.

    CLI option takes precedence over the environment variable.
    Returns None when neither is set (agent default behaviour).
    """
    value: Optional[str] = request.config.getoption("--session-runtime")
    if value is None:
        value = os.environ.get("WORKER_AGENT_SESSION_RUNTIME")
        if value is not None and value not in _VALID_SESSION_RUNTIMES:
            raise pytest.UsageError(
                f"WORKER_AGENT_SESSION_RUNTIME={value!r} is invalid. "
                f"Valid choices: {', '.join(_VALID_SESSION_RUNTIMES)}"
            )
    return value


@dataclass(frozen=True)
class DeadlineResources:
    farm: Farm = field(init=False)
    queue_a: Queue = field(init=False)
    queue_b: Queue = field(init=False)
    jobs_run_as_agent_user_queue: Queue = field(init=False)
    non_valid_role_queue: Queue = field(init=False)
    fleet: Fleet = field(init=False)
    scaling_queue: Queue = field(init=False)
    scaling_fleet: Fleet = field(init=False)
    queue_a_job_storage_profile_id: str = field(init=False)
    windows_job_storage_profile_id: str
    fleet_storage_profile_id: str
    windows_fleet_storage_profile_id: str

    farm_id: InitVar[str]
    queue_a_id: InitVar[str]
    queue_b_id: InitVar[str]
    jobs_run_as_agent_user_queue_id: InitVar[str]
    non_valid_role_queue_id: InitVar[str]
    fleet_id: InitVar[str]
    scaling_queue_id: InitVar[str]
    scaling_fleet_id: InitVar[str]
    job_storage_profile_id: InitVar[str]

    def __post_init__(
        self,
        farm_id: str,
        queue_a_id: str,
        queue_b_id: str,
        jobs_run_as_agent_user_queue_id: str,
        non_valid_role_queue_id: str,
        fleet_id: str,
        scaling_queue_id: str,
        scaling_fleet_id: str,
        job_storage_profile_id: str,
    ) -> None:
        object.__setattr__(self, "farm", Farm(id=farm_id))
        object.__setattr__(self, "queue_a", Queue(id=queue_a_id, farm=self.farm))
        object.__setattr__(self, "queue_b", Queue(id=queue_b_id, farm=self.farm))
        object.__setattr__(
            self,
            "jobs_run_as_agent_user_queue",
            Queue(id=jobs_run_as_agent_user_queue_id, farm=self.farm),
        )
        object.__setattr__(
            self,
            "non_valid_role_queue",
            Queue(id=non_valid_role_queue_id, farm=self.farm),
        )
        object.__setattr__(self, "fleet", Fleet(id=fleet_id, farm=self.farm, autoscaling=False))
        object.__setattr__(self, "scaling_queue", Queue(id=scaling_queue_id, farm=self.farm))
        object.__setattr__(self, "scaling_fleet", Fleet(id=scaling_fleet_id, farm=self.farm))
        object.__setattr__(self, "queue_a_job_storage_profile_id", job_storage_profile_id)


def _shutdown_s3_transfer_manager(
    s3_client: BaseClient,
    desc: str,
) -> None:
    """
    The s3transfer.manager.TransferManager maintains concurrent.futures.Exeuctor instance (by
    default a ThreadPoolExecutor) which needs to be shut down. The deadline library maintains an
    lru_cache mapping s3 client -> TransferManager, so these instances are long-lived for the life
    of the Python process.

    This helper method will obtain the TransferManager instance mapped to a s3 client instance and
    shut it down.

    Parameters:
        s3_client: The boto3 s3 client instance to use to lookup the TransferManager instance
        desc: A description for the shutdown context
    """
    num_threads_before = threading.active_count()
    s3_transfer_manager = get_s3_transfer_manager(s3_client)
    LOG.info(f"Shutting down S3 Transfer Manager: {desc}")
    s3_transfer_manager.shutdown()
    num_threads_after = threading.active_count()
    num_threads_joined = num_threads_before - num_threads_after
    LOG.info(f"Shut down S3 Transfer Manager: {desc} (num threads joined: {num_threads_joined})")


@pytest.fixture(scope="session")
def deadline_resources() -> Generator[DeadlineResources, None, None]:
    """
    Gets Deadline resources required for running tests.

    Environment Variables:
        FARM_ID: ID of the Deadline farm to use.
        QUEUE_A_ID: ID of a non scaling Deadline queue to use for tests.
        QUEUE_B_ID: ID of a non scaling Deadline queue to use for tests.
        JOBS_RUN_AS_AGENT_USER_QUEUE_ID: ID of a Queue configured to run jobs as the worker agent user
        NON_VALID_ROLE_QUEUE_ID: ID of a non scaling Deadline queue with a role that cannot read the S3 bucket to use for tests
        FLEET_ID: ID of a non scaling Deadline fleet to use for tests.
        SCALING_QUEUE_ID: ID of the Deadline scaling queue to use.
        SCALING_FLEET_ID: ID of the Deadline scaling fleet to use.
        JOB_STORAGE_PROFILE_ID: ID of the Deadline storage profile to use for Linux jobs
        WINDOWS_JOB_STORAGE_PROFILE_ID: ID of the Deadline storage profile to use for Windows jobs
        FLEET_STORAGE_PROFILE_ID: ID of the Deadline storage profile to use for the Linux fleet
        WINDOWS_FLEET_STORAGE_PROFILE_ID: ID of the Deadline storage profile to use for the Windows fleet
        TEST_CATEGORY: The type of test that's being run. Will usually be one of dev, <OS>Mainline, or <OS>Release

    Returns:
        DeadlineResources: The Deadline resources used for tests
    """
    farm_id = os.environ["FARM_ID"]
    queue_a_id = os.environ["QUEUE_A_ID"]
    queue_b_id = os.environ["QUEUE_B_ID"]
    jobs_run_as_agent_user_queue_id = os.environ["JOBS_RUN_AS_AGENT_USER_QUEUE_ID"]
    non_valid_role_queue_id = os.environ["NON_VALID_ROLE_QUEUE_ID"]
    fleet_id = os.environ["FLEET_ID"]

    scaling_queue_id = os.environ["SCALING_QUEUE_ID"]
    scaling_fleet_id = os.environ["SCALING_FLEET_ID"]
    job_storage_profile_id = os.environ["JOB_STORAGE_PROFILE_ID"]
    windows_job_storage_profile_id = os.environ["WINDOWS_JOB_STORAGE_PROFILE_ID"]
    fleet_storage_profile_id = os.environ["FLEET_STORAGE_PROFILE_ID"]
    windows_fleet_storage_profile_id = os.environ["WINDOWS_FLEET_STORAGE_PROFILE_ID"]
    test_category = os.environ.get("TEST_CATEGORY", "dev")

    LOG.info(
        f"Configured Deadline Cloud Resources - Farm ID: {farm_id}, "
        f"Scaling Fleet ID: {scaling_fleet_id}, "
        f"Scaling Queue ID: {scaling_queue_id}, "
        f"Queue A ID: {queue_a_id}, "
        f"Queue A Storage Profile ID: {job_storage_profile_id}, "
        f"Queue A Windows Storage Profile ID: {windows_job_storage_profile_id}, "
        f"Queue B ID: {queue_b_id}, "
        f"Fleet ID: {fleet_id}, "
        f"Fleet Storage Profile ID: {fleet_storage_profile_id}, "
        f"Fleet Windows Storage Profile ID: {windows_fleet_storage_profile_id}, "
        f"Jobs Run As Agent User Queue ID: {jobs_run_as_agent_user_queue_id}, "
        f"Test Type: {test_category}, "
    )

    sts_client = boto3.client("sts")
    response = sts_client.get_caller_identity()
    LOG.info("Running tests with credentials from: %s" % response.get("Arn"))

    yield DeadlineResources(
        farm_id=farm_id,
        queue_a_id=queue_a_id,
        queue_b_id=queue_b_id,
        jobs_run_as_agent_user_queue_id=jobs_run_as_agent_user_queue_id,
        non_valid_role_queue_id=non_valid_role_queue_id,
        fleet_id=fleet_id,
        scaling_queue_id=scaling_queue_id,
        scaling_fleet_id=scaling_fleet_id,
        job_storage_profile_id=job_storage_profile_id,
        windows_job_storage_profile_id=windows_job_storage_profile_id,
        fleet_storage_profile_id=fleet_storage_profile_id,
        windows_fleet_storage_profile_id=windows_fleet_storage_profile_id,
    )

    # HACK: Cleanup the s3transfer.manager.TransferManager instances that are held in lru_cache.
    # Each one maintains a concurrent.futures.ThreadPoolExecutor instance which contains a pool
    # of threads. This must be explicitly shutdown since the threads are non-daemon and will keep
    # the Python process open.
    config = ConfigParser()
    set_deadline_setting("defaults.farm_id", farm_id, config)
    deadline = get_boto3_client("deadline", config=config)

    # For job attachment upload, deadline Python library assumes the role of the queue like below to
    # create a s3 boto client. Only the queues below use the deadline Python library to submit jobs
    queues_to_shutdown = (
        ("queue_a", queue_a_id),
        ("non_valid_role_queue_id", non_valid_role_queue_id),
    )
    for queue_label, queue_id in queues_to_shutdown:
        set_deadline_setting("defaults.queue_id", queue_id, config)
        queue = deadline.get_queue(
            farmId=farm_id,
            queueId=queue_id,
        )
        queue_role_session = get_queue_user_boto3_session(
            deadline=deadline,
            config=config,
            farm_id=farm_id,
            queue_id=queue_id,
            queue_display_name=queue["displayName"],
        )
        queue_s3_client = get_s3_client(queue_role_session)
        _shutdown_s3_transfer_manager(
            s3_client=queue_s3_client,
            desc=f"for queue {queue_label} ({queue_id})",
        )

    # The wait_for_job_output function in e2e.utils does not provide a session, so the default
    # s3 client is used like below
    default_s3_client = get_s3_client()
    _shutdown_s3_transfer_manager(
        s3_client=default_s3_client,
        desc="for the default s3 client",
    )


@pytest.fixture(scope="session")
def test_runner_identity() -> dict[str, str]:
    sts_client = boto3.client("sts")
    return sts_client.get_caller_identity()


@pytest.fixture(scope="session")
def worker_config(
    posix_job_user: PosixSessionUser,
    posix_env_override_job_user: PosixSessionUser,
    posix_config_override_job_user: PosixSessionUser,
    worker_config: DeadlineWorkerConfiguration,
    windows_job_users: list[str],
    session_runtime_option: Optional[str],
) -> DeadlineWorkerConfiguration:
    """
    Builds the configuration for a DeadlineWorker.

    Environment Variables:
        WORKER_POSIX_USER: The POSIX user to configure the worker for
            Defaults to "deadline-worker"
        WORKER_POSIX_SHARED_GROUP: The shared POSIX group to configure the worker user and job user with
            Defaults to "shared-group"
        WORKER_AGENT_WHL_PATH: Path to the Worker agent wheel file to use.
        WORKER_AGENT_REQUIREMENT_SPECIFIER: PEP 508 requirement specifier for the Worker agent package.
            If WORKER_AGENT_WHL_PATH is provided, this option is ignored.
        LOCAL_MODEL_PATH: Path to a local Deadline model file to use for API calls.
            If DEADLINE_SERVICE_MODEL_S3_URI was provided, this option is ignored.
        WORKER_AGENT_SESSION_RUNTIME: Pin the session_runtime worker config setting.
            Accepts: python, rust, service-selected. Overridden by --session-runtime CLI option.

    Returns:
        DeadlineWorkerConfiguration: Configuration for use by DeadlineWorker.
    """

    return dataclasses.replace(
        worker_config,
        job_users=[
            posix_job_user,
            posix_config_override_job_user,
            posix_env_override_job_user,
        ],
        windows_job_users=windows_job_users,
        # TODO: Temporary workaround due to AWS CLI v2 upgrade causing canary failures when copying over AWS models for deadline
        service_model_path=None,
        session_runtime=session_runtime_option,
    )


@pytest.fixture(scope="session")
def _session_worker_impl(
    request: pytest.FixtureRequest,
    worker_config: DeadlineWorkerConfiguration,
) -> Generator[DeadlineWorker, None, None]:
    with create_worker(worker_config, request) as worker:
        # Recorded so create_worker can tell "the incumbent is the session worker" from "the
        # incumbent is another per-test worker" without requesting this fixture and creating it.
        request.session.stash[_SESSION_WORKER_KEY] = worker
        yield worker

    stop_worker(request, worker)


@pytest.fixture
def session_worker(
    _session_worker_impl: DeadlineWorker,
) -> DeadlineWorker:
    """The session-scoped worker, reinstalled first if a per-test worker displaced it.

    A thin function-scoped wrapper over the cached fixture: pytest hands back the same object for
    the whole session, so without a per-use hook a worker stopped by a macOS per-test install would
    stay stale and every later assertion would run against a host with no agent. A no-op on Linux
    and Windows, where workers are separate instances.
    """
    reinstate_session_worker(_session_worker_impl)
    return _session_worker_impl


@pytest.fixture(scope="class")
def asset_sync_worker_config(
    request: pytest.FixtureRequest,
    posix_job_user: PosixSessionUser,
    posix_env_override_job_user: PosixSessionUser,
    posix_config_override_job_user: PosixSessionUser,
    worker_config: DeadlineWorkerConfiguration,
    windows_job_users: list[str],
) -> DeadlineWorkerConfiguration:
    """
    Worker configuration fixture for asset sync testing.
    """
    return dataclasses.replace(
        worker_config,
        job_users=[
            posix_job_user,
            posix_config_override_job_user,
            posix_env_override_job_user,
        ],
        windows_job_users=windows_job_users,
    )


@pytest.fixture(scope="class")
def asset_sync_class_worker(
    request: pytest.FixtureRequest,
    asset_sync_worker_config: DeadlineWorkerConfiguration,
    ec2_worker_type: Type[EC2InstanceWorker],
) -> Generator[DeadlineWorker, None, None]:
    with create_worker(asset_sync_worker_config, request) as worker:
        yield worker

    stop_worker(request, worker)


@pytest.fixture(scope="class")
def class_worker(
    request: pytest.FixtureRequest,
    worker_config: DeadlineWorkerConfiguration,
    ec2_worker_type: Type[EC2InstanceWorker],
) -> Generator[DeadlineWorker, None, None]:
    with create_worker(worker_config, request) as worker:
        yield worker

    stop_worker(request, worker)


@pytest.fixture(scope="function")
def function_worker(
    request: pytest.FixtureRequest,
    worker_config: DeadlineWorkerConfiguration,
    ec2_worker_type: Type[EC2InstanceWorker],
) -> Generator[DeadlineWorker, None, None]:
    with create_worker(worker_config, request) as worker:
        yield worker

    stop_worker(request, worker)


@pytest.fixture(scope="function")
def function_worker_factory(
    request: pytest.FixtureRequest,
    ec2_worker_type: Type[EC2InstanceWorker],
) -> Generator[Callable[[DeadlineWorkerConfiguration], EC2InstanceWorker], None, None]:
    created_workers = []

    def _create_function_worker(
        custom_worker_config: DeadlineWorkerConfiguration,
    ):
        with create_worker(custom_worker_config, request) as worker:
            created_workers.append(worker)
            return worker

    yield _create_function_worker
    for worker in created_workers:
        stop_worker(request, worker)


def _grab_bootstrap_log(worker: DeadlineWorker) -> None:
    """Best-effort grab of the worker bootstrap log after a start failure."""
    if not isinstance(worker, EC2InstanceWorker):
        return
    try:
        if hasattr(worker, "WIN2022_AMI_NAME"):
            log_path = r"C:\ProgramData\Amazon\Deadline\Logs\worker-agent-bootstrap.log"
            toml_path = r"C:\ProgramData\Amazon\Deadline\Config\worker.toml"
            cmd = (
                f'Get-Content "{log_path}" -Tail 100 -ErrorAction SilentlyContinue; '
                f'echo "--- worker.toml ---"; '
                f'Get-Content "{toml_path}" -ErrorAction SilentlyContinue; '
                f'echo "--- toml validation ---"; '
                f"python -c \"import tomllib,sys; tomllib.load(open(sys.argv[1],'rb')); print('valid')\" \"{toml_path}\" 2>&1; "
                f'echo "--- worker agent process ---"; '
                f"Get-Process pythonservice -ErrorAction SilentlyContinue; "
                f"Get-Service DeadlineWorker -ErrorAction SilentlyContinue"
            )
        else:
            log_path = "/var/log/amazon/deadline/worker-agent-bootstrap.log"
            cmd = f"tail -n 100 {log_path}"
        result = worker.send_command(cmd, {"Delay": 5, "MaxAttempts": 6})
        LOG.error(f"--- Bootstrap log ({log_path}) ---\n{result.stdout}")
        if result.stderr:
            LOG.error(f"--- Debug command stderr ---\n{result.stderr}")
    except Exception as log_err:
        LOG.warning(f"Could not retrieve bootstrap log: {log_err}")


# One host, one agent. LocalMacWorker installs onto the machine running the tests, so two workers
# cannot coexist: the second start() overwrites the first's worker.toml, worker.json and
# LaunchDaemon, and whichever configuration landed last is the one under test.
#
# Rather than skip every test that wants a differently-configured worker, macOS installs them one at
# a time. A worker created here displaces whatever is installed, and the session worker is
# reinstated on next use if it was the one displaced -- lazily, so a run that never returns to it
# pays nothing. stop() leaves the venv and the account, so reinstalling is the installer plus a
# launchd bootstrap rather than a full provision.
#
# Not a mechanism on Linux or Windows: there each worker is its own instance and they are genuinely
# concurrent, so nothing here changes for them.
_MACOS = os.environ.get("OPERATING_SYSTEM") == "macos"
_SESSION_WORKER_KEY: pytest.StashKey[DeadlineWorker] = pytest.StashKey()
_installed_worker: Optional[DeadlineWorker] = None
_displaced_session_worker: Optional[DeadlineWorker] = None


def _claim_host(worker: DeadlineWorker, session_worker: Optional[DeadlineWorker]) -> None:
    """Stop whatever currently holds the host so `worker` can install onto it."""
    global _installed_worker, _displaced_session_worker
    current = _installed_worker
    if current is not None and current is not worker:
        LOG.info("Stopping the worker currently installed on this host before installing another")
        try:
            current.stop()
        except Exception:  # pragma: no cover
            LOG.exception("Failed to stop the incumbent worker; the next install may fail")
        if session_worker is not None and current is session_worker:
            _displaced_session_worker = current
    # Set after the stop above, so a stop_worker racing in between sees the incumbent rather than
    # the newcomer and does not skip a teardown that was still owed.
    _installed_worker = worker


def reinstate_session_worker(worker: DeadlineWorker) -> None:
    """Reinstall the session worker if a per-test worker displaced it.

    Called from the `session_worker` fixture on every use, so the cost is one reinstall per
    transition back rather than one per test.
    """
    global _installed_worker, _displaced_session_worker
    if not _MACOS or _displaced_session_worker is not worker:
        return
    LOG.info("Reinstating the session worker, displaced by a per-test worker")
    _displaced_session_worker = None
    worker.start()
    _installed_worker = worker


def create_worker(
    worker_config: DeadlineWorkerConfiguration,
    request: pytest.FixtureRequest,
):
    def __init__(self):
        pass

    def __enter_(self):
        print("Entering the context")

    def __exit__(self, exc_type, exc_value, traceback):
        print("Exiting the context")

    """
    Gets a DeadlineWorker for use in tests.

    Environment Variables:
        SUBNET_ID: The subnet ID to deploy the EC2 worker into.
            This is required for EC2 workers. Does not apply if USE_DOCKER_WORKER is true.
        SECURITY_GROUP_ID: The security group ID to deploy the EC2 worker into.
            This is required for EC2 workers. Does not apply if USE_DOCKER_WORKER is true.
        AMI_ID: The AMI ID to use for the Worker agent.
            Defaults to the latest AL2023 AMI.
            Does not apply if USE_DOCKER_WORKER is true.
        USE_DOCKER_WORKER: If set to "true", this fixture will create a Worker that runs in a local Docker container instead of an EC2 instance.
        KEEP_WORKER_AFTER_FAILURE: If set to "true", will not destroy the Worker when it fails. Useful for debugging. Default is "false"

    Returns:
        DeadlineWorker: Instance of the DeadlineWorker class that can be used to interact with the Worker.
    """

    # Resolved here rather than taken as a parameter: every call site passed the same fixture
    # value, and the macOS branch below needs the operating system anyway.
    operating_system: OperatingSystem = request.getfixturevalue("operating_system")
    worker_type = request.getfixturevalue("ec2_worker_type")

    worker: DeadlineWorker
    if os.environ.get("USE_DOCKER_WORKER", "").lower() == "true":
        LOG.info("Creating Docker worker")
        worker = DockerContainerWorker(
            configuration=worker_config,
        )
    elif operating_system.is_macos():
        # Ahead of the EC2 branch, because none of what it needs exists here: LocalMacWorker
        # installs the agent onto this host, so there is no instance to place in a subnet or a
        # security group and no instance profile to attach. The asserts below would fail on a
        # host that is otherwise able to run the suite.
        LOG.info("Creating local macOS worker")
        worker = cast(Type[LocalMacWorker], worker_type)(
            configuration=worker_config,
            deadline_client=boto3.client("deadline"),
        )
    else:
        LOG.info("Creating EC2 worker")
        ami_id = os.getenv("AMI_ID")
        subnet_id = os.getenv("SUBNET_ID")
        security_group_id = os.getenv("SECURITY_GROUP_ID")
        instance_type = os.getenv("WORKER_INSTANCE_TYPE", default="t3.large")
        instance_shutdown_behavior = os.getenv("WORKER_INSTANCE_SHUTDOWN_BEHAVIOR", default="stop")
        test_category = os.getenv("TEST_CATEGORY", "dev")

        assert subnet_id, "SUBNET_ID is required when deploying an EC2 worker"
        assert security_group_id, "SECURITY_GROUP_ID is required when deploying an EC2 worker"

        bootstrap_resources: BootstrapResources = request.getfixturevalue("bootstrap_resources")
        assert bootstrap_resources.worker_instance_profile_name, (
            "Worker instance profile is required when deploying an EC2 worker"
        )

        ec2_client = boto3.client("ec2")
        s3_client = boto3.client("s3")
        ssm_client = boto3.client("ssm")
        deadline_client = boto3.client("deadline")

        worker = cast(Type[EC2InstanceWorker], worker_type)(
            ec2_client=ec2_client,
            s3_client=s3_client,
            deadline_client=deadline_client,
            bootstrap_bucket_name=bootstrap_resources.bootstrap_bucket_name,
            ssm_client=ssm_client,
            override_ami_id=ami_id,
            subnet_id=subnet_id,
            security_group_id=security_group_id,
            instance_profile_name=bootstrap_resources.worker_instance_profile_name,
            configuration=worker_config,
            instance_type=instance_type,
            instance_shutdown_behavior=instance_shutdown_behavior,
            additional_tags=[Ec2Tag(key="TestCategory", value=test_category)],
        )

    @contextmanager
    def _context_for_fixture():
        try:
            if _MACOS:
                # Before start(), so the incumbent is stopped rather than overwritten. The session
                # worker is looked up rather than requested, because requesting it here would
                # create it for tests that never wanted one.
                _claim_host(worker, request.session.stash.get(_SESSION_WORKER_KEY, None))
            worker.start()
        except Exception as e:
            LOG.error(f"Failed to start worker: {e}")
            _grab_bootstrap_log(worker)
            LOG.info("Stopping worker because it failed to start")
            stop_worker(request, worker)
            raise
        yield worker

    return _context_for_fixture()


def stop_worker(request: pytest.FixtureRequest, worker: DeadlineWorker) -> None:
    # A displaced macOS worker has already been stopped, and stopping it again raises: stop()
    # deletes the worker record, and the second DeleteWorker on the same id fails. Only the worker
    # that still holds the host has anything left to tear down.
    global _installed_worker
    if _MACOS:
        if _installed_worker is not worker:
            LOG.info("Worker was already displaced from this host; nothing to stop")
            return
        _installed_worker = None

    if request.session.testsfailed > 0:
        if os.getenv("KEEP_WORKER_AFTER_FAILURE", "false").lower() == "true":
            LOG.info("KEEP_WORKER_AFTER_FAILURE is set, not stopping worker")
            return

    def _giveup_unless_conflict(e: ClientError) -> bool:
        return e.response["Error"]["Code"] != "ConflictException"

    @backoff.on_exception(
        backoff.constant,
        ClientError,
        max_tries=5,
        interval=30,
        giveup=_giveup_unless_conflict,
    )
    def _stop_with_retry() -> None:
        worker.stop()

    try:
        _stop_with_retry()
    except Exception as e:
        LOG.exception(f"Error while stopping worker: {e}")
        LOG.error(
            "Failed to stop worker. Resources may be left over that need to be cleaned up manually."
        )
        raise


@pytest.fixture(scope="session")
def region() -> str:
    return os.getenv("REGION", os.getenv("AWS_DEFAULT_REGION", "us-west-2"))


@pytest.fixture(scope="session")
def posix_job_user() -> PosixSessionUser:
    return PosixSessionUser(
        user="job-user",
        group="job-user",
    )


@pytest.fixture(scope="session")
def posix_config_override_job_user() -> PosixSessionUser:
    return PosixSessionUser(
        user="config-override",
        group="job-override-group",
    )


@pytest.fixture(scope="session")
def posix_env_override_job_user() -> PosixSessionUser:
    return PosixSessionUser(
        user="env-override",
        group="job-override-group",
    )


@pytest.fixture(scope="session")
def generic_non_queue_job_user() -> PosixSessionUser:
    return PosixSessionUser(
        user="non-queue-user",
        group="job-override-group",
    )


@pytest.fixture(scope="session")
def windows_job_users() -> list[str]:
    return [
        "job-user",
        "cli-override",
        "config-override",
        "install-override",
        "env-override",
    ]


@pytest.fixture(scope="session")
def operating_system() -> OperatingSystem:
    os_env_var = os.environ.get("OPERATING_SYSTEM")
    if os_env_var == "linux":
        return OperatingSystem(name="AL2023")
    elif os_env_var == "windows":
        return OperatingSystem(name="WIN2022")
    elif os_env_var == "macos":
        # deadline-cloud-test-fixtures accepts MACOS from 0.18.21 and ships LocalMacWorker, but
        # nothing selects it: its ec2_worker_type raises ValueError for anything but AL2023 and
        # WIN2022, and create_worker asserts a subnet and security group a native Mac host has
        # not got. Wiring MACOS to LocalMacWorker is still to do; until then this branch reaches
        # a worker fixture that rejects it.
        return OperatingSystem(name="MACOS")
    else:
        assert False, (
            f'Expected OPERATING_SYSTEM env var to be "linux", "windows", or "macos", '
            f"but got {os_env_var}"
        )


def pytest_collection_modifyitems(items):
    sorted_list = list(items)
    session_worker_tests = []
    asset_sync_class_worker_tests = []

    for item in items:
        # Check for conflicting fixture usage
        has_session_worker = "session_worker" in item.fixturenames
        has_asset_sync_class_worker = "asset_sync_class_worker" in item.fixturenames

        if has_session_worker and has_asset_sync_class_worker:
            raise ValueError(
                f"Test {item.nodeid} requests both 'session_worker' and 'asset_sync_class_worker' fixtures. "
                "This would create conflicting workers. Use only one session worker fixture per test."
            )

        # Separate session worker tests into two groups to prevent worker conflicts
        if has_asset_sync_class_worker:
            sorted_list.remove(item)
            asset_sync_class_worker_tests.append(item)
        elif has_session_worker:
            sorted_list.remove(item)
            session_worker_tests.append(item)

    # Run asset sync class worker tests first, then session worker tests. This ensures
    # 1. job attachments tests are run by one asset sync worker at a time
    sorted_list.extend(asset_sync_class_worker_tests)
    # 2. only one session worker is active at a time
    sorted_list.extend(session_worker_tests)

    items[:] = sorted_list


def _output_thread_stack_traces() -> None:
    print("\n*** THREAD STACKTRACE - START ***\n", file=sys.stderr)

    for threadId, stack in sys._current_frames().items():
        print(f"\n{'=' * 60}", file=sys.stderr)
        print(f"ThreadID: {threadId}", file=sys.stderr)
        print(f"{'=' * 60}", file=sys.stderr)

        for filename, lineno, name, line in traceback.extract_stack(stack):
            print(f'File: "{filename}", line {lineno}, in {name}', file=sys.stderr)
            if line:
                print(f"  {line.strip()}", file=sys.stderr)
        print("", file=sys.stderr)

    print("\n*** THREAD STACKTRACE - END ***\n", file=sys.stderr)


def pytest_unconfigure(config):
    """Pytest hook that runs at the end of the test session to output thread stack traces."""
    if not os.getenv("DEBUG_THREAD_STACKS"):
        return

    _output_thread_stack_traces()


def pytest_sessionfinish(session, exitstatus):
    """Pytest hook that runs at the end of the test session to output thread stack traces."""
    if not os.getenv("DEBUG_THREAD_STACKS"):
        return

    _output_thread_stack_traces()
