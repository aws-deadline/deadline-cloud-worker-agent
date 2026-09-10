# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import dataclasses
import logging
import os
import sys
import threading
import traceback
from collections.abc import Generator
from configparser import ConfigParser
from contextlib import ExitStack, contextmanager
from dataclasses import InitVar, dataclass, field
from typing import Callable, Optional, Type, TypeVar

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
    CodeArtifactRepositoryInfo,
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
    QueueFleetAssociation,
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


def _scaffold_deadline_resources(
    request: pytest.FixtureRequest,
    operating_system: OperatingSystem,
) -> Generator[DeadlineResources, None, None]:
    """Create the Deadline resources for this run, then delete them.

    Used when BYO_DEADLINE is not "true". CI passes pre-provisioned IDs instead,
    which is cheaper per run; this path exists so a run can stand itself up on a
    platform CI has no resources for yet, and so a developer needs only
    credentials rather than twelve resource IDs.

    Resource roles come from `bootstrap_resources`, which either deploys the
    bootstrap CloudFormation stack or reads its own env vars when
    BYO_BOOTSTRAP=true. CreateFleet requires a roleArn, so there is no
    role-free variant of this path.
    """
    bootstrap: BootstrapResources = request.getfixturevalue("bootstrap_resources")
    deadline_client = boto3.client("deadline")

    os_family = (
        "MACOS"
        if operating_system.is_macos()
        else ("WINDOWS" if operating_system.name == "WIN2022" else "LINUX")
    )
    cpu_architecture = "arm64" if operating_system.is_macos() else "x86_64"

    def _storage_profile(display_name: str, family: str, path: str) -> str:
        # The path mapping under test works by giving the job's profile and the fleet's
        # profile a location with the SAME name and different paths; the agent then
        # rewrites one to the other. A profile with no fileSystemLocations makes the
        # test that reads them fail on a missing key, so mirror the shape the
        # CloudFormation scaffolding in scripts/e2e_testing_infrastructure.yaml uses.
        response = deadline_client.create_storage_profile(
            farmId=farm.id,
            displayName=display_name,
            osFamily=family,
            fileSystemLocations=[{"name": "StorageProfileTest", "path": path, "type": "LOCAL"}],
        )
        storage_profile_id = response["storageProfileId"]
        # Storage profiles are not modelled by deadline-cloud-test-fixtures, so
        # register the delete directly. Registered on the same stack as everything
        # else, which unwinds in reverse, so these go before the farm they belong to.
        stack.callback(
            lambda: deadline_client.delete_storage_profile(
                farmId=farm.id, storageProfileId=storage_profile_id
            )
        )
        return storage_profile_id

    def _fleet_configuration(mode: str, storage_profile_id: str | None = None) -> dict:
        customer_managed: dict = {
            "mode": mode,
            "workerCapabilities": {
                "vCpuCount": {"min": 1},
                "memoryMiB": {"min": 1024},
                # The API models osFamily as WINDOWS | LINUX | MACOS. botocore does
                # not validate enums client-side, so a wrong casing fails at the
                # service rather than locally.
                "osFamily": os_family,
                "cpuArchitectureType": cpu_architecture,
            },
        }
        if storage_profile_id:
            # Without this the service sends the worker no path mapping rules, so the
            # agent remaps job attachment roots into the session directory instead of the
            # fleet's file system location, and the path mapping tests see no outputs at
            # all. The CloudFormation scaffolding sets it on the non-scaling fleet only.
            customer_managed["storageProfileId"] = storage_profile_id
        return {"customerManaged": customer_managed}

    T = TypeVar("T", Farm, Fleet, Queue, QueueFleetAssociation)

    @contextmanager
    def deletable(resource: T) -> Generator[T, None, None]:
        try:
            yield resource
        finally:
            resource.delete(client=deadline_client)

    _FARM_NAME = "worker-agent-e2e-farm"

    def _report_leftover_farms() -> str:
        """Name any farm this fixture appears to have leaked, for the quota error message.

        A run that is killed rather than failed, such as a cancelled CI job, never unwinds
        the ExitStack, so its farm survives. The farm quota is small enough that one
        leftover makes every later run fail here, and CreateFarm's own message does not say
        which farms are consuming the quota.
        """
        try:
            farms = deadline_client.list_farms()["farms"]
        except Exception:  # pragma: no cover - diagnostics only
            return ""
        leftovers = [f for f in farms if f.get("displayName") == _FARM_NAME]
        if not leftovers:
            return f" No farm named {_FARM_NAME} exists, so the quota is consumed elsewhere."
        ids = ", ".join(f["farmId"] for f in leftovers)
        return (
            f" A farm named {_FARM_NAME} already exists ({ids}), which most likely leaked"
            " from a run that was cancelled before it could clean up. Delete it and retry."
        )

    with ExitStack() as stack:
        try:
            created_farm = Farm.create(client=deadline_client, display_name=_FARM_NAME)
        except ClientError as e:
            if e.response["Error"]["Code"] != "ServiceQuotaExceededException":
                raise
            raise RuntimeError(f"Could not create the e2e farm: {e}.{_report_leftover_farms()}")
        farm = stack.enter_context(deletable(created_farm))

        # Created before the queues, which must allowlist them: a queue rejects a job
        # carrying a storage profile absent from its allowedStorageProfileIds. Four
        # distinct profiles, a job/fleet pair for this run's platform plus a job/fleet
        # pair for Windows, which keeps that family regardless of the run's platform
        # because the tests reading them exercise cross-platform path mapping. The job
        # and fleet halves must differ, or the mapping they prove is the identity.
        # /private/tmp on macOS, where /tmp is a symlink to it: paths the agent resolves
        # come back physical, and a location recorded under the symlink then fails to
        # match. The same substitution is what made the session_root_dir test pass.
        storage_profile_root = (
            "/private/tmp/storageprofiletest"
            if operating_system.is_macos()
            else "/tmp/storageprofiletest"
        )
        run_job_storage_profile = _storage_profile(
            f"e2e-{os_family.lower()}-job-storage-profile", os_family, storage_profile_root
        )
        run_fleet_storage_profile = _storage_profile(
            f"e2e-{os_family.lower()}-fleet-storage-profile",
            os_family,
            f"{storage_profile_root}/dest",
        )
        windows_job_storage_profile = _storage_profile(
            "e2e-windows-job-storage-profile", "WINDOWS", "C:\\Users\\Public\\submission"
        )
        windows_fleet_storage_profile = _storage_profile(
            "e2e-windows-fleet-storage-profile", "WINDOWS", "C:\\Users\\Public\\worker"
        )
        all_storage_profile_ids = [
            run_job_storage_profile,
            run_fleet_storage_profile,
            windows_job_storage_profile,
            windows_fleet_storage_profile,
        ]

        def _queue(display_name: str, **kwargs) -> Queue:
            return stack.enter_context(
                deletable(
                    Queue.create(
                        client=deadline_client,
                        display_name=display_name,
                        farm=farm,
                        job_attachments=bootstrap.job_attachments,
                        raw_kwargs={
                            **kwargs.pop("raw_kwargs", {}),
                            "allowedStorageProfileIds": all_storage_profile_ids,
                        },
                        **kwargs,
                    )
                )
            )

        # CreateQueueFleetAssociation rejects a queue with no jobRunAsUser, so every queue
        # here sets one. Sent as the API shape rather than as a JobRunAsUser dataclass:
        # Queue.create runs that through asdict(), which emits the windows member with an
        # empty passwordArn instead of omitting it, failing client-side validation. The
        # windows member is unnecessary here because these queues only associate with a
        # macOS or Linux fleet.
        job_user: PosixSessionUser = request.getfixturevalue("posix_job_user")
        queue_configured_user = {
            "jobRunAsUser": {
                "runAs": "QUEUE_CONFIGURED_USER",
                "posix": {"user": job_user.user, "group": job_user.group},
            }
        }

        queue_a = _queue(
            "e2e-queue-a",
            role_arn=bootstrap.session_role_arn,
            raw_kwargs={
                **queue_configured_user,
                # Allowing the storage profiles is not enough to get path mapping: the
                # queue has to require the file system location by name before the service
                # emits pathMappingRules for it, and without them the agent silently
                # remaps job attachment roots into the session directory instead. Set on
                # this queue alone, matching MainQueue in the CloudFormation scaffolding.
                "requiredFileSystemLocationNames": ["StorageProfileTest"],
            },
        )
        queue_b = _queue(
            "e2e-queue-b",
            role_arn=bootstrap.session_role_arn,
            raw_kwargs=queue_configured_user,
        )
        scaling_queue = _queue(
            "e2e-scaling-queue",
            role_arn=bootstrap.session_role_arn,
            raw_kwargs=queue_configured_user,
        )
        jobs_run_as_agent_user_queue = _queue(
            "e2e-jobs-run-as-agent-user-queue",
            role_arn=bootstrap.session_role_arn,
            raw_kwargs={"jobRunAsUser": {"runAs": "WORKER_AGENT_USER"}},
        )
        # Deliberately given the worker role rather than the session role: its trust
        # policy does not admit queue-user credential vending, which is the failure
        # the tests using this queue assert on.
        non_valid_role_queue = _queue(
            "e2e-non-valid-role-queue",
            role_arn=bootstrap.worker_role_arn,
            raw_kwargs=queue_configured_user,
        )

        # Matches the count the pre-provisioned CI fleets use. One worker per fleet is not
        # enough even though a run only ever has one host: the suite creates a worker per
        # test in places, and a worker record keeps counting against this until it is
        # deleted, so a low cap makes CreateWorker fail for the rest of the run.
        max_worker_count = 15

        fleet = stack.enter_context(
            deletable(
                Fleet.create(
                    client=deadline_client,
                    display_name="e2e-fleet",
                    farm=farm,
                    configuration=_fleet_configuration("NO_SCALING", run_fleet_storage_profile),
                    max_worker_count=max_worker_count,
                    role_arn=bootstrap.worker_role_arn,
                )
            )
        )
        scaling_fleet = stack.enter_context(
            deletable(
                Fleet.create(
                    client=deadline_client,
                    display_name="e2e-scaling-fleet",
                    farm=farm,
                    configuration=_fleet_configuration("EVENT_BASED_AUTO_SCALING"),
                    max_worker_count=max_worker_count,
                    role_arn=bootstrap.worker_role_arn,
                )
            )
        )

        for queue, target_fleet in (
            (queue_a, fleet),
            (queue_b, fleet),
            (jobs_run_as_agent_user_queue, fleet),
            (non_valid_role_queue, fleet),
            (scaling_queue, scaling_fleet),
        ):
            stack.enter_context(
                deletable(
                    QueueFleetAssociation.create(
                        client=deadline_client, farm=farm, queue=queue, fleet=target_fleet
                    )
                )
            )

        LOG.info(
            f"Created Deadline resources - Farm ID: {farm.id}, Fleet ID: {fleet.id}, "
            f"Queue A ID: {queue_a.id}, osFamily: {os_family}, arch: {cpu_architecture}"
        )

        yield DeadlineResources(
            farm_id=farm.id,
            queue_a_id=queue_a.id,
            queue_b_id=queue_b.id,
            jobs_run_as_agent_user_queue_id=jobs_run_as_agent_user_queue.id,
            non_valid_role_queue_id=non_valid_role_queue.id,
            fleet_id=fleet.id,
            scaling_queue_id=scaling_queue.id,
            scaling_fleet_id=scaling_fleet.id,
            job_storage_profile_id=run_job_storage_profile,
            windows_job_storage_profile_id=windows_job_storage_profile,
            fleet_storage_profile_id=run_fleet_storage_profile,
            windows_fleet_storage_profile_id=windows_fleet_storage_profile,
        )


@pytest.fixture(scope="session")
def deadline_resources(
    request: pytest.FixtureRequest,
    operating_system: OperatingSystem,
) -> Generator[DeadlineResources, None, None]:
    """
    Gets Deadline resources required for running tests.

    Environment Variables:
        BYO_DEADLINE: Whether to use pre-provisioned resources. Defaults to "true",
            which is what CI does. Set it to "false" to have the run create and then
            delete its own farm, queues, fleets, and storage profiles, in which case
            none of the ID variables below are read.
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
    if os.environ.get("BYO_DEADLINE", "true").lower() != "true":
        yield from _scaffold_deadline_resources(request, operating_system)
        return

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
def codeartifact(operating_system: OperatingSystem) -> CodeArtifactRepositoryInfo:
    """Overrides the fixtures package's fixture, which requires four CODEARTIFACT_* vars.

    `worker_config` depends on this unconditionally, so the variables are mandatory even
    for a run that never reaches a CodeArtifact repository. EC2 and container workers do
    need one, because they pip install the agent from a VPC with no route to PyPI. A
    worker on a GitHub-hosted runner reaches PyPI directly and installs the agent from a
    wheel built in the same job, so it needs no repository, and `worker_config` below
    drops this from the install command rather than logging in to it.
    """
    if operating_system.is_macos() and "CODEARTIFACT_DOMAIN" not in os.environ:
        return CodeArtifactRepositoryInfo(
            region=os.getenv("REGION", "us-west-2"),
            domain="unused",
            domain_owner="unused",
            repository="unused",
        )
    return CodeArtifactRepositoryInfo(
        region=os.environ["CODEARTIFACT_REGION"],
        domain=os.environ["CODEARTIFACT_DOMAIN"],
        domain_owner=os.environ["CODEARTIFACT_ACCOUNT_ID"],
        repository=os.environ["CODEARTIFACT_REPOSITORY"],
    )


@pytest.fixture(scope="session")
def worker_config(
    posix_job_user: PosixSessionUser,
    posix_env_override_job_user: PosixSessionUser,
    posix_config_override_job_user: PosixSessionUser,
    worker_config: DeadlineWorkerConfiguration,
    windows_job_users: list[str],
    session_runtime_option: Optional[str],
    operating_system: OperatingSystem,
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

    worker_agent_install = worker_config.worker_agent_install
    if operating_system.is_macos() and "CODEARTIFACT_DOMAIN" not in os.environ:
        # Without this the install command begins with `aws codeartifact login`, which
        # fails against the placeholder repository the fixture above returns. The agent
        # comes from a locally built wheel and its dependencies from PyPI.
        worker_agent_install = dataclasses.replace(worker_agent_install, codeartifact=None)

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
        worker_agent_install=worker_agent_install,
    )


@pytest.fixture(scope="session")
def session_worker(
    request: pytest.FixtureRequest,
    worker_config: DeadlineWorkerConfiguration,
) -> Generator[DeadlineWorker, None, None]:
    with create_worker(worker_config, request) as worker:
        yield worker

    stop_worker(request, worker)


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
) -> Generator[DeadlineWorker, None, None]:
    with create_worker(asset_sync_worker_config, request) as worker:
        yield worker

    stop_worker(request, worker)


@pytest.fixture(scope="class")
def class_worker(
    request: pytest.FixtureRequest,
    worker_config: DeadlineWorkerConfiguration,
) -> Generator[DeadlineWorker, None, None]:
    with create_worker(worker_config, request) as worker:
        yield worker

    stop_worker(request, worker)


@pytest.fixture(scope="function")
def function_worker(
    request: pytest.FixtureRequest,
    worker_config: DeadlineWorkerConfiguration,
) -> Generator[DeadlineWorker, None, None]:
    with create_worker(worker_config, request) as worker:
        yield worker

    stop_worker(request, worker)


@pytest.fixture(scope="function")
def function_worker_factory(
    request: pytest.FixtureRequest,
) -> Generator[Callable[[DeadlineWorkerConfiguration], DeadlineWorker], None, None]:
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

    operating_system: OperatingSystem = request.getfixturevalue("operating_system")

    worker: DeadlineWorker
    if os.environ.get("USE_DOCKER_WORKER", "").lower() == "true":
        LOG.info("Creating Docker worker")
        worker = DockerContainerWorker(
            configuration=worker_config,
        )
    elif operating_system.is_macos():
        # macOS workers run on the host executing the tests rather than a provisioned
        # instance: Mac hardware is only available on EC2 dedicated hosts, which bill a
        # 24-hour minimum per allocation. The CI runner is itself ephemeral, so each job
        # gets a clean host.
        LOG.info("Creating local macOS worker")
        worker = LocalMacWorker(
            configuration=worker_config,
            deadline_client=boto3.client("deadline"),
        )
    else:
        LOG.info("Creating EC2 worker")
        # Resolved here rather than as a fixture parameter: it raises for any non-EC2
        # operating system, so requesting it eagerly would fail the macOS path before it
        # is reached.
        ec2_worker_type: Type[EC2InstanceWorker] = request.getfixturevalue("ec2_worker_type")
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

        worker = ec2_worker_type(
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
