# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from dataclasses import dataclass, asdict, fields, field
from time import sleep
from typing import Any, Dict, Optional, Tuple, cast
import json
import logging as _logging
import stat
import sys

from botocore.exceptions import ClientError
import requests

from ..aws.deadline import (
    DeadlineRequestConditionallyRecoverableError,
    DeadlineRequestUnrecoverableError,
    WorkerHostConfiguration,
    WorkerLogConfig,
    construct_worker_log_config,
    create_worker,
    update_worker,
)
from ..config import Configuration
from .host_properties import get_host_properties as _get_host_properties
from ..api_models import WorkerStatus
from ..boto import DEADLINE_BOTOCORE_CONFIG, DeadlineClient, Session
from ..aws_credentials import WorkerBoto3Session
from ..session_events import configure_session_events
from ..log_messages import (
    AwsCredentialsLogEvent,
    AwsCredentialsLogEventOp,
    FilesystemLogEvent,
    FilesystemLogEventOp,
    WorkerLogEvent,
    WorkerLogEventOp,
)

__all__ = [
    "WorkerPersistenceInfo",
    "bootstrap_worker",
]

_logger = _logging.getLogger(__name__)


# The number of attempts and sleep duration (in seconds) between attempts used to check for instance
# profile disassociation before giving up and quiting the program.
#
# The delay time over all attempts totals two minutes which should be sufficient for an external
# workflow to disassociate the instance profile under normal circumstances.
INSTANCE_PROFILE_REMOVAL_ATTEMPTS = 120
INSTANCE_PROFILE_CHECK_DURATION_SECONDS = 1


class WorkerDeregisteredError(Exception):
    """Exception raised when Worker is deregistered"""

    _worker_id: str

    def __init__(self, *, worker_id: str) -> None:
        self._worker_id = worker_id

    def __str__(self) -> str:  # pragma: no cover
        return f"Worker {self._worker_id} is DEREGISTERED"


class InstanceProfileAttachedError(Exception):
    """
    Exception raised when the Worker resides on an EC2 instance with an instance profile
    attached, but instance profile is not allowed.
    """

    def __str__(self) -> str:  # pragma: no cover
        return (
            "Worker's EC2 instance profile not allowed but attached after "
            f"{INSTANCE_PROFILE_REMOVAL_ATTEMPTS} attempts"
        )


class BootstrapWithoutWorkerLoad(Exception):
    """
    This exception is raised to worker_bootstrap, and indicates that the
    worker_bootstrap function should be recursively called with
    use_existing_worker=False
    """

    pass


@dataclass
class WorkerPersistenceInfo:
    """Information about the Worker that must be persisted between launches of the Worker Agent"""

    worker_id: str
    """The Worker ID"""

    instance_id: str | None = field(
        default=None,
    )
    """The EC2 instance ID of the Worker, if applicable"""

    host_configuration_succeeded: bool | None = field(default=None)

    @classmethod
    def load(cls, *, config: Configuration) -> Optional[WorkerPersistenceInfo]:
        """Load the Worker Bootstrap from the Worker Agent state persistence file"""
        if not config.worker_state_file.is_file():
            return None

        _logger.info(
            FilesystemLogEvent(
                op=FilesystemLogEventOp.READ,
                filepath=str(config.worker_state_file),
                message="Worker state from previous run.",
            )
        )

        with config.worker_state_file.open("r", encoding="utf8") as fh:
            data: dict[str, str | bool] = json.load(fh)

        own_fields = set(f.name for f in fields(class_or_instance=WorkerPersistenceInfo))
        selected_data = {key: value for key, value in data.items() if key in own_fields}
        ignored_keys = data.keys() - own_fields
        if ignored_keys:
            _logger.warning(
                FilesystemLogEvent(
                    op=FilesystemLogEventOp.READ,
                    filepath=str(config.worker_state_file),
                    message=f"Ignoring unknown keys in worker state file: {', '.join(ignored_keys)}",
                )
            )

        return cls(**cast(Dict[str, Any], selected_data))

    def save(self, *, config: Configuration) -> None:
        """Save the Worker Bootstrap to the Worker Agent state persistence file"""
        if not (
            config.worker_state_file.parent.exists() and config.worker_state_file.parent.is_dir()
        ):
            _logger.error(
                FilesystemLogEvent(
                    op=FilesystemLogEventOp.WRITE,
                    filepath=str(config.worker_state_file.parent),
                    message="The configured directory for the worker state file does not exist",
                )
            )
            raise RuntimeError("Cannot save worker state file")

        if (
            config.worker_state_file.is_file()
            and config.worker_state_file.stat().st_mode & stat.S_IWOTH
        ):
            _logger.warning(
                FilesystemLogEvent(
                    op=FilesystemLogEventOp.WRITE,
                    filepath=str(config.worker_state_file),
                    message="Worker state file is world writeable. Any Job can tamper with it.",
                )
            )

        config.worker_state_file.touch(mode=stat.S_IWUSR | stat.S_IRUSR, exist_ok=True)
        with config.worker_state_file.open("w", encoding="utf8") as fh:
            json.dump(
                {k: v for k, v in asdict(self).items() if v is not None},
                fh,
            )
        _logger.info(
            FilesystemLogEvent(
                op=FilesystemLogEventOp.WRITE,
                filepath=str(config.worker_state_file),
                message="Worker state saved.",
            )
        )


@dataclass
class WorkerBootstrap:
    """Return value of the bootstrap_worker() function"""

    worker_info: WorkerPersistenceInfo
    """Stateful information about the Worker"""

    session: WorkerBoto3Session
    """A boto3 session with Worker credentials"""

    log_config: Optional[WorkerLogConfig] = None
    """The log configuration for the Worker"""

    host_config: Optional[WorkerHostConfiguration] = None
    """The host configuration for the Worker"""


def bootstrap_worker(config: Configuration, *, use_existing_worker: bool = True) -> WorkerBootstrap:
    """Contains startup logic to ensure that the Worker is created and started"""

    # Session that will store AWS Credentials used during the initial bootstrapping until
    # we have obtained Fleet Role Credentials from the service.
    bootstrap_session = Session(profile_name=config.profile)
    configure_session_events(boto3_session=bootstrap_session)

    # raises: SystemExit
    worker_info, has_existing_worker = _load_or_create_worker(
        session=bootstrap_session, config=config, use_existing_worker=use_existing_worker
    )

    try:
        # raises: BootstrapWithoutWorkerLoad, SystemExit
        worker_session = _get_boto3_session_for_fleet_role(
            session=bootstrap_session,
            config=config,
            worker_id=worker_info.worker_id,
            has_existing_worker=has_existing_worker,
        )
    except BootstrapWithoutWorkerLoad:
        # No need to log anything here:
        #  1) _get_boto3_session_for_fleet_role will have logged the error; and
        #  2) _load_or_create_worker will log that we're creating a new Worker.
        return bootstrap_worker(config, use_existing_worker=False)

    deadline_client = worker_session.client("deadline", config=DEADLINE_BOTOCORE_CONFIG)

    try:
        # raises: BootstrapWithoutWorkerLoad, SystemExit
        log_config, host_config = _start_worker(
            deadline_client=deadline_client,
            config=config,
            worker_id=worker_info.worker_id,
            has_existing_worker=has_existing_worker,
        )
    except BootstrapWithoutWorkerLoad:
        _logger.error(
            WorkerLogEvent(
                op=WorkerLogEventOp.LOAD,
                farm_id=config.farm_id,
                fleet_id=config.fleet_id,
                worker_id=worker_info.worker_id,
                message="Worker status could not be set to STARTED. Creating a new Worker.",
            )
        )
        return bootstrap_worker(config, use_existing_worker=False)

    # raises: InstanceProfileAttachedError
    _enforce_no_instance_profile_or_stop_worker(
        config=config,
        deadline_client=deadline_client,
        worker_id=worker_info.worker_id,
    )

    return WorkerBootstrap(
        worker_info=worker_info,
        session=worker_session,
        log_config=log_config,
        host_config=host_config,
    )


def _load_or_create_worker(
    *, session: Session, config: Configuration, use_existing_worker: bool
) -> tuple[WorkerPersistenceInfo, bool]:
    """Used by bootstrap_worker to obtain a WorkerPersistenceInfo for the Worker that is this Agent's identity in the
    service.

    If `use_existing_worker` is True, then this will load a persisted worker info from disk if one is available.
    Otherwise (whether False or a persisted one doesn't exist), it will Create a new Worker in the service, persist
    its info to disk, and return that info.

    Returns:
        (WorkerPersistenceInfo, bool)
            WorkerPersistenceInfo -- Information about the Worker that was loaded/created&saved
            bool -- True if, and only if, a Worker was *LOADED* from the host's local storage,
                rather than created anew in the service.

    Raises:
        SystemExit - Any error here is unrecoverable, and the Agent should exit.
    """
    instance_id = _get_instance_id()

    worker_info: Optional[WorkerPersistenceInfo] = None
    has_existing_worker = False
    if use_existing_worker:
        worker_info = WorkerPersistenceInfo.load(config=config)
        if worker_info:
            if (
                instance_id is not None
                and worker_info.instance_id is not None
                and worker_info.instance_id != instance_id
            ):
                # This can happen if an AMI is created from an instance that started the worker agent and left behind a
                # worker persistence file. Ideally the file is removed before creating the AMI but in the event it still
                # exists, simply create a new worker and replace the file.
                _logger.warning(
                    WorkerLogEvent(
                        op=WorkerLogEventOp.LOAD,
                        farm_id=config.farm_id,
                        fleet_id=config.fleet_id,
                        worker_id=worker_info.worker_id,
                        message=(
                            f"Worker state file contains an instance ID ({worker_info.instance_id}) different to that of the running host ({instance_id}). "
                            "This could be the result of a worker state file being included in the image being launched. "
                            "For guidance on preparing a worker image, please refer to https://docs.aws.amazon.com/deadline-cloud/latest/developerguide/create-ami.html#prepare-the-instance. "
                            "Ignoring the persisted worker identity and creating a new worker. "
                        ),
                    )
                )
                worker_info = None
            elif worker_info.instance_id is None and instance_id is not None:
                # No instance id in the state file could be the result of an upgrade
                # Add the instance ID to the state file but still load the same worker id.
                worker_info.instance_id = instance_id
                _logger.info(
                    WorkerLogEvent(
                        op=WorkerLogEventOp.LOAD,
                        farm_id=config.farm_id,
                        fleet_id=config.fleet_id,
                        worker_id=worker_info.worker_id,
                        message=(
                            "Worker state file did not contain an instance ID. "
                            f"Adding instance ID ({worker_info.instance_id}) to worker state file."
                        ),
                    )
                )
                worker_info.save(config=config)
                has_existing_worker = True
            else:
                has_existing_worker = True
                _logger.info(
                    WorkerLogEvent(
                        op=WorkerLogEventOp.LOAD,
                        farm_id=config.farm_id,
                        fleet_id=config.fleet_id,
                        worker_id=worker_info.worker_id,
                        message="Worker identity loaded from prior run.",
                    )
                )

    if not worker_info:
        # Worker creation must be done using bootstrap credentials from the environment
        deadline_client = session.client("deadline", config=DEADLINE_BOTOCORE_CONFIG)

        host_properties = _get_host_properties()
        _logger.info(
            WorkerLogEvent(
                op=WorkerLogEventOp.LOAD,
                farm_id=config.farm_id,
                fleet_id=config.fleet_id,
                message='Creating worker for hostname "%s"' % host_properties["hostName"],
            )
        )
        try:
            # raises: DeadlineRequestUnrecoverableError
            create_worker_response = create_worker(
                deadline_client=deadline_client, config=config, host_properties=host_properties
            )
        except DeadlineRequestUnrecoverableError as e:
            _logger.error("CreateWorker received an unrecoverable error: %s", str(e))
            # Raises: SystemExit
            sys.exit(1)
        worker_id = create_worker_response["workerId"]
        worker_info = WorkerPersistenceInfo(worker_id=worker_id, instance_id=instance_id)
        _logger.info(
            WorkerLogEvent(
                op=WorkerLogEventOp.CREATE,
                farm_id=config.farm_id,
                fleet_id=config.fleet_id,
                worker_id=worker_id,
                message="Worker successfully created",
            )
        )
        worker_info.save(config=config)

    return worker_info, has_existing_worker


def _get_boto3_session_for_fleet_role(
    *, session: Session, config: Configuration, worker_id: str, has_existing_worker: bool
) -> WorkerBoto3Session:
    """Used by bootstrap_worker to obtain a boto3 Session that contains AWS Credentials from the worker's Fleet
    for this specific Worker.

    `use_existing_worker` must be set to the value returned from _load_or_create_worker(). It being true tells us
    that we loaded a previous worker_id from disk, and so we can recover by creating a new Worker if we get a
    ResourceNotFound when trying to obtain credentials.

    Returns:
        Session - A boto3 Session that contains the AWS Credentials for use by the Worker Agent going forward.

    Raises:
        SystemExit - Any error here is unrecoverable, and the Agent should exit.
        BootstrapWithoutWorkerLoad - If the worker has been deleted and we used an existing worker.
    """

    try:
        # Create the session.
        # Note that the constructor will force a credential refresh if
        # either there are no credentials stored on disk or the credentials
        # stored on disk are expired.
        worker_session = WorkerBoto3Session(
            bootstrap_session=session, config=config, worker_id=worker_id
        )
    except DeadlineRequestUnrecoverableError as e:
        if isinstance(inner_exc := e.inner_exc, ClientError):
            code = inner_exc.response.get("Error", {}).get("Code", None)
            # If we're using a pre-existing workerId and we got a ResourceNotFoundException,
            # then we can retry without using the existing worker.
            # Otherwise, the error is terminal and we must exit.
            if code == "ResourceNotFoundException" and has_existing_worker:
                _logger.error(
                    WorkerLogEvent(
                        op=WorkerLogEventOp.DELETE,
                        farm_id=config.farm_id,
                        fleet_id=config.fleet_id,
                        worker_id=worker_id,
                        message="Worker no longer exists.",
                    )
                )
                raise BootstrapWithoutWorkerLoad()
        _logger.exception(
            AwsCredentialsLogEvent(
                op=AwsCredentialsLogEventOp.LOAD,
                resource=worker_id,
                message="Could not obtain AWS Credentials.",
            )
        )
        sys.exit(1)
    except Exception:
        # Note: A naked exception should be impossible, but let's be paranoid.
        _logger.exception(
            AwsCredentialsLogEvent(
                op=AwsCredentialsLogEventOp.LOAD,
                resource=worker_id,
                message="Could not obtain AWS Credentials.",
            )
        )
        sys.exit(1)

    return worker_session


def _start_worker(
    *,
    deadline_client: DeadlineClient,
    config: Configuration,
    worker_id: str,
    has_existing_worker: bool,
) -> Tuple[Optional[WorkerLogConfig], Optional[WorkerHostConfiguration]]:
    """Updates the Worker in the service to the STARTED state.

    Returns:
        Optional[WorkerLogConfig] -- Non-None only if the UpdateWorker request
            contained a log configuration for the Worker Agent to use for writing
            its own logs. The returned WorkerLogConfig is the configuration that it
            should use.
        Optional[WorkerHostConfiguration] -- Non-None if UpdateWorker request contains a
            host configuration for the Worker Agent to configure the host.
    Raises:
        BootstrapWithoutWorkerLoad
        SystemExit
    """

    host_properties = _get_host_properties()

    try:
        response = update_worker(
            deadline_client=deadline_client,
            farm_id=config.farm_id,
            fleet_id=config.fleet_id,
            worker_id=worker_id,
            status=WorkerStatus.STARTED,
            capabilities=config.capabilities,
            host_properties=host_properties,
        )
    except DeadlineRequestUnrecoverableError:
        _logger.exception(
            WorkerLogEvent(
                op=WorkerLogEventOp.STATUS,
                farm_id=config.farm_id,
                fleet_id=config.fleet_id,
                worker_id=worker_id,
                message="Failed to set status to STARTED.",
            )
        )
        sys.exit(1)
    except DeadlineRequestConditionallyRecoverableError:
        if has_existing_worker:
            raise BootstrapWithoutWorkerLoad()
        _logger.exception(
            WorkerLogEvent(
                op=WorkerLogEventOp.STATUS,
                farm_id=config.farm_id,
                fleet_id=config.fleet_id,
                worker_id=worker_id,
                message="Failed to set status to STARTED.",
            )
        )
        sys.exit(1)

    _logger.info(
        WorkerLogEvent(
            op=WorkerLogEventOp.STATUS,
            farm_id=config.farm_id,
            fleet_id=config.fleet_id,
            worker_id=worker_id,
            message="Status set to STARTED.",
        )
    )

    logging_config = None
    if log_config := response.get("log"):
        logging_config = construct_worker_log_config(log_config=log_config)

    host_config = None
    if response_host_config := response.get("hostConfiguration"):
        # scriptTimeoutSeconds is technically always there from the backend.
        host_config = WorkerHostConfiguration(
            script_body=response_host_config.get("scriptBody", ""),
            script_timeout_seconds=response_host_config["scriptTimeoutSeconds"],
        )

    return logging_config, host_config


def _enforce_no_instance_profile_or_stop_worker(
    *,
    config: Configuration,
    deadline_client: DeadlineClient,
    worker_id: str,
) -> None:
    """
    If the configuration does not allow instance profiles while the Worker is running, then
    this function will wait until instance profile IAM credentials are no longer available or
    until reaching the maximum number of retries.

    If the maximum number of retries is reached, the Worker will attempt to be stopped. This is a
    best-effort attempt and will utilize boto3's default retry behavior.
    """

    _logger.debug("Allow instance profile: %s", config.allow_instance_profile)
    if config.allow_instance_profile:
        return

    try:
        _enforce_no_instance_profile()
    except InstanceProfileAttachedError:
        try:
            update_worker(
                deadline_client=deadline_client,
                farm_id=config.farm_id,
                fleet_id=config.fleet_id,
                worker_id=worker_id,
                status=WorkerStatus.STOPPED,
            )
        except DeadlineRequestUnrecoverableError:
            _logger.critical(
                WorkerLogEvent(
                    op=WorkerLogEventOp.STATUS,
                    farm_id=config.farm_id,
                    fleet_id=config.fleet_id,
                    worker_id=worker_id,
                    message="Failed to set status to STOPPED.",
                ),
                exc_info=True,
            )
        except Exception:
            _logger.critical(
                WorkerLogEvent(
                    op=WorkerLogEventOp.STATUS,
                    farm_id=config.farm_id,
                    fleet_id=config.fleet_id,
                    worker_id=worker_id,
                    message="Failed to set status to STOPPED.",
                ),
                exc_info=True,
            )
        else:
            _logger.info(
                WorkerLogEvent(
                    op=WorkerLogEventOp.STATUS,
                    farm_id=config.farm_id,
                    fleet_id=config.fleet_id,
                    worker_id=worker_id,
                    message="Status set to STOPPED.",
                )
            )
        raise


def _get_instance_id() -> str | None:
    """
    This function attempts to query the IMDS /instance-id endpoint for the instance ID.
    The query will return the instance ID on success, and will return None if the query fails or
    times out. The timeout is limited to half a second and will occur if running off AWS.
    """
    response = _get_metadata("instance-id")
    if response is None:
        _logger.info(
            "IMDS is not reachable. Worker host is not an EC2 instance or IMDs is turned off."
        )
        return None
    _logger.debug("IMDS /instance-id response %d", response.status_code)
    if response.status_code != 200:
        _logger.info(
            f"Error attempting to detect instance ID: Recieved HTTP {response.status_code} from IMDS."
        )
        return None
    _logger.info(f"Worker host is running on instance {response.text}.")
    return response.text


def _enforce_no_instance_profile() -> None:
    """
    This function will query the IMDS /iam/info endpoint in a loop until either:

    1.  The endpoint returns a HTTP 404 response (the instance profile is no longer associated with
        the host EC2 instance)
    2.  The maximum number of attempts (see INSTANCE_PROFILE_REMOVAL_ATTEMPTS) is reached
        (raises InstanceProfileAttachedError)

    The function will sleep for a number of seconds (see INSTANCE_PROFILE_CHECK_DURATION_SECONDS)
    between attempts.
    """
    for i in range(INSTANCE_PROFILE_REMOVAL_ATTEMPTS):
        response = _get_metadata("iam/info")
        if response is None:
            _logger.warning("Not running on EC2 but --no-allow-instance-profile argument specified")
            break
        _logger.info("IMDS /iam/info response %d", response.status_code)
        if response.status_code == 404:
            _logger.info("Instance profile disassociated, proceeding to run tasks.")
            break
        elif response.status_code == 200:
            _logger.info(
                "Instance profile is still associated (attempt %d of %d)",
                i + 1,
                INSTANCE_PROFILE_REMOVAL_ATTEMPTS,
            )
        else:
            _logger.warning(
                "Unexpected HTTP status code (%d) from /iam/info IMDS response",
                response.status_code,
            )
        sleep(INSTANCE_PROFILE_CHECK_DURATION_SECONDS)
    else:
        raise InstanceProfileAttachedError()


def _get_metadata(metadata_type: str) -> requests.Response | None:
    """Getting the information from the metadata service then returning it.

    More information on the ec2 metadata service:
        https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html

    Args:
        metadata_type (str): The metadata information to retrieve.

    Returns:
        str: The requested metadata information.
    """
    try:
        response = requests.put(
            "http://169.254.169.254/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "30"},
            timeout=0.5,  # Non-aws worker hosts will time-out, but the default timeout can be > 20s
        )
        token = response.text
        response = requests.get(
            f"http://169.254.169.254/latest/meta-data/{metadata_type}",
            headers={"X-aws-ec2-metadata-token": token},
        )
    except Exception:
        _logger.info("Not running on EC2 or the metadata service was unable to be found!")
        return None
    else:
        return response
