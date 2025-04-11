# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import logging
from time import sleep, monotonic
from typing import Any, Callable, Dict, List, Optional, TypeVar, cast
from threading import Event
from dataclasses import asdict, dataclass
import random

from botocore.retries.standard import RetryContext
from botocore.exceptions import ClientError

from deadline.client.api import get_telemetry_client, TelemetryClient
from deadline.client import version as deadline_client_lib_version
from deadline.job_attachments.progress_tracker import SummaryStatistics
from openjd.model import version as openjd_model_version
from openjd.sessions import version as openjd_sessions_version

from ..._version import __version__ as version  # noqa
from ...config import Configuration
from ...capabilities import Capabilities
from ...boto import DeadlineClient, NoOverflowExponentialBackoff as Backoff
from ...api_models import (
    AssumeFleetRoleForWorkerResponse,
    AssumeQueueRoleForWorkerResponse,
    BatchGetJobEntityResponse,
    CreateWorkerResponse,
    EntityIdentifier,
    HostProperties,
    LogConfiguration,
    UpdatedSessionActionInfo,
    UpdateWorkerScheduleResponse,
    UpdateWorkerResponse,
    WorkerStatus,
)
from ...log_sync.cloudwatch import (
    LOG_CONFIG_OPTION_GROUP_NAME_KEY,
    LOG_CONFIG_OPTION_STREAM_NAME_KEY,
)

__cached_telemetry_client: Optional[TelemetryClient] = None

_logger = logging.getLogger(__name__)

# Generic function return type.
F = TypeVar("F", bound=Callable[..., Any])


class DeadlineRequestError(Exception):
    """Base class for all exceptions raised by functions in this module."""

    _inner_exc: ClientError

    def __init__(self, inner_exc: ClientError) -> None:
        super().__init__(str(inner_exc))
        self._inner_exc = inner_exc

    @property
    def inner_exc(self) -> ClientError:
        return self._inner_exc


class DeadlineRequestUnrecoverableError(DeadlineRequestError):
    """A generic exception that signals that the API that was invoked
    returned an exception that the Worker Agent should consider to be
    unrecoverable from.
    """

    pass


class DeadlineRequestRecoverableError(DeadlineRequestError):
    """A generic exception that signals that the API that was invoked
    returned an exception that the Worker Agent should consider to be
    recoverable through some sort of retry.
    """

    pass


class DeadlineRequestConditionallyRecoverableError(DeadlineRequestError):
    """A generic exception that signals that the API that was invoked
    returned an exception that the Worker Agent may consider to be recoverable
    depending on the specific circumstances and exception raised.
    """

    pass


class DeadlineRequestWorkerOfflineError(DeadlineRequestConditionallyRecoverableError):
    """Raised by some APIs when invoking the API discovers that the requesting
    Worker is no longer considered to be online in the service (e.g. its
    status has been set to NOT_RESPONDING))
    """

    pass


class DeadlineRequestWorkerNotFound(DeadlineRequestUnrecoverableError):
    """Raised by some APIs when invoking the API discovers that the requesting
    Worker is not found (likely deleted).
    """

    pass


class DeadlineRequestInterrupted(Exception):
    """Raised by some APIs when the interrupt event that was passed to
    the function has been set.
    """

    pass


@dataclass(frozen=True)
class WorkerHostConfiguration:
    """Host Configuration after a worker has started."""

    script_body: str
    """Host Configuration Script Body."""

    script_timeout_seconds: int
    """Customer Supplied timeout."""


@dataclass(frozen=True)
class WorkerLogConfig:
    """The destination where the Worker Agent should synchronize its logs to"""

    cloudwatch_log_group: str
    """The name of the CloudWatch Log Group that the Agent log should be streamed to"""

    cloudwatch_log_stream: str
    """The name of the CloudWatch Log Stream that the Agent log should be streamed to"""


def _get_error_code_from_header(response: dict[str, Any]) -> Optional[str]:
    return response.get("Error", {}).get("Code", None)


def _get_error_reason_from_header(response: dict[str, Any]) -> Optional[str]:
    return response.get("reason", None)


def _get_retry_after_seconds_from_header(response: dict[str, Any]) -> Optional[int]:
    return response.get("retryAfterSeconds", None)


def _apply_lower_bound_to_delay(delay: float, lower_bound: Optional[float] = None) -> float:
    if lower_bound is not None and delay < lower_bound:
        # We add just a tiny bit of jitter (20%) to the lower bound to reduce the probability
        # of a group of workers all retry-storming in lock-step.
        delay = lower_bound + random.uniform(0, 0.2 * lower_bound)
    return delay


def _get_resource_id_and_status_from_conflictexception_header(
    response: dict[str, Any],
) -> tuple[Optional[str], Optional[str]]:
    context = response.get("context", {})
    resourceId = response.get("resourceId")
    return resourceId, context.get("status")


def assume_fleet_role_for_worker(
    *, deadline_client: DeadlineClient, farm_id: str, fleet_id: str, worker_id: str
) -> AssumeFleetRoleForWorkerResponse:
    """Calls the AssumeFleetRoleForWorker API, with automatic infinite retries
    when throttled.
    """
    backoff = Backoff(max_backoff=30)
    retry = 0

    # Note: Frozen credentials could expire while doing a retry loop; that's
    #  probably going to manifest as AccessDenied, but I'm not 100% certain.
    while True:
        try:
            response = deadline_client.assume_fleet_role_for_worker(
                farmId=farm_id,
                fleetId=fleet_id,
                workerId=worker_id,
            )
            break
        except ClientError as e:
            # Terminal errors:
            #   AccessDeniedException, ValidationException, ResourceNotFoundException, ConflictException
            #   * AccessDeniedException during advisory -> Drain the worker
            #   * ResourceNotFoundException -> Go back to startup, if able.
            # Retry:
            #   ThrottlingException, InternalServerException
            delay = backoff.delay_amount(RetryContext(retry))
            delay = _apply_lower_bound_to_delay(
                delay, _get_retry_after_seconds_from_header(e.response)
            )
            code = _get_error_code_from_header(e.response)
            if code == "ThrottlingException":
                _logger.info(
                    f"Throttled while attempting to refresh Worker AWS Credentials. Retrying in {delay} seconds..."
                )
            elif code == "InternalServerException":
                _logger.info(
                    f"InternalServerException while attempting to refresh Worker AWS Credentials ({str(e)}). Retrying in {delay} seconds..."
                )
            else:
                # All of the other exceptions are terminal, so re-raise them and let the caller sort it out.
                raise DeadlineRequestUnrecoverableError(e)

            sleep(delay)
            retry += 1
        except Exception as e:
            # General catch-all for the unexpected, so that the agent can try to handle it gracefully.
            _logger.critical(
                "Unexpected exception calling AssumeFleetRoleForWorker. Please report this to the service team.",
                exc_info=True,
            )
            raise DeadlineRequestUnrecoverableError(e)

    return response


def _assume_queue_role_for_worker_eventual_consistency_time_elapsed(
    time_start: float, time_now: float
) -> bool:
    """A helper for use in assume_queue_role_for_worker. We encapsulate this trivial check to allow for mocking
    it in unit tests; avoiding a unit test having to run for 10+ seconds to test the corresponding code paths.
    """

    return time_now - time_start > 10


def assume_queue_role_for_worker(
    *,
    deadline_client: DeadlineClient,
    farm_id: str,
    fleet_id: str,
    worker_id: str,
    queue_id: str,
    interrupt_event: Optional[Event] = None,
) -> AssumeQueueRoleForWorkerResponse:
    """Calls the AssumeFleetRoleForWorker API, with automatic infinite retries
    when throttled.

    Raises:
      DeadlineRequestWorkerOfflineError - The request's response indicated that the Worker is no
        longer online in the service (e.g. it may have had its status set to NOT_RESPONDING).
      DeadlineRequestUnrecoverableError - Worker should fail the Session Actions for the queue.
      DeadlineRequestConditionallyRecoverableError - Could not obtain the AWS Credentials due to an
        error that is considered recoverable. Follow the instructions in "When Failing to Obtain Session
        AWS Credentials" of the Worker API contract in response.
      DeadlineRequestInterrupted - If the interrupt_event was set.
    """
    backoff = Backoff(max_backoff=30)
    retry = 0
    query_start_time = monotonic()

    # Note: Frozen credentials could expire while doing a retry loop; that's
    #  probably going to manifest as AccessDenied, but I'm not 100% certain.
    while True:
        if interrupt_event and interrupt_event.is_set():
            raise DeadlineRequestInterrupted("AssumeQueueRoleForWorker interrupted")
        try:
            response = deadline_client.assume_queue_role_for_worker(
                farmId=farm_id, fleetId=fleet_id, workerId=worker_id, queueId=queue_id
            )
            break
        except ClientError as e:
            # Terminal errors:
            #   AccessDeniedException, ValidationException, ResourceNotFoundException, ConflictException
            #   * AccessDeniedException during advisory -> Drain the worker
            #   * ResourceNotFoundException -> Go back to startup, if able.
            # Retry:
            #   ThrottlingException, InternalServerException
            delay = backoff.delay_amount(RetryContext(retry))
            delay = _apply_lower_bound_to_delay(
                delay, _get_retry_after_seconds_from_header(e.response)
            )
            code = _get_error_code_from_header(e.response)
            if code == "ThrottlingException":
                _logger.info(
                    f"Throttled while attempting to refresh Worker AWS Credentials. Retrying in {delay} seconds..."
                )
            elif code == "InternalServerException":
                _logger.info(
                    f"InternalServerException while attempting to refresh Worker AWS Credentials ({str(e)}). Retrying in {delay} seconds..."
                )
            elif code == "AccessDeniedException":
                # Let the caller decide what to do.
                raise DeadlineRequestConditionallyRecoverableError(e)
            elif code == "ResourceNotFoundException":
                # Either the Worker or Queue cannot be found. It's unrecoverable either way. This'll tell the
                # caller to fail Session Actions for the Queue.
                # If it turns out to be the Worker that's gone, then we'll find out in response to an
                # UpdateWorkerSchedule call and respond accordingly there.
                raise DeadlineRequestUnrecoverableError(e)
            elif code == "ConflictException":
                exception_reason = _get_error_reason_from_header(e.response)
                if exception_reason == "STATUS_CONFLICT":
                    resource_id, _ = _get_resource_id_and_status_from_conflictexception_header(
                        e.response
                    )
                    if resource_id == worker_id:
                        raise DeadlineRequestWorkerOfflineError(e)
                    elif resource_id == queue_id:
                        # The Queue's status is in conflict. This could be eventual consistency,
                        # so we do a backoff & retry loop unless it's already been more than 10
                        # seconds since we started querying.
                        now = monotonic()
                        if _assume_queue_role_for_worker_eventual_consistency_time_elapsed(
                            query_start_time, now
                        ):
                            # It's been too long. Let the caller decide what to do.
                            raise DeadlineRequestConditionallyRecoverableError(e)
                    else:
                        # Unknown/unhandled error. Let the caller decide what to do.
                        raise DeadlineRequestConditionallyRecoverableError(e)
                else:
                    # Unknown/unhandled error. Let the caller decide what to do.
                    raise DeadlineRequestConditionallyRecoverableError(e)
            elif code == "ValidationException":
                # In all likelihood the Worker has a bug; we should only see these if the request shape is incorrect, or
                # the Worker/Fleet doesn't belong to the Fleet/Farm in the request.
                _logger.error(
                    "ValidationException invoking AssumeQueueRoleForWorker. Please report this error to the service team.",
                    exc_info=True,
                )
                raise DeadlineRequestConditionallyRecoverableError(e)
            else:
                # All of the other exceptions are terminal, so re-raise them and let the caller sort it out.
                raise DeadlineRequestUnrecoverableError(e)

            if interrupt_event:
                interrupt_event.wait(delay)
            else:
                sleep(delay)
            retry += 1
        except Exception as e:
            # General catch-all for the unexpected, so that the agent can try to handle it gracefully.
            _logger.critical(
                "Unexpected exception calling AssumeQueueRoleForWorker. Please report this to the service team.",
                exc_info=True,
            )
            raise DeadlineRequestUnrecoverableError(e)

    return response


def batch_get_job_entity(
    *,
    deadline_client: DeadlineClient,
    farm_id: str,
    fleet_id: str,
    worker_id: str,
    identifiers: list[EntityIdentifier],
) -> BatchGetJobEntityResponse:
    """Calls the BatchGetJobEntity API, with automatic infinite retries
    when throttled.
    """
    backoff = Backoff(max_backoff=30)
    retry = 0

    # Note: Frozen credentials could expire while doing a retry loop; that's
    #  probably going to manifest as AccessDenied, but I'm not 100% certain.
    while True:
        try:
            response = deadline_client.batch_get_job_entity(
                farmId=farm_id, fleetId=fleet_id, workerId=worker_id, identifiers=identifiers
            )
            break
        except ClientError as e:
            # Terminal errors:
            #   AccessDeniedException, ValidationException, ResourceNotFoundException
            # Retry:
            #   ThrottlingException, InternalServerException
            delay = backoff.delay_amount(RetryContext(retry))
            delay = _apply_lower_bound_to_delay(
                delay, _get_retry_after_seconds_from_header(e.response)
            )
            code = _get_error_code_from_header(e.response)
            if code == "ThrottlingException":
                _logger.info(f"Throttled calling BatchGetJobEntity. Retrying in {delay} seconds...")
            elif code == "InternalServerException":
                _logger.info(
                    f"InternalServerException calling BatchGetJobEntity ({str(e)}). Retrying in {delay} seconds..."
                )
            elif code == "ResourceNotFoundException":
                raise DeadlineRequestWorkerNotFound(e)
            else:
                # All of the other exceptions are terminal, so re-raise them and let the caller sort it out.
                raise DeadlineRequestUnrecoverableError(e)

            sleep(delay)
            retry += 1
        except Exception as e:
            # General catch-all for the unexpected, so that the agent can try to handle it gracefully.
            _logger.critical(
                "Unexpected exception calling BatchGetJobEntity. Please report this to the service team.",
                exc_info=True,
            )
            raise DeadlineRequestUnrecoverableError(e)

    return response


def create_worker(
    *, deadline_client: DeadlineClient, config: Configuration, host_properties: HostProperties
) -> CreateWorkerResponse:
    """Calls the CreateWorker API to register this machine's worker and get a worker ID"""

    # Retry API call when being throttled
    backoff = Backoff(max_backoff=30)
    retry = 0
    while True:
        try:
            response = deadline_client.create_worker(
                farmId=config.farm_id,
                fleetId=config.fleet_id,
                hostProperties=host_properties,
            )
            break
        except ClientError as e:
            delay = backoff.delay_amount(RetryContext(retry))
            delay = _apply_lower_bound_to_delay(
                delay, _get_retry_after_seconds_from_header(e.response)
            )
            code = _get_error_code_from_header(e.response)
            if code == "ThrottlingException":
                _logger.info(f"CreateWorker throttled. Retrying in {delay} seconds...")
            elif code == "InternalServerException":
                _logger.warning(
                    f"CreateWorker received InternalServerException ({str(e)}). Retrying in {delay} seconds..."
                )
            elif code == "ConflictException":
                exception_reason = _get_error_reason_from_header(e.response)
                if exception_reason == "RESOURCE_ALREADY_EXISTS":
                    # Let's provide a useful error message.
                    _logger.error(
                        "Could not CreateWorker. A Worker for these credentials already exists."
                    )
                    _logger.error(
                        "Either Delete that Worker, or configure the Agent to use that Worker's workerId."
                    )
                    raise DeadlineRequestUnrecoverableError(e)
                elif exception_reason == "STATUS_CONFLICT":
                    resource_id, status = _get_resource_id_and_status_from_conflictexception_header(
                        e.response
                    )
                    if resource_id == config.fleet_id and status == "CREATE_IN_PROGRESS":
                        _logger.info(
                            f"Fleet {config.fleet_id} is still being created. Retrying in {delay} seconds..."
                        )
                    else:
                        raise DeadlineRequestUnrecoverableError(e)
                else:
                    # Unknown exception_reason. Treat as unrecoverable
                    raise DeadlineRequestUnrecoverableError(e)
            else:
                # The error is unrecoverable. One of:
                #  AccessDeniedException, ValidationException, ResourceNotFoundException,
                # or something unexpected
                raise DeadlineRequestUnrecoverableError(e)

            sleep(delay)
            retry += 1
        except Exception as e:
            # General catch-all for the unexpected, so that the agent can try to handle it gracefully.
            _logger.critical(
                "Unexpected exception calling CreateWorker. Please report this to the service team.",
                exc_info=True,
            )
            raise DeadlineRequestUnrecoverableError(e)

    return response


def delete_worker(
    *, deadline_client: DeadlineClient, config: Configuration, worker_id: str
) -> None:
    """Calls the DeleteWorker API for the given Worker."""

    # Retry API call when being throttled
    backoff = Backoff(max_backoff=30)
    retry = 0

    while True:
        try:
            deadline_client.delete_worker(
                farmId=config.farm_id, fleetId=config.fleet_id, workerId=worker_id
            )
            break
        except ClientError as e:
            delay = backoff.delay_amount(RetryContext(retry))
            delay = _apply_lower_bound_to_delay(
                delay, _get_retry_after_seconds_from_header(e.response)
            )
            code = _get_error_code_from_header(e.response)
            if code == "ThrottlingException":
                _logger.info(f"DeleteWorker throttled. Retrying in {delay} seconds...")
            elif code == "InternalServerException":
                _logger.warning(
                    f"DeleteWorker received InternalServerException ({str(e)}). Retrying in {delay} seconds..."
                )
            elif code == "ConflictException":
                exception_reason = _get_error_reason_from_header(e.response)
                if exception_reason == "STATUS_CONFLICT":
                    resourceId, status = _get_resource_id_and_status_from_conflictexception_header(
                        e.response
                    )
                    if resourceId == worker_id and status in (
                        "STARTED",
                        "STOPPING",
                        "NOT_RESPONDING",
                        "NOT_COMPATIBLE",
                        "RUNNING",
                        "IDLE",
                    ):
                        raise DeadlineRequestRecoverableError(e)
                raise DeadlineRequestUnrecoverableError(e)
            else:
                # The error is unrecoverable. One of:
                #  AccessDeniedException, ValidationException, ResourceNotFoundException,
                # or something unexpected
                raise DeadlineRequestUnrecoverableError(e) from None

            sleep(delay)
            retry += 1
        except Exception as e:
            # General catch-all for the unexpected, so that the agent can try to handle it gracefully.
            _logger.critical(
                "Unexpected exception calling DeleteWorker. Please report this to the service team.",
                exc_info=True,
            )
            raise DeadlineRequestUnrecoverableError(e)


def update_worker(
    *,
    deadline_client: DeadlineClient,
    farm_id: str,
    fleet_id: str,
    worker_id: str,
    status: WorkerStatus,
    capabilities: Optional[Capabilities] = None,
    host_properties: Optional[HostProperties] = None,
    interrupt_event: Optional[Event] = None,
) -> UpdateWorkerResponse:
    """Calls the UpdateWorker API to update this Worker's status, capabilities, and/or host properties with the service.

    Raises:
       DeadlineRequestConditionallyRecoverableError
       DeadlineRequestUnrecoverableError
       DeadlineRequestInterrupted
    """

    # Retry API call when being throttled
    backoff = Backoff(max_backoff=30)
    retry = 0

    request: dict[str, Any] = dict(
        farmId=farm_id,
        fleetId=fleet_id,
        workerId=worker_id,
        status=status.value,
    )
    if capabilities:
        request["capabilities"] = capabilities.for_update_worker()
    if host_properties:
        request["hostProperties"] = host_properties

    while True:
        # If true, then we're trying to go to STARTED but have determined that we must first
        # go to STOPPED
        must_stop_first = False

        if interrupt_event and interrupt_event.is_set():
            raise DeadlineRequestInterrupted("UpdateWorker interrupted")
        try:
            response = deadline_client.update_worker(**request)
            break
        except ClientError as e:
            delay = backoff.delay_amount(RetryContext(retry))
            delay = _apply_lower_bound_to_delay(
                delay, _get_retry_after_seconds_from_header(e.response)
            )
            code = _get_error_code_from_header(e.response)

            skip_sleep = False

            if code == "ThrottlingException":
                _logger.info(f"UpdateWorker throttled. Retrying in {delay} seconds...")
            elif code == "InternalServerException":
                _logger.warning(
                    f"UpdateWorker received InternalServerException ({str(e)}). Retrying in {delay} seconds..."
                )
            elif code == "ResourceNotFoundException":
                raise DeadlineRequestConditionallyRecoverableError(e)
            elif code == "AccessDeniedException" or code == "ValidationException":
                raise DeadlineRequestUnrecoverableError(e)
            elif code == "ConflictException":
                exception_reason = _get_error_reason_from_header(e.response)
                if exception_reason == "CONCURRENT_MODIFICATION":
                    # Something else modified the Worker at the same time. Just retry.
                    _logger.info(f"UpdateWorker conflict. Retrying in {delay} seconds...")
                elif exception_reason == "STATUS_CONFLICT":
                    (
                        resource_id,
                        resource_status,
                    ) = _get_resource_id_and_status_from_conflictexception_header(e.response)
                    if resource_id == worker_id:
                        if resource_status == "ASSOCIATED":
                            _logger.info(
                                f"UpdateWorker indicates that the ec2 instance profile is still attached. Retrying in {delay} seconds..."
                            )
                        elif status == WorkerStatus.STARTED and (
                            resource_status == "STOPPING" or resource_status == "NOT_COMPATIBLE"
                        ):
                            # We need to go to STOPPED before we can continue
                            _logger.info(
                                f"Worker has status={resource_status} and must be set to status=STOPPED before setting status=STARTED."
                            )
                            skip_sleep = True
                            must_stop_first = True
                        else:
                            # Any other status for the Worker is unrecoverable.
                            raise DeadlineRequestUnrecoverableError(e)
                    else:
                        # A conflict in any other resource is terminal
                        raise DeadlineRequestUnrecoverableError(e)
                else:
                    # Any unknown exception_reason is a terminal error
                    raise DeadlineRequestUnrecoverableError(e)
            else:
                # Unknown/unhandled exception code.
                raise DeadlineRequestUnrecoverableError(e)

            if not skip_sleep:
                if interrupt_event:
                    interrupt_event.wait(delay)
                else:
                    sleep(delay)
                retry += 1
        except Exception as e:
            raise DeadlineRequestUnrecoverableError(e)

        if must_stop_first:
            # We've determined that we need to STOPPED before we can try again
            try:
                update_worker(
                    deadline_client=deadline_client,
                    farm_id=farm_id,
                    fleet_id=fleet_id,
                    worker_id=worker_id,
                    status=WorkerStatus.STOPPED,
                    capabilities=capabilities,
                    host_properties=host_properties,
                )
            except Exception:
                # Something blew up; just pass the exception along
                raise

            # Reset our throttle retry count to treat the attempts at going to STARTED
            # as fresh
            retry = 0

    return response


def construct_worker_log_config(log_config: LogConfiguration) -> Optional[WorkerLogConfig]:
    """Parses the LogConfiguration response value from an API response in to a WorkerLogConfig
    if possible.

    Returns:
        None - if the provided log configuration is not supported by this Worker Agent.
    """
    log_driver = log_config.get("logDriver", "not-defined")
    if log_driver == "awslogs":
        # Worker Agent logs should be sent to CloudWatch Logs
        log_config_options = log_config.get("options")

        if error := log_config.get("error"):
            # Surface a warning about the error first to inform the customer.
            _logger.warning("Service reported an error with the log configuration: %s", error)

        if not log_config_options:
            _logger.warning(
                "No options provided for awslogs log driver. Logging will only be local."
            )
            return None

        log_group_name = log_config_options.get(LOG_CONFIG_OPTION_GROUP_NAME_KEY)
        log_stream_name = log_config_options.get(LOG_CONFIG_OPTION_STREAM_NAME_KEY)
        if not log_group_name or not log_stream_name:
            _logger.warning(
                "Options are missing for awslogs log driver configuration. Logging will only be local."
            )
            return None

        return WorkerLogConfig(
            cloudwatch_log_group=log_group_name,
            cloudwatch_log_stream=log_stream_name,
        )
    else:
        _logger.warning(
            f"Worker is configured to use unknown log driver {log_driver}. Logging will only be local."
        )
        return None


def update_worker_schedule(
    *,
    deadline_client: DeadlineClient,
    farm_id: str,
    fleet_id: str,
    worker_id: str,
    updated_session_actions: Optional[dict[str, UpdatedSessionActionInfo]] = None,
    interrupt_event: Optional[Event] = None,
) -> UpdateWorkerScheduleResponse:
    """Calls the UpdateWorkerSchedule API get information about this Worker's schedule, and
    provide updates on Session Actions that had previously been given to the Worker.

    Raises:
        DeadlineRequestInterrupted - If the given interrupt_event has been set.
        DeadlineRequestWorkerNotFound - If the call raises a ResourceNotFoundException.
        DeadlineRequestWorkerOfflineError - If the call raises an exception that indicates that
            the Worker no longer has online status in the service (e.g. it is NOT_RESPONDING).
        DeadlineRequestUnrecoverableError - If an unrecoverable exception is raised by the API.
    """

    request: dict[str, Any] = {
        "farmId": farm_id,
        "fleetId": fleet_id,
        "workerId": worker_id,
    }
    if updated_session_actions is not None:
        request["updatedSessionActions"] = updated_session_actions
    else:
        # Note: Though it's not a required request parameter (it has a default
        # defined) we get a Parameter validation error from botocore if we
        # don't provide a value for this field.
        request["updatedSessionActions"] = dict[str, UpdatedSessionActionInfo]()

    # Retry API call when being throttled
    backoff = Backoff(max_backoff=30)
    retry = 0
    while True:
        if interrupt_event is not None and interrupt_event.is_set():
            raise DeadlineRequestInterrupted("UpdateWorkerSchedule interrupted")
        try:
            response = deadline_client.update_worker_schedule(**request)
            _logger.debug("UpdateWorkerSchedule response: %s", response)
            break
        except ClientError as e:
            delay = backoff.delay_amount(RetryContext(retry))
            delay = _apply_lower_bound_to_delay(
                delay, _get_retry_after_seconds_from_header(e.response)
            )
            code = _get_error_code_from_header(e.response)

            if code == "ThrottlingException":
                _logger.info(f"UpdateWorkerSchedule throttled. Retrying in {delay} seconds...")
            elif code == "InternalServerException":
                _logger.warning(
                    f"UpdateWorkerSchedule received InternalServerException ({str(e)}). Retrying in {delay} seconds..."
                )
            elif code == "ResourceNotFoundException":
                raise DeadlineRequestWorkerNotFound(e)
            elif code == "ConflictException":
                exception_reason = _get_error_reason_from_header(e.response)
                if exception_reason == "STATUS_CONFLICT":
                    resource_id, _ = _get_resource_id_and_status_from_conflictexception_header(
                        e.response
                    )
                    if resource_id == worker_id:
                        # Worker in STATUS_CONFLICT => It's not online.
                        raise DeadlineRequestWorkerOfflineError(e)
                    # If anything else is in STATUS_CONFLICT, then we can't recover.
                    raise DeadlineRequestUnrecoverableError(e)
                elif exception_reason == "CONCURRENT_MODIFICATION":
                    # Something else modified the Worker at the same time. Just retry.
                    _logger.info(f"UpdateWorkerSchedule conflict. Retrying in {delay} seconds...")
                else:
                    # Unknown exception_reason. Treat as unrecoverable
                    raise DeadlineRequestUnrecoverableError(e) from None
            else:
                # The error is unrecoverable. One of:
                #  AccessDeniedException, ValidationException, or something unexpected
                raise DeadlineRequestUnrecoverableError(e) from None

            if interrupt_event:
                interrupt_event.wait(delay)
            else:
                sleep(delay)
            retry += 1
        except Exception as e:
            # General catch-all for the unexpected, so that the agent can try to handle it gracefully.
            _logger.critical(
                "Unexpected exception calling UpdateWorkerSchedule. Please report this to the service team.",
                exc_info=True,
            )
            raise DeadlineRequestUnrecoverableError(e)

    return response


def _get_deadline_telemetry_client() -> TelemetryClient:
    """Wrapper around the Deadline Client Library telemetry client, in order to set package-specific information"""
    global __cached_telemetry_client
    if not __cached_telemetry_client:
        __cached_telemetry_client = get_telemetry_client(
            "deadline-cloud-worker-agent", ".".join(version.split(".")[:3])
        )
        __cached_telemetry_client.update_common_details(
            {"openjd-sessions-version": ".".join(openjd_sessions_version.split(".")[:3])}
        )
        __cached_telemetry_client.update_common_details(
            {"openjd-model-version": ".".join(openjd_model_version.split(".")[:3])}
        )
        __cached_telemetry_client.update_common_details(
            {"deadline-cloud": ".".join(deadline_client_lib_version.split(".")[:3])}
        )
    return __cached_telemetry_client


def record_worker_start_telemetry_event(capabilities: Capabilities) -> None:
    """Calls the telemetry client to record an event capturing generic machine information."""
    _get_deadline_telemetry_client().record_event(
        event_type="com.amazon.rum.deadline.worker_agent.start", event_details=capabilities.dict()
    )


def record_uncaught_exception_telemetry_event(exception_type: str) -> None:
    """Calls the telemetry client to record an event signaling an uncaught exception occurred."""
    _get_deadline_telemetry_client().record_error(
        event_details={"exception_scope": "uncaught"}, exception_type=exception_type
    )


def record_sync_inputs_telemetry_event(queue_id: str, summary: SummaryStatistics) -> None:
    """Calls the telemetry client to record an event capturing the sync-inputs summary."""
    details: Dict[str, Any] = asdict(summary)
    details["queue_id"] = queue_id
    _get_deadline_telemetry_client().record_event(
        event_type="com.amazon.rum.deadline.worker_agent.sync_inputs_summary",
        event_details=details,
    )


def record_sync_outputs_telemetry_event(queue_id: str, summary: SummaryStatistics) -> None:
    """Calls the telemetry client to record an event capturing the sync-outputs summary."""
    details: Dict[str, Any] = asdict(summary)
    details["queue_id"] = queue_id
    _get_deadline_telemetry_client().record_event(
        event_type="com.amazon.rum.deadline.worker_agent.sync_outputs_summary",
        event_details=details,
    )


def record_sync_inputs_fail_telemetry_event(
    queue_id: str,
    failure_reason: str,
) -> None:
    """Calls the telemetry client to record an event capturing the sync-inputs failure."""
    details = {
        "queue_id": queue_id,
        "failure_reason": failure_reason,
    }
    _get_deadline_telemetry_client().record_event(
        event_type="com.amazon.rum.deadline.worker_agent.sync_inputs_failure",
        event_details=details,
    )


def record_success_fail_telemetry_event(**decorator_kwargs: Dict[str, Any]) -> Callable[..., F]:
    """
    Decorator to try catch a function. Sends a success / fail telemetry event.
    :param ** Python variable arguments. See https://docs.python.org/3/glossary.html#term-parameter
    """

    def inner(function: F) -> F:
        def wrapper(*args: List[Any], **kwargs: Dict[str, Any]) -> Any:
            """
            Wrapper to actually try-catch
            :param * Python variable argument. See https://docs.python.org/3/glossary.html#term-parameter
            :param ** Python variable argument. See https://docs.python.org/3/glossary.html#term-parameter
            """
            success: bool = True
            try:
                return function(*args, **kwargs)
            except Exception as e:
                success = False
                raise e
            finally:
                event_name = decorator_kwargs.get("metric_name", function.__name__)
                _get_deadline_telemetry_client().record_event(
                    event_type=f"com.amazon.rum.deadline.worker_agent.{event_name}",
                    event_details={"is_success": success},
                )

        return cast(F, wrapper)

    return inner
