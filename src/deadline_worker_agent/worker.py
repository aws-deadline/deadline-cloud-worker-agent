# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import json
import signal
import sys
import traceback
from concurrent.futures import Executor, Future, ThreadPoolExecutor, wait
from datetime import datetime, timedelta, timezone
from logging import getLogger
from threading import Event
from types import FrameType
from typing import Any, NamedTuple, cast
from pathlib import Path

import boto3
import requests

from .boto import DeadlineClient
from .errors import ServiceShutdown
from .scheduler import WorkerScheduler
from .sessions import Session
from .startup.config import ImpersonationOverrides
from .aws_credentials import WorkerBoto3Session, AwsCredentialsRefresher

logger = getLogger(__name__)


class WorkerSessionCollection:
    def __init__(self, *, worker: Worker) -> None:
        ...

    def __getitem__(self, session_id: str) -> Session:
        raise NotImplementedError()

    def create(self, *, id: str) -> Session:
        raise NotImplementedError("WorkerSessionCollection.create() method not implemented")


class WorkerShutdown(NamedTuple):
    """An error indicating that the Worker is shutting down"""

    grace_time: timedelta
    """The amount of grace time before the Worker Node will shutdown"""

    fail_message: str
    """A human-friendly message explaining the shutdown"""


class Worker:
    _EC2_SHUTDOWN_MONITOR_RATE = timedelta(seconds=1)
    """The rate that the Worker polls for EC2 instance termination notifications (spot interruption
    an Auto-scaling life-cycle events)"""

    _ASG_LIFECYCLE_SHUTDOWN_GRACE = timedelta(minutes=2)
    """The amount of time to allow the Worker to gracefully shutdown after detecting an auto-scaling
    life-cycle event."""

    _farm_id: str
    _fleet_id: str
    _worker_id: str
    _scheduler: WorkerScheduler
    _executor: Executor
    _stop: Event
    _deadline_client: DeadlineClient
    _s3_client: boto3.client
    _logs_client: boto3.client
    _boto_session: WorkerBoto3Session
    _worker_persistence_dir: Path

    def __init__(
        self,
        *,
        farm_id: str,
        fleet_id: str,
        worker_id: str,
        deadline_client: DeadlineClient,
        s3_client: boto3.client,
        logs_client: boto3.client,
        boto_session: WorkerBoto3Session,
        impersonation: ImpersonationOverrides,
        cleanup_session_user_processes: bool,
        worker_persistence_dir: Path,
        worker_logs_dir: Path | None,
    ) -> None:
        self._deadline_client = deadline_client
        self._s3_client = s3_client
        self._logs_client = logs_client
        self._executor = ThreadPoolExecutor(max_workers=3)
        self._farm_id = farm_id
        self._fleet_id = fleet_id
        self._worker_id = worker_id
        self._scheduler = WorkerScheduler(
            deadline=deadline_client,
            farm_id=farm_id,
            fleet_id=fleet_id,
            worker_id=worker_id,
            impersonation=impersonation,
            boto_session=boto_session,
            cleanup_session_user_processes=cleanup_session_user_processes,
            worker_persistence_dir=worker_persistence_dir,
            worker_logs_dir=worker_logs_dir,
        )
        self._stop = Event()
        self._boto_session = boto_session
        self._worker_persistence_dir = worker_persistence_dir

        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        # TODO: Remove this once WA is stable or put behind a debug flag
        signal.signal(signal.SIGUSR1, self._output_thread_stacks)

    def _signal_handler(self, signum: int, frame: FrameType | None = None) -> None:
        """
        Signal handler for SIGTERM/SIGINT that intercepts the signals and lets the worker
        gracefully wind-down what it's currently doing.
        This will set the _interrupted flag to True when we get such a signal.
        """
        if signum in (signal.SIGTERM, signal.SIGINT):
            logger.info(f"Received signal {signum}. Initiating application shutdown.")
            self._interrupted = True
            self._scheduler.shutdown(
                grace_time=timedelta(seconds=4),
                fail_message=f"Worker Agent received OS signal {signum}",
            )
            self._stop.set()

    # TODO: Remove this once WA is stable or put behind a debug flag
    def _output_thread_stacks(self, signum: int, frame: FrameType | None = None) -> None:
        """
        Signal handler for SIGUSR1

        This signal is designated for application-defined behaviors. In our case, we want to output
        stack traces for all running threads.
        """
        if signum in (signal.SIGUSR1,):
            logger.info(f"Received signal {signum}. Initiating application shutdown.")
            # OUTPUT STACK TRACE FOR ALL THREADS
            print("\n*** STACKTRACE - START ***\n", file=sys.stderr)
            code = []
            for threadId, stack in sys._current_frames().items():
                code.append("\n# ThreadID: %s" % threadId)
                for filename, lineno, name, line in traceback.extract_stack(stack):
                    code.append('File: "%s", line %d, in %s' % (filename, lineno, name))
                    if line:
                        code.append("  %s" % (line.strip()))

            for line in code:
                print(line, file=sys.stderr)
            print("\n*** STACKTRACE - END ***\n", file=sys.stderr)

    @property
    def id(self) -> str:
        raise NotImplementedError("Worker.id property not implemented")

    @property
    def sessions(self) -> WorkerSessionCollection:
        raise NotImplementedError("Worker.sessions property not implemeneted")

    def run(self) -> None:
        """Runs the main Worker loop for processing sessions."""

        monitor_ec2_shutdown: Future[WorkerShutdown | None] | None = None
        with self._executor, AwsCredentialsRefresher(
            identifier="Worker Agent",
            session=self._boto_session,
            failure_callback=self._aws_credentials_refresh_failure,
        ):
            scheduler_future = self._executor.submit(self._scheduler.run)
            futures: list[Future[Any]] = [
                scheduler_future,
            ]
            if self._get_ec2_metadata_imdsv2_token():
                # Create a future for monitoring EC2 shutdown events
                monitor_ec2_shutdown = self._executor.submit(self._monitor_ec2_shutdown)
                futures.append(monitor_ec2_shutdown)

            try:
                complete_futures, _ = wait(
                    fs=futures,
                    return_when="FIRST_COMPLETED",
                )
            except BaseException as e:
                logger.exception(e)
                logger.info("Shutting down scheduler...")
                self._scheduler.shutdown(
                    grace_time=timedelta(seconds=5),
                    fail_message=f"Worker Agent encountered error: {e}",
                )
                logger.info("Shutting down monitoring threads...")
                self._stop.set()
                raise
            else:
                for future in complete_futures:
                    if monitor_ec2_shutdown and future is monitor_ec2_shutdown:
                        logger.debug("monitor ec2 shutdown future complete")
                        worker_shutdown: WorkerShutdown | None = future.result()
                        # We only stop the other threads if we detected an imminent EC2 shutdown.
                        # The monitoring thread returns None if:
                        #     1. The Worker is not on EC2, or IMDS is not turned on
                        #     2. The monitor thread was stopped by the OS signal handler
                        if worker_shutdown:
                            self._stop.set()
                            self._scheduler.shutdown(
                                grace_time=worker_shutdown.grace_time,
                                fail_message=worker_shutdown.fail_message,
                            )
                    elif future is scheduler_future:
                        logger.debug("scheduler future complete")
                        try:
                            future.result()
                        except ServiceShutdown:
                            # Suppress logging
                            raise
                        except Exception as e:
                            logger.exception(e)
                            raise
                        finally:
                            self._stop.set()
                    else:
                        raise NotImplementedError(f"Future not handled {future}")
            logger.debug("Waiting for threads to join...")
        logger.info("Worker shutdown complete")

    def _aws_credentials_refresh_failure(self, exception: Exception) -> None:
        """Called when we fail to refresh the Worker Agent's AWS Credentials.
        The given exception will be either:
        1) TimeoutException - indicating that the credentials are either expired or
            will expire soon. args[0] of the exception is a UTC datetime indicating when
            the credentials will expire.
        2) DeadlineRequestError/DeadlineRequestUnrecoverableError - Indicating that
            we encountered a fatal error trying to refresh the credentials (e.g. the Fleet
            Role is missing permissions to refresh itself).

        In either case, we initiate a scheduler shutdown.
        """
        if isinstance(exception, TimeoutError):
            expiry_time = cast(datetime, exception.args[0])
            time_remaining = datetime.now(timezone.utc) - expiry_time
            if time_remaining < timedelta(minutes=0):
                logger.critical("Worker AWS Credentials have expired!")
                grace_time = timedelta(seconds=5)
                fail_message = "Worker AWS Credentials have expired!"
            else:
                logger.error(
                    "Worker AWS Credentials could not be refreshed. They will expire in %s seconds",
                    time_remaining.total_seconds(),
                )
                grace_time = time_remaining
                fail_message = "Worker AWS Credentials are expiring and cannot be refreshed."
        else:
            # exception is: DeadlineRequestError or DeadlineRequestUnrecoverableError
            grace_time = timedelta(seconds=30)
            fail_message = "Fatal error refreshing Worker AWS Credentials. See log for details."
            logger.critical("Fatal error refreshing Worker AWS Credentials: %s", str(exception))
        self._stop.set()
        self._scheduler.shutdown(grace_time=grace_time, fail_message=fail_message)

    def _monitor_ec2_shutdown(self) -> WorkerShutdown | None:
        """Monitors for external shutdown events.

        This includes:
        1.  EC2 spot interruptions
        2.  EC2 auto-scaling life-cycle scale-in events

        This is a synchronous blocking call, so it should be run as a future.

        Returns
        -------
        WorkerShutdown | None
            An optional WorkerShutdown which specifies the amount of grace time before the shutdown
            occurs and a human-friendly message describing the shutdown reason.
        """
        monitor_ec2_shutdown_rate = Worker._EC2_SHUTDOWN_MONITOR_RATE.total_seconds()
        while not self._stop.wait(timeout=monitor_ec2_shutdown_rate):
            if not (imdsv2_token := self._get_ec2_metadata_imdsv2_token()):
                # Not on EC2 or IMDSv2 is inactive.
                logger.info(
                    "IMDS unavailable - unable to monitor for spot interruption or ASG life-cycle "
                    "changes"
                )
                return None

            # Check for spot interruption or shutdown
            if (
                spot_shutdown_grace := self._get_spot_instance_shutdown_action_timeout(
                    imdsv2_token=imdsv2_token
                )
            ) is not None:
                logger.info("Spot interruption detected. Termination in %s", spot_shutdown_grace)
                return WorkerShutdown(
                    grace_time=spot_shutdown_grace,
                    fail_message="The Worker received an EC2 spot interruption",
                )
            elif self._is_asg_terminated(imdsv2_token=imdsv2_token):
                logger.info(
                    "Auto-scaling life-cycle change event detected. Termination in %s",
                    Worker._ASG_LIFECYCLE_SHUTDOWN_GRACE,
                )
                return WorkerShutdown(
                    grace_time=Worker._ASG_LIFECYCLE_SHUTDOWN_GRACE,
                    fail_message="The Worker receieved an auto-scaling life-cycle change event",
                )

        logger.debug("EC2 shutdown monitoring thread exited")

        return None

    def _get_ec2_metadata_imdsv2_token(self) -> str | None:
        """Query the EC2 Metadata service to obtain an IMDSv2 token to use in further queries to the
        service.

        Returns
        -------
        str | None
            None if we're not on EC2 or could not get a token from the metadata service. A token
            with a 10 second TTL otherwise.
        """
        # See:
        #  https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-instance-metadata-service.html
        try:
            response = requests.put(
                "http://169.254.169.254/latest/api/token",
                headers={"X-aws-ec2-metadata-token-ttl-seconds": "10"},
            )
        except requests.ConnectionError:
            # Could not connect to the metadata service. Either it's not enabled or we're not
            # on an EC2 instance.
            return None

        if response.status_code == 200:
            return response.text
        return None

    def _get_spot_instance_shutdown_action_timeout(self, *, imdsv2_token: str) -> timedelta | None:
        """Query the EC2 instance metadata service to check whether or not this instance is being
        stopped/terminated by the EC2 Spot service.

        Parameters
        ----------
        imdsv2_token : str
            An IMDSv2 token to authenticate the query.

        Returns
        -------
        timedelta | None
            None if we're not a Spot instance or if we have no pending EC2 Spot-driven shutdown.
            Otherwise, the time remaining before EC2 Spot is going to terminate the instance.
        """

        # See: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-instance-termination-notices.html#instance-action-metadata # noqa: E501
        try:
            response = requests.get(
                "http://169.254.169.254/latest/meta-data/spot/instance-action",
                headers={"X-aws-ec2-metadata-token": imdsv2_token},
            )
        except requests.ConnectionError:
            # Could not connect to the metadata service. Either it's inactive or we're not
            # on an EC2 instance.
            return None

        if response.status_code == 200:
            decoded_response = json.loads(response.text)
            if (action := decoded_response.get("action", None)) in ("stop", "terminate"):
                # We're getting shut down.
                if (shutdown_time := decoded_response.get("time", None)) is None:
                    # Should never happen. Being paranoid
                    logger.error(
                        "Missing 'time' property from ec2 metadata instance-action response"
                    )
                    return None
                logger.info(f"Spot {action} happening at {shutdown_time}")
                # Spot gives the time in UTC with a trailing Z, but Python can't handle
                # the Z so we strip it
                shutdown_time = datetime.fromisoformat(shutdown_time[:-1]).astimezone(timezone.utc)
                current_time = datetime.now(timezone.utc)
                time_delta = shutdown_time - current_time
                time_delta_seconds = int(time_delta.total_seconds())
                # Being paranoid. This will always be positive.
                if time_delta_seconds > 0:
                    return timedelta(seconds=time_delta_seconds)
                logger.error(f"Spot {action} time is in the past!")
        return None

    def _is_asg_terminated(self, *, imdsv2_token: str) -> bool:
        """Query the EC2 instance metadata service to determine whether an AutoscalingGroup has
        set this instance to transition to Terminated state.

        Parameters
        ----------
        imdsv2_token : str
            An IMDSv2 token to authenticate the query.

        Returns
        -------
        bool
            True if the instance is transitioning to Terminated; False otherwise.
        """
        # Return number of seconds until shutdown, if we're getting shut-down

        # See: https://docs.aws.amazon.com/autoscaling/ec2/userguide/retrieving-target-lifecycle-state-through-imds.html # noqa: E501
        try:
            response = requests.get(
                "http://169.254.169.254/latest/meta-data/autoscaling/target-lifecycle-state",
                headers={"X-aws-ec2-metadata-token": imdsv2_token},
            )
        except requests.ConnectionError:
            return False

        if response.status_code == 200:
            return response.text == "Terminated"
        return False
