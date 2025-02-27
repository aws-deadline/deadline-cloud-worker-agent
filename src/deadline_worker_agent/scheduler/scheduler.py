# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from concurrent.futures import (
    Future,
    ThreadPoolExecutor,
    TimeoutError as FutureTimeoutError,
    wait,
)
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import partial
from pathlib import Path
from threading import Event, RLock, Lock, Timer
from typing import Callable, Literal, Tuple, Union, cast, Optional, Any
import json
import logging
import os
import stat
import sys
import getpass

from openjd.sessions import ActionState, ActionStatus, SessionUser
from openjd.sessions import LOG as OPENJD_SESSION_LOG
from deadline.job_attachments.asset_sync import AssetSync

from ..aws.deadline import update_worker
from ..aws_credentials import QueueBoto3Session, AwsCredentialsRefresher
from ..boto import DeadlineClient, Session as BotoSession
from ..errors import ServiceShutdown
from ..sessions import JobEntities, Session
from ..sessions.actions import SessionActionDefinition
from ..sessions.log_config import (
    LogConfiguration,
    LogProvisioningError,
    SessionLogConfigurationParameters,
)
from ..sessions.job_entities.job_details import JobRunAsUser
from ..api_models import (
    AssignedSession,
    UpdateWorkerScheduleResponse,
    UpdatedSessionActionInfo,
    WorkerStatus,
    EnvironmentAction,
    TaskRunAction,
    SyncInputJobAttachmentsAction,
    AttachmentDownloadAction,
)
from ..aws.deadline import (
    DeadlineRequestConditionallyRecoverableError,
    DeadlineRequestError,
    DeadlineRequestInterrupted,
    DeadlineRequestWorkerOfflineError,
    DeadlineRequestUnrecoverableError,
    update_worker_schedule,
)
from ..config import JobsRunAsUserOverride
from ..utils import MappingWithCallbacks
from ..file_system_operations import FileSystemPermissionEnum, make_directory, touch_file
from ..log_messages import (
    AwsCredentialsLogEvent,
    AwsCredentialsLogEventOp,
    FilesystemLogEvent,
    FilesystemLogEventOp,
    SessionLogEvent,
    SessionLogEventSubtype,
    WorkerLogEvent,
    WorkerLogEventOp,
)
from .log import LOGGER
from .session_cleanup import SessionUserCleanupManager
from .session_queue import SessionActionQueue, SessionActionStatus

if sys.platform == "win32":
    from ..windows.win_credentials_resolver import WindowsCredentialsResolver
    from ..windows.win_logon import unload_and_close
else:
    WindowsCredentialsResolver = Any


logger = LOGGER

JOB_ATTACHMENTS_LOGGER = logging.getLogger("deadline.job_attachments")

# API limit on length of "progressMessage" field for session actions in UpdateWorkerSchedule API
UPDATE_WORKER_SCHEDULE_MAX_MESSAGE_CHARS = 4096


@dataclass(frozen=True)
class SchedulerSession:
    """A structure of fields related to a Worker session required for scheduling"""

    future: Future[None]
    """The future for the action running"""

    queue: SessionActionQueue
    """The session's action queue"""

    session: Session
    """The session that the action is being run within"""

    job_entities: JobEntities
    """Job entities collection associated with the session
    These are cached throughout the session.
    """

    log_configuration: LogConfiguration
    """The log configuration for the session"""


@dataclass(frozen=True)
class QueueAwsCredentials:
    """This holds the AWS Credentials for a particular Queue to use for all actions
    performed on behalf of the Open Job Description Session for Jobs from that Queue.
    This includes:
    1. all Job Attachments behaviors; and
    2. things done by the running SessionActions' subprocesses.

    It also holds the AwsCredentialsRefresher context manager that is actively
    refreshing those credentials as needed. Each refresh will update the
    credentials process that running subprocesses are using to obtain credentials.
    """

    session: QueueBoto3Session
    refresher: AwsCredentialsRefresher


class SessionMap(MappingWithCallbacks[str, SchedulerSession]):
    """
    Singleton mapping of session IDs to sessions.

    This class hooks into dict operations to register session with SessionCleanupManager
    """

    __session_map_instance: SessionMap | None = None
    _session_cleanup_manager: SessionUserCleanupManager

    def __new__(cls, *args, **kwargs) -> SessionMap:
        if cls.__session_map_instance is None:
            cls.__session_map_instance = super().__new__(cls)
        return cls.__session_map_instance

    def __init__(
        self,
        *args,
        cleanup_session_user_processes: bool = True,
        **kwargs,
    ) -> None:
        self._session_cleanup_manager = SessionUserCleanupManager(
            cleanup_session_user_processes=cleanup_session_user_processes
        )
        super().__init__(
            *args,
            setitem_callback=self.setitem_callback,
            delitem_callback=self.delitem_callback,
            **kwargs,
        )

    def setitem_callback(self, key: str, value: SchedulerSession):
        self._session_cleanup_manager.register(value.session)

    def delitem_callback(self, key: str):
        if not (scheduler_session := self.get(key, None)):
            # Nothing to do, base class will raise KeyError
            return
        self._session_cleanup_manager.deregister(scheduler_session.session)

    @classmethod
    def get_session_map(cls) -> SessionMap | None:
        return cls.__session_map_instance


class WorkerScheduler:
    _INITIAL_POLL_INTERVAL = timedelta(seconds=15)

    _deadline: DeadlineClient
    _sessions: SessionMap
    _shutdown: Event
    _shutdown_grace: timedelta | None
    _shutdown_fail_message: str | None = None
    _wakeup: Event
    _executor: ThreadPoolExecutor
    _farm_id: str
    _fleet_id: str
    _worker_id: str
    _action_updates_map: dict[str, SessionActionStatus]
    _action_completes: list[SessionActionStatus]
    _action_update_lock: RLock
    _job_run_as_user_override: JobsRunAsUserOverride
    _boto_session: BotoSession
    _worker_persistence_dir: Path
    _worker_logs_dir: Path | None
    _retain_session_dir: bool
    _session_root_dir: Path

    # Map from queueId -> QueueAwsCredentials.
    _queue_aws_credentials: dict[str, QueueAwsCredentials]
    # Lock that must be grabbed when mutating the _queue_aws_credentials in any way; we have
    # threads, so let's be thread safe.
    _queue_aws_credentials_lock: Lock

    def __init__(
        self,
        *,
        farm_id: str,
        fleet_id: str,
        worker_id: str,
        deadline: DeadlineClient,
        job_run_as_user_override: JobsRunAsUserOverride,
        boto_session: BotoSession,
        cleanup_session_user_processes: bool,
        worker_persistence_dir: Path,
        worker_logs_dir: Path | None,
        session_root_dir: Path,
        retain_session_dir: bool = False,
        stop: Event | None = None,
    ) -> None:
        """Queue of Worker Sessions and their actions

        Parameters
        ----------
        deadline_client : DeadlineClient
            Deadline client used for making API requests
        worker_logs_dir: Path
            A path to the base directory where local session log files should be stored. Each
            session log will be written to:

                <worker_logs_dir>/<queue_id>/<session_id>.log

            If the value is None, then no local session logs will be written.
        """
        self._deadline = deadline
        self._executor = ThreadPoolExecutor(max_workers=100)
        self._sessions = SessionMap(cleanup_session_user_processes=cleanup_session_user_processes)
        self._wakeup = Event()
        self._shutdown = stop or Event()
        self._farm_id = farm_id
        self._fleet_id = fleet_id
        self._worker_id = worker_id
        self._action_completes = []
        self._action_updates_map = {}
        self._action_update_lock = RLock()
        self._job_run_as_user_override = job_run_as_user_override
        self._shutdown_grace = None
        self._boto_session = boto_session
        self._queue_aws_credentials = dict[str, QueueAwsCredentials]()
        self._queue_aws_credentials_lock = Lock()
        self._worker_persistence_dir = worker_persistence_dir
        self._worker_logs_dir = worker_logs_dir
        self._retain_session_dir = retain_session_dir
        self._windows_credentials_resolver: Optional[WindowsCredentialsResolver]
        self._session_root_dir = session_root_dir

        if os.name == "nt" and not (
            self._job_run_as_user_override.job_user or self._job_run_as_user_override.run_as_agent
        ):
            self._windows_credentials_resolver = WindowsCredentialsResolver(self._boto_session)
        else:
            self._windows_credentials_resolver = None

    def _assign_sessions(self) -> None:
        """Handles an AssignSessions API cycle"""

    def run(self) -> None:
        """Runs the Worker scheduler.

        The Worker begins by hydrating its assigned work using the UpdateWorkerSchedule API.

        The scheduler then enters a loop of processing assigned actions - creating and deleting
        Worker sessions as required. If no actions are assigned, the Worker idles for 5 seconds.
        If any action completes, finishes cancelation, or if the Worker is done idling, an
        UpdateWorkerSchedule API request is made with any relevant changes specified in the request.

        The scheduler is responsible for heart-beating which also includes reporting progress and
        status of ongoing active session actions, receiving session action cancelations, and
        also receiving commands from the service to shutdown.

        The function returns normally if the WorkerScheduler instance's `stop` method is called from
        another thread and all sessions are able to gracefully shut down.

        Raises
        ------
        ServiceShutdown
            The service has issued a shutdown command in a NotifyProgress response.
        """

        timeout = WorkerScheduler._INITIAL_POLL_INTERVAL

        with self._executor:
            try:
                while not self._shutdown.is_set():
                    self._wakeup.clear()

                    # Raises:
                    #  ServiceShutdown - When we are undergoing a service-initiated drain, and
                    # that drain is now complete.
                    #  DeadlineRequestWorkerNotFoundError, DeadlineRequestWorkerOfflineError, and
                    # DeadlineRequestUnrecoverableError - All are unrecoverable at this level
                    # so we re-raise which exits the scheduler and causes it to drain.
                    # The more appropriate place to try to recover is either the Worker or
                    # the entrypoint.
                    try:
                        interval = self._sync(interruptable=True)
                    except DeadlineRequestInterrupted:
                        # Occurs if self._shutdown has been set, so go back to the
                        # top of the loop and drain naturally.
                        continue

                    if self._windows_credentials_resolver:
                        self._windows_credentials_resolver.prune_cache()

                    logger.debug("interval = %s", interval)
                    timeout = timedelta(seconds=interval)

                    self._wakeup.wait(timeout=timeout.total_seconds())
            except ServiceShutdown:
                # Suppress logging
                raise
            except (DeadlineRequestError, Exception):
                logger.exception("Exception in WorkerScheduler", exc_info=True)
                raise
            finally:
                logger.info("Main event loop exited.")
                self._drain_scheduler()
                if sys.platform == "win32":
                    if (
                        self._job_run_as_user_override is not None
                        and self._job_run_as_user_override.logon_token is not None
                    ):
                        unload_and_close(
                            self._job_run_as_user_override.user_profile,
                            self._job_run_as_user_override.logon_token,
                        )
                    elif self._windows_credentials_resolver is not None:
                        self._windows_credentials_resolver.clear()

    def _drain_scheduler(self) -> None:
        # Called only from self.run() during shutdown.

        # Note:
        #   When we're doing a worker-initiated drain we will have self._shutdown set. We may, optionally,
        #  have a value for self._shutdown_grace as well.
        logger.info("Draining any remaining Sessions.")
        if self._sessions:
            logger.info("Shutting down %d Sessions", len(self._sessions))

        if self._shutdown.is_set() and self._sessions:
            # This is a worker-initiated drain. Inform the service that we're
            # STOPPING, and thus it should not give us any additional work.

            # Give it more time, up to a point, if we have the gracetime for it.
            if self._shutdown_grace is not None:
                state_transition_timeout = min(timedelta(seconds=5), 0.1 * self._shutdown_grace)
                self._shutdown_grace -= state_transition_timeout
            else:
                state_transition_timeout = timedelta(seconds=1)
            self._transition_to_stopping(timeout=state_transition_timeout)

        session_shutdown_futures = self._shutdown_sessions(
            self._shutdown_grace, self._shutdown_fail_message
        )

        # Join on all session shutdown futures
        if session_shutdown_futures:
            # Wait a little less than our gracetime so that we have time to
            # tell the service that we've STOPPED.
            max_waittime = (
                max(1, (self._shutdown_grace - timedelta(seconds=1)).total_seconds())
                if self._shutdown_grace
                else None
            )
            if max_waittime is not None:
                logger.info("Waiting %s seconds for Sessions to end.", max_waittime)
            else:
                logger.info("Waiting for Sessions to end.")
            wait(
                session_shutdown_futures,
                timeout=max_waittime,
            )

        # Make sure that any existing QueueBoto3Credentials objects have cleaned up their
        # filesystem mutations.
        # Do this before calling _sync(), just in case the _sync() raises an exception
        for credentials_dataclass in self._queue_aws_credentials.values():
            credentials_dataclass.session.cleanup()
        self._queue_aws_credentials.clear()

        # If the Worker initiated the shutdown, then must notify the service about interrupted
        # actions
        if self._shutdown.is_set() and session_shutdown_futures:
            try:
                # Send the information that we have.
                # TODO - We don't presently handle the case where the service gives us
                # ENV_EXIT actions to complete as part of the drain.
                # Note: self._shutdown is already set, so we don't want this
                # call to be interruptable; doing so would cause it to immediately
                # exit and not actually make the API call
                self._sync(interruptable=False)
            except DeadlineRequestInterrupted:
                # Receiving this indicates a logic error. This should never actually happen.
                raise RuntimeError(
                    "UpdateWorkerSchedule during Worker drain was interrupted. This is a bug. Please contact the service team."
                )
            except ServiceShutdown:
                pass

    def _shutdown_sessions(
        self, gracetime: Optional[timedelta], fail_message: Optional[str]
    ) -> list[Future[None]]:
        return [
            self._executor.submit(
                session.session.stop,
                grace_time=gracetime,
                current_action_result="INTERRUPTED",
                fail_message=fail_message,
            )
            for session in self._sessions.values()
        ]

    def _sync(self, *, interruptable: bool = True) -> int:
        """Sends updates to the service, receives and orchestrates work to sessions.

        This will also persist the idle and healthy timeouts as member variables of the class
        instance.

        Returns
        -------
        int
            The interval (in seconds) to sync with the service returned in the UpdateWorkerSchedule response
        """
        # Called by self.run() in the main event loop.

        # 1. collect info to be sent in the UpdateWorkerSchedule API request
        #    1.1. finished/in-progress action results
        updated_actions, commit_completed_actions = self._updated_session_actions()

        #    1.2. TODO: IP address changes

        # 2. make request
        request: dict[str, Any] = {
            "deadline_client": self._deadline,
            "farm_id": self._farm_id,
            "fleet_id": self._fleet_id,
            "worker_id": self._worker_id,
            "updated_session_actions": updated_actions,
        }
        if interruptable:
            # Pass our shutdown Event to interrupt the retry loop in the
            # API wrapper. If we get shutdown and the API is doing backoff+retries
            # then it'll stop retrying and exit.
            request["interrupt_event"] = self._shutdown

        # Raises: DeadlineRequestInterrupted, DeadlineRequestWorkerNotFoundError,
        # DeadlineRequestWorkerOfflineError, and DeadlineRequestUnrecoverableError
        #  - Let these go to the caller
        response = update_worker_schedule(**request)

        commit_completed_actions()

        # 3. take action based on response
        #    3.1. create new sessions
        #    3.2  delete old sessions
        #    3.3. cancel actions in existing sessions
        #    3.4. update the queues for existing sessions
        #    3.5. persist the idle and healthy timeouts
        self._update_sessions(response=response)

        if response.get("desiredWorkerStatus", None) == "STOPPED":
            logger.warning("Service requested shutdown initiated")
            raise ServiceShutdown()

        # Return the timers
        return response["updateIntervalSeconds"]

    def _transition_to_stopping(self, timeout: timedelta) -> None:
        """Calls out to the service to inform it that the Worker should be set
        to the STOPPING state. This is a signal to the service that the Worker has
        initiated a worker-initiated drain operation, and that it must not be
        given additional new tasks to work on.
        """

        # We're only being given timeout seconds to successfully make this request.
        # That is because the drain operation may be expedited, and we need to move
        # fast to get to transitioning to STOPPED state after this.
        timeout_event = Event()
        timer = Timer(interval=timeout.total_seconds(), function=timeout_event.set)

        try:
            update_worker(
                deadline_client=self._deadline,
                farm_id=self._farm_id,
                fleet_id=self._fleet_id,
                worker_id=self._worker_id,
                status=WorkerStatus.STOPPING,
                interrupt_event=timeout_event,
            )
            logger.info(
                WorkerLogEvent(
                    op=WorkerLogEventOp.STATUS,
                    farm_id=self._farm_id,
                    fleet_id=self._fleet_id,
                    worker_id=self._worker_id,
                    message="Status set to STOPPING.",
                )
            )
        except DeadlineRequestInterrupted:
            logger.info(
                "Timeout reached trying to update Worker to STOPPING status. Proceeding without changing status..."
            )
        except (
            DeadlineRequestUnrecoverableError,
            DeadlineRequestConditionallyRecoverableError,
        ) as exc:
            logger.warning(
                f"Exception updating Worker to STOPPING status. Continuing with drain operation regardless. Exception: {str(exc)}"
            )
        finally:
            timer.cancel()

    def _updated_session_actions(
        self,
    ) -> Tuple[dict[str, UpdatedSessionActionInfo], Callable[[], None]]:
        # Returns a unique identifier for a session action update.
        # This is a tuple of the action ID and the completed state or the update time
        # This is used to only commit deletes from the _action_updates_map that are not newer
        # than what was sent in the UpdateWorkerSchedule request when calling the commit() function
        def compute_update_id(
            session_action_status: SessionActionStatus,
        ) -> tuple[str, datetime | str]:
            return (
                session_action_status.id,
                cast(
                    Union[datetime, str],
                    session_action_status.completed_status or session_action_status.update_time,
                ),
            )

        # The transaction which is a set of update IDs we are trying to commit
        tx: set[tuple[str, datetime | str]]

        with self._action_update_lock:
            updated = {
                action_id: self._updated_action_to_boto(updated_action)
                for action_id, updated_action in self._action_updates_map.items()
            }
            # Persist the update to the transaction
            tx = {
                compute_update_id(session_action_status)
                for session_action_status in self._action_updates_map.values()
            }

        # Return a commit function. This is a closure that maintains a reference to the transaction
        # Calling this commit() function removes all updates from the _action_updates_map that are
        # present in the transaction. If there is a more recent action update in the
        # _action_updates_map that is written while the request is in-flight, it will not be
        # removed from the map.
        def commit() -> None:
            with self._action_update_lock:
                action_ids_to_delete = [
                    session_action_update.id
                    for session_action_update in self._action_updates_map.values()
                    if compute_update_id(session_action_update) in tx
                ]
                for action_id in action_ids_to_delete:
                    del self._action_updates_map[action_id]

        return updated, commit

    def _updated_action_to_boto(
        self,
        action_updated: SessionActionStatus,
    ) -> UpdatedSessionActionInfo:
        updated_action = UpdatedSessionActionInfo()

        def _exit_code_to_32bit_signed(exitcode: int) -> int:
            # Workaround to ensure that the process exit code is returned in range of
            # a 32-bit signed integer as expected by the UpdateWorkerSchedule API.
            as_uint32_bytes = (exitcode & 0xFFFFFFFF).to_bytes(4, "big", signed=False)
            return int.from_bytes(as_uint32_bytes, "big", signed=True)

        # Optional fields
        if action_updated.start_time:
            updated_action["startedAt"] = action_updated.start_time
        if action_updated.completed_status:
            updated_action["completedStatus"] = action_updated.completed_status
        elif action_updated.update_time:
            updated_action["updatedAt"] = action_updated.update_time
        if action_updated.status:
            if action_updated.status.exit_code is not None:
                updated_action["processExitCode"] = _exit_code_to_32bit_signed(
                    action_updated.status.exit_code
                )
            if action_updated.completed_status:
                if action_updated.status.fail_message:
                    updated_action["progressMessage"] = action_updated.status.fail_message
                elif action_updated.status.status_message:
                    updated_action["progressMessage"] = action_updated.status.status_message
            elif action_updated.status.status_message:
                updated_action["progressMessage"] = action_updated.status.status_message
            if action_updated.status.progress:
                updated_action["progressPercent"] = min(max(0, action_updated.status.progress), 100)
        if action_updated.end_time:
            updated_action["endedAt"] = action_updated.end_time

        # Truncate message to max bytes allowed by UpdateWorkerSchedule API
        if (
            "progressMessage" in updated_action
            and len(updated_action["progressMessage"]) > UPDATE_WORKER_SCHEDULE_MAX_MESSAGE_CHARS
        ):
            updated_action["progressMessage"] = updated_action["progressMessage"][
                :UPDATE_WORKER_SCHEDULE_MAX_MESSAGE_CHARS
            ]

        return updated_action

    def _update_sessions(
        self,
        *,
        response: UpdateWorkerScheduleResponse,
    ) -> None:
        assigned_sessions = response["assignedSessions"]
        canceled_session_action = response["cancelSessionActions"]
        self._remove_finished_sessions(assigned_sessions=assigned_sessions)
        self._cleanup_queue_aws_credentials(assigned_sessions=assigned_sessions)
        created_session_ids = self._create_new_sessions(assigned_sessions=assigned_sessions)
        existing_sessions = {
            session_id: assigned_session
            for session_id, assigned_session in assigned_sessions.items()
            if session_id not in created_session_ids
        }
        self._update_session_actions_from_scheduler(
            assigned_sessions=existing_sessions, canceled_session_action=canceled_session_action
        )
        self._update_session_logging(assigned_sessions=existing_sessions)

    def _remove_finished_sessions(
        self,
        *,
        assigned_sessions: dict[str, AssignedSession],
    ) -> None:
        assigned_session_ids = assigned_sessions.keys()
        removed_session_ids = self._sessions.keys() - assigned_session_ids
        for removed_session_id in removed_session_ids:
            ses = self._sessions[removed_session_id]
            ses.session.stop(grace_time=timedelta())
            # Wait until the Session has fully completed before continuing.
            # Reason: There's a data race here. We *think* that the Session has been
            #   ended, by virtue of it having been "stopped", but it may actually
            #   still be running cleanup. We wait until it has fully completed cleanup
            #   before continuing.
            # Note: The cleanup should be very fast since the service only removes a Session
            #   from us once it has acknowledged all updates for all of its SessionActions and
            #   it has no SessionActions in it.
            ses.session.wait()
            del self._sessions[removed_session_id]
            logger.info(
                SessionLogEvent(
                    subtype=SessionLogEventSubtype.COMPLETE,
                    queue_id=ses.session._queue_id,
                    job_id=ses.session._job_id,
                    session_id=removed_session_id,
                    message="Session complete.",
                )
            )

    def _handle_session_action_update(
        self,
        action_status: SessionActionStatus,
    ) -> None:
        with self._action_update_lock:
            self._action_updates_map[action_status.id] = action_status

            if any(session_entry.session.idle for session_entry in self._sessions.values()):
                self._wakeup.set()

    def _fail_all_actions(
        self,
        assigned_session: AssignedSession,
        error_message: str,
    ) -> None:
        # Called only in self._create_new_sessions() to fail all of the queued SessionActions
        # if we experience an unrecoverable error during the setup phases of a new Session, but
        # before we've started the Session's actions running
        actions = assigned_session["sessionActions"]
        now = datetime.now(tz=timezone.utc)
        self._action_updates_map.update(
            {
                action["sessionActionId"]: SessionActionStatus(
                    id=action["sessionActionId"],
                    completed_status="FAILED" if action is actions[0] else "NEVER_ATTEMPTED",
                    start_time=now if action is actions[0] else None,
                    end_time=now if action is actions[0] else None,
                    status=ActionStatus(
                        state=ActionState.FAILED,
                        fail_message=str(error_message),
                    ),
                )
                for action in actions
            }
        )
        self._wakeup.set()

    @staticmethod
    def _determine_user_for_session(
        *,
        host_is_posix: bool,
        job_run_as_user: Optional[JobRunAsUser],
        job_run_as_user_override: JobsRunAsUserOverride,
        queue_id: str,
        job_id: str,
        session_id: str,
    ) -> Optional[SessionUser]:
        # Called only in self._create_new_sessions() to determine what os_user the Session should
        # run as.
        # Raises a ValueError if an impossible situation arises and we need to fail the Session.
        os_user: Optional[SessionUser] = None
        if not job_run_as_user_override.run_as_agent:
            if job_run_as_user_override.job_user is not None:
                os_user = job_run_as_user_override.job_user
                logger.info(
                    SessionLogEvent(
                        subtype=SessionLogEventSubtype.USER,
                        queue_id=queue_id,
                        job_id=job_id,
                        session_id=session_id,
                        user=os_user.user,
                        message="Running as host-configured override user.",
                    )
                )
            elif job_run_as_user is None:
                # Terminal error. We need to fail the Session.
                # This should *never* happen; it occuring would mean that a service invariant has
                # been violated.
                message = (
                    "FATAL: Queue does not have a jobRunAsUser. This should not be possible. "
                    "Please report this to the service team."
                )
                raise ValueError(message)
            elif not job_run_as_user.is_worker_agent_user:
                # If we do not have a job-user override & we're not explicitly running
                # as the agent's user, then we *MUST* have a jobRunAsUser from the JobDetails.
                # Reasons:
                #  1) The service always allows service-managed Fleets to associate with
                #     a Queue. The SMF Worker Agent is *always* run with a local user override.
                #  2) The service only allows a customer-managed Fleet to associate with a
                #     Queue if either:
                #       a) The jobRunAsUser is explicitly set to WORKER_AGENT_USER; or
                #       b) The jobRunAsUser is explicitly set to QUEUE_CONFIGURED_USER and
                #          a user has been defined for the CMF's OS Platform (Linux/MacOS Fleets must
                #          have a "posix" user; and Windows Fleets must have a "windows" user)
                #  3) The service does not allow a Queue's jobRunAsUser to be updated if the constraint
                #     imposed by (2) above would be violated for one or more of that Queue's current QFAs.
                if host_is_posix:
                    os_user = job_run_as_user.posix
                else:
                    os_user = job_run_as_user.windows
                if os_user is None:
                    # Terminal error. We need to fail the Session.
                    # This should *never* happen; it occuring would mean that a service invariant has
                    # been violated.
                    message = (
                        "FATAL: Queue's jobRunAsUser does not define a QUEUE_CONFIGURED_USER for this platform. "
                        "Please report this to the service team."
                    )
                    raise ValueError(message)
                else:
                    logger.info(
                        SessionLogEvent(
                            subtype=SessionLogEventSubtype.USER,
                            queue_id=queue_id,
                            job_id=job_id,
                            session_id=session_id,
                            user=os_user.user,
                            message="Running as Queue's jobRunAsUser.",
                        )
                    )
        if os_user is None:
            try:
                user_to_log = getpass.getuser()
            except Exception:
                # This is best-effort. If we cannot determine the user we will not log
                user_to_log = "UNKNOWN"
            logger.warning(
                SessionLogEvent(
                    subtype=SessionLogEventSubtype.USER,
                    queue_id=queue_id,
                    job_id=job_id,
                    session_id=session_id,
                    user=user_to_log,
                    message="Running as the Worker Agent's user. This configuration is not recommended; please see the Security chapter of the User Guide.",
                )
            )
        return os_user

    def _create_new_sessions(
        self,
        *,
        assigned_sessions: dict[str, AssignedSession],
    ) -> set[str]:
        new_session_ids = assigned_sessions.keys() - self._sessions.keys()

        for new_session_id in new_session_ids:
            session_spec = assigned_sessions[new_session_id]
            logger.debug(f"session spec: {session_spec}")
            job_id = session_spec["jobId"]
            queue_id = session_spec["queueId"]

            logger.info(
                SessionLogEvent(
                    subtype=SessionLogEventSubtype.STARTING,
                    queue_id=queue_id,
                    job_id=job_id,
                    session_id=new_session_id,
                    message="Starting new Session.",
                )
            )

            # Log path
            session_log_file: Path | None = None
            if self._worker_logs_dir:
                queue_log_dir = self._queue_log_dir_path(queue_id=session_spec["queueId"])
                try:
                    if os.name == "posix":
                        queue_log_dir.mkdir(mode=stat.S_IRWXU, exist_ok=True)
                    else:
                        make_directory(
                            dir_path=queue_log_dir,
                            exist_ok=True,
                            agent_user_permission=FileSystemPermissionEnum.FULL_CONTROL,
                        )
                except OSError as e:
                    error_msg = (
                        f"Failed to create local session log directory on worker: {queue_log_dir}"
                    )
                    self._fail_all_actions(session_spec, error_message=error_msg)
                    logger.error(
                        FilesystemLogEvent(
                            op=FilesystemLogEventOp.CREATE,
                            filepath=str(queue_log_dir),
                            message="Could not create local session log directory: %s" % str(e),
                        )
                    )
                    logger.error(
                        SessionLogEvent(
                            subtype=SessionLogEventSubtype.FAILED,
                            queue_id=queue_id,
                            job_id=job_id,
                            session_id=new_session_id,
                            message="Could not create local session log directory.",
                        )
                    )
                    continue

                session_log_file = self._session_log_file_path(
                    session_id=new_session_id, queue_log_dir=queue_log_dir
                )
                try:
                    if os.name == "posix":
                        session_log_file.touch(mode=stat.S_IWUSR | stat.S_IRUSR, exist_ok=True)
                    else:
                        touch_file(
                            file_path=session_log_file,
                            agent_user_permission=FileSystemPermissionEnum.READ_WRITE,
                        )
                except OSError as e:
                    error_msg = (
                        f"Failed to create local session log file on worker: {session_log_file}"
                    )
                    self._fail_all_actions(session_spec, error_message=error_msg)
                    logger.error(
                        FilesystemLogEvent(
                            op=FilesystemLogEventOp.CREATE,
                            filepath=str(session_log_file),
                            message="Could not create local session log file: %s" % str(e),
                        )
                    )
                    logger.error(
                        SessionLogEvent(
                            subtype=SessionLogEventSubtype.FAILED,
                            queue_id=queue_id,
                            job_id=job_id,
                            session_id=new_session_id,
                            message="Could not create local session log file.",
                        )
                    )
                    continue

            # TODO: Ideally, this would be before we create the log file and directory locally, but we currently
            # require the session_log_file to construct the LogConfiguration.
            try:
                log_config = LogConfiguration.from_boto(
                    loggers=[OPENJD_SESSION_LOG, JOB_ATTACHMENTS_LOGGER],
                    log_configuration=session_spec["logConfiguration"],
                    session_log_file=session_log_file,
                )
            except LogProvisioningError as log_provision_error:
                self._fail_all_actions(session_spec, str(log_provision_error))
                logger.error(
                    SessionLogEvent(
                        subtype=SessionLogEventSubtype.FAILED,
                        queue_id=queue_id,
                        job_id=job_id,
                        session_id=new_session_id,
                        message=str(log_provision_error),
                    )
                )
                continue

            job_entities = JobEntities(
                farm_id=self._farm_id,
                fleet_id=self._fleet_id,
                worker_id=self._worker_id,
                job_id=job_id,
                deadline_client=self._deadline,
                windows_credentials_resolver=self._windows_credentials_resolver,
                job_run_as_user_override=self._job_run_as_user_override,
            )
            # TODO: Would be great to merge Session + SessionActionQueue
            # and move all job entities calls within the Session thread.
            # Requires some updates to the code below
            try:
                job_details = job_entities.job_details()

                # For Windows the WA runs as Administrator so fail jobs that were configured to runAs - WORKER_AGENT_USER as that would provide Admin privileges to the job
                if (
                    os.name == "nt"
                    and self._job_run_as_user_override.job_user is None
                    and not self._job_run_as_user_override.run_as_agent
                    and job_details.job_run_as_user
                    and job_details.job_run_as_user.is_worker_agent_user
                ):
                    err_msg = "Job cannot run as WORKER_AGENT_USER. Worker Agent is running with Administrator privileges."
                    self._fail_all_actions(session_spec, err_msg)
                    logger.error(
                        SessionLogEvent(
                            subtype=SessionLogEventSubtype.FAILED,
                            queue_id=queue_id,
                            job_id=job_id,
                            session_id=new_session_id,
                            message=err_msg,
                        )
                    )
                    continue

            except (ValueError, RuntimeError) as error:
                # Can't even start a session right now if we don't
                # get valid job_details, so let's fail the actions
                # in the same way as the log provisioning error
                self._fail_all_actions(session_spec, str(error))
                logger.error(
                    SessionLogEvent(
                        subtype=SessionLogEventSubtype.FAILED,
                        queue_id=queue_id,
                        job_id=job_id,
                        session_id=new_session_id,
                        message=str(error),
                    )
                )
                continue

            queue = SessionActionQueue(
                queue_id=queue_id,
                job_id=job_id,
                session_id=new_session_id,
                job_entities=job_entities,
                action_update_callback=self._handle_session_action_update,
            )

            queue.replace(actions=session_spec["sessionActions"])

            os_user: Optional[SessionUser] = None
            try:
                os_user = self._determine_user_for_session(
                    host_is_posix=os.name == "posix",
                    job_run_as_user=job_details.job_run_as_user,
                    job_run_as_user_override=self._job_run_as_user_override,
                    queue_id=queue_id,
                    job_id=job_id,
                    session_id=new_session_id,
                )
            except ValueError as e:
                message = str(e)
                self._fail_all_actions(session_spec, message)
                logger.error(
                    SessionLogEvent(
                        subtype=SessionLogEventSubtype.USER,
                        queue_id=queue_id,
                        job_id=job_id,
                        session_id=new_session_id,
                        message=message,
                    )
                )
                continue

            queue_credentials: QueueAwsCredentials | None = None
            asset_sync: AssetSync | None = None
            if job_details.queue_role_arn:
                try:
                    queue_credentials = self._get_queue_aws_credentials(
                        queue_id,
                        job_details.queue_role_arn,
                        new_session_id,
                        os_user,
                    )
                except (
                    DeadlineRequestWorkerOfflineError,
                    DeadlineRequestUnrecoverableError,
                    RuntimeError,
                ) as e:
                    # Terminal error. We need to fail the Session.
                    message = "Error obtaining AWS Credentials for the Queue Role: %s" % str(e)
                    self._fail_all_actions(session_spec, message)
                    logger.error(
                        SessionLogEvent(
                            subtype=SessionLogEventSubtype.AWSCREDS,
                            queue_id=queue_id,
                            job_id=job_id,
                            session_id=new_session_id,
                            message=message,
                        )
                    )
                    continue

                if queue_credentials is not None:
                    logger.info(
                        SessionLogEvent(
                            subtype=SessionLogEventSubtype.AWSCREDS,
                            queue_id=queue_id,
                            job_id=job_id,
                            session_id=new_session_id,
                            message="AWS Credentials are available.",
                        )
                    )
                else:
                    logger.warning(
                        SessionLogEvent(
                            subtype=SessionLogEventSubtype.AWSCREDS,
                            queue_id=queue_id,
                            job_id=job_id,
                            session_id=new_session_id,
                            message="AWS Credentials are not available: Failed to obtain credentials.",
                        )
                    )
            else:
                logger.warning(
                    SessionLogEvent(
                        subtype=SessionLogEventSubtype.AWSCREDS,
                        queue_id=queue_id,
                        job_id=job_id,
                        session_id=new_session_id,
                        message="AWS Credentials are not available: Queue has no IAM Role.",
                    )
                )

            if queue_credentials:
                asset_sync = AssetSync(
                    farm_id=self._farm_id,
                    boto3_session=queue_credentials.session,
                    session_id=new_session_id,
                )

            is_ja_settings_empty = job_details.job_attachment_settings is None or (
                len(job_details.job_attachment_settings.s3_bucket_name) == 0
                and len(job_details.job_attachment_settings.root_prefix) == 0
            )
            if not is_ja_settings_empty and asset_sync is None:
                # The Queue is configured to use Job Attachments, but there are no Queue credentials
                # available. This is a recipe for disaster. Fail the Session quickly to surface the
                # problem in a clear way.
                fail_message: str
                if job_details.queue_role_arn:
                    fail_message = "Job Attachments are configured on the Queue, but AWS Credentials for the Queue are not available."
                else:
                    fail_message = "Misconfiguration. Job Attachments are configured on the Queue, but the Queue has no IAM Role."
                self._fail_all_actions(session_spec, fail_message)
                logger.error(
                    SessionLogEvent(
                        subtype=SessionLogEventSubtype.FAILED,
                        queue_id=queue_id,
                        job_id=job_id,
                        session_id=new_session_id,
                        message=fail_message,
                    )
                )
                continue

            env = {
                "DEADLINE_SESSION_ID": new_session_id,
                "DEADLINE_FARM_ID": self._farm_id,
                "DEADLINE_QUEUE_ID": queue_id,
                "DEADLINE_JOB_ID": job_id,
                "DEADLINE_FLEET_ID": self._fleet_id,
                "DEADLINE_WORKER_ID": self._worker_id,
            }
            if queue_credentials:
                env.update(
                    {
                        "AWS_PROFILE": queue_credentials.session.credential_process_profile_name,
                        "AWS_CONFIG_FILE": str(queue_credentials.session.aws_config.path),
                        "AWS_SHARED_CREDENTIALS_FILE": str(
                            queue_credentials.session.aws_credentials.path
                        ),
                    }
                )

            logger.debug("env = \n%s", json.dumps(env, indent=2))

            session = Session(
                id=new_session_id,
                queue=queue,
                queue_id=queue_id,
                job_id=job_id,
                env=env,
                asset_sync=asset_sync,
                job_details=job_details,
                os_user=os_user,
                retain_session_dir=self._retain_session_dir,
                action_update_callback=self._handle_session_action_update,
                action_update_lock=self._action_update_lock,
                session_root_dir=self._session_root_dir,
            )

            def run_session(
                session: Session, queue_credentials: QueueAwsCredentials | None
            ) -> None:
                queue_credentials_context: nullcontext | AwsCredentialsRefresher
                if queue_credentials is not None:
                    queue_credentials_context = queue_credentials.refresher
                else:
                    queue_credentials_context = nullcontext()
                with (
                    log_config.log_session(
                        queue_id=queue_id,
                        job_id=job_id,
                        session_id=new_session_id,
                        boto_session=self._boto_session,
                    ),
                    session,
                    queue_credentials_context,
                ):
                    if isinstance(queue_credentials_context, nullcontext):
                        session.logger.warning("Session running with no AWS Credentials.")
                    if session.os_user is not None:
                        session.logger.info(
                            "Running Session Actions as user: %s" % session.os_user.user
                        )
                    try:
                        session.run()
                    except Exception as e:
                        logger.exception(e)
                        raise
                    finally:
                        self._wakeup.set()

            self._sessions[new_session_id] = SchedulerSession(
                future=self._executor.submit(run_session, session, queue_credentials),
                queue=queue,
                session=session,
                job_entities=job_entities,
                log_configuration=log_config,
            )
        return new_session_ids

    def _session_log_file_path(
        self,
        *,
        session_id: str,
        queue_log_dir: Path,
    ) -> Path:
        """Determines the path where a session should be logged

        Parameters
        ----------
        session_id : str
            The unique session identifier
        queue_log_dir : Path
            The path to the queue log directory

        Returns
        -------
        Path
            The path to the session log
        """
        return queue_log_dir / f"{session_id}.log"

    def _queue_log_dir_path(
        self,
        *,
        queue_id: str,
    ) -> Path:
        """Determines the path where a queue's session logs should be written

        Parameters
        ----------
        queue_id : str
            The unique queue identifier
        queue_log_dir : Path
            The path to the queue log directory

        Returns
        -------
        Path
            The path to the session log
        """
        assert self._worker_logs_dir is not None
        return self._worker_logs_dir / queue_id

    def _cleanup_queue_aws_credentials(
        self, *, assigned_sessions: dict[str, AssignedSession]
    ) -> None:
        """Deletes the Queue AWS Credentials manager for a Queue if we no longer have assigned sessions for
        that particular Queue.
        """
        with self._queue_aws_credentials_lock:
            if not self._queue_aws_credentials:
                return

            assigned_queues = set(session["queueId"] for session in assigned_sessions.values())
            created_manager_keys = set(self._queue_aws_credentials.keys())
            for key in created_manager_keys:
                queue_id, role_arn = key.split(":", maxsplit=1)
                if queue_id not in assigned_queues:
                    credentials_dataclass = self._queue_aws_credentials[key]
                    credentials_dataclass.session.cleanup()
                    del self._queue_aws_credentials[key]
                    logger.debug(
                        AwsCredentialsLogEvent(
                            op=AwsCredentialsLogEventOp.DELETE,
                            resource=queue_id,
                            role_arn=role_arn,
                            message="AWS Credentials deleted.",
                        )
                    )

    def _get_queue_aws_credentials(
        self, queue_id: str, queue_role_arn: str, session_id: str, os_user: Optional[SessionUser]
    ) -> Optional[QueueAwsCredentials]:
        """Creates an AWS Credentials Manager for the given Queue if necessary.
        Returns the credentials profile name for the credentials if there is one.
        """
        hash_key = f"{queue_id}:{queue_role_arn}"
        with self._queue_aws_credentials_lock:
            if self._queue_aws_credentials.get(hash_key) is None:
                # We don't already have one, so we create it.
                try:
                    # Note: Makes a call to AssumeQueueRoleForWorker to fetch the initial
                    # AWS Credentials.
                    session = QueueBoto3Session(
                        deadline_client=self._deadline,
                        farm_id=self._farm_id,
                        fleet_id=self._fleet_id,
                        worker_id=self._worker_id,
                        queue_id=queue_id,
                        role_arn=queue_role_arn,
                        os_user=os_user,
                        interrupt_event=self._shutdown,
                        worker_persistence_dir=self._worker_persistence_dir,
                        region=self._boto_session.region_name,
                    )
                except (DeadlineRequestWorkerOfflineError, DeadlineRequestUnrecoverableError):
                    # These are terminal errors for the Session. We need to fail it, without attempting,
                    # if we have a terminal error.
                    # The caller will log a message.
                    raise
                except (DeadlineRequestError, DeadlineRequestInterrupted):
                    # We treat any non-terminal error as recoverable. We simply run the Session with no AWS Credentials,
                    # but will log to the customer that it's running with no Credentials.
                    return None

                refresher = AwsCredentialsRefresher(
                    resource={"resource": queue_id, "role_arn": queue_role_arn},
                    session=session,
                    failure_callback=partial(
                        self._queue_credentials_refresh_failed, hash_key=hash_key
                    ),
                )

                credentials_dataclass = QueueAwsCredentials(session=session, refresher=refresher)
                self._queue_aws_credentials[hash_key] = credentials_dataclass
                logger.debug(
                    f"Created new AWS Credentials for Queue {queue_id} with IAM Role {queue_role_arn}."
                )

            return self._queue_aws_credentials[hash_key]

        # Unreachable, but play it safe.
        return None

    def _queue_credentials_refresh_failed(self, exception: Exception, *, hash_key: str) -> None:
        """Called by an AwsCredentialsRefresher instance when it was unable to refresh
        AWS Credentials for a Queue.
        In response we interrupt all Sessions that are currently in flight.
        """

        # TODO: To be fully correct, we'd want to only interrupt the Sessions that
        # are using the particular credentials that failed to refresh.
        if isinstance(exception, DeadlineRequestError):
            # Unrecoverable. Delete the credential refresher. This will cause a future
            # Session start for the same queue to attempt to recreate it if it can obtain
            # credentials; the alternative would leave the Queue credentials permanently
            # expired with no way to refresh them.
            with self._queue_aws_credentials_lock:
                if hash_key in self._queue_aws_credentials:
                    credentials_dataclass = self._queue_aws_credentials[hash_key]
                    credentials_dataclass.session.cleanup()
                    del self._queue_aws_credentials[hash_key]

        gracetime = None  # Let the cancels happen as defined in the Job Template
        message = "Fatal error attempting to refresh AWS Credentials for the Queue. Please see logs for details."
        shutdown_futures = self._shutdown_sessions(gracetime, message)
        wait(shutdown_futures)

    def _update_session_actions_from_scheduler(
        self,
        *,
        assigned_sessions: dict[str, AssignedSession],
        canceled_session_action: dict[str, list[str]],
    ) -> None:
        for session_id, session_assignment in assigned_sessions.items():
            session_entry = self._sessions[session_id]
            session = session_entry.session

            session_exception: BaseException | None = None
            try:
                session_exception = session_entry.future.exception(timeout=0.2)
            except FutureTimeoutError:
                pass
            else:
                if session_exception is None:
                    session_exception = Exception("Session has previously been stopped.")

            with self._action_update_lock:
                # 1. cancel in-flight actions
                if canceled_action_ids := canceled_session_action.get(session_id, None):
                    canceled_action_ids = [
                        action_id
                        for action_id in canceled_action_ids
                        if not (update := self._action_updates_map.get(action_id, None))
                        or update.completed_status is None
                    ]
                    session.cancel_actions(action_ids=canceled_action_ids)

                # 2. update the queue actions
                assigned_session_actions = session_assignment["sessionActions"]
                if not session_exception:
                    assigned_session_actions = [
                        entry
                        for entry in assigned_session_actions
                        if not (
                            update := self._action_updates_map.get(entry["sessionActionId"], None)
                        )
                        or update.completed_status is None
                    ]
                    session.replace_assigned_actions(actions=assigned_session_actions)
                else:
                    # The thread that normally runs session actions crashed or was stopped through a separate
                    # failure flow (e.g. from an API response that said to stop it).
                    self._return_sessionactions_from_stopped_session(
                        assigned_session_actions=assigned_session_actions,
                        failure_message=str(session_exception),
                    )
                    self._wakeup.set()

    def _return_sessionactions_from_stopped_session(
        self,
        *,
        assigned_session_actions: list[
            EnvironmentAction
            | TaskRunAction
            | SyncInputJobAttachmentsAction
            | AttachmentDownloadAction
        ],
        failure_message: str,
    ) -> None:
        # The thread that normally runs session actions crashed or was stopped through a separate
        # failure flow (e.g. from an API response that said to stop it).

        # We need to return the actions that we were given according to the API contract.
        # 1. First Action in the pipeline returns as failed;
        # 2. Subsequent Actions returned as NEVER_ATTEMPTED
        # 3. Exception for ENV_EXIT Actions corresponding to ENV_ENTERS that were attempted;
        #    these must always be attempted, and should not return as NEVER_ATTEMPTED. We just
        #    return those as FAILED
        #
        # We also check to see if there's already a status update queued to be reported
        # for an action, and if so then we defer to what's already there.
        #
        # This is far from perfect, but we'll do the best that we can with the information
        # that we have at this point.

        # Cases that should be handled a-okay by this code:
        #  1)
        #    ENV_ENTER 1 (SUCCESS)
        #    ENV_ENTER 2 (FAILED/INTERRUPTED) - Session main thread exited
        #    ENV_ENTER 3 (NEVER_ATTEMPTED)
        #    TASK_RUN(s) (NEVER_ATTEMPED)
        # and gets the expected cleanup actions:
        #    ENV_EXIT 2  (will be returned as FAILED)
        #    ENV_EXIT 1  (will be returned as FAILED)
        #
        #  2)
        #    ENV_ENTER(s) (SUCCESS)
        #    TASK_RUN(s) (SUCCESS)
        #    TASK_RUN (FAILED/INTERRUPTED) - Session main thread exited
        #    TASK_RUN(s) (NEVER_ATTEMPTED)
        # and gets the expected cleanup actions:
        #    ENV_EXIT(s) (will be returned as FAILED)
        #
        #  3)
        #    ENV_ENTER 1 (SUCCESS)
        #    ENV_ENTER 2 (SUCCESS)
        #    ENV_ENTER 3 (SUCCESS)
        #    TASK_RUN(s)  (SUCCESS)
        #    ENV_EXIT 3 (FAILED/INTERRUPTED) - Session main thread exited
        #    ENV_EXIT 2 (SUCCESS/FAILED)
        #    ENV_EXIT 1 (SUCCESS/FAILED)
        #  agent will not get any cleanup actions queued to it in the response.
        #  If the Session thread has a problem during an ENV_EXIT, then the Agent's
        #  pipeline already contains *ALL* of the ENV_EXITs for the Session; if it has
        #  one then it has them all. The Session's post-main-thread cleanup will ensure
        #  that the ENV_EXIT 2 & 1 are properly returned. If somehow it doesn't, and we
        #  get those actions back in the response from the service then this'll be just
        #  like case (2).
        #
        #  4)
        #    ENV_ENTER(s) (SUCCESS)
        #    TASK_RUN(s) (SUCCESS)
        #    TASK_RUN a (FAILED/INTERRUPTED) - Session main thread exited
        #    TASK_RUN(s) (NEVER_ATTEMPTED)
        # with the FAILED/INTERRUPTED TASK_RUN, and Actions after it not being reported to the
        # service.
        # Service response is it's understanding of the pipeline based on the responses that
        # it has seen.
        #    TASK_RUN a (will be returned as FAILED)
        #    TASK_RUN(s) (will be returned as NEVER_ATTEMPTED)
        #    ...
        #    ENV_EXIT(s) (will be returned as FAILED)
        # This case only possible if the dieing Agent's Session fails to return the
        # status for the FAILED/INTERRUPTED TASK_RUN.
        # Note: The service will only respond with TASK_RUN(s) if it never received the
        #  FAILED/INTERRUPTED status for the TASK_RUN.

        for i, action in enumerate(assigned_session_actions):
            session_action_id = action["sessionActionId"]
            if self._action_updates_map.get(session_action_id) is not None:
                # Prefer the existing record; it must have been put there by the
                # Session before it exited.
                continue

            completed_status: Optional[
                Literal["SUCCEEDED", "FAILED", "INTERRUPTED", "CANCELED", "NEVER_ATTEMPTED"]
            ]
            start_time: Optional[datetime] = None
            end_time: Optional[datetime] = None
            if i == 0 or action["actionType"] == "ENV_EXIT":
                # Fail the first Action, or all ENV_EXITs
                completed_status = "FAILED"
                start_time = end_time = datetime.now(tz=timezone.utc)
            else:
                # NEVER_ATTEMPTED all other Actions
                # Note: NEVER_ATTEMPED must not be reported with a started/ended time.
                completed_status = "NEVER_ATTEMPTED"

            self._action_updates_map[action["sessionActionId"]] = SessionActionStatus(
                id=session_action_id,
                # FAILED for the first one in the list, NEVER_ATTEMPTED for all of the others.
                completed_status=completed_status,
                start_time=start_time,
                end_time=end_time,
                status=ActionStatus(
                    # The 'state' is ignored; we just need this for the fail message.
                    state=ActionState.FAILED,
                    fail_message=failure_message,
                ),
            )

    def _update_session_logging(
        self,
        *,
        assigned_sessions: dict[str, AssignedSession],
    ) -> None:
        """Updates the run-time logging parameters of the session

        Parameters
        ----------
        assigned_sessions : dict[str, AssignedSession]
            A dictionary of sessions to update. The keys are the session ID and the values are
            the entries from the UpdateWorkerSchedule "assignedSessions" response field.
        """
        for session_id, session_spec in assigned_sessions.items():
            if not (session_entry := self._sessions.get(session_id, None)):
                logger.warning("No session found: %s", session_id)
                continue
            parameters = SessionLogConfigurationParameters.from_boto(
                session_spec["logConfiguration"]["parameters"]
            )
            session_entry.log_configuration.update(parameters=parameters)

    def shutdown(
        self,
        *,
        fail_message: str | None = None,
        grace_time: timedelta | None = None,
    ) -> None:
        """Stops all active Work and notifies the AWS Deadline Cloud service that the Worker is shutting down.

        Parameters
        ----------
        fail_message : str | None
            An optional message associated with interrupted or skipped session actions.
        grace_time : timedelta
            The amount of time to wait before force-stopping all work
        """
        self._shutdown_fail_message = fail_message
        self._shutdown_grace = grace_time

        # THIS ORDER IS IMPORTANT FOR DATA RACES
        # This is based on the logic in the main run() loop
        self._shutdown.set()
        self._wakeup.set()

    @property
    def session_queues(self) -> dict[str, list[SessionActionDefinition]]:
        """ "
        Returns a mapping of session ID to a list of session actions that are assigned to the Worker

        Returns
        -------
        dict[str, list[SessionAction]]
            A mapping where the key is a unique session ID and the value is an ordered list of
            SessionAction instances to be run against the session
        """
        raise NotImplementedError("WorkerQueue.session_queues property not implemented")
