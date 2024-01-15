# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import os
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import partial
from logging import getLogger, LoggerAdapter
from threading import Event, RLock
from time import monotonic, sleep
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Literal,
    Optional,
    Tuple,
    TypeVar,
)

from deadline_worker_agent.api_models import (
    EntityIdentifier,
    SyncInputJobAttachmentsAction,
)

if TYPE_CHECKING:
    from ..api_models import CompletedActionStatus, EnvironmentAction, TaskRunAction
    from ..scheduler.session_queue import SessionActionQueue
    from .actions import SessionActionDefinition
    from .job_entities import JobAttachmentDetails, JobDetails

from openjd.sessions import (
    ActionState,
    ActionStatus,
    EnvironmentIdentifier,
    EnvironmentModel,
    Parameter,
    PathMappingRule,
    PosixSessionUser,
    StepScriptModel,
    SessionUser,
)
from openjd.sessions import Session as OPENJDSession
from openjd.sessions import LOG as OPENJD_LOG

from deadline.job_attachments.asset_sync import AssetSync
from deadline.job_attachments.asset_sync import logger as ASSET_SYNC_LOGGER
from deadline.job_attachments.models import (
    Attachments,
    JobAttachmentS3Settings,
    ManifestProperties,
    PathFormat,
)
from deadline.job_attachments.os_file_permission import (
    FileSystemPermissionSettings,
    PosixFileSystemPermissionSettings,
)
from deadline.job_attachments.progress_tracker import ProgressReportMetadata, SummaryStatistics

from ..aws.deadline import record_sync_inputs_telemetry_event, record_sync_outputs_telemetry_event
from ..scheduler.session_action_status import SessionActionStatus
from ..sessions.errors import SessionActionError

# TODO: Un-comment this when pipelined actions can be reported as NEVER_ATTEMPTED before the
# currently canceling action is completed
# from .errors import CancelationError

OPENJD_ACTION_STATE_TO_DEADLINE_COMPLETED_STATUS: dict[
    ActionState,
    CompletedActionStatus,
] = {
    ActionState.CANCELED: "CANCELED",
    ActionState.FAILED: "FAILED",
    ActionState.SUCCESS: "SUCCEEDED",
}
TIME_DELTA_ZERO = timedelta()


logger = getLogger(__name__)


@dataclass(frozen=True)
class ActiveEnvironment:
    session_env_id: EnvironmentIdentifier
    """An identifier that is unique to the Open Job Description session used to exit the environment"""

    job_env_id: str
    """A unique identifier that identifies the environment within the Open Job Description job model"""


@dataclass(frozen=True)
class CurrentAction:
    definition: SessionActionDefinition
    """The action definition"""

    start_time: datetime
    """The start time of the action"""


class Session:
    """A Worker session corresponding to an Open Job Description session

    This class manages:

    - a queue of session actions
    - the asynchronous orchestration of actions against the underlying Open Job Description session
    - notifying the progress and failure/completion of the active session action
    - asynchronous interruption/cancellation of the active session action

    Parameters
    ----------
    id : str
        A unique session identifier
    queue : SessionActionQueue
        An ordered queue of upcoming actions
    """

    _action_update_lock: RLock
    _active_envs: list[ActiveEnvironment]
    _asset_sync: Optional[AssetSync]
    _id: str
    _interrupted: bool = False
    _queue: SessionActionQueue
    _report_action_update: Callable[[SessionActionStatus], None]
    _stop: Event
    _current_action: CurrentAction | None = None
    _current_action_lock: RLock
    _stop_current_action_result: Literal["INTERRUPTED", "FAILED"] = "FAILED"
    _stop_grace_time: timedelta | None = None
    _stop_fail_message: str | None = None

    _os_user: SessionUser | None = None
    _queue_id: str
    _job_details: JobDetails
    _job_attachment_details: JobAttachmentDetails | None = None
    _initial_action_exception: Exception | None = None

    # Event that is set only when this Session is not running at all
    # i.e. it has exited, or never started, its main run loop/logic.
    _stopped_running: Event

    logger: LoggerAdapter

    def __init__(
        self,
        *,
        id: str,
        queue: SessionActionQueue,
        env: dict[str, str] | None = None,
        queue_id: str,
        asset_sync: Optional[AssetSync],
        os_user: SessionUser | None,
        job_details: JobDetails,
        action_update_callback: Callable[[SessionActionStatus], None],
        action_update_lock: RLock,
    ) -> None:
        self._id = id
        self._action_update_lock = action_update_lock
        self._active_envs = []
        self._asset_sync = asset_sync
        self._current_action_lock = RLock()
        self._queue_id = queue_id
        self._os_user = os_user
        self._job_details = job_details
        self._report_action_update = action_update_callback
        self._env = env
        self._executor = ThreadPoolExecutor(max_workers=1)

        def openjd_session_action_callback(session_id: str, action_status: ActionStatus) -> None:
            self.update_action(action_status)

        self._session = OPENJDSession(
            session_id=self._id,
            job_parameter_values=self._job_details.parameters,
            path_mapping_rules=self._job_details.path_mapping_rules,
            user=self._os_user,
            callback=openjd_session_action_callback,
            os_env_vars=self._env,
        )

        self._queue = queue
        self._stop = Event()
        self._stopped_running = Event()
        self._stopped_running.set()

        # Use the Open Job Description logger here since it:
        # 1. Is the log that openjd is sending action logs to;
        # 2. It is already set up to not propagate to the agent log; and
        # 3. We're already capturing it to send to cloudwatch.
        self.logger = LoggerAdapter(OPENJD_LOG, extra={"session_id": self._id})

    @property
    def id(self) -> str:
        """The unique session ID"""
        return self._id

    @property
    def os_user(self) -> Optional[SessionUser]:
        """The session user"""
        return self._os_user

    def _warm_job_entities_cache(self) -> None:
        """Attempts to cache the job entities response for all
        actions in the SessionActionQueue within the Session thread.

        Only logs a warning if it fails
        """
        identifiers: list[EntityIdentifier] = self._queue.list_all_action_identifiers()

        logger.info("Warming Job Entity Cache")
        try:
            self._queue._job_entities.cache_entities(identifiers)
        except Exception as e:
            logger.warning(f"Did not fully warm job entity cache: {str(e)}. Continuing")
        else:
            logger.info("Fully warmed Job Entity Cache")

    def run(self) -> None:
        """Runs the Worker session.

        This code will loop until Session.stop() is called from another thread.
        """
        self._warm_job_entities_cache()

        logger.info("[%s]: Session started", self._id)
        self._stopped_running.clear()

        try:
            self._run()
        except Exception as e:
            # We set the stop event to inform the Open Job Description action update callback (the
            # Session._action_updated() method) that action updates are no longer reported to the
            # service. If an action was running at the time of this exception, its failure is
            # reported immediately in the call to Session._cleanup() below.
            self._stop_fail_message = f"Worker encountered an unexpected error: {e}"
            self._stop.set()
            raise
        finally:
            try:
                self._cleanup()
            except Exception as error:
                logger.exception(
                    f"Unexpected exception while performing cleanup of Session {self._id}: {error}."
                )
            finally:
                self._stopped_running.set()

        logger.info("[%s]: Session complete", self._id)

    def wait(self, timeout: timedelta | None = None) -> None:
        # Wait until this Session is not running anymore.
        self._stopped_running.wait(timeout=timeout.seconds if timeout else None)

    def _run(self) -> None:
        """The contains the main run loop for processing session actions.

        This code will loop until Session.stop() is called from another thread.
        """

        with ThreadPoolExecutor(max_workers=1) as executor:
            self._executor = executor
            while not self._stop.wait(timeout=0.1):
                # Start session action if needed
                with (
                    # NOTE: Lock acquisition order is important. Must be:
                    #     1.  action update lock (scheduler owned)
                    #     2.  current action lock
                    #
                    # If the action starts running successfully, the Open Job Description session will invoke
                    # the session action update callback with state = RUNNING. That callback
                    # requires acquiring self._action_update_lock, but another thread may hold that
                    # lock and cause a deadlock. To avoid the deadlock, we proactively acquire the
                    # lock here first
                    self._action_update_lock,
                    self._current_action_lock,
                ):
                    if not self._current_action:
                        self._start_action()

    def _cleanup(self) -> None:
        """Attempt to clean up the session.

        If an action is running, the action is canceled and awaited. Once there is no running
        action, any active environments are exited.
        """
        #################
        # Stop workflow #
        #################

        # Build up an ordered list of cleanup actions for the session
        actions: list[Tuple[Callable[[], None], str]] = []

        # If we have a running action in the session, we cancel it
        with (
            self._action_update_lock,
            self._current_action_lock,
        ):
            if current_action := self._current_action:
                actions.append(
                    (
                        partial(
                            self._start_canceling_current_action,
                            time_limit=self._stop_grace_time,
                        ),
                        "cancel running action",
                    )
                )
                self._interrupted = True
                self._report_action_update(
                    SessionActionStatus(
                        completed_status=self._stop_current_action_result,
                        start_time=current_action.start_time,
                        end_time=datetime.now(tz=timezone.utc),
                        id=current_action.definition.id,
                        status=ActionStatus(
                            state=ActionState.CANCELED,
                            fail_message=self._stop_fail_message,
                        ),
                    )
                )

        self._queue.cancel_all(
            cancel_outcome="NEVER_ATTEMPTED",
            message=self._stop_fail_message,
        )

        # After canceling the running action, we exit any active environments
        actions.extend(
            (
                partial(self._session.exit_environment, identifier=env.session_env_id),
                f"exit environment {env.job_env_id}",
            )
            for env in reversed(self._active_envs)
        )

        # Here we attempt to run as many cleanup actions as we can in the correct order and within
        # the allowed grace time. Any unfinished cleanup actions will be aborted/skipped
        start_time = monotonic()
        cur_time = start_time

        try:
            for action, desc in actions:
                try:
                    action()
                except Exception as e:
                    logger.warning("Failed to %s: %s", desc, e)
                    continue

                elapsed_time = timedelta(seconds=cur_time - start_time)
                action_timeout = (
                    max(timedelta(), self._stop_grace_time - elapsed_time)
                    if self._stop_grace_time is not None
                    else None
                )
                try:
                    # Raises:
                    #   TimeoutError
                    for _ in self._monitor_action(timeout=action_timeout):
                        pass
                except TimeoutError:
                    # Log, cancel the cleanup action and break the loop if we've run out of grace time
                    logger.warning("%s timed out", desc)
                    self._session.cancel_action()
                    break
                else:
                    logger.info("%s successful", desc)
                cur_time = monotonic()
        finally:
            # Clean-up the Open Job Description session
            self._session.cleanup()

    def replace_assigned_actions(
        self,
        *,
        actions: Iterable[EnvironmentAction | TaskRunAction | SyncInputJobAttachmentsAction],
    ) -> None:
        """Replaces the assigned actions

        This method only supports the following modifications to the assigned actions:
        1.  Adding new actions
        2.  Keeping/reordering existing ones

        The method DOES NOT handle cancelations. Cancelations should be done by calling
        Session.cancel_actions() before calling this method. Doing this in the wrong order results
        in undefined behaviour.

        Parameters
        ----------
        actions : Iterable[EnvironmentAction | TaskRunAction | SyncInputJobAttachmentsAction]
            The new sequence of actions to be assigned to the session. The order of the actions
            provided is used as the processing order
        """
        with self._current_action_lock:
            self._replace_assigned_actions_impl(actions=actions)

    def _replace_assigned_actions_impl(
        self,
        *,
        actions: Iterable[EnvironmentAction | TaskRunAction | SyncInputJobAttachmentsAction],
    ) -> None:
        """Replaces the assigned actions

        This is the implementation code for replacing actions. It merely forwards the action
        replacements to the SessionActionQueue associated with the Session instance (after
        filtering out the currently running action).

        Session.replace_assigned_actions() is a thin wrapper of
        Session._replace_assigned_actions_impl that acquires Session._current_action_lock
        before/after calling this method. The separation exists to more easily test the locking
        semantics independently from the business logic.
        """
        running_action_id: str | None = None
        if running_action := self._current_action:
            running_action_id = running_action.definition.id
        self._queue.replace(
            actions=(action for action in actions if action["sessionActionId"] != running_action_id)
        )

    def cancel_actions(
        self,
        *,
        action_ids: list[str],
    ) -> None:
        """Cancels the specified queued or running action(s)

        Parameters
        ----------
        action_ids : list[str]
            The unique IDs of the actions to be canceled. Any non-matching actions are ignored.
        """
        with (
            self._action_update_lock,
            self._current_action_lock,
        ):
            self._cancel_actions_impl(action_ids=action_ids)

    def _cancel_actions_impl(
        self,
        *,
        action_ids: list[str],
    ) -> None:
        """The internal implementation for canceling queued or running action(s).

        The caller should acquire the Session._current_action_lock before calling this method.

        Parameters
        ----------
        action_ids : list[str]
            The unique IDs of the actions to be canceled. Any non-matching actions are ignored.
        """
        for canceled_action_id in action_ids:
            if self._current_action and self._current_action.definition.id == canceled_action_id:
                self._start_canceling_current_action()
            # TODO: Uncomment the code below once the service allows completing canceled actions
            # out-of-order (while the current action is still canceling). In the meantime,
            # the logic in Session._action_updated_impl() will mark all non-ENV_EXIT actions as
            # NEVER_ATTEMPTED when the current action is canceled.
            # else:
            #     try:
            #         self._queue.cancel(id=canceled_action_id)
            #     except CancelationError as e:
            #         logger.warning(str(e))
            #     except Exception as e:
            #         logger.error(
            #             "[%s] [%s] (%s): Error canceling action: %s",
            #             self.id,
            #             canceled_action_id,
            #             self._current_action.definition.human_readable(),
            #             e,
            #         )

    def _start_canceling_current_action(self, *, time_limit: timedelta | None = None) -> None:
        """Begins cancelling the current action"""

        if not (current_action := self._current_action):
            raise ValueError("Current action not assigned")

        # Cancel the action
        logger.info(
            "[%s] [%s] (%s): Canceling action",
            self._id,
            current_action.definition.id,
            current_action.definition.human_readable(),
        )
        current_action.definition.cancel(session=self, time_limit=time_limit)

    def _start_action(self) -> None:
        # Imported in function to avoid a circular import
        from .actions import ExitEnvironmentAction

        try:
            if not (action_definition := self._queue.dequeue()):
                self._current_action = None
                return
        except SessionActionError as e:
            self._report_action_update(
                SessionActionStatus(
                    completed_status="FAILED",
                    start_time=datetime.now(tz=timezone.utc),
                    end_time=datetime.now(tz=timezone.utc),
                    id=e.action_id,
                    status=ActionStatus(
                        state=ActionState.FAILED,
                        fail_message=str(e),
                    ),
                )
            )
            self._queue.cancel_all(
                message=f"Error starting prior action {e.action_id}",
                cancel_outcome="FAILED",
                ignore_env_exits=True,
            )
            self._current_action = None
            return

        now = datetime.now(tz=timezone.utc)

        # If we have an initial failure (log provisioning), then we fail
        # any actions except environment exits for cleanup.
        if self._initial_action_exception and not isinstance(
            action_definition, ExitEnvironmentAction
        ):
            error_msg = str(self._initial_action_exception)
            self._report_action_update(
                SessionActionStatus(
                    completed_status="FAILED",
                    start_time=datetime.now(tz=timezone.utc),
                    end_time=datetime.now(tz=timezone.utc),
                    id=action_definition.id,
                    status=ActionStatus(
                        state=ActionState.FAILED,
                        fail_message=error_msg,
                    ),
                )
            )
            self._queue.cancel_all(
                message=f"Error starting prior action {action_definition.id}",
                cancel_outcome="FAILED",
                ignore_env_exits=True,
            )
            self._current_action = None
            return

        logger.info(
            "[%s] [%s] (%s): Starting action",
            self._id,
            action_definition.id,
            action_definition.human_readable(),
        )

        try:
            self._current_action = CurrentAction(
                definition=action_definition,
                start_time=now,
            )
            action_definition.start(
                session=self,
                executor=self._executor,
            )
        except Exception as e:
            logger.warn(
                "[%s] [%s] (%s): Error starting action: %s",
                self.id,
                action_definition.id,
                action_definition.human_readable(),
                e,
            )
            self._report_action_update(
                SessionActionStatus(
                    id=action_definition.id,
                    completed_status="FAILED",
                    start_time=now,
                    end_time=now,
                    status=ActionStatus(
                        state=ActionState.FAILED,
                        fail_message=str(e),
                    ),
                )
            )
            self._queue.cancel_all(
                message=f"Error starting prior action {action_definition.id}",
                # TODO: Change this after session actions failures before a task run count as
                # overall failures and do not cause retry sessions to be scheduled indefinitely
                cancel_outcome="FAILED",
                ignore_env_exits=True,
            )
            self._current_action = None

    def _report_action_failure(
        self,
        *,
        current_action: CurrentAction,
        exception: BaseException,
        end_time: datetime,
    ) -> None:
        """The method reports the action as failed to the scheduler.

        Parameters
        ----------
        current_action : CurrentAction
            The action to report as failed
        exception : BaseException
            The exception that caused the failure
        end_time: datetime
            The time of the failure
        """
        # This must come before calling Session._report_action_update() because the handler needs to
        # be able to determine if the Session is idle and make an immediate UpdateWorkerSchedule request if
        # so.
        self._current_action = None
        self._report_action_update(
            SessionActionStatus(
                id=current_action.definition.id,
                status=ActionStatus(
                    state=ActionState.FAILED,
                    fail_message=str(exception),
                    progress=None,
                    exit_code=None,
                    status_message=None,
                ),
                completed_status="FAILED",
                start_time=current_action.start_time,
                end_time=end_time,
                update_time=end_time,
            )
        )

    def _monitor_action(
        self, timeout: timedelta | None = None, frequency: timedelta = timedelta(seconds=0.3)
    ) -> Generator[ActionStatus, None, None]:
        """A generator function to help a caller monitor the life-cycle of an action.

        The generator will yield the status of the action at a regular frequency. The generator
        will exhaust once the action has reached a terminal state (CANCELED, FAILED, SUCCESS).

        An optional timeout can be given. If the action does not finish before the specified
        timeout, then a TimeoutError is raised.

        Parameters
        ----------
        timeout : timedelta | None
            The maximum time to wait before raising a TimeoutError
        yield_frequency : timedelta
            The amount of time to wait before yielding (default is 300ms).

        Returns
        -------
        Generator[ActionStatus, None, None]
            A generator function that yields the action status to the caller at the specified yield
            frequency. Callers should loop over the generator and can perform any intermediary
            actions when the generator yields.

        Raises
        ------
        TimeoutError
            Raised when the action has not completed within the specified timeout.
        """

        # Validation
        if timeout is not None and timeout < TIME_DELTA_ZERO:
            raise ValueError(f"timeout must be a positive timedelta or None, but got {timeout}")
        if frequency is not None and frequency <= TIME_DELTA_ZERO:
            raise ValueError(
                f"yield_frequency must be a positive timedelta or None, but got {frequency}"
            )

        if self._session.action_status is None:
            raise RuntimeError("No action is running")

        start_time = monotonic()
        cur_time = start_time
        remaining_time: timedelta | None = timeout
        if timeout:
            while (
                self._session.action_status.state
                not in OPENJD_ACTION_STATE_TO_DEADLINE_COMPLETED_STATUS
            ):
                elapsed_time = timedelta(seconds=cur_time - start_time)
                remaining_time = timeout - elapsed_time

                if elapsed_time > TIME_DELTA_ZERO:
                    yield self._session.action_status

                if remaining_time <= TIME_DELTA_ZERO:
                    raise TimeoutError()
                sleep(min(frequency, remaining_time).total_seconds())
                cur_time = monotonic()
        else:
            if self._session.action_status.state == ActionState.RUNNING:
                sleep(frequency.total_seconds())
            while self._session.action_status.state == ActionState.RUNNING:
                yield self._session.action_status
                sleep(frequency.total_seconds())

    def enter_environment(
        self,
        *,
        job_env_id: str,
        environment: EnvironmentModel,
    ) -> None:
        session_env_id = self._session.enter_environment(
            environment=environment, identifier=job_env_id
        )
        self._active_envs.append(
            ActiveEnvironment(
                job_env_id=job_env_id,
                session_env_id=session_env_id,
            )
        )

    def exit_environment(
        self,
        *,
        job_env_id: str,
    ) -> None:
        if not self._active_envs or self._active_envs[-1].job_env_id != job_env_id:
            env_stack_str = ", ".join(env.job_env_id for env in self._active_envs)
            raise ValueError(
                f"Specified environment ({job_env_id}) is not the inner-most active environment."
                f"Active environments from outer-most to inner-most are: {env_stack_str}"
            )
        active_env = self._active_envs[-1]
        self._session.exit_environment(identifier=active_env.session_env_id)
        self._active_envs.pop()

    def _notifier_callback(
        self,
        current_action: CurrentAction,
        progress_report: ProgressReportMetadata,
    ) -> bool:
        """Callback to be passed into JobAttachments to track the file transfer.
        Returns True if the operation should continue as normal or False to cancel.

        current_action is added by the Worker Agent (via partial)
        progress and status message are passed in by Job Attachments."""
        return True
        # TODO: Since moving to the Open Job Description callback, asset sync no longer blocks
        # the next action. Therefore we can end up in a situation where
        # this callback attempts to re-open a completed session action
        # and/or attempts to complete a session action in the wrong order.

        # status = ActionStatus(
        #     state=ActionState.RUNNING,
        #     progress=float(progress_report.progress),
        #     status_message=progress_report.progressMessage,
        # )
        # self._action_update_callback(SessionActionStatus(
        #     id=current_action.definition.id,
        #     start_time=current_action.start_time,
        #     status=status,
        #     update_time=datetime.now(tz=timezone.utc),
        # ))

    def sync_asset_inputs(
        self,
        *,
        cancel: Event,
        job_attachment_details: JobAttachmentDetails | None = None,
        step_dependencies: list[str] | None = None,
    ) -> None:
        """Sync the inputs on session start using Job Attachments"""
        if self._asset_sync is None:
            return

        def progress_handler(job_upload_status: ProgressReportMetadata) -> bool:
            self.update_action(
                action_status=ActionStatus(
                    state=ActionState.RUNNING,
                    status_message=job_upload_status.progressMessage,
                    progress=job_upload_status.progress,
                ),
            )
            return not cancel.is_set()

        if not (job_attachment_settings := self._job_details.job_attachment_settings):
            raise RuntimeError("Job attachment settings were not contained in JOB_DETAILS entity")

        if job_attachment_details:
            self._job_attachment_details = job_attachment_details

        # Validate that job attachment details have been provided before syncing step dependencies.
        if self._job_attachment_details is None:
            raise RuntimeError(
                "Job attachments must be synchronized before downloading Step dependencies."
            )

        assert job_attachment_settings.s3_bucket_name is not None
        assert job_attachment_settings.root_prefix is not None

        s3_settings = JobAttachmentS3Settings(
            s3BucketName=job_attachment_settings.s3_bucket_name,
            rootPrefix=job_attachment_settings.root_prefix,
        )

        manifest_properties_list: list[ManifestProperties] = []
        if not step_dependencies:
            for manifest_properties in self._job_attachment_details.manifests:
                manifest_properties_list.append(
                    ManifestProperties(
                        rootPath=manifest_properties.root_path,
                        fileSystemLocationName=manifest_properties.file_system_location_name,
                        rootPathFormat=PathFormat(manifest_properties.root_path_format),
                        inputManifestPath=manifest_properties.input_manifest_path,
                        inputManifestHash=manifest_properties.input_manifest_hash,
                        outputRelativeDirectories=manifest_properties.output_relative_directories,
                    )
                )

        attachments = Attachments(
            manifests=manifest_properties_list,
            fileSystem=self._job_attachment_details.job_attachments_file_system,
        )

        storage_profiles_path_mapping_rules_dict: dict[str, str] = {
            str(rule.source_path): str(rule.destination_path)
            for rule in self._job_details.path_mapping_rules
        }

        fs_permission_settings: Optional[FileSystemPermissionSettings] = None
        if self._os_user is not None:
            if os.name == "posix":
                if not isinstance(self._os_user, PosixSessionUser):
                    raise ValueError(f"The user must be a posix-user. Got {type(self._os_user)}")
                fs_permission_settings = PosixFileSystemPermissionSettings(
                    os_user=self._os_user.user,
                    os_group=self._os_user.group,
                    dir_mode=0o20,
                    file_mode=0o20,
                )
            else:
                # TODO: Support Windows file system permission settings
                raise NotImplementedError(
                    "File system permission settings for non-posix systems are not currently supported."
                )

        # Add path mapping rules for root paths in job attachments
        ASSET_SYNC_LOGGER.info("Syncing inputs using Job Attachments")
        download_summary_statistics: SummaryStatistics
        path_mapping_rules: List[Dict[str, str]]
        (download_summary_statistics, path_mapping_rules) = self._asset_sync.sync_inputs(
            s3_settings=s3_settings,
            attachments=attachments,
            queue_id=self._queue_id,  # only used for error message
            job_id=self._queue._job_id,  # only used for error message
            session_dir=self._session.working_directory,
            fs_permission_settings=fs_permission_settings,  # type: ignore[arg-type]
            storage_profiles_path_mapping_rules=storage_profiles_path_mapping_rules_dict,
            step_dependencies=step_dependencies,
            on_downloading_files=progress_handler,
            os_env_vars=self._env,
        )

        ASSET_SYNC_LOGGER.info(
            f"Summary Statistics for file downloads:\n{download_summary_statistics}"
        )

        # Send the summary stats of input syncing through the telemetry client.
        record_sync_inputs_telemetry_event(self._queue_id, download_summary_statistics)

        job_attachment_path_mappings = [
            PathMappingRule.from_dict(rule) for rule in path_mapping_rules
        ]

        # Open Job Description session implementation details -- path mappings are sorted.
        # bisect.insort only supports the 'key' arg in 3.10 or later, so
        # we first extend the list and sort it afterwards.
        if self._session._path_mapping_rules:
            self._session._path_mapping_rules.extend(job_attachment_path_mappings)
        else:
            self._session._path_mapping_rules = job_attachment_path_mappings

        # Open Job Description Sessions sort the path mapping rules based on length of the parts make
        # rules that are subsets of each other behave in a predictable manner. We must
        # sort here since we're modifying that internal list appending to the list.
        self._session._path_mapping_rules.sort(key=lambda rule: -len(rule.source_path.parts))

    def update_action(self, action_status: ActionStatus) -> None:
        """Callback called on every Open Job Description status/progress update and the completion/exit of the
        current action.

        This is a thin wrapper of Session._action_updated_impl that acquires the
        _current_action_lock. It exists to more easily separate the tests of the semantics (this
        method) from the business logic (Session._action_updated_impl).

        Parameters
        ----------
        action_status : deadline_lib_open_job_io.processing.ActionStatus
            The status of the action that has updated/completed
        """

        now = datetime.now(tz=timezone.utc)

        with (
            # NOTE: Lock acquisition order is important. Must be:
            #     1.  action update lock (scheduler owned)
            #     2.  current action lock
            self._action_update_lock,
            self._current_action_lock,
        ):
            self._action_updated_impl(action_status=action_status, now=now)

    def _action_updated_impl(
        self,
        *,
        action_status: ActionStatus,
        now: datetime,
    ) -> None:
        """Internal implementation for the callback invoked on every Open Job Description status/progress
        update and the completion/exit of the current action. The caller should acquire the
        Session._current_action_lock before calling this method.

        The method:

        1.  Forwards action status/progress updates back to the service
        2.  Reacts to Open Job Description action completions.

            In the case of a successful task run, output job attachments are uploaded as a
            post-processing step.

            If the action failed, we mark any pending actions as FAILED or NEVER_ATTEMPTED depending
            on type of action that failed:

                -   If the current action is an ENV_ENTER or SYNC_INPUT_JOB_ATTACHMENTS action,
                    pending actions are marked as FAILED.
                -   Otherwise the pending actions are marked as NEVER_ATTEMPTED.

            Finally, we forward the action completion to the SessionActionQueue for the next
            UpdateWorkerSchedule API request.

        Parameters
        ----------
        action_status : deadline_lib_open_job_io.processing.ActionStatus
            The status of the action that has updated/completed
        now : datetime
            The time the action was updated
        """

        # avoid circular import
        from .actions import RunStepTaskAction

        # There is special-case handling when the current action was interrupted. In such cases, the
        # interruption is reported immediately, so we should not report any Open Job Description action updates
        # to the scheduler regardless of the result. We only need to reset internal state attributes
        # when the Open Job Description action completes and then return early.
        if self._interrupted:
            if action_status.state != ActionState.RUNNING:
                self._current_action = None
                self._interrupted = False
            return

        current_action = self._current_action
        if current_action is None:
            assert self._stop.is_set(), "current_action is None or stopping"
            return

        is_unsuccessful = action_status.state in (ActionState.FAILED, ActionState.CANCELED)

        if (
            action_status.state == ActionState.SUCCESS
            and isinstance(current_action.definition, RunStepTaskAction)
            and self._asset_sync is not None
        ):
            # Synchronizing job output attachments is currently bundled together with the
            # RunStepTaskAction. The synchronization happens after the task run succeeds,
            # and both must be successful in order to mark the action as SUCCEEDED.
            future = self._executor.submit(
                self._sync_asset_outputs,
                current_action=current_action,
            )
            on_done_with_sync_asset_outputs = partial(
                self._on_done_with_sync_asset_outputs,
                is_unsuccessful=is_unsuccessful,
                action_status=action_status,
                current_action=current_action,
            )
            future.add_done_callback(on_done_with_sync_asset_outputs)

        else:
            self._handle_action_update(is_unsuccessful, action_status, current_action, now)

    def _on_done_with_sync_asset_outputs(
        self,
        future: Future[None],
        is_unsuccessful: bool,
        action_status: ActionStatus,
        current_action: CurrentAction,
    ):
        try:
            future.result()
        except Exception as e:
            # Log and fail the task run action if we are unable to sync output job attachments
            fail_message = f"Failed to sync job output attachments for {current_action.definition.human_readable()}: {e}"
            self.logger.warning(fail_message)
            action_status = ActionStatus(state=ActionState.FAILED, fail_message=fail_message)
            is_unsuccessful = True
        finally:
            # The time when the action is completed should be the moment when
            # the synchronization have been finished.
            now = datetime.now(tz=timezone.utc)
            with (
                # NOTE: Acquire the locks here to ensure thread-safe access during action update.
                # Lock acquisition order is important. Must be:
                #     1.  action update lock (scheduler owned)
                #     2.  current action lock
                self._action_update_lock,
                self._current_action_lock,
            ):
                self._handle_action_update(is_unsuccessful, action_status, current_action, now)

    def _handle_action_update(
        self,
        is_unsuccessful: bool,
        action_status: ActionStatus,
        current_action: CurrentAction,
        now: datetime,
    ):
        if is_unsuccessful:
            fail_message = (
                action_status.fail_message
                or f"Action {current_action.definition.human_readable()} failed"
            )

            # If the current action failed, we mark future actions assigned to the session as
            # NEVER_ATTEMPTED except for envExit actions.
            self._queue.cancel_all(
                cancel_outcome="NEVER_ATTEMPTED",
                message=fail_message,
                ignore_env_exits=True,
            )

        if action_status.state != ActionState.RUNNING:
            # This must come before calling Session._report_action_update() because the handler
            # needs to be able to determine if the Session is idle and make an immediate
            # UpdateWorkerSchedule request if so.
            self._current_action = None

        completed_status = OPENJD_ACTION_STATE_TO_DEADLINE_COMPLETED_STATUS.get(
            action_status.state, None
        )
        self._report_action_update(
            SessionActionStatus(
                id=current_action.definition.id,
                status=action_status,
                start_time=current_action.start_time,
                end_time=now if action_status.state != ActionState.RUNNING else None,
                update_time=now if action_status.state == ActionState.RUNNING else None,
                completed_status=completed_status,
            )
        )
        if completed_status:
            logger.info(
                "[%s] [%s] (%s): Action completed as %s",
                self.id,
                current_action.definition.id,
                current_action.definition.human_readable(),
                completed_status,
            )

    def _sync_asset_outputs(
        self,
        *,
        current_action: CurrentAction,
    ) -> None:
        """Sync the outputs after a TASK_RUN if using Job Attachments"""
        if not (queue_settings := self._job_details.job_attachment_settings):
            return
        if not (job_attachment_details := self._job_attachment_details):
            return
        if self._asset_sync is None:
            # Shouldn't get here, but let's be defensive.
            return

        # assist type check
        assert queue_settings.root_prefix is not None

        # Turn worker agent job attachment settings into job attachment settings
        s3_settings = JobAttachmentS3Settings(
            s3BucketName=queue_settings.s3_bucket_name,
            rootPrefix=queue_settings.root_prefix,
        )

        manifest_properties_list: list[ManifestProperties] = []
        for manifest_properties in job_attachment_details.manifests:
            manifest_properties_list.append(
                ManifestProperties(
                    rootPath=manifest_properties.root_path,
                    fileSystemLocationName=manifest_properties.file_system_location_name,
                    rootPathFormat=PathFormat(manifest_properties.root_path_format),
                    inputManifestPath=manifest_properties.input_manifest_path,
                    inputManifestHash=manifest_properties.input_manifest_hash,
                    outputRelativeDirectories=manifest_properties.output_relative_directories,
                )
            )

        attachments = Attachments(
            manifests=manifest_properties_list,
            fileSystem=job_attachment_details.job_attachments_file_system,
        )

        storage_profiles_path_mapping_rules_dict: dict[str, str] = {
            str(rule.source_path): str(rule.destination_path)
            for rule in self._job_details.path_mapping_rules
        }

        ASSET_SYNC_LOGGER.info("Started syncing outputs using Job Attachments")
        # avoid circular import
        from .actions import RunStepTaskAction

        assert isinstance(current_action.definition, RunStepTaskAction)
        upload_summary_statistics: SummaryStatistics = self._asset_sync.sync_outputs(
            s3_settings=s3_settings,
            attachments=attachments,
            queue_id=self._queue_id,
            job_id=self._queue._job_id,
            step_id=current_action.definition.step_id,
            task_id=current_action.definition.task_id,
            session_action_id=current_action.definition.id,
            start_time=current_action.start_time.timestamp(),
            session_dir=self._session.working_directory,
            storage_profiles_path_mapping_rules=storage_profiles_path_mapping_rules_dict,
            on_uploading_files=partial(self._notifier_callback, current_action),
        )

        ASSET_SYNC_LOGGER.info(f"Summary Statistics for file uploads:\n{upload_summary_statistics}")

        # Send the summary stats of output syncing through the telemetry client.
        record_sync_outputs_telemetry_event(self._queue_id, upload_summary_statistics)

        ASSET_SYNC_LOGGER.info("Finished syncing outputs using Job Attachments")

    def run_task(
        self,
        *,
        step_script: StepScriptModel,
        task_parameter_values: list[Parameter],
    ) -> None:
        self._session.run_task(
            step_script=step_script,
            task_parameter_values=task_parameter_values,
        )

    def stop(
        self,
        *,
        current_action_result: Literal["INTERRUPTED", "FAILED"] = "FAILED",
        grace_time: Optional[timedelta] = None,
        fail_message: str | None = None,
    ) -> None:
        """Synchronously stops the session

        If there is an active action, the action is cancelled. If there are any active environments,
        the environments are deactivated

        Parameters
        ----------
        current_action_result : Literal["INTERRUPTED", "FAILED"]
            An optional result to report for an actively running action (if any) when sending the
            action completion in the UpdateWorkerSchedule request. If not specified, this defaults to
            FAILED.
        fail_message : str
            An optional display message associated with the result of the interrupted and skipped
            session actions.
        grace_time : Optional[timedelta]
            If specified, then the session stop is aborted after the maximum duration has elapsed.
            The active action will be forcibly terminated, but any environments that are still
            active will remain so.
        """

        self._stop_current_action_result = current_action_result
        self._stop_grace_time = grace_time
        self._stop_fail_message = fail_message

        # Tell the session thread to stop
        self._stop.set()

    @property
    def idle(self) -> bool:
        """Returns whether the session is idle or has running/queued actions.

        Returns
        -------
        bool
            True if the session has a running action or any queued action(s), False otherwise
        """
        with self._current_action_lock:
            return not self._current_action and self._queue.is_empty()

    def __enter__(
        self,
    ) -> Session:
        return self

    def __exit__(
        self,
        type: TypeVar,
        value: Any,
        traceback: TracebackType,
    ) -> None:
        self.stop()
