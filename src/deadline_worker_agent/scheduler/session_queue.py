# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from logging import getLogger
from threading import Event
from typing import Any, Callable, Iterable, Generic, Literal, TypeVar, TYPE_CHECKING, cast

from openjd.model import UnsupportedSchema
from openjd.sessions import ActionState, ActionStatus

from ..feature_flag import ASSET_SYNC_JOB_USER_FEATURE
from ..api_models import (
    EnvironmentAction as EnvironmentActionApiModel,
    SyncInputJobAttachmentsAction as SyncInputJobAttachmentsActionApiModel,
    AttachmentDownloadAction as AttachmentDownloadActionApiModel,
    AttachmentUploadAction as AttachmentUploadActionApiModel,
    TaskRunAction as TaskRunActionApiModel,
    EntityIdentifier,
    EnvironmentDetailsIdentifier,
    EnvironmentDetailsIdentifierFields,
    JobAttachmentDetailsIdentifier,
    JobAttachmentDetailsIdentifierFields,
    StepDetailsIdentifier,
    StepDetailsIdentifierFields,
)
from ..sessions.actions import (
    EnterEnvironmentAction,
    ExitEnvironmentAction,
    RunStepTaskAction,
    SessionActionDefinition,
    SyncInputJobAttachmentsAction,
    AttachmentUploadAction,
    AttachmentDownloadAction,
)
from .session_action_status import SessionActionStatus
from ..sessions.errors import (
    EnvironmentDetailsError,
    JobAttachmentDetailsError,
    JobEntityUnsupportedSchemaError,
    StepDetailsError,
)
from ..sessions.job_entities.job_details import parameters_from_api_response
from ..log_messages import SessionLogEvent, SessionLogEventSubtype, SessionActionLogKind

if TYPE_CHECKING:
    from ..sessions.job_entities import JobEntities

    D = TypeVar(
        "D",
        EnvironmentActionApiModel,
        TaskRunActionApiModel,
        SyncInputJobAttachmentsActionApiModel,
        AttachmentDownloadActionApiModel,
        AttachmentUploadActionApiModel,
    )
else:
    D = TypeVar("D")
    JobAttachmentDetails = Any
    EnvironmentDetails = Any
    StepDetails = Any


logger = getLogger(__name__)


@dataclass(frozen=True)
class SessionActionQueueEntry(Generic[D]):
    cancel: Event
    """An event used to cancel the session action"""

    definition: D
    """The action as received from UpdateWorkerSchedule"""


EnvironmentQueueEntry = SessionActionQueueEntry[EnvironmentActionApiModel]
TaskRunQueueEntry = SessionActionQueueEntry[TaskRunActionApiModel]
SyncInputJobAttachmentsQueueEntry = SessionActionQueueEntry[SyncInputJobAttachmentsActionApiModel]
SyncInputJobAttachmentsStepDependenciesQueueEntry = SessionActionQueueEntry[
    SyncInputJobAttachmentsActionApiModel
]
AttachmentUploadActionQueueEntry = SessionActionQueueEntry[AttachmentUploadActionApiModel]
AttachmentDownloadActionQueueEntry = SessionActionQueueEntry[AttachmentDownloadActionApiModel]
AttachmentDownloadActionStepDependenciesQueueEntry = SessionActionQueueEntry[
    AttachmentDownloadActionApiModel
]
CancelOutcome = Literal["FAILED", "NEVER_ATTEMPTED"]


class SessionActionQueue:
    """A queue of actions for a session

    Parameters
    ----------
    action_update_callback: Callable[[SessionActionStatus], None]
        A callback function which is called every time an action status is updated. The final call
        will be the terminal status of the action.
    job_entities: deadline_worker_agent.sessions.JobEntities:
        JobEntities instance responsible for fetching job entities.
    """

    _actions: list[
        EnvironmentQueueEntry
        | TaskRunQueueEntry
        | SyncInputJobAttachmentsQueueEntry
        | SyncInputJobAttachmentsStepDependenciesQueueEntry
        | AttachmentUploadActionQueueEntry
        | AttachmentDownloadActionQueueEntry
        | AttachmentDownloadActionStepDependenciesQueueEntry
    ]
    _actions_by_id: dict[
        str,
        EnvironmentQueueEntry
        | TaskRunQueueEntry
        | SyncInputJobAttachmentsQueueEntry
        | SyncInputJobAttachmentsStepDependenciesQueueEntry
        | AttachmentUploadActionQueueEntry
        | AttachmentDownloadActionQueueEntry
        | AttachmentDownloadActionStepDependenciesQueueEntry,
    ]
    _action_update_callback: Callable[[SessionActionStatus], None]
    _job_entities: JobEntities
    _queue_id: str
    _job_id: str
    _session_id: str

    def __init__(
        self,
        *,
        queue_id: str,
        job_id: str,
        session_id: str,
        job_entities: JobEntities,
        action_update_callback: Callable[[SessionActionStatus], None],
    ) -> None:
        self._action_update_callback = action_update_callback
        self._actions_by_id = {}
        self._actions = []
        self._job_entities = job_entities
        self._queue_id = queue_id
        self._job_id = job_id
        self._session_id = session_id

    def is_empty(self) -> bool:
        """Returns whether the queue is empty

        Returns
        -------
        bool
            True if the action queue is empty, False otherwise"""
        return len(self._actions) == 0

    def list_all_action_identifiers(self) -> list[EntityIdentifier]:
        """Used for warming the job entities cache"""
        all_action_identifiers: list[EntityIdentifier] = []
        for action in self._actions:
            identifier: EntityIdentifier
            action_definition = action.definition
            action_type = action_definition["actionType"]

            if action_type.startswith("ENV_"):
                action_definition = cast(EnvironmentActionApiModel, action_definition)
                identifier = EnvironmentDetailsIdentifier(
                    environmentDetails=EnvironmentDetailsIdentifierFields(
                        jobId=self._job_id,
                        environmentId=action_definition["environmentId"],
                    ),
                )
            elif action_type == "TASK_RUN":
                action_definition = cast(TaskRunActionApiModel, action_definition)
                identifier = StepDetailsIdentifier(
                    stepDetails=StepDetailsIdentifierFields(
                        jobId=self._job_id,
                        stepId=action_definition["stepId"],
                    ),
                )
            elif action_type == "SYNC_INPUT_JOB_ATTACHMENTS":
                if ASSET_SYNC_JOB_USER_FEATURE:
                    action_definition = cast(AttachmentDownloadActionApiModel, action_definition)
                else:
                    action_definition = cast(
                        SyncInputJobAttachmentsActionApiModel, action_definition
                    )

                if "stepId" in action_definition:
                    identifier = StepDetailsIdentifier(
                        stepDetails=StepDetailsIdentifierFields(
                            jobId=self._job_id,
                            stepId=action_definition["stepId"],
                        ),
                    )
                else:
                    identifier = JobAttachmentDetailsIdentifier(
                        jobAttachmentDetails=JobAttachmentDetailsIdentifierFields(
                            jobId=self._job_id,
                        ),
                    )
            else:
                logger.critical(f"Unknown action type in the session action queue: {action_type}")
                continue

            all_action_identifiers.append(identifier)

        return all_action_identifiers

    def _cancel(
        self,
        *,
        id: str,
        message: str | None = None,
        cancel_outcome: CancelOutcome = "NEVER_ATTEMPTED",
    ) -> None:
        """Cancels a queued or running action.

        Parameters
        ----------
        id : str
            The identifier of the action to be canceled
        message : str | None
            An optional message to include explaining why this action was canceled
        cancel_outcome : Literal["NEVER_ATTEMPTED", "FAILED"]
            Whether to fail the action or mark it as never attempted
        """
        action: SessionActionQueueEntry
        action = self._actions_by_id.pop(id)

        self._actions.remove(action)
        action.cancel.set()

        # We provide start/end timestamps iff cancel_outcome is FAILED
        timestamp = datetime.now(tz=timezone.utc) if cancel_outcome == "FAILED" else None

        self._action_update_callback(
            SessionActionStatus(
                id=id,
                completed_status=cancel_outcome,
                start_time=timestamp,
                end_time=timestamp,
                # TODO: This is semantically incorrect, but status.state is a required field. We
                # only need this to communicate the message. In the future, we may want to remove
                # the "status" field from Open Job Description here and hoist the fields we care about up to the
                # SessionActionStatus class.
                status=ActionStatus(
                    state=ActionState.FAILED,
                    fail_message=message,
                ),
            )
        )

    def cancel_all(
        self,
        *,
        message: str | None = None,
        ignore_env_exits: bool = True,
    ) -> None:
        """Cancels all queued actions

        Parameters
        ----------
        message : str | None
            An optional message to include explaining why this action was canceled
        ignore_env_exits : bool
            If True, ENV_EXIT actions will not be canceled. Defaults to canceling ENV_EXIT actions.
        """

        action_ids = [
            action.definition["sessionActionId"]
            for action in self._actions
            # Conditionally ignore env exits
            if not (ignore_env_exits and action.definition["actionType"] == "ENV_EXIT")
        ]

        for action_id in action_ids:
            # Ignore ids that are missing; cause would likely be a data race.
            if action_id in self._actions_by_id:
                self._cancel(
                    id=action_id,
                    message=message,
                    cancel_outcome="NEVER_ATTEMPTED",
                )
        if action_ids:
            logger.info(
                SessionLogEvent(
                    subtype=SessionLogEventSubtype.REMOVE,
                    queue_id=self._queue_id,
                    job_id=self._job_id,
                    session_id=self._session_id,
                    action_ids=action_ids,
                    queued_action_count=len(self._actions),
                    message="Removed SessionActions.",
                )
            )

    def insert_front(
        self,
        *,
        action: AttachmentUploadActionApiModel,
    ) -> None:
        """Inserts an attachment upload action at the front of the queue

        Parameters
        ----------
        action : AttachmentUploadActionApiModel
            The attachment upload action to be inserted to the front of queue
        """
        action_type = action["actionType"]
        action_id = action["sessionActionId"]
        cancel_event = Event()

        action = cast(AttachmentUploadActionApiModel, action)
        queue_entry = AttachmentUploadActionQueueEntry(
            cancel=cancel_event,
            definition=action,
        )

        self._actions.insert(0, queue_entry)
        self._actions_by_id[action_id] = queue_entry
        logger.debug("Successfully inserted front of queue: %s action: %s", action_type, action_id)

    def replace(
        self,
        *,
        actions: Iterable[
            EnvironmentActionApiModel
            | TaskRunActionApiModel
            | SyncInputJobAttachmentsActionApiModel
            | AttachmentDownloadActionApiModel
            | AttachmentUploadActionApiModel
        ],
    ) -> None:
        """Update the queue's actions"""
        queue_entries: list[
            TaskRunQueueEntry
            | EnvironmentQueueEntry
            | SyncInputJobAttachmentsQueueEntry
            | SyncInputJobAttachmentsStepDependenciesQueueEntry
            | AttachmentDownloadActionQueueEntry
            | AttachmentDownloadActionStepDependenciesQueueEntry
            | AttachmentUploadActionQueueEntry
        ] = []

        action_ids_added = list[str]()

        for action in actions:
            action_type = action["actionType"]
            action_id = action["sessionActionId"]
            logger.debug("Processing action: %s", action_id)
            cancel_event = Event()

            if (queue_entry := self._actions_by_id.get(action_id, None)) is None:
                if action_type.startswith("ENV_"):
                    action = cast(EnvironmentActionApiModel, action)
                    queue_entry = EnvironmentQueueEntry(
                        cancel=cancel_event,
                        definition=action,
                    )
                elif action_type == "TASK_RUN":
                    action = cast(TaskRunActionApiModel, action)
                    queue_entry = TaskRunQueueEntry(
                        cancel=cancel_event,
                        definition=action,
                    )
                elif action_type == "SYNC_INPUT_JOB_ATTACHMENTS":
                    action = cast(AttachmentDownloadActionApiModel, action)
                    if ASSET_SYNC_JOB_USER_FEATURE:
                        action = cast(AttachmentDownloadActionApiModel, action)
                        if "stepId" not in action:
                            queue_entry = AttachmentDownloadActionQueueEntry(
                                cancel=cancel_event,
                                definition=action,
                            )
                        else:
                            queue_entry = AttachmentDownloadActionStepDependenciesQueueEntry(
                                cancel=cancel_event,
                                definition=action,
                            )
                    else:
                        action = cast(SyncInputJobAttachmentsActionApiModel, action)
                        if "stepId" not in action:
                            queue_entry = SyncInputJobAttachmentsQueueEntry(
                                cancel=cancel_event,
                                definition=action,
                            )
                        else:
                            queue_entry = SyncInputJobAttachmentsStepDependenciesQueueEntry(
                                cancel=cancel_event,
                                definition=action,
                            )
                else:
                    raise NotImplementedError(f"Unknown action type '{action_type}'")
                self._actions_by_id[action_id] = queue_entry
                action_ids_added.append(action_id)
            else:
                logger.debug("Action %s already queued", action_id)
            queue_entries.append(queue_entry)

        self._actions = queue_entries

        if action_ids_added:
            logger.info(
                SessionLogEvent(
                    subtype=SessionLogEventSubtype.ADD,
                    queue_id=self._queue_id,
                    job_id=self._job_id,
                    session_id=self._session_id,
                    action_ids=action_ids_added,
                    queued_action_count=len(self._actions),
                    message="Appended new SessionActions.",
                )
            )

    def dequeue(self) -> SessionActionDefinition | None:
        """Removes and returns an action from the front of the queue.

        Raises
        ------
            JobEntityUnsupportedSchemaError:
                When the details for an OpenjdAction have a schema that the Worker Agent
                does not support. Allows the action to gracefully report the failure
                to the service.

            EnvironmentDetailsError
            JobAttachmentDetailsError
            StepDetailsError
                These detail errors all subclasses of SessionActionError to
                capture the action id so we can fail job entity errors gracefully

        Returns
        -------
        SessionActionDefinition | None
            The next action to be run in the session (if any). If no actions are pending,
            then None is returned.
        """

        next_action: SessionActionDefinition | None = None
        if len(self._actions) > 0:
            action_queue_entry = self._actions[0]
            action_type = action_queue_entry.definition["actionType"]
            action_definition = action_queue_entry.definition
            action_id = action_definition["sessionActionId"]
            if action_type.startswith("ENV_"):
                action_queue_entry = cast(EnvironmentQueueEntry, action_queue_entry)
                action_definition = action_queue_entry.definition
                environment_id = action_definition["environmentId"]
                try:
                    environment_details = self._job_entities.environment_details(
                        environment_id=environment_id
                    )
                except UnsupportedSchema as e:
                    if action_type == "ENV_ENTER":
                        raise JobEntityUnsupportedSchemaError(
                            action_id, SessionActionLogKind.ENV_ENTER, e._version
                        )
                    else:
                        raise JobEntityUnsupportedSchemaError(
                            action_id, SessionActionLogKind.ENV_EXIT, e._version
                        )
                except (ValueError, RuntimeError) as e:
                    if action_type == "ENV_ENTER":
                        raise EnvironmentDetailsError(
                            action_id, SessionActionLogKind.ENV_ENTER, str(e)
                        ) from e
                    else:
                        raise EnvironmentDetailsError(
                            action_id, SessionActionLogKind.ENV_EXIT, str(e)
                        ) from e
                if action_type == "ENV_ENTER":
                    next_action = EnterEnvironmentAction(
                        id=action_id,
                        job_env_id=environment_id,
                        details=environment_details,
                    )
                elif action_type == "ENV_EXIT":
                    next_action = ExitEnvironmentAction(
                        id=action_id,
                        environment_id=environment_id,
                    )
                else:
                    raise ValueError(f'Unknown action type "{action_type}".')
            elif action_type == "TASK_RUN":
                action_queue_entry = cast(TaskRunQueueEntry, action_queue_entry)
                action_definition = action_queue_entry.definition
                step_id = action_definition["stepId"]
                task_id = action_definition["taskId"]
                try:
                    step_details = self._job_entities.step_details(step_id=step_id)
                except UnsupportedSchema as e:
                    raise JobEntityUnsupportedSchemaError(
                        action_id,
                        SessionActionLogKind.TASK_RUN,
                        e._version,
                        step_id=step_id,
                        task_id=task_id,
                    ) from e
                except (ValueError, RuntimeError) as e:
                    raise StepDetailsError(
                        action_id,
                        SessionActionLogKind.TASK_RUN,
                        str(e),
                        step_id=step_id,
                        task_id=task_id,
                    ) from e
                task_parameters_data: dict = action_definition.get("parameters", {})
                task_parameters = parameters_from_api_response(task_parameters_data)

                next_action = RunStepTaskAction(
                    id=action_id,
                    details=step_details,
                    task_parameter_values=task_parameters,
                    task_id=action_definition["taskId"],
                )
            elif action_type == "SYNC_OUTPUT_JOB_ATTACHMENTS":
                action_queue_entry = cast(AttachmentUploadActionQueueEntry, action_queue_entry)
                action_definition = action_queue_entry.definition
                step_id = action_definition["stepId"]
                task_id = action_definition["taskId"]

                next_action = AttachmentUploadAction(
                    id=action_id,
                    session_id=self._session_id,
                    step_id=step_id,
                    task_id=task_id,
                )

            elif action_type == "SYNC_INPUT_JOB_ATTACHMENTS":
                action_definition = action_queue_entry.definition
                if ASSET_SYNC_JOB_USER_FEATURE:
                    action_definition = cast(AttachmentDownloadActionApiModel, action_definition)
                    if "stepId" not in action_definition:
                        action_queue_entry = cast(
                            AttachmentDownloadActionQueueEntry, action_queue_entry
                        )
                        try:
                            job_attachment_details = self._job_entities.job_attachment_details()
                        except UnsupportedSchema as e:
                            raise JobEntityUnsupportedSchemaError(
                                action_id, SessionActionLogKind.JA_SYNC_INPUT, e._version
                            ) from e
                        except ValueError as e:
                            raise JobAttachmentDetailsError(
                                action_id, SessionActionLogKind.JA_SYNC_INPUT, str(e)
                            ) from e
                        next_action = AttachmentDownloadAction(
                            id=action_id,
                            session_id=self._session_id,
                            job_attachment_details=job_attachment_details,
                        )
                    else:
                        action_queue_entry = cast(
                            AttachmentDownloadActionStepDependenciesQueueEntry, action_queue_entry
                        )

                        try:
                            step_details = self._job_entities.step_details(
                                step_id=action_definition["stepId"],
                            )
                        except UnsupportedSchema as e:
                            raise JobEntityUnsupportedSchemaError(
                                action_id,
                                SessionActionLogKind.JA_DEP_SYNC,
                                e._version,
                                step_id=action_definition["stepId"],
                            ) from e
                        except ValueError as e:
                            raise StepDetailsError(
                                action_id,
                                SessionActionLogKind.JA_DEP_SYNC,
                                str(e),
                                step_id=action_definition["stepId"],
                            ) from e
                        next_action = AttachmentDownloadAction(
                            id=action_id,
                            session_id=self._session_id,
                            step_details=step_details,
                        )

                else:
                    action_definition = cast(
                        SyncInputJobAttachmentsActionApiModel, action_definition
                    )
                    if "stepId" not in action_definition:
                        action_queue_entry = cast(
                            SyncInputJobAttachmentsQueueEntry, action_queue_entry
                        )
                        try:
                            job_attachment_details = self._job_entities.job_attachment_details()
                        except UnsupportedSchema as e:
                            raise JobEntityUnsupportedSchemaError(
                                action_id, SessionActionLogKind.JA_SYNC_INPUT, e._version
                            ) from e
                        except ValueError as e:
                            raise JobAttachmentDetailsError(
                                action_id, SessionActionLogKind.JA_SYNC_INPUT, str(e)
                            ) from e
                        next_action = SyncInputJobAttachmentsAction(
                            id=action_id,
                            session_id=self._session_id,
                            job_attachment_details=job_attachment_details,
                        )
                    else:
                        action_queue_entry = cast(
                            SyncInputJobAttachmentsStepDependenciesQueueEntry, action_queue_entry
                        )

                        try:
                            step_details = self._job_entities.step_details(
                                step_id=action_definition["stepId"],
                            )
                        except UnsupportedSchema as e:
                            raise JobEntityUnsupportedSchemaError(
                                action_id,
                                SessionActionLogKind.JA_DEP_SYNC,
                                e._version,
                                step_id=action_definition["stepId"],
                            ) from e
                        except ValueError as e:
                            raise StepDetailsError(
                                action_id,
                                SessionActionLogKind.JA_DEP_SYNC,
                                str(e),
                                step_id=action_definition["stepId"],
                            ) from e
                        next_action = SyncInputJobAttachmentsAction(
                            id=action_id,
                            session_id=self._session_id,
                            step_details=step_details,
                        )
            else:
                raise ValueError(
                    f'Unknown action type "{action_type}". Complete action = {action_definition}'
                )
            del self._actions[0]
            del self._actions_by_id[action_id]
        return next_action
