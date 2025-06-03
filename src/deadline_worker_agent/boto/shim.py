# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Module for mock boto3 deadline implementation"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Optional, TYPE_CHECKING, Union
from uuid import uuid4

from boto3 import Session as _Session

from ..feature_flag import ASSET_SYNC_JOB_USER_FEATURE
from ..api_models import (
    AssignedSession,
    AssumeFleetRoleForWorkerResponse,
    AssumeQueueRoleForWorkerResponse,
    BatchGetJobEntityResponse,
    CreateWorkerResponse,
    EnvironmentAction,
    HostProperties,
    SyncInputJobAttachmentsAction,
    AttachmentDownloadAction,
    TaskRunAction,
    UpdatedSessionActionInfo,
    UpdateWorkerResponse,
    UpdateWorkerScheduleResponse,
    WorkerStatus,
)
from .logger import logger

if TYPE_CHECKING:
    from .api_models import EntityIdentifier


class DeadlineClient:
    """
    A shim layer for boto deadline client. This class will check if a method exists on the real
    boto3 deadline client and call it if it exists. If it doesn't exist, mocked values are returned
    to simulate it.
    """

    _real_client: Any

    def __init__(self, real_client: Any):
        self._real_client = real_client

    def create_worker(
        self,
        farmId: str,
        fleetId: str,
        hostProperties: HostProperties | None = None,
    ) -> CreateWorkerResponse:
        """Create a Worker and get a worker ID to be used to identify it."""
        create_worker_method: Optional[Callable]

        create_worker_method = getattr(self._real_client, "create_worker", None)

        if create_worker_method:
            logger.debug("using boto implementation of deadline create_worker")

            # Creating the metadata from the instance id and instance type.
            create_worker_kwargs: dict[str, Any] = {
                "farmId": farmId,
                "fleetId": fleetId,
            }
            if hostProperties is not None:
                create_worker_kwargs["hostProperties"] = hostProperties

            # mypy complains about they kwargs type
            return create_worker_method(**create_worker_kwargs)  # type: ignore

        else:
            logger.warning(
                "CreateWorker API missing from service model. Testing with hard-coded response values."
            )
            worker_id = str(uuid4()).replace("-", "")
            return {"workerId": f"worker-{worker_id}"}

    def delete_worker(
        self,
        farmId: str,
        fleetId: str,
        workerId: str,
    ) -> dict[str, Any]:
        """De-Register this worker agent for the given worker ID."""

        delete_worker_method: Optional[Callable]

        delete_worker_method = getattr(self._real_client, "delete_worker", None)

        if delete_worker_method:
            logger.debug("using boto implementation of deadline delete_worker")
            return delete_worker_method(farmId=farmId, fleetId=fleetId, workerId=workerId)
        else:
            logger.warning(
                "DeleteWorker API missing from service model. Testing with hard-coded response values."
            )
            logger.debug("using mock implementation of deadline delete_worker")
            return {}

    def update_worker_schedule(
        self,
        *,
        farmId: str,
        fleetId: str,
        workerId: str,
        updatedSessionActions: dict[str, UpdatedSessionActionInfo] | None = None,
    ) -> UpdateWorkerScheduleResponse:
        if hasattr(self._real_client, "update_worker_schedule"):
            return DeadlineClient._parse_update_worker_schedule_response(
                self._real_client.update_worker_schedule(
                    farmId=farmId,
                    fleetId=fleetId,
                    workerId=workerId,
                    updatedSessionActions=updatedSessionActions,
                )
            )
        logger.warning(
            "UpdateWorkerSchedule API missing from service model. Testing with hard-coded response values."
        )
        return UpdateWorkerScheduleResponse(
            assignedSessions={
                "session-ba9b4c82308a4001af405c964388ffea": AssignedSession(
                    sessionActions=[
                        EnvironmentAction(
                            actionType="ENV_ENTER",
                            environmentId="env1",
                            sessionActionId="session-ba9b4c82308a4001af405c964388ffea-1",
                        ),
                        TaskRunAction(
                            actionType="TASK_RUN",
                            sessionActionId="session-ba9b4c82308a4001af405c964388ffea-2",
                            stepId="step-a50bcbf7a86848dabc46480db936b4a7",
                            taskId="step-a50bcbf7a86848dabc46480db936b4a7-1",
                        ),
                        EnvironmentAction(
                            actionType="ENV_EXIT",
                            environmentId="env1",
                            sessionActionId="session-ba9b4c82308a4001af405c964388ffea-3",
                        ),
                    ],
                    jobId="job-21432d89b44a46cbaaeb2f1d5254e548",
                    queueId="queue-f8d5f7eba14c485f9f80ea31cec738bf",
                ),
            },
            cancelSessionActions={},
            updateIntervalSeconds=5,
        )

    # TODO: Remove this once we've changed the API shape everywhere in WA code
    @staticmethod
    def _parse_update_worker_schedule_response(response: dict) -> UpdateWorkerScheduleResponse:
        # Needed to properly parse into NotRequired field for TypedDict
        def parse_task_run_action(action: dict, action_id: str) -> TaskRunAction:
            mapped_action = TaskRunAction(
                sessionActionId=action_id,
                actionType="TASK_RUN",
                taskId=action["taskId"],
                stepId=action["stepId"],
            )
            if parameters := action.get("parameters", None):
                mapped_action["parameters"] = parameters
            return mapped_action

        def parse_sync_input_job_attachments_action(
            action: dict, action_id: str
        ) -> SyncInputJobAttachmentsAction:
            mapped_action = SyncInputJobAttachmentsAction(
                sessionActionId=action_id,
                actionType="SYNC_INPUT_JOB_ATTACHMENTS",
            )
            if step_id := action.get("stepId", None):
                mapped_action["stepId"] = step_id
            return mapped_action

        def parse_attachment_download_action(
            action: dict, action_id: str
        ) -> AttachmentDownloadAction:
            mapped_action = AttachmentDownloadAction(
                sessionActionId=action_id,
                actionType="SYNC_INPUT_JOB_ATTACHMENTS",
            )
            if step_id := action.get("stepId", None):
                mapped_action["stepId"] = step_id
            return mapped_action

        SESSION_ACTION_MAP: dict[
            str,
            Callable[
                [Any, str],
                EnvironmentAction
                | TaskRunAction
                | SyncInputJobAttachmentsAction
                | AttachmentDownloadAction,
            ],
        ] = {
            "envEnter": lambda action, action_id: EnvironmentAction(
                sessionActionId=action_id,
                actionType="ENV_ENTER",
                environmentId=action["environmentId"],
            ),
            "envExit": lambda action, action_id: EnvironmentAction(
                sessionActionId=action_id,
                actionType="ENV_EXIT",
                environmentId=action["environmentId"],
            ),
            "taskRun": parse_task_run_action,
            "syncInputJobAttachments": (
                parse_sync_input_job_attachments_action
                if not ASSET_SYNC_JOB_USER_FEATURE
                else parse_attachment_download_action
            ),
        }

        # Map the new session action structure to our internal model
        mapped_sessions: dict[str, AssignedSession] = {}
        for session_id, session in response["assignedSessions"].items():
            mapped_actions: list[
                EnvironmentAction
                | TaskRunAction
                | SyncInputJobAttachmentsAction
                | AttachmentDownloadAction
            ] = []
            for session_action in session["sessionActions"]:
                assert len(session_action["definition"].items()) == 1
                (definition,) = session_action["definition"].items()
                action_name, action = definition
                assert action_name in SESSION_ACTION_MAP
                mapped_actions.append(
                    SESSION_ACTION_MAP[action_name](action, session_action["sessionActionId"])
                )

            mapped_session = AssignedSession(
                queueId=session["queueId"],
                jobId=session["jobId"],
                sessionActions=mapped_actions,
            )
            if log_configuration := session.get("logConfiguration", None):
                mapped_session["logConfiguration"] = log_configuration

            mapped_sessions[session_id] = mapped_session

        return UpdateWorkerScheduleResponse(
            assignedSessions=mapped_sessions,
            cancelSessionActions=response["cancelSessionActions"],
            desiredWorkerStatus=response.get("desiredWorkerStatus", None),  # type: ignore
            updateIntervalSeconds=response["updateIntervalSeconds"],
        )

    def batch_get_job_entity(
        self,
        farmId: str,
        fleetId: str,
        workerId: str,
        identifiers: list[EntityIdentifier],
    ) -> BatchGetJobEntityResponse:
        if hasattr(self._real_client, "batch_get_job_entity"):
            return self._real_client.batch_get_job_entity(
                farmId=farmId,
                fleetId=fleetId,
                workerId=workerId,
                identifiers=identifiers,
            )

        else:
            logger.warning(
                "BatchGetJobEntity API missing from service model. Testing with hard-coded response values."
            )
            return BatchGetJobEntityResponse(
                entities=[
                    {
                        "jobDetails": {
                            "jobId": "job-21432d89b44a46cbaaeb2f1d5254e548",
                            "schemaVersion": "jobtemplate-2023-09",
                            "jobAttachmentSettings": {
                                "s3BucketName": "asset-bucket",
                                "rootPrefix": "my-queue",
                            },
                            "logGroupName": "/aws/deadline/queue-abc",
                            "jobRunAsUser": {
                                "runAs": "QUEUE_CONFIGURED_USER",
                                "posix": {
                                    "user": "job-user",
                                    "group": "job-group",
                                },
                                "windows": {
                                    "user": "job-user",
                                    "group": "job-group",
                                    "passwordArn": "job-password-arn",
                                },
                            },
                            "pathMappingRules": [
                                {
                                    "sourcePathFormat": "windows",
                                    "sourcePath": "C:/windows/path",
                                    "destinationPath": "/linux/path",
                                },
                            ],
                        },
                    },
                    {
                        "jobAttachmentDetails": {
                            "jobId": "job-21432d89b44a46cbaaeb2f1d5254e548",
                            "attachments": {
                                "manifests": [],
                                "fileSystem": "COPIED",
                            },
                        },
                    },
                    {
                        "stepDetails": {
                            "jobId": "job-abac",
                            "stepId": "step-a50bcbf7a86848dabc46480db936b4a7",
                            "schemaVersion": "jobtemplate-2023-09",
                            "template": {},
                            "dependencies": [],
                        },
                    },
                    {
                        "environmentDetails": {
                            "environmentId": "env1",
                            "jobId": "job-21432d89b44a46cbaaeb2f1d5254e548",
                            "schemaVersion": "jobtemplate-2023-09",
                            "template": {
                                "name": "foo",
                                "script": {},
                                "variables": {},
                            },
                        },
                    },
                ],
                errors=[],
            )

    def get_step(
        self,
        farmId: str,
        queueId: str,
        jobId: str,
        stepId: str,
    ) -> dict[str, Any]:
        """Retrieve a task"""
        if hasattr(self._real_client, "get_step"):
            return self._real_client.get_step(
                farmId=farmId,
                queueId=queueId,
                jobId=jobId,
                stepId=stepId,
                includeStepScript=True,
            )
        else:
            logger.warning(
                "GetStep API missing from service model. Testing with hard-coded response values."
            )
            return {
                "name": "my-task-name",
                "queueId": queueId,
                "stepId": stepId,
                "parameters": {},
                "template": {
                    "version": "2022-05-01",
                    "script": {
                        "attachments": {
                            "hello": {
                                "name": "hello",
                                "type": "TEXT",
                                "runnable": True,
                                "data": '#!/usr/bin/bash\n\necho "hello $@"\n',
                            },
                            "wrapper": {
                                "type": "TEXT",
                                "runnable": True,
                                "data": "\n".join(
                                    [
                                        "#!/usr/bin/bash",
                                        "",
                                        "echo -n wrapper: ",
                                        "{{ Task.Attachment.hello.Path }} $@",
                                        "",
                                    ]
                                ),
                            },
                        },
                        "actions": {
                            "onRun": {
                                "command": "{{ Task.Attachment.wrapper.Path }}",
                                "arguments": [
                                    "{{ Job.Parameter.foo }}",
                                    "{{ Task.Parameter.task_id }}",
                                    "{{ Task.Parameter.frame }}",
                                ],
                            },
                            "onSessionStart": {
                                "command": "{{ Task.Attachment.hello.Path }}",
                                "arguments": ["session start"],
                            },
                            "on_session_end": {"command": "echo", "arguments": ["session end"]},
                            "on_cancel": {"command": "echo", "arguments": ["cancel"]},
                        },
                    },
                },
            }

    def get_job(self, farmId: str, queueId: str, jobId: str) -> dict[str, Any]:
        """Get the specified Job"""
        return self._real_client.get_job(farmId=farmId, queueId=queueId, jobId=jobId)

    def get_queue(self, farmId: str, queueId: str) -> dict[str, Any]:
        """Get the specified Queue"""
        return self._real_client.get_queue(farmId=farmId, queueId=queueId)

    def assume_fleet_role_for_worker(
        self,
        farmId: str,
        fleetId: str,
        workerId: str,
    ) -> AssumeFleetRoleForWorkerResponse:
        if hasattr(self._real_client, "assume_fleet_role_for_worker"):
            return self._real_client.assume_fleet_role_for_worker(
                farmId=farmId,
                fleetId=fleetId,
                workerId=workerId,
            )
        else:
            logger.warning(
                "AssumeFleetRoleForWorker API missing from service model. Testing with hard-coded response values."
            )
            return {
                "credentials": {
                    "accessKeyId": "fake-access-key",
                    "secretAccessKey": "fake-secret-key",
                    "sessionToken": "fake-session-token",
                    "expiration": datetime(year=2020, month=1, day=1),
                }
            }

    def update_worker(
        self,
        farmId: str,
        fleetId: str,
        workerId: str,
        status: Union[str, WorkerStatus],
        capabilities: dict[str, Any] | None = None,
        hostProperties: HostProperties | None = None,
    ) -> UpdateWorkerResponse:
        if hasattr(self._real_client, "update_worker"):
            request: dict[str, Any] = {
                "farmId": farmId,
                "fleetId": fleetId,
                "workerId": workerId,
                "status": status,
            }
            if capabilities is not None:
                request["capabilities"] = capabilities
            if hostProperties is not None:
                request["hostProperties"] = hostProperties
            return self._real_client.update_worker(**request)
        else:
            logger.warning("UpdateWorker API missing from service model.")
            raise NotImplementedError("DeadlineClient.update_worker() not implemented")

    def assume_queue_role_for_worker(
        self, farmId: str, fleetId: str, workerId: str, queueId: str
    ) -> AssumeQueueRoleForWorkerResponse:
        """Get the AWS Credentials from the Queue for this Worker"""
        if hasattr(self._real_client, "assume_queue_role_for_worker"):
            return self._real_client.assume_queue_role_for_worker(
                farmId=farmId, fleetId=fleetId, workerId=workerId, queueId=queueId
            )
        else:
            logger.warning(
                "AssumeQueueRoleForWorker API missing from service model. Testing with hard-coded response values."
            )
            return {
                "credentials": {
                    "accessKeyId": "fake-access-key",
                    "secretAccessKey": "fake-secret-key",
                    "sessionToken": "fake-session-token",
                    "expiration": datetime(year=2020, month=1, day=1),
                }
            }


class Session(_Session):
    """A mock for a boto session"""

    def client(self, service: str, *args: Any, **kwargs: Any) -> DeadlineClient:
        """Returns a real boto3 client for any service other than deadline.
        If 'deadline' is requested, the mocked class is returned."""
        real_client = super(Session, self).client(service, *args, **kwargs)
        if service != "deadline":
            return real_client
        return DeadlineClient(real_client)
