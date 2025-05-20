# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Literal, TypedDict, Union

from typing_extensions import NotRequired

__all__ = [
    "ManifestProperties",
    "AssignedSession",
    "BatchGetJobEntityResponse",
    "EnvironmentAction",
    "EnvironmentDetails",
    "EnvironmentDetailsData",
    "EnvironmentDetailsError",
    "EnvironmentDetailsIdentifier",
    "HostProperties",
    "IntParameter",
    "JobAttachmentDetailsData",
    "JobAttachmentDetailsIdentifier",
    "JobAttachmentQueueSettings",
    "Attachments",
    "JobDetails",
    "JobDetailsData",
    "JobDetailsError",
    "JobDetailsIdentifier",
    "LogConfiguration",
    "PathMappingRule",
    "PathParameter",
    "StepDetails",
    "StepDetailsData",
    "StepDetailsError",
    "StepDetailsIdentifier",
    "StringParameter",
    "TaskRunAction",
    "UpdatedSessionActionInfo",
    "UpdatedSessionActionInfo",
    "UpdateWorkerResponse",
    "UpdateWorkerScheduleRequest",
    "UpdateWorkerScheduleResponse",
]

EnvironmentActionType = Literal["ENV_ENTER", "ENV_EXIT"]
StepActionType = Literal["TASK_RUN"]  # noqa
SyncInputJobAttachmentsActionType = Literal["SYNC_INPUT_JOB_ATTACHMENTS"]  # noqa
AttachmentDownloadActionType = Literal["SYNC_INPUT_JOB_ATTACHMENTS"]  # noqa
AttachmentUploadActionType = Literal["SYNC_OUTPUT_JOB_ATTACHMENTS"]  # noqa
CompletedActionStatus = Literal["SUCCEEDED", "FAILED", "INTERRUPTED", "CANCELED", "NEVER_ATTEMPTED"]


class EnvironmentAction(TypedDict):
    sessionActionId: str
    actionType: EnvironmentActionType
    environmentId: str


class StringParameter(TypedDict):
    string: str


class PathParameter(TypedDict):
    path: str


class IntParameter(TypedDict):
    int: str


class FloatParameter(TypedDict):
    float: str


class ChunkIntParameter(TypedDict):
    chunkInt: str


class TaskRunAction(TypedDict):
    sessionActionId: str
    actionType: StepActionType
    taskId: str
    stepId: str
    parameters: NotRequired[
        dict[
            str, StringParameter | PathParameter | IntParameter | FloatParameter | ChunkIntParameter
        ]
    ]


class SyncInputJobAttachmentsAction(TypedDict):
    sessionActionId: str
    actionType: SyncInputJobAttachmentsActionType
    stepId: NotRequired[str]


class AttachmentDownloadAction(TypedDict):
    sessionActionId: str
    actionType: AttachmentDownloadActionType
    stepId: NotRequired[str]


# This action is not from API, kepping it here for all action models to be in one place
class AttachmentUploadAction(TypedDict):
    sessionActionId: str
    actionType: AttachmentUploadActionType
    stepId: str
    taskId: str


class HostConfiguration(TypedDict):
    scriptBody: str
    """Host Configuration Script Body."""

    scriptTimeoutSeconds: int
    """Customer Supplied timeout."""


class LogConfiguration(TypedDict):
    error: NotRequired[str]
    logDriver: str
    options: NotRequired[dict[str, str]]
    parameters: NotRequired[dict[str, str]]


class AssignedSession(TypedDict):
    queueId: str
    jobId: str
    sessionActions: list[
        EnvironmentAction | TaskRunAction | SyncInputJobAttachmentsAction | AttachmentDownloadAction
    ]
    logConfiguration: NotRequired[LogConfiguration]


class UpdateWorkerScheduleResponse(TypedDict):
    assignedSessions: dict[str, AssignedSession]
    cancelSessionActions: dict[str, list[str]]
    updateIntervalSeconds: int
    desiredWorkerStatus: NotRequired[Literal["STOPPED"]]


class BaseIdentifierFields(TypedDict):
    jobId: str
    """The unique identifier of the job the entity belongs to"""


class BaseEntityErrorFields(TypedDict):
    code: str
    """The machine-readable error code"""

    message: str
    """A human-readable error message"""


class StepDetailsIdentifierFields(BaseIdentifierFields):
    stepId: str
    """The unique identifier for the job's step"""


class StepDetailsIdentifier(TypedDict):
    stepDetails: StepDetailsIdentifierFields


class StepDetailsData(StepDetailsIdentifierFields):
    schemaVersion: str
    """The Open Job Description schema version that corresponds to the template"""

    template: dict[str, Any]
    """The template of the step"""

    dependencies: NotRequired[list[str]]
    """A list of step identifiers that this step depends on"""


class StepDetails(TypedDict):
    stepDetails: StepDetailsData
    """The step details data"""


class StepDetailsErrorFields(StepDetailsIdentifierFields, BaseEntityErrorFields):
    pass


class StepDetailsError(TypedDict):
    stepDetails: StepDetailsErrorFields


class JobDetailsIdentifierFields(BaseIdentifierFields):
    pass


class JobDetailsIdentifier(TypedDict):
    jobDetails: JobDetailsIdentifierFields


class JobDetailsErrorFields(JobDetailsIdentifierFields, BaseEntityErrorFields):
    pass


class JobDetailsError(TypedDict):
    jobDetails: JobDetailsErrorFields


class JobAttachmentQueueSettings(TypedDict):
    """
    Contains the configuration of job attachments for a AWS Deadline Cloud queue. This includes the name of
    the S3 bucket as well as the object key structure. The structure of the objects with respect to
    this structure's fields is illustrated below:

    {s3BucketName}/
        {rootPrefix}/
            Data/
                04dbd85fc3238721a33a164edae56b54     # Data file
                ...
            Manifests/
                {farm_id}/{queue_id}/
                    Inputs/
                        {GUID}/
                            039d8d14949cf461bdaf54bb34b4b2e2_input.xxh128     # Manifest file for input
                    {job_id}/{step_id}/{task-id}/
                        {ISO-Z_string_with_milliseconds_precision}_{session_action_id}
                            3f96fd471e53c2e64337c4800de2881e_output.xxh128     # Manifest file for output
    """

    s3BucketName: str
    """The name of the S3 bucket where job attachments are transferred to/from"""

    rootPrefix: str
    """The prefix for all job attachment object keys"""


class ManifestProperties(TypedDict):
    rootPath: str
    """The path to the root directory where assets reside"""

    fileSystemLocationName: NotRequired[str]
    """The name of the file system location"""

    rootPathFormat: str
    """The operating system family (posix/windows) associated with the asset's rootPath"""

    inputManifestPath: NotRequired[str]
    """A (partial) key path of an S3 object that points to a file manifest.
    It is relative to the location configured in the Queue."""

    inputManifestHash: NotRequired[str]
    """The hash of the manifest, for data provenance"""

    outputRelativeDirectories: list[str]
    """Paths relative to the root path where output will be monitored and uploaded after the
    workload completes"""


class PathMappingRule(TypedDict):
    sourcePathFormat: str
    """The path format associated with the source path (windows vs posix)"""

    sourcePath: str
    """The path we're looking to change"""

    destinationPath: str
    """The path to transform the source path to"""


class PosixUser(TypedDict):
    user: str
    """The posix user name to run session actions as, as well as session file ownership"""

    group: str
    """The posix group name associated with session file ownership"""


class WindowsUser(TypedDict):
    user: str
    """The windows user name to run session actions as, as well as session file ownership"""

    group: NotRequired[str]
    """The windows group name associated with session file ownership"""

    passwordArn: str
    """The ARN of a AWS Secrets Manager secret that the password of the user name to run actions as"""


class JobRunAsUser(TypedDict):
    posix: NotRequired[PosixUser]
    windows: NotRequired[WindowsUser]
    runAs: Literal["QUEUE_CONFIGURED_USER", "WORKER_AGENT_USER"]


class JobDetailsData(JobDetailsIdentifierFields):
    jobAttachmentSettings: NotRequired[JobAttachmentQueueSettings]
    """The queue's job attachment settings"""

    jobRunAsUser: NotRequired[JobRunAsUser]
    """The queue's info on how to run the job processes (ie. posix or windows user/group)"""

    logGroupName: str
    """The name of the CloudWatch Log Group containing the Worker session's Log Stream"""

    schemaVersion: str
    """The Open Job Description job template schema version"""

    parameters: NotRequired[
        dict[
            str,
            StringParameter
            | PathParameter
            | IntParameter
            | FloatParameter
            | ChunkIntParameter
            | str,
        ]
    ]
    """The job parameters"""

    pathMappingRules: NotRequired[list[PathMappingRule]]
    """The path mapping rules from the service (before job attachments rules are added)"""

    queueRoleArn: NotRequired[str]
    """An optional IAM role ARN corresponding used for worker sessions on the job's queue"""


class JobDetails(TypedDict):
    jobDetails: JobDetailsData
    """The job details data"""


class EnvironmentDetailsIdentifierFields(BaseIdentifierFields):
    """The job entity type"""

    environmentId: str


class EnvironmentDetailsIdentifier(TypedDict):
    environmentDetails: EnvironmentDetailsIdentifierFields


class EnvironmentDetailsData(EnvironmentDetailsIdentifierFields):
    schemaVersion: str
    """The Open Job Description schema version"""
    template: dict[str, Any]
    """The template of the environment."""


class EnvironmentDetails(TypedDict):
    environmentDetails: EnvironmentDetailsData
    """The data for the step details"""


class EnvironmentDetailsErrorFields(EnvironmentDetailsIdentifierFields, BaseEntityErrorFields):
    pass


class EnvironmentDetailsError(TypedDict):
    environmentDetails: EnvironmentDetailsErrorFields


class JobAttachmentDetailsIdentifierFields(BaseIdentifierFields):
    stepId: NotRequired[str]
    """An optional step whose input job attachments must be synchronized"""


class JobAttachmentDetailsIdentifier(TypedDict):
    jobAttachmentDetails: JobAttachmentDetailsIdentifierFields


class Attachments(TypedDict):
    manifests: list[ManifestProperties]
    """A list of all manifests and their configuration"""

    fileSystem: NotRequired[str]
    """Method to use when loading assets required for a job"""


class JobAttachmentDetailsData(JobAttachmentDetailsIdentifierFields):
    attachments: Attachments
    """Information of the input assets attached to the job"""


class JobAttachmentDetails(TypedDict):
    jobAttachmentDetails: JobAttachmentDetailsData
    """Information of the input assets attached to the job"""


class JobAttachmentDetailsErrorFields(JobAttachmentDetailsIdentifierFields, BaseEntityErrorFields):
    pass


class JobAttachmentDetailsError(TypedDict):
    jobAttachmentDetails: JobAttachmentDetailsErrorFields


EntityIdentifier = Union[
    EnvironmentDetailsIdentifier,
    JobDetailsIdentifier,
    StepDetailsIdentifier,
    JobAttachmentDetailsIdentifier,
]
EntityDetails = Union[
    EnvironmentDetails,
    JobAttachmentDetails,
    JobDetails,
    StepDetails,
]
EntityError = Union[
    EnvironmentDetailsError,
    JobAttachmentDetailsError,
    JobDetailsError,
    StepDetailsError,
]


class BatchGetJobEntityResponse(TypedDict):
    entities: list[EntityDetails]
    errors: list[EntityError]


class UpdatedSessionActionInfo(TypedDict):
    completedStatus: NotRequired[CompletedActionStatus]
    processExitCode: NotRequired[int]
    progressMessage: NotRequired[str]
    startedAt: NotRequired[datetime]
    endedAt: NotRequired[datetime]
    updatedAt: NotRequired[datetime]
    progressPercent: NotRequired[float]


class UpdateWorkerScheduleRequest(TypedDict):
    farmId: str
    fleetId: str
    workerId: str
    requestedAssignedActions: list[int]
    updatedSessionActions: NotRequired[dict[str, UpdatedSessionActionInfo]]


class UpdateWorkerResponse(TypedDict):
    log: NotRequired[LogConfiguration]
    hostConfiguration: NotRequired[HostConfiguration]


class IpAddresses(TypedDict):
    ipV4Addresses: NotRequired[list[str]]
    ipV6Addresses: NotRequired[list[str]]


class HostProperties(TypedDict):
    hostName: NotRequired[str]
    ipAddresses: NotRequired[IpAddresses]


class AwsCredentials(TypedDict):
    accessKeyId: str
    secretAccessKey: str
    sessionToken: str
    expiration: datetime


class AssumeFleetRoleForWorkerResponse(TypedDict):
    credentials: AwsCredentials


class AssumeQueueRoleForWorkerResponse(TypedDict):
    credentials: AwsCredentials


class CreateWorkerResponse(TypedDict):
    workerId: str


class WorkerStatus(str, Enum):
    """AWS Deadline Cloud Worker states"""

    STARTED = "STARTED"
    """The Worker is online and ready to begin work"""

    STOPPED = "STOPPED"
    """The Worker is offline"""

    STOPPING = "STOPPING"
    """The Worker has initiated a drain operation."""
