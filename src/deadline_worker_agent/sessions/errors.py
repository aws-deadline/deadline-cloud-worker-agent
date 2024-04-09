# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from typing import Optional

from .._version import version

from ..log_messages import SessionActionLogKind


class CancelationError(Exception):
    """Raised when there was an error trying to cancel a session action"""

    pass


class SessionActionError(Exception):
    """Captures the action_id of an action that failed"""

    action_id: str
    action_log_kind: SessionActionLogKind
    step_id: Optional[str]
    task_id: Optional[str]
    message: str

    def __init__(
        self,
        action_id: str,
        action_log_kind: SessionActionLogKind,
        message: str,
        *,
        step_id: Optional[str] = None,
        task_id: Optional[str] = None,
    ):
        super().__init__()
        self.action_id = action_id
        self.action_log_kind = action_log_kind
        self.message = message

    def __str__(self) -> str:
        return self.message


class EnvironmentDetailsError(SessionActionError):
    """Raised when environment_details fails in an unrecoverable way"""

    pass


class JobAttachmentDetailsError(SessionActionError):
    """Raised when job_attachments_details fails in an unrecoverable way"""

    pass


class StepDetailsError(SessionActionError):
    """Raised when step_details fails in an unrecoverable way"""

    pass


class JobEntityUnsupportedSchemaError(SessionActionError):
    """Raised when the worker agent does support the schema version
    of a job entity that turns into a SessionAction"""

    schema_version: str

    def __init__(
        self,
        action_id: str,
        action_log_kind: SessionActionLogKind,
        schema_version: str,
        *,
        step_id: Optional[str] = None,
        task_id: Optional[str] = None,
    ):
        self.schema_version = schema_version
        self.message = (
            f"Worker Agent: {version} does not support Open Job Description Schema Version {self.schema_version}. "
            f"Consider upgrading to a newer Worker Agent."
        )
        super().__init__(
            action_id=action_id,
            action_log_kind=action_log_kind,
            message=self.message,
            step_id=step_id,
            task_id=task_id,
        )
