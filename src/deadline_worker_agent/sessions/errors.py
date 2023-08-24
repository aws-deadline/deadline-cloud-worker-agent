# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from .._version import version


class CancelationError(Exception):
    """Raised when there was an error trying to cancel a session action"""

    pass


class SessionActionError(Exception):
    """Captures the action_id of an action that failed"""

    action_id: str
    message: str

    def __init__(self, action_id: str, message: str):
        super().__init__()
        self.action_id = action_id
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

    def __init__(self, action_id: str, schema_version: str):
        self.schema_version = schema_version
        self.message = (
            f"Worker Agent: {version} does not support OpenJobIO Schema Version {self.schema_version}. "
            f"Consider upgrading to a newer Worker Agent."
        )
        super().__init__(action_id=action_id, message=self.message)
