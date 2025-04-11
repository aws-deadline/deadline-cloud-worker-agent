# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

import sys
from enum import Enum
import logging
import json
from typing import Any, Optional, Union, TYPE_CHECKING
from types import MethodType
from pathlib import Path
from getpass import getuser

from ._version import __version__
from openjd.model import version as openjd_model_version
from openjd.sessions import version as openjd_sessions_version
from openjd.sessions import LogContent
from openjd.sessions import LOG as openjd_logger
from deadline.job_attachments import version as deadline_job_attach_version

if TYPE_CHECKING:
    from .scheduler.scheduler import SessionMap

# ========================
#  Generic types of log messages


class BaseLogEvent:
    ti: Optional[str] = None
    type: Optional[str] = None
    subtype: Optional[str] = None
    exc_text: Optional[str] = None

    def desc(self) -> str:
        dd = BaseLogEvent.asdict(self)
        if not dd:
            return ""
        # We always have at least type; ti & subtype is optional
        fmt_parts = list[str]()
        if self.ti is not None:
            fmt_parts.append("%(ti)s ")
        fmt_parts.append("%(type)s")
        if self.subtype is not None:
            fmt_parts.append(".%(subtype)s")
        if self.ti is not None:
            fmt_parts.append(" %(ti)s")
        fmt_str = "%s " % "".join(fmt_parts)
        return fmt_str % dd

    def asdict(self) -> dict[str, Any]:
        return {
            k: getattr(self, k) for k in ("ti", "type", "subtype") if getattr(self, k) is not None
        }

    def add_exception_to_dict(self, d: dict[str, Any]) -> dict[str, Any]:
        if self.exc_text:
            d.update(exception=self.exc_text)
        return d

    def add_exception_to_message(self, message: str) -> str:
        if self.exc_text:
            return "%s\n%s" % (message, self.exc_text)
        return message


class StringLogEvent(BaseLogEvent):
    """A log message translated from one of:
    logger.info()
    logger.warn()
    logger.error()
    logger.critical()
    """

    msg: str

    def __init__(self, message: str) -> None:
        self.msg = message

    def getMessage(self) -> str:
        return self.add_exception_to_message(self.msg)

    def asdict(self) -> dict[str, Any]:
        dd = {"message": self.msg}
        return self.add_exception_to_dict(dd)


class AgentInfoLogEvent(BaseLogEvent):
    type = "AgentInfo"

    def __init__(self) -> None:
        pass

    def getMessage(self) -> str:
        info = self.asdict()
        return self.add_exception_to_message(
            (
                "\n"
                f"Python Interpreter: {info['python']['interpreter']}\n"
                f"Python Version: {info['python']['version']}\n"
                f"Platform: {info['platform']}\n"
                f"Agent Version: {info['agent']['version']}\n"
                f"Installed at: {info['agent']['installedAt']}\n"
                f"Running as user: {info['agent']['runningAs']}\n"
                "Dependency versions installed:\n"
                + "\n".join(f"\t{k}: {v}" for k, v in info["dependencies"].items())
            )
        )

    def asdict(self) -> dict[str, Any]:
        dd = super().asdict()
        try:
            user = getuser()
        except Exception:
            # This is best-effort. If we cannot determine the user we will not log
            user = "UNKNOWN"
        dd.update(
            **{
                "platform": sys.platform,
                "python": {
                    "interpreter": sys.executable,
                    "version": sys.version.replace("\n", " - "),
                },
                "agent": {
                    "version": __version__,
                    "installedAt": str(Path(__file__).resolve().parent.parent),
                    "runningAs": user,
                },
                "dependencies": {
                    "openjd.model": openjd_model_version,
                    "openjd.sessions": openjd_sessions_version,
                    "deadline.job_attachments": deadline_job_attach_version,
                },
            }
        )
        return self.add_exception_to_dict(dd)


class MetricsLogEventSubtype(str, Enum):
    SYSTEM = "System"


class MetricsLogEvent(BaseLogEvent):
    ti = "📊"
    type = "Metrics"
    metrics: dict[str, str]

    def __init__(self, *, subtype: MetricsLogEventSubtype, metrics: dict[str, str]) -> None:
        self.subtype = subtype.value
        self.metrics = metrics

    def getMessage(self) -> str:
        return self.add_exception_to_message(
            " ".join("%s %s" % (k, v) for k, v in self.metrics.items())
        )

    def asdict(self) -> dict[str, Any]:
        dd = super().asdict()
        dd.update(**self.metrics)
        return self.add_exception_to_dict(dd)


class WorkerLogEventOp(str, Enum):
    CREATE = "Create"
    LOAD = "Load"
    ID = "ID"  # The ID that the Agent is running as
    STATUS = "Status"
    DELETE = "Delete"
    HOST_CONFIGURATION = "HostConfiguration"


class WorkerLogEvent(BaseLogEvent):
    ti = "💻"
    type = "Worker"

    def __init__(
        self,
        *,
        op: WorkerLogEventOp,
        farm_id: str,
        fleet_id: str,
        worker_id: Optional[str] = None,
        message: str,
    ) -> None:
        self.subtype = op.value
        self.farm_id = farm_id
        self.fleet_id = fleet_id
        self.worker_id = worker_id
        self.msg = message

    def getMessage(self) -> str:
        if self.worker_id:
            s = "%s [%s/%s/%s]" % (self.msg, self.farm_id, self.fleet_id, self.worker_id)
        else:
            s = "%s [%s/%s]" % (self.msg, self.farm_id, self.fleet_id)
        return self.add_exception_to_message(s)

    def asdict(self) -> dict[str, Any]:
        dd = super().asdict()
        dd.update(message=self.msg)
        dd.update(farm_id=self.farm_id, fleet_id=self.fleet_id)
        if self.worker_id:
            dd.update(worker_id=self.worker_id)
        return self.add_exception_to_dict(dd)


class WorkerHostConfigurationStatus(str, Enum):
    RUNNING = "Running"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    SKIPPED = "Skipped"


class WorkerHostConfigurationLogEvent(WorkerLogEvent):
    ti = "📜"
    type = "Worker"
    status: WorkerHostConfigurationStatus
    exit_code: Optional[int]
    success: Optional[bool]

    def __init__(
        self,
        *,
        farm_id: str,
        fleet_id: str,
        message: str,
        status: WorkerHostConfigurationStatus,
        worker_id: Optional[str] = None,
        exit_code: Optional[int] = None,
        success: Optional[bool] = None,
    ) -> None:
        self.exit_code = exit_code
        self.success = success
        self.status = status

        super().__init__(
            op=WorkerLogEventOp.HOST_CONFIGURATION,
            farm_id=farm_id,
            fleet_id=fleet_id,
            worker_id=worker_id,
            message=message,
        )

    def asdict(self) -> dict[str, str]:
        dd = super().asdict()
        dd.update(status=self.status)
        if self.exit_code:
            dd.update(exit_code=self.exit_code)
        if self.success:
            dd.update(success=self.success)
        return self.add_exception_to_dict(dd)


class FilesystemLogEventOp(str, Enum):
    READ = "Read"
    WRITE = "Write"
    CREATE = "Create"
    DELETE = "Delete"


class FilesystemLogEvent(BaseLogEvent):
    ti = "💾"
    type = "FileSystem"
    msg: str
    fmt = "%(message)s [%(filepath)s]"

    def __init__(self, *, op: FilesystemLogEventOp, filepath: str | Path, message: str) -> None:
        self.subtype = op.value
        self.filepath = str(filepath)
        self.msg = message

    def getMessage(self) -> str:
        s = self.fmt % {"filepath": self.filepath, "message": self.msg}
        return self.add_exception_to_message(s)

    def asdict(self) -> dict[str, str]:
        dd = super().asdict()
        dd.update(message=self.msg, filepath=self.filepath)
        return self.add_exception_to_dict(dd)


class AwsCredentialsLogEventOp(str, Enum):
    LOAD = "Load"
    QUERY = "Query"
    INSTALL = "Install"
    DELETE = "Delete"
    REFRESH = "Refresh"
    EXPIRED = "Expired"


class AwsCredentialsLogEvent(BaseLogEvent):
    """For messages related to AWS & OS Credentials."""

    ti = "🔑"
    type = "AWSCreds"
    resource: str
    msg: str
    role_arn: Optional[str] = None  # For Queue credentials
    expiry: Optional[str] = None  # For Query & refresh
    scheduled_time: Optional[str] = None  # For Refresh

    def __init__(
        self,
        *,
        op: AwsCredentialsLogEventOp,
        resource: str,
        role_arn: Optional[str] = None,
        message: str,
        expiry: Optional[str] = None,
        scheduled_time: Optional[str] = None,
    ) -> None:
        self.subtype = op.value
        self.resource = resource
        self.role_arn = role_arn
        self.msg = message
        self.expiry = expiry
        self.scheduled_time = scheduled_time

    def getMessage(self) -> str:
        dd = self.asdict()
        fmt_str_bits = [r"%(message)s"]
        if self.expiry:
            fmt_str_bits.append(r" (Expires: %(expiry)s)")
        if self.scheduled_time:
            fmt_str_bits.append(r" (ScheduledTime: %(scheduled_time)s)")
        fmt_str_bits.append(r" [%(resource)s]")
        if self.role_arn:
            fmt_str_bits.append(r"[%(role_arn)s]")
        fmt_str = "".join(fmt_str_bits)
        return self.add_exception_to_message(fmt_str % dd)

    def asdict(self) -> dict[str, str]:
        dd = super().asdict()
        dd.update(message=self.msg)
        if self.expiry:
            dd.update(expiry=self.expiry)
        if self.scheduled_time:
            dd.update(scheduled_time=self.scheduled_time)
        dd.update(resource=self.resource)
        if self.role_arn:
            dd.update(role_arn=self.role_arn)
        return self.add_exception_to_dict(dd)


class ApiRequestLogEvent(BaseLogEvent):
    ti = "📤"
    type = "API"
    subtype = "Req"

    operation: str
    request_url: str
    params: Union[dict[str, Any], str]
    deadline_resource: Optional[dict[str, str]] = None

    def __init__(
        self,
        *,
        operation: str,
        request_url: str,
        params: Union[dict[str, Any], str],
        deadline_resource: Optional[dict[str, str]] = None,
    ) -> None:
        self.operation = operation
        self.request_url = request_url
        self.params = params
        self.deadline_resource = deadline_resource

    def getMessage(self) -> str:
        dd = self.asdict()
        if "resource" in dd:
            fmt_str = "[%(operation)s] resource=%(resource)s params=%(params)s request_url=%(request_url)s"
        else:
            fmt_str = "[%(operation)s] params=%(params)s request_url=%(request_url)s"
        return self.add_exception_to_message(fmt_str % dd)

    def asdict(self) -> dict[str, Any]:
        dd = super().asdict()
        dd.update(
            **{
                "operation": self.operation,
                "params": self.params,
                "request_url": self.request_url,
            }
        )
        if self.deadline_resource:
            dd["resource"] = self.deadline_resource
        return self.add_exception_to_dict(dd)


class ApiResponseLogEvent(BaseLogEvent):
    ti = "📥"
    type = "API"
    subtype = "Resp"

    operation: str
    status_code: str
    request_id: str
    params: Union[dict[str, Any], str]
    # We might have an error, but we always have parameters.
    error: Optional[dict[str, str]] = None

    def __init__(
        self,
        *,
        operation: str,
        params: Union[dict[str, Any], str],
        status_code: str,
        request_id: str,
        error: Optional[dict[str, str]] = None,
    ) -> None:
        self.operation = operation
        self.params = params
        self.error = error
        self.status_code = status_code
        self.request_id = request_id

    def getMessage(self) -> str:
        dd = self.asdict()
        if "error" in dd:
            fmt_str = "[%(operation)s](%(status_code)s) error=%(error)s params=%(params)s request_id=%(request_id)s"
        else:
            fmt_str = "[%(operation)s](%(status_code)s) params=%(params)s request_id=%(request_id)s"
        return self.add_exception_to_message(fmt_str % dd)

    def asdict(self) -> dict[str, Any]:
        dd = super().asdict()
        dd.update(operation=self.operation, status_code=self.status_code)
        if self.error:
            dd.update(error=self.error)
        dd.update(params=self.params, request_id=self.request_id)
        return self.add_exception_to_dict(dd)


class SessionLogEventSubtype(str, Enum):
    STARTING = "Starting"
    FAILED = "Failed"  # Failed to start the session
    AWSCREDS = "AWSCreds"
    USER = "User"  # User that the session is running as
    ADD = "Add"  # Adding actions
    REMOVE = "Remove"  # Removing actions (cancel/interrupt/never_attempted)
    COMPLETE = "Complete"
    INFO = "Info"  # Generic information about the session
    LOGS = "Logs"  # Info on where the logs are going
    RUNTIME = "Runtime"  # Runtime logs from the openjd.sessions module applicable to the worker log


class SessionLogEvent(BaseLogEvent):
    ti = "🔷"
    type = "Session"
    queue_id: str
    job_id: str
    session_id: str
    user: Optional[str]
    action_ids: Optional[list[str]]  # for Add/Cancel
    log_dest: Optional[str]
    queued_action_count: Optional[int]

    def __init__(
        self,
        *,
        subtype: SessionLogEventSubtype,
        queue_id: str,
        job_id: str,
        session_id: str,
        user: Optional[str] = None,
        message: str,
        action_ids: Optional[list[str]] = None,
        log_dest: Optional[str] = None,
        queued_action_count: Optional[int] = None,
    ) -> None:
        self.subtype = subtype.value
        self.session_id = session_id
        self.queue_id = queue_id
        self.job_id = job_id
        self.user = user
        self.msg = message
        self.action_ids = action_ids
        self.log_dest = log_dest
        self.queued_action_count = queued_action_count

    def getMessage(self) -> str:
        dd = self.asdict()
        if self.subtype == SessionLogEventSubtype.USER.value and self.user is not None:
            fmt_str = "[%(session_id)s] %(message)s (User: %(user)s) [%(queue_id)s/%(job_id)s]"
        elif self.subtype in (
            SessionLogEventSubtype.ADD.value,
            SessionLogEventSubtype.REMOVE.value,
        ):
            fmt_str = "[%(session_id)s] %(message)s (ActionIds: %(action_ids)s) (QueuedActionCount: %(queued_action_count)s) [%(queue_id)s/%(job_id)s]"
        elif self.subtype == SessionLogEventSubtype.LOGS.value and self.log_dest is not None:
            fmt_str = "[%(session_id)s] %(message)s (LogDestination: %(log_dest)s) [%(queue_id)s/%(job_id)s]"
        else:
            fmt_str = "[%(session_id)s] %(message)s [%(queue_id)s/%(job_id)s]"

        return self.add_exception_to_message(fmt_str % dd)

    def asdict(self) -> dict[str, Any]:
        dd = super().asdict()
        dd.update(
            session_id=self.session_id,
            message=self.msg,
        )
        if self.subtype == SessionLogEventSubtype.USER.value:
            dd.update(user=self.user)
        if self.action_ids is not None:
            dd.update(action_ids=self.action_ids)
        if self.queued_action_count is not None:
            dd.update(queued_action_count=self.queued_action_count)
        if self.log_dest is not None:
            dd.update(log_dest=self.log_dest)
        dd.update(queue_id=self.queue_id, job_id=self.job_id)
        return self.add_exception_to_dict(dd)


class SessionActionLogEventSubtype(str, Enum):
    START = "Start"
    CANCEL = "Cancel"
    INTERRUPT = "Interrupt"
    END = "End"  # will have a status key


class SessionActionLogKind(str, Enum):
    ENV_ENTER = "EnvEnter"
    ENV_EXIT = "EnvExit"
    TASK_RUN = "TaskRun"
    JA_SYNC_INPUT = "JobAttachSyncInput"
    JA_SYNC_OUTPUT = "JobAttachSyncOutput"
    JA_DEP_SYNC = "JobAttachSyncDeps"


class SessionActionLogEvent(BaseLogEvent):
    type = "Action"

    queue_id: str
    job_id: str
    step_id: Optional[str]
    task_id: Optional[str]
    session_id: str
    kind: SessionActionLogKind
    action_id: str
    status: Optional[str]
    msg: str

    def __init__(
        self,
        *,
        subtype: SessionActionLogEventSubtype,
        queue_id: str,
        job_id: str,
        step_id: Optional[str] = None,
        task_id: Optional[str] = None,
        session_id: str,
        action_log_kind: SessionActionLogKind,
        action_id: str,
        message: str,
        status: Optional[str] = None,
    ) -> None:
        if subtype in (SessionActionLogEventSubtype.START,):
            self.ti = "🟢"
        elif subtype in (
            SessionActionLogEventSubtype.CANCEL,
            SessionActionLogEventSubtype.INTERRUPT,
        ):
            self.ti = "🟨"
        else:
            self.ti = "🟣"
        self.subtype = subtype.value
        self.session_id = session_id
        self.kind = action_log_kind
        self.queue_id = queue_id
        self.job_id = job_id
        self.step_id = step_id
        self.task_id = task_id
        self.action_id = action_id
        self.msg = message
        self.status = status

    def getMessage(self) -> str:
        dd = self.asdict()
        if self.step_id is None:
            resource_id = "[%(queue_id)s/%(job_id)s]"
        elif self.task_id is None:
            resource_id = "[%(queue_id)s/%(job_id)s/%(step_id)s]"
        else:
            resource_id = "[%(queue_id)s/%(job_id)s/%(step_id)s/%(task_id)s]"
        if self.subtype == SessionActionLogEventSubtype.END.value and self.status is not None:
            fmt_str = (
                "[%(session_id)s](%(action_id)s) %(message)s (Status: %(status)s) (Kind: %(kind)s) "
                + resource_id
            )
        else:
            fmt_str = "[%(session_id)s](%(action_id)s) %(message)s (Kind: %(kind)s) " + resource_id
        return self.add_exception_to_message(fmt_str % dd)

    def asdict(self) -> dict[str, Any]:
        dd = super().asdict()
        dd.update(
            session_id=self.session_id,
            action_id=self.action_id,
            kind=self.kind.value,
            message=self.msg,
        )
        if self.subtype == SessionActionLogEventSubtype.END.value and self.status is not None:
            dd.update(status=self.status)
        dd.update(queue_id=self.queue_id, job_id=self.job_id)
        if self.step_id is not None:
            dd.update(step_id=self.step_id)
        if self.task_id is not None:
            dd.update(task_id=self.task_id)
        return self.add_exception_to_dict(dd)


# ===========================


class LogRecordStringTranslationFilter(logging.Filter):
    """A log filter that translates LogRecords generated by
    logger.<level>(<string>,  ...) style logger calls into one where
    the logged string is encapsulated within a LogMessage-derived class
    instance.

    Notes:
    - The filter is *modifying* the LogRecord. That means that it applies to
      every log handler that will be handling the log message after the translation.
      Python 3.12 allows for localized translation by having the filter() method
      return a modified log record, but that is not currently available to us since
      we must support Python 3.9+.
    """

    formatter = logging.Formatter()
    openjd_worker_log_content = (
        LogContent.EXCEPTION_INFO | LogContent.PROCESS_CONTROL | LogContent.HOST_INFO
    )
    _session_map: "SessionMap" | None = None

    @property
    def session_map(self) -> Optional["SessionMap"]:
        if self._session_map is None:
            from .scheduler.scheduler import SessionMap

            self._session_map = SessionMap.get_session_map()
        return self._session_map

    def _is_from_openjd(self, record: logging.LogRecord) -> bool:
        """Returns True if the record is from openjd.sessions"""
        return record.name == openjd_logger.name and isinstance(record.msg, str)

    def _is_openjd_message_to_log(self, record: logging.LogRecord) -> bool:
        """
        Return True if the record is from openjd.sessions and has content that should be logged in the worker logs.
        """
        if not self._is_from_openjd(record):
            return False
        if not hasattr(record, "openjd_log_content") or not isinstance(
            record.openjd_log_content, LogContent
        ):
            # Message from openjd.sessions does not have the openjd_log_content property, so we
            # do not know what content the message contains. Do not log.
            return False
        elif record.openjd_log_content not in self.openjd_worker_log_content:
            # Message contains content that does not belong in the worker logs. Do not log.
            return False
        else:
            return True

    def _replace_openjd_log_message(self, record: logging.LogRecord) -> None:
        """
        Best effort replaces the .msg attribute of a LogRecord from openjd.sessions with a SessionLogEvent.
        If the record does not have a session_id attribute, then the .msg attribute is not replaced.
        """
        if not hasattr(record, "session_id") or not isinstance(record.session_id, str):
            # This should never happen. If somehow it does, just fall back to a StringLogEvent.
            record.msg += " The Worker Agent could not determine the session ID of this log originating from OpenJD. Please report this to the service team."
            return

        session_id = record.session_id
        queue_id = None
        job_id = None

        if self.session_map is not None and session_id in self.session_map:
            scheduler_session = self.session_map[session_id]
            queue_id = scheduler_session.session._queue_id
            job_id = scheduler_session.session._job_id
            record.msg = SessionLogEvent(
                subtype=SessionLogEventSubtype.RUNTIME,
                queue_id=queue_id,
                job_id=job_id,
                session_id=session_id,
                message=record.getMessage(),
                user=None,  # User is only used for SessionLogEventSubtype.USER
            )
        else:
            # This can happen at the very beginning of a session. Fall back to a StringLogEvent.
            return
        record.getMessageReplaced = True
        record.getMessage = MethodType(lambda self: self.msg.getMessage(), record)  # type: ignore

    def filter(self, record: logging.LogRecord) -> bool:
        """Translate plain string log messages into a LogMessage instance
        based on the loglevel of the record.
        Log records don't have a str typed msg pass-through as-is.
        """
        if self._is_from_openjd(record):
            if self._is_openjd_message_to_log(record):
                # Message is from openjd.sessions and only contains content we intend to log in the worker logs.
                self._replace_openjd_log_message(record)
            else:
                return False

        if isinstance(record.msg, str):
            message = record.getMessage()
            record.msg = StringLogEvent(message)
            # We must replace record.getMessage() so that a string is returned and not the LogEvent type.
            # getMessageReplaced is used to indicate we already have done so, to avoid replacing twice.
            record.getMessageReplaced = True
            record.getMessage = MethodType(lambda self: self.msg.getMessage(), record)  # type: ignore
            record.args = None
        elif isinstance(record.msg, BaseLogEvent) and not hasattr(record, "getMessageReplaced"):
            record.getMessageReplaced = True
            record.getMessage = MethodType(lambda self: self.msg.getMessage(), record)  # type: ignore

        if record.exc_info and record.exc_text is None:
            record.msg.exc_text = self.formatter.formatException(record.exc_info)
            record.exc_info = None

        if not hasattr(record, "json"):
            # Order is important here; we want 'level' to be the first thing
            # when printing the dictionary as a string.
            structure = {
                "level": record.levelname,
            }
            if isinstance(record.msg, BaseLogEvent):
                structure.update(**record.msg.asdict())
            else:
                structure.update(msg=record.getMessage())
            record.json = json.dumps(structure, ensure_ascii=False)

        if not hasattr(record, "desc"):
            if isinstance(record.msg, BaseLogEvent):
                record.desc = record.msg.desc()
            else:
                record.desc = ""
        return True
