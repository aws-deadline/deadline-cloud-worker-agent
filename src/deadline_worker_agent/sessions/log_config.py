# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from contextlib import closing, contextmanager, nullcontext
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from pathlib import Path
from typing import ContextManager, Generator
import logging

from ..log_sync.cloudwatch import (
    LOG_CONFIG_OPTION_GROUP_NAME_KEY,
    LOG_CONFIG_OPTION_STREAM_NAME_KEY,
)
from ..api_models import LogConfiguration as BotoSessionLogConfiguration
from ..boto import (
    Session as BotoSession,
    OTHER_BOTOCORE_CONFIG,
)
from ..log_sync.cloudwatch import CloudWatchHandler
from ..log_messages import SessionLogEvent, SessionLogEventSubtype


logger = logging.getLogger(__name__)


# The logging format string used to format session logs being written to the local file-system
SESSION_LOCAL_LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"


class LogDriver(Enum):
    """A log driver represents a destination type for logs. Each LogDriver has independent logic
    for synchronizing logs to the corresponding destination based on its options and parameters."""

    AWSLOGS = "awslogs"


ERROR_KEY = "error"
LOG_DRIVER_FMT_STRINGS: dict[LogDriver, str] = {LogDriver.AWSLOGS: "%(message)s"}


class LogProvisioningError(Exception):
    """Exception raised when there was an error encountered provisioning logs

    Parameters
    ----------
    msg : str
        The error message provided by the service explaining the log provisioning failure
    """

    message: str

    def __init__(self, *, message: str) -> None:
        super().__init__()
        self.message = message

    def __str__(self) -> str:
        return f"Log provisioning error: {self.message}"


@dataclass
class SessionLogConfigurationParameters:
    """The session's log configuration parameters

    Parameters
    ----------
    interval : datetime.timedelta
        The frequency that logs should be flushed to their destination.
    """

    interval: timedelta = timedelta(seconds=30)

    @classmethod
    def from_boto(
        cls,
        parameters: dict[str, str],
    ) -> SessionLogConfigurationParameters:
        interval = parameters.get("interval", "30")
        if not isinstance(interval, str):
            raise TypeError(f'Expected str for "interval" but got {type(interval)}')
        try:
            interval_int = int(interval)
        except ValueError:
            raise ValueError(f'Expected integer value for "interval" parameter, got {interval}')

        if interval_int <= 0:
            raise ValueError(
                f'Expected positive value for "interval" parameter, got {interval_int}'
            )

        interval_delta = timedelta(seconds=interval_int)

        return SessionLogConfigurationParameters(interval=interval_delta)


class SessionLogFilter(logging.Filter):
    _session_id: str

    def __init__(
        self,
        name: str = "",
        *,
        session_id: str,
    ) -> None:
        super(SessionLogFilter, self).__init__(name)
        self._session_id = session_id

    def filter(self, record: logging.LogRecord) -> bool:
        return (
            not (record_session_id := getattr(record, "session_id", None))
            or record_session_id == self._session_id
        )


@dataclass
class LogConfiguration:
    """The session's log configuration

    Parameters
    ----------
    loggers : list[logging.Logger]
        Loggers whose events will be streamed to the session log
    options : dict[str, str]
        A set of key/value string pairs that configure the log driver for the session.
    parameters : SessionLogConfigurationParameters
        A set of parameters that can be modified throughout the session. For example, the interval
        that logs are flushed can be adjusted dynamically for a session to balance the
        responsiveness/interactiveness of reading logs with costs and API request throughput.
    log_driver: LogDriver
        The log driver for the session.
    """

    loggers: list[logging.Logger]
    options: dict[str, str]
    session_log_file: Path | None
    parameters: SessionLogConfigurationParameters = field(compare=False)
    log_driver: LogDriver = LogDriver.AWSLOGS
    log_provisioning_error: LogProvisioningError | None = None

    @classmethod
    def from_boto(
        cls,
        *,
        loggers: list[logging.Logger],
        log_configuration: BotoSessionLogConfiguration,
        session_log_file: Path | None,
    ) -> LogConfiguration:
        """
        Parameters
        ----------
        loggers : list[logging.Logger]
            Loggers whose events will be streamed to the session log
        log_configuration : BotoSessionLogConfiguration
            The log configuration as returned for a session in the UpdateWorkerSchedule response
        session_log_file : Path
            Path to the log file for the session

        Returns
        -------
        LogConfiguration
            The LogConfiguration parsed from the UpdateWorkerSchedule "logConfiguration" format

        Raises
        ------
        LogProvisioningError
            Raised if the log configuration contains an error message
        """
        # Check for error first
        if log_provision_error_msg := log_configuration.get("error", None):
            raise LogProvisioningError(message=log_provision_error_msg)

        # Parse the log driver
        try:
            log_driver = LogDriver(log_configuration["logDriver"])
        except ValueError:
            raise ValueError(
                f'Unsupported log driver: "{log_configuration["logDriver"]}"'
            ) from None

        return LogConfiguration(
            loggers=loggers,
            log_driver=log_driver,
            options=log_configuration["options"].copy(),
            parameters=SessionLogConfigurationParameters.from_boto(log_configuration["parameters"]),
            session_log_file=session_log_file,
        )

    def create_remote_handler(
        self,
        *,
        # TODO: figure out a better architecture to generalize this
        boto_session: BotoSession,
    ) -> logging.Handler:
        """Creates a log handler for the session"""
        if self.log_driver != LogDriver.AWSLOGS:
            raise NotImplementedError(f'log driver "{self.log_driver}" not supported')

        if not (log_group := self.options.get(LOG_CONFIG_OPTION_GROUP_NAME_KEY, None)):
            raise KeyError(f'No "{LOG_CONFIG_OPTION_GROUP_NAME_KEY}" in logConfiguration.options')
        elif not (log_stream := self.options.get(LOG_CONFIG_OPTION_STREAM_NAME_KEY, None)):
            raise KeyError(f'No "{LOG_CONFIG_OPTION_STREAM_NAME_KEY}" in logConfiguration.options')

        return CloudWatchHandler(
            log_group_name=log_group,
            log_stream_name=log_stream,
            logs_client=boto_session.client("logs", config=OTHER_BOTOCORE_CONFIG),
        )

    def create_local_file_handler(self) -> logging.FileHandler:
        assert self.session_log_file is not None
        return logging.FileHandler(filename=self.session_log_file)

    def update(
        self,
        *,
        parameters: SessionLogConfigurationParameters,
    ) -> None:
        # TODO: implement updating run-time parameters of the SessionLogConfiguration
        pass

    @contextmanager
    def log_session(
        self,
        *,
        queue_id: str,
        job_id: str,
        session_id: str,
        boto_session: BotoSession,
    ) -> Generator[logging.Handler | None, None, None]:
        """Returns a context manager that provisions a log handler, and configures logs to be
        streamed to the log destination.

        Parameters
        ----------
        session_id : str
            The unique identifier for the session
        boto_session : BotoSession
            The boto session which may be used by log drivers to provision log destinations and
            deliver logs to those destinations
        """
        ctx_mgr: ContextManager
        remote_handler = self.create_remote_handler(
            boto_session=boto_session,
        )
        if self.session_log_file is not None:
            local_file_handler = self.create_local_file_handler()
        else:
            local_file_handler = None
        if isinstance(remote_handler, CloudWatchHandler):
            ctx_mgr = remote_handler
        else:
            ctx_mgr = nullcontext()

        log_filter = SessionLogFilter(session_id=session_id)

        with (
            ctx_mgr,
            closing(local_file_handler) if local_file_handler else nullcontext(),
        ):
            if local_file_handler:
                local_file_handler.setFormatter(logging.Formatter(SESSION_LOCAL_LOG_FORMAT))
                local_file_handler.addFilter(log_filter)
            remote_handler.setFormatter(logging.Formatter(LOG_DRIVER_FMT_STRINGS[self.log_driver]))
            remote_handler.addFilter(log_filter)
            for log in self.loggers:
                log.addHandler(remote_handler)
                if local_file_handler:
                    log.addHandler(local_file_handler)
            log_group_name = self.options[LOG_CONFIG_OPTION_GROUP_NAME_KEY]
            log_stream_name = self.options[LOG_CONFIG_OPTION_STREAM_NAME_KEY]
            logger.info(
                SessionLogEvent(
                    subtype=SessionLogEventSubtype.LOGS,
                    queue_id=queue_id,
                    job_id=job_id,
                    session_id=session_id,
                    message="Logs streamed to: AWS CloudWatch Logs.",
                    log_dest=f"{log_group_name}/{log_stream_name}",
                )
            )
            if local_file_handler:
                logger.info(
                    SessionLogEvent(
                        subtype=SessionLogEventSubtype.LOGS,
                        queue_id=queue_id,
                        job_id=job_id,
                        session_id=session_id,
                        message="Logs streamed to: local file.",
                        log_dest=str(self.session_log_file),
                    )
                )
            try:
                yield remote_handler
            finally:
                for log in self.loggers:
                    log.removeHandler(remote_handler)
                    if local_file_handler:
                        log.removeHandler(local_file_handler)
                remote_handler.removeFilter(log_filter)
                if local_file_handler:
                    local_file_handler.removeFilter(log_filter)
