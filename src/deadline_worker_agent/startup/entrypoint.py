# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""AWS Deadline Cloud Worker Agent entrypoint"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from time import sleep
from botocore.exceptions import NoRegionError
from logging.handlers import TimedRotatingFileHandler
from threading import Event
from typing import Optional
from pathlib import Path

from ..aws_credentials.worker_boto3_session import WorkerBoto3Session

from ..api_models import WorkerStatus
from ..aws.deadline import (
    update_worker,
    update_worker_schedule,
    record_worker_start_telemetry_event,
    record_uncaught_exception_telemetry_event,
)
from ..boto import DEADLINE_BOTOCORE_CONFIG, OTHER_BOTOCORE_CONFIG, DeadlineClient
from ..capabilities import detect_system_capabilities
from ..config import Configuration, ConfigurationError
from ..errors import ServiceShutdown
from ..linux.capabilities import drop_kill_cap_from_inheritable
from ..log_messages import (
    AgentInfoLogEvent,
    LogRecordStringTranslationFilter,
    WorkerHostConfigurationStatus,
    WorkerLogEvent,
    WorkerLogEventOp,
    WorkerHostConfigurationLogEvent,
)
from ..log_sync.cloudwatch import stream_cloudwatch_logs
from ..log_sync.loggers import ROOT_LOGGER, logger as log_sync_logger
from ..worker import Worker
from .bootstrap import WorkerBootstrap, bootstrap_worker
from .host_configuration_script import HostConfigurationScriptRunner

__all__ = ["entrypoint"]
_logger = logging.getLogger(__name__)


def _repeatedly_attempt_host_shutdown() -> bool:
    # This is here solely for the purpose of being mocked in tests so that we don't infinite loop.
    return True


def entrypoint(cli_args: Optional[list[str]] = None, *, stop: Optional[Event] = None) -> None:
    """Entrypoint for the Worker Agent. The worker gets registered and then polls for tasks to
    complete.

    Parameters
    ----------
    cli_args : Optional[list[str]]
        An optional sequence of command-line arguments to be parsed and applied to the
        worker agent configuration
    """
    try:
        # Load Worker Agent config
        config = Configuration.load(cli_args=cli_args)

        # Setup logging
        bootstrap_log_handler = _configure_base_logging(
            worker_logs_dir=config.worker_logs_dir,
            verbose=config.verbose,
            structured_logs=config.structured_logs,
        )

        # Log startup message
        _logger.info("👋 Worker Agent starting")
        _log_agent_info()

        # if customer manually provided the capabilities (to be added in this function)
        # then we default to the customer provided ones
        system_capabilities = detect_system_capabilities()
        record_worker_start_telemetry_event(system_capabilities)
        config.capabilities = system_capabilities.merge(config.capabilities)

        # Log the configuration (logs to DEBUG by default)
        config.log()

        # If we have the CAP_KILL Linux capability, we must programmatically
        # remove it from the inheritable capability set so it is not inherited
        # by session action subprocesses
        drop_kill_cap_from_inheritable()

        # Register the Worker
        try:
            worker_bootstrap = bootstrap_worker(config=config)
        except NoRegionError:
            _logger.warn(
                "The Worker Agent was started with no AWS region specified. Refer to the Deadline Cloud Worker Agent documentation for guidance: https://github.com/aws-deadline/deadline-cloud-worker-agent/blob/release/README.md#running-outside-of-an-operating-system-service"
            )
            raise
        if worker_bootstrap.log_config is None:
            _logger.critical(
                "This version of the Worker Agent does not support log configurations other than 'awslogs'"
            )
            raise NotImplementedError("Log Configurations other than 'awslogs' are not supported.")

        worker_info = worker_bootstrap.worker_info
        worker_id = worker_info.worker_id

        _logger.info(
            WorkerLogEvent(
                op=WorkerLogEventOp.ID,
                farm_id=config.farm_id,
                fleet_id=config.fleet_id,
                worker_id=worker_id,
                message="Agent identity.",
            )
        )

        # Get the boto3 session
        session = worker_bootstrap.session
        deadline_client = session.client(
            "deadline",
            config=DEADLINE_BOTOCORE_CONFIG,
        )
        s3_client = session.client("s3", config=OTHER_BOTOCORE_CONFIG)
        logs_client = session.client("logs", config=OTHER_BOTOCORE_CONFIG)

        # Shutdown behavior flags set by Worker below
        shutdown_requested_by_service = False

        # IMPORTANT
        # ---------
        # Treat this log line as a contract! It's the last thing that we'll
        # emit to the bootstrapping log, and external systems use it as a
        # sentinel to know that the worker has progressed successfully to processing
        # jobs.
        _logger.info("Worker successfully bootstrapped and is now running.")
        # ---------

        _remove_logging_handler(bootstrap_log_handler)

        with stream_cloudwatch_logs(
            logs_client=logs_client,
            log_group_name=worker_bootstrap.log_config.cloudwatch_log_group,
            log_stream_name=worker_bootstrap.log_config.cloudwatch_log_stream,
            logger=ROOT_LOGGER,
        ) as agent_cw_log_handler:
            # Filter log sync DEBUG level logs from being streamed to CloudWatch. This avoids an infinite (and expensive) loop of
            # log messages
            class LogSyncFilter(logging.Filter):
                def filter(self, record: logging.LogRecord) -> bool:
                    return record.name != log_sync_logger.name or record.levelno > logging.DEBUG

            agent_cw_log_handler.addFilter(LogSyncFilter())

            if not config.structured_logs:
                _logger.warning(
                    "The content and formatting of unstructured logs may change at any time and without warning. We recommend structured logs for programmatic log queries."
                )
            # Always print this one because structured logs always go to cloudwatch
            _logger.warning(
                "The content and formatting of structured log records that do not have a 'type' field may change at any time and without warning and must not be relied upon for programmatic log queries."
            )

            # Re-send the agent info to the log so that it also appears in the
            # logs that we forward to CloudWatch.
            _log_agent_info()

            # Run Worker Host Configuration.
            _host_configuration(
                config=config,
                deadline_client=deadline_client,
                worker_bootstrap=worker_bootstrap,
                worker_id=worker_id,
                session=session,
            )

            worker_sessions = Worker(
                farm_id=config.farm_id,
                fleet_id=config.fleet_id,
                worker_id=worker_id,
                deadline_client=deadline_client,
                s3_client=s3_client,
                logs_client=logs_client,
                boto_session=session,
                job_run_as_user_override=config.job_run_as_user_overrides,
                cleanup_session_user_processes=config.cleanup_session_user_processes,
                worker_persistence_dir=config.worker_persistence_dir,
                worker_logs_dir=config.worker_logs_dir if config.local_session_logs else None,
                host_metrics_logging=config.host_metrics_logging,
                host_metrics_logging_interval_seconds=config.host_metrics_logging_interval_seconds,
                retain_session_dir=config.retain_session_dir,
                stop=stop,
                session_root_dir=config.session_root_dir,
            )
            try:
                worker_sessions.run()
            except ServiceShutdown:
                shutdown_requested_by_service = True
            except Exception as e:
                _logger.exception("Worker Agent abnormal exit: %s", e)
                raise

            _agent_shutdown(deadline_client, config, worker_id, shutdown_requested_by_service)

    except ConfigurationError as e:
        sys.stderr.write(f"ERROR: {e}{os.linesep}")
        sys.exit(1)
    except Exception as e:
        if isinstance(e, SystemExit):
            raise
        else:
            _logger.critical(e, exc_info=True)
            record_uncaught_exception_telemetry_event(exception_type=str(type(e)))
            sys.exit(1)
    finally:
        _logger.info("🚪 Worker Agent exiting")


def _agent_shutdown(
    deadline_client: DeadlineClient,
    config: Configuration,
    worker_id: str,
    shutdown_requested_by_service: bool,
) -> None:
    if shutdown_requested_by_service:
        # The service will only request a shutdown if this Worker's Fleet is subject to autoscaling.
        # So, we transition to STOPPING and then repeatedly try to shutdown the host while heartbeating.
        # When we eventually succeed and the host is shutdown the service will notice the lack of heartbeats
        # and Delete the Worker.
        # If we fail to shutdown, then the service has signals that it can use to know that something
        # has gone sideways (we're in STOPPED and still heartbeating).
        _logger.info(
            "The service has requested that the host be shutdown. Setting Worker state to STOPPING."
        )
        try:
            update_worker(
                deadline_client=deadline_client,
                farm_id=config.farm_id,
                fleet_id=config.fleet_id,
                worker_id=worker_id,
                status=WorkerStatus.STOPPING,
            )
        except Exception as e:
            _logger.error(
                WorkerLogEvent(
                    op=WorkerLogEventOp.STATUS,
                    farm_id=config.farm_id,
                    fleet_id=config.fleet_id,
                    worker_id=worker_id,
                    message="Failed to set status to STOPPING: %s" % str(e),
                )
            )
        else:
            _logger.info(
                WorkerLogEvent(
                    op=WorkerLogEventOp.STATUS,
                    farm_id=config.farm_id,
                    fleet_id=config.fleet_id,
                    worker_id=worker_id,
                    message="Status set to STOPPING.",
                )
            )
        while _repeatedly_attempt_host_shutdown():
            _host_shutdown(config=config)
            try:
                update_worker_schedule(
                    deadline_client=deadline_client,
                    farm_id=config.farm_id,
                    fleet_id=config.fleet_id,
                    worker_id=worker_id,
                )
            except Exception as e:
                # Just swallow the error and keep looping until the host shutsdown
                _logger.error(
                    WorkerLogEvent(
                        op=WorkerLogEventOp.STATUS,
                        farm_id=config.farm_id,
                        fleet_id=config.fleet_id,
                        worker_id=worker_id,
                        message="Failed to heartbeat with AWS Deadline Cloud: %s" % str(e),
                    )
                )
            # Sleep for 30s and then try again; hopefully we never wake up
            sleep(30)
    else:
        _logger.info("Setting Worker state to STOPPED.")
        # Worker-initiated shutdown. We tell the service that we've STOPPED and then exit
        try:
            update_worker(
                deadline_client=deadline_client,
                farm_id=config.farm_id,
                fleet_id=config.fleet_id,
                worker_id=worker_id,
                status=WorkerStatus.STOPPED,
            )
        except Exception as e:
            _logger.error(
                WorkerLogEvent(
                    op=WorkerLogEventOp.STATUS,
                    farm_id=config.farm_id,
                    fleet_id=config.fleet_id,
                    worker_id=worker_id,
                    message="Failed to set status to STOPPED: %s" % str(e),
                )
            )
        else:
            _logger.info(
                WorkerLogEvent(
                    op=WorkerLogEventOp.STATUS,
                    farm_id=config.farm_id,
                    fleet_id=config.fleet_id,
                    worker_id=worker_id,
                    message="Status set to STOPPED.",
                )
            )


def _host_shutdown(config: Configuration) -> None:
    """Shuts the system down"""

    if config.no_shutdown:
        _logger.info("NOT shutting down the host. Local configuration settings say not to.")
        return

    _logger.info("Shutting down the host")

    shutdown_command: list[str]

    if sys.platform == "win32":
        shutdown_command = ["shutdown", "-s"]
    else:
        shutdown_command = ["sudo", "shutdown", "now"]

    # flush all the logs before initiating the shutdown command.
    for handler in _logger.handlers:
        handler.flush()

    process = subprocess.Popen(
        shutdown_command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    stdout, _ = process.communicate()
    command_output = stdout.decode("utf-8")
    if process.returncode != 0:
        _logger.error(
            f"Shutdown command ({shutdown_command}) failed with return code: {process.returncode}: {command_output}"
        )


def _configure_base_logging(
    worker_logs_dir: Path, verbose: bool, structured_logs: bool
) -> logging.Handler:
    """Configures the logger to write to both the console and a file"""
    root_logger = logging.getLogger()
    # Set the log level
    root_logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    # Only emit boto logs of WARNING or higher
    for logger_name in (
        "boto3",
        "botocore",
        "urllib3.connectionpool",
        "deadline_worker_agent.log_sync",
    ):
        logging.getLogger(logger_name).setLevel(logging.WARNING)

    # Job Attachments is a feature that only runs in the context of a
    # Session. So, its logs should not propagate to the root logger. Instead,
    # the Job Attachments logs will route to the Session Logs only.
    JOB_ATTACHMENTS_LOGGER = logging.getLogger("deadline.job_attachments")
    JOB_ATTACHMENTS_LOGGER.propagate = False

    translation_filter = LogRecordStringTranslationFilter()

    # Add quiet stderr output logger
    console_handler: logging.Handler
    try:
        from rich.logging import RichHandler
    except ImportError:
        console_handler = logging.StreamHandler(sys.stderr)
    else:  # pragma: no cover
        console_handler = RichHandler(rich_tracebacks=True, tracebacks_show_locals=verbose)

    if structured_logs:
        fmt_str = "[%(asctime)s] %(json)s"
    else:
        fmt_str = "[%(asctime)s][%(levelname)-8s] %(desc)s%(message)s"
    console_handler.formatter = logging.Formatter(fmt_str)
    root_logger.addHandler(console_handler)
    console_handler.addFilter(translation_filter)

    if not (worker_logs_dir.exists() and worker_logs_dir.is_dir()):
        raise RuntimeError(
            f"The configured directory for worker logs does not exist:\n{worker_logs_dir}"
        )

    # Add a separate file handler for just the bootstrapping/startup
    # phase of the worker. This will be removed once the bootstrap
    # sequence is complete.
    bootstrapping_handler = TimedRotatingFileHandler(
        worker_logs_dir / "worker-agent-bootstrap.log",
        # Daily rotation
        when="d",
        interval=1,
        encoding="utf-8",
    )
    # Bootstrap file should always be json. It's primarily intended
    # for use by Service Managed Fleet workers, and needs to be queryable
    # via AWS CloudWatch logs.
    bootstrapping_handler.formatter = logging.Formatter("%(json)s")
    root_logger.addHandler(bootstrapping_handler)
    bootstrapping_handler.addFilter(translation_filter)

    # Add rotating file handler with more verbose output
    rotating_file_handler = TimedRotatingFileHandler(
        worker_logs_dir / "worker-agent.log",
        # Daily rotation
        when="d",
        interval=1,
        encoding="utf-8",
    )
    rotating_file_handler.formatter = logging.Formatter(fmt_str)
    root_logger.addHandler(rotating_file_handler)
    rotating_file_handler.addFilter(translation_filter)

    return bootstrapping_handler


def _host_configuration(
    config: Configuration,
    deadline_client: DeadlineClient,
    session: WorkerBoto3Session,
    worker_bootstrap: WorkerBootstrap,
    worker_id: str,
) -> None:
    """
    Runs all business logic related to Host Configuration.
    This method must be run within a cloudwatch stream context to stream logs.
    If the host configuration run fails, the worker agent exits.
    """

    # If there was a host config, and it was bootstrapped before, only log a message.
    if worker_bootstrap.host_config and worker_bootstrap.worker_info.host_configuration_succeeded:
        _logger.info(
            WorkerHostConfigurationLogEvent(
                farm_id=config.farm_id,
                fleet_id=config.fleet_id,
                worker_id=worker_id,
                message="Host Configuration has been setup before. Not running config scripts.",
                status=WorkerHostConfigurationStatus.SKIPPED,
            )
        )
        return
    # Before the run loop starts, run the Host Configuration script.
    elif worker_bootstrap.host_config:
        _logger.info(
            WorkerHostConfigurationLogEvent(
                farm_id=config.farm_id,
                fleet_id=config.fleet_id,
                worker_id=worker_id,
                message="Running host configuration script.",
                status=WorkerHostConfigurationStatus.RUNNING,
            )
        )
        host_config_runner = HostConfigurationScriptRunner(
            logger=_logger,
            configuration=config,
            worker_id=worker_id,
            session_directory=config.worker_persistence_dir,
            worker_boto3_session=session,
            host_configuration_script=worker_bootstrap.host_config.script_body,
            host_configuration_timeout_seconds=worker_bootstrap.host_config.script_timeout_seconds,
        )
        exit_code = host_config_runner.run()
        if exit_code == 0:
            _logger.info(
                WorkerHostConfigurationLogEvent(
                    farm_id=config.farm_id,
                    fleet_id=config.fleet_id,
                    worker_id=worker_id,
                    message="Worker Agent host configuration succeeded. Starting worker session loop.",
                    status=WorkerHostConfigurationStatus.SUCCEEDED,
                    exit_code=exit_code,
                )
            )
            # Persist host configuration has been performed.
            worker_bootstrap.worker_info.host_configuration_succeeded = True
            worker_bootstrap.worker_info.save(config=config)
            return
        else:
            _logger.critical(
                WorkerHostConfigurationLogEvent(
                    farm_id=config.farm_id,
                    fleet_id=config.fleet_id,
                    worker_id=worker_id,
                    message=f"Worker Agent host configuration failed with exit code {exit_code}. Cannot run jobs, exiting.",
                    status=WorkerHostConfigurationStatus.FAILED,
                    exit_code=exit_code,
                )
            )
            # Set this worker to stopped.
            try:
                update_worker(
                    deadline_client=deadline_client,
                    farm_id=config.farm_id,
                    fleet_id=config.fleet_id,
                    worker_id=worker_id,
                    status=WorkerStatus.STOPPED,
                )
            except Exception as e:
                _logger.error(
                    WorkerLogEvent(
                        op=WorkerLogEventOp.STATUS,
                        farm_id=config.farm_id,
                        fleet_id=config.fleet_id,
                        worker_id=worker_id,
                        message="Failed to set status to STOPPED: %s" % str(e),
                    )
                )

            # Attempt to shutdown if possible.
            if config.no_shutdown:
                _logger.info("NOT shutting down the host. Local configuration settings say not to.")
                sys.exit(1)

            _logger.critical(
                WorkerHostConfigurationLogEvent(
                    farm_id=config.farm_id,
                    fleet_id=config.fleet_id,
                    worker_id=worker_id,
                    message="Worker Agent host configuration failed. Attempting host shut down.",
                    status=WorkerHostConfigurationStatus.FAILED,
                    exit_code=exit_code,
                )
            )
            while _repeatedly_attempt_host_shutdown():
                _host_shutdown(config=config)
                # Sleep for 30s and then try again; hopefully we never wake up
                sleep(30)


def _remove_logging_handler(handler: logging.Handler) -> None:
    """Removes a given handler from the root logger"""
    root_logger = logging.getLogger()
    root_logger.removeHandler(handler)


def _log_agent_info() -> None:
    _logger.info(AgentInfoLogEvent())
