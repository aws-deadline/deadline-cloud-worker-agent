# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Amazon Deadline Cloud Worker Agent entrypoint"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from logging.handlers import TimedRotatingFileHandler
from typing import Optional
from pathlib import Path

from openjd.model import version as openjd_model_version
from openjd.sessions import version as openjd_sessions_version
from openjd.sessions import LOG as OPENJD_SESSION_LOG
from deadline.job_attachments import version as deadline_job_attach_version

from .._version import __version__
from ..api_models import WorkerStatus
from ..boto import DEADLINE_BOTOCORE_CONFIG, OTHER_BOTOCORE_CONFIG
from ..errors import ServiceShutdown
from ..log_sync.cloudwatch import stream_cloudwatch_logs
from ..log_sync.loggers import ROOT_LOGGER, logger as log_sync_logger
from ..worker import Worker
from .bootstrap import bootstrap_worker
from .capabilities import detect_system_capabilities
from .config import Configuration, ConfigurationError
from ..aws.deadline import (
    DeadlineRequestError,
    delete_worker,
    update_worker,
    record_worker_start_telemetry_event,
)

__all__ = ["entrypoint"]
_logger = logging.getLogger(__name__)


def entrypoint(cli_args: Optional[list[str]] = None) -> None:
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
            worker_logs_dir=config.worker_logs_dir, verbose=config.verbose
        )

        # Log startup message
        _logger.info("Worker Agent starting")
        _log_agent_info()

        # if customer manually provided the capabilities (to be added in this function)
        # then we default to the customer provided ones
        system_capabilities = detect_system_capabilities()
        record_worker_start_telemetry_event(system_capabilities)
        config.capabilities = system_capabilities.merge(config.capabilities)

        # Log the configuration
        config.log()

        # Register the Worker
        worker_bootstrap = bootstrap_worker(config=config)
        if worker_bootstrap.log_config is None:
            _logger.critical(
                "This version of the Worker Agent does not support log configurations other than 'awslogs'"
            )
            raise NotImplementedError("Log Configurations other than 'awslogs' are not supported.")

        worker_info = worker_bootstrap.worker_info
        worker_id = worker_info.worker_id

        # Get the boto3 session
        session = worker_bootstrap.session
        deadline_client = session.client(
            "deadline",
            config=DEADLINE_BOTOCORE_CONFIG,
        )
        s3_client = session.client("s3", config=OTHER_BOTOCORE_CONFIG)
        logs_client = session.client("logs", config=OTHER_BOTOCORE_CONFIG)

        # Shutdown behavior flags set by Worker below
        should_delete_worker = False
        shutdown_requested = False

        # Let's treat this log line as a contract. It's the last thing that we'll
        # emit to the bootstrapping log, and external systems can use it as a
        # sentinel to know that the worker has progressed successfully to processing
        # jobs.
        _logger.info("Worker successfully bootstrapped and is now running.")

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

            # Re-send the agent info to the log so that it also appears in the
            # logs that we forward to CloudWatch.
            _log_agent_info()

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
            )
            try:
                worker_sessions.run()
            except ServiceShutdown:
                shutdown_requested = True
                should_delete_worker = True
            except Exception as e:
                _logger.exception("Failed running worker: %s", e)
                raise

            try:
                update_worker(
                    deadline_client=deadline_client,
                    farm_id=config.farm_id,
                    fleet_id=config.fleet_id,
                    worker_id=worker_id,
                    status=WorkerStatus.STOPPED,
                )
            except Exception as e:
                _logger.error("Failed to stop Worker: %s", e)
            else:
                _logger.info("Worker %s successfully stopped", worker_id)
                if should_delete_worker:
                    _logger.info('Deleting worker with id "%s"', worker_id)
                    try:
                        delete_worker(
                            deadline_client=deadline_client, config=config, worker_id=worker_id
                        )
                    except DeadlineRequestError as e:
                        _logger.error("Failed to delete Worker: %s", e.inner_exc)
                    else:
                        _logger.info('Worker "%s" successfully deleted', worker_id)

            # conditional shutdown
            if shutdown_requested:
                _system_shutdown(config=config)
    except ConfigurationError as e:
        sys.stderr.write(f"ERROR: {e}{os.linesep}")
        sys.exit(1)
    except Exception as e:
        if isinstance(e, SystemExit):
            raise
        else:
            _logger.critical(e, exc_info=True)
            sys.exit(1)
    finally:
        _logger.info("Worker Agent exiting")


def _system_shutdown(config: Configuration) -> None:
    """Shuts the system down"""

    _logger.info("Shutting down the instance")

    shutdown_command: list[str]

    if sys.platform == "win32":
        shutdown_command = ["shutdown", "-s"]
    else:
        shutdown_command = ["sudo", "shutdown", "now"]

    if config.no_shutdown:
        _logger.debug(
            f"Skipping system shutdown. The following command would have been run: '{shutdown_command}'"
        )
        return

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


def _configure_base_logging(worker_logs_dir: Path, verbose: bool) -> logging.Handler:
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

    # We don't want the Session logs to appear in the Worker Agent logs, so
    # set the Open Job Description library's logger to not propagate.
    # We do this because the Session log will contain job-specific customer
    # sensitive data. The Worker's log is intended for IT admins that may
    # have different/lesser permissions/access-rights/need-to-know than the
    # folk submitting jobs, so keep the sensitive stuff out of the agent log.
    OPENJD_SESSION_LOG.propagate = False

    JOB_ATTACHMENTS_LOGGER = logging.getLogger("deadline.job_attachments")
    JOB_ATTACHMENTS_LOGGER.propagate = False

    # Add quiet stderr output logger
    console_handler: logging.Handler
    try:
        from rich.logging import RichHandler
    except ImportError:
        console_handler = logging.StreamHandler(sys.stderr)
        console_fmt_str = "[%(levelname)8s] %(message)s"
        if verbose:
            console_fmt_str = "[%(asctime)s] [%(levelname)8s] [%(name)-50s] --- %(message)s"
    else:  # pragma: no cover
        console_fmt_str = "%(message)s"
        if verbose:
            console_fmt_str = "[%(name)-50s] --- %(message)s"
        console_handler = RichHandler(rich_tracebacks=True, tracebacks_show_locals=verbose)

    console_handler.formatter = logging.Formatter(console_fmt_str)
    root_logger.addHandler(console_handler)

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
    )
    bootstrapping_handler.formatter = logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] %(message)s"
    )
    root_logger.addHandler(bootstrapping_handler)

    # Add rotating file handler with more verbose output
    rotating_file_handler = TimedRotatingFileHandler(
        worker_logs_dir / "worker-agent.log",
        # Daily rotation
        when="d",
        interval=1,
    )
    rotating_file_handler.formatter = logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] %(message)s"
    )
    root_logger.addHandler(rotating_file_handler)

    return bootstrapping_handler


def _remove_logging_handler(handler: logging.Handler) -> None:
    """Removes a given handler from the root logger"""
    root_logger = logging.getLogger()
    root_logger.removeHandler(handler)


def _log_agent_info() -> None:
    _logger.info(f"Python Interpreter: {sys.executable}")
    _logger.info("Python Version: %s", sys.version.replace("\n", " - "))
    _logger.info(f"Platform: {sys.platform}")
    _logger.info("Agent Version: %s", __version__)
    _logger.info("Installed at: %s", str(Path(__file__).resolve().parent.parent))
    _logger.info("Dependency versions installed:")
    _logger.info("\topenjd.model: %s", openjd_model_version)
    _logger.info("\topenjd.sessions: %s", openjd_sessions_version)
    _logger.info("\tdeadline.job_attachments: %s", deadline_job_attach_version)
