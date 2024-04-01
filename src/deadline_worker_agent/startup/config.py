# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import os
import logging as _logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Sequence, Tuple, cast

from pydantic import ValidationError

from openjd.sessions import PosixSessionUser, SessionUser

from ..errors import ConfigurationError
from .capabilities import Capabilities
from .cli_args import ParsedCommandLineArguments, get_argument_parser
from .settings import WorkerSettings

_logger = _logging.getLogger(__name__)


@dataclass(frozen=True)
class JobsRunAsUserOverride:
    run_as_agent: bool
    """True -> All jobs run as the agent process' user."""

    job_user: Optional[SessionUser] = None
    """If provided and run_as_agent is False, then all Jobs run by this agent will run as this user."""


# Default paths for the Worker persistence directory subdirectories.
# The persistence directory is expected to be located on a file-system that is local to the Worker
# Node. The Worker's ID and credentials are persisted and these should not be accessible by other
# Worker Nodes.
DEFAULT_WORKER_CREDENTIALS_RELDIR = "credentials"
DEFAULT_WORKER_STATE_FILE = "worker.json"


class Configuration:
    """AWS Deadline Cloud Worker Agent configuration

    Parameters
    ----------
    cli_args: ParsedCommandLineArguments
        The parsed command-line arguments
    """

    farm_id: str
    fleet_id: str
    cleanup_session_user_processes: bool
    profile: Optional[str]
    verbose: bool
    no_shutdown: bool
    job_run_as_user_overrides: JobsRunAsUserOverride
    allow_instance_profile: bool
    capabilities: Capabilities
    """Whether to use the new Worker Sessions API (UpdateWorkerSchedule)"""
    worker_persistence_dir: Path
    """Path to the directory where the Worker Agent persists files"""
    worker_credentials_dir: Path
    """Path to the directory where Worker credentials are persisted."""
    worker_state_file: Path
    """Path to file containing persisted Worker state between runs."""
    worker_logs_dir: Path
    """Path to the directory where the Worker Agent writes its logs."""
    local_session_logs: bool
    """Whether to write session logs to the local filesystem"""
    host_metrics_logging: bool
    """Whether host metrics logging is enabled"""
    host_metrics_logging_interval_seconds: float
    """The interval in seconds between host metrics logs"""
    retain_session_dir: bool
    """Whether to retain the OpenJD's session directory on completion"""
    structured_logs: bool
    """Whether or not the Worker Agent logs are structured logs."""

    # Used to optimize the memory allocation and attribute lookup speed. Tells python to not create a dict
    # for the attributes.
    __slots__ = (
        "farm_id",
        "fleet_id",
        "cleanup_session_user_processes",
        "profile",
        "verbose",
        "no_shutdown",
        "job_run_as_user_overrides",
        "allow_instance_profile",
        "capabilities",
        "worker_persistence_dir",
        "worker_credentials_dir",
        "worker_state_file",
        "worker_logs_dir",
        "local_session_logs",
        "host_metrics_logging",
        "host_metrics_logging_interval_seconds",
        "retain_session_dir",
        "structured_logs",
    )

    def __init__(
        self,
        parsed_cli_args: ParsedCommandLineArguments,
    ):
        settings_kwargs: dict[str, Any] = {}
        if parsed_cli_args.farm_id is not None:
            settings_kwargs["farm_id"] = parsed_cli_args.farm_id
        if parsed_cli_args.fleet_id is not None:
            settings_kwargs["fleet_id"] = parsed_cli_args.fleet_id
        if parsed_cli_args.cleanup_session_user_processes is not None:
            settings_kwargs["cleanup_session_user_processes"] = (
                parsed_cli_args.cleanup_session_user_processes
            )
        if parsed_cli_args.profile is not None:
            settings_kwargs["profile"] = parsed_cli_args.profile
        if parsed_cli_args.verbose is not None:
            settings_kwargs["verbose"] = parsed_cli_args.verbose
        if parsed_cli_args.no_shutdown is not None:
            settings_kwargs["no_shutdown"] = parsed_cli_args.no_shutdown
        if parsed_cli_args.run_jobs_as_agent_user is not None:
            settings_kwargs["run_jobs_as_agent_user"] = parsed_cli_args.run_jobs_as_agent_user
        if parsed_cli_args.posix_job_user is not None:
            settings_kwargs["posix_job_user"] = parsed_cli_args.posix_job_user
        if parsed_cli_args.disallow_instance_profile is not None:
            settings_kwargs["allow_instance_profile"] = (
                not parsed_cli_args.disallow_instance_profile
            )
        if parsed_cli_args.logs_dir is not None:
            settings_kwargs["worker_logs_dir"] = parsed_cli_args.logs_dir.absolute()
        if parsed_cli_args.persistence_dir is not None:
            settings_kwargs["worker_persistence_dir"] = parsed_cli_args.persistence_dir.absolute()
        if parsed_cli_args.local_session_logs is not None:
            settings_kwargs["local_session_logs"] = parsed_cli_args.local_session_logs
        if parsed_cli_args.host_metrics_logging is not None:
            settings_kwargs["host_metrics_logging"] = parsed_cli_args.host_metrics_logging
        if parsed_cli_args.host_metrics_logging_interval_seconds is not None:
            settings_kwargs["host_metrics_logging_interval_seconds"] = (
                parsed_cli_args.host_metrics_logging_interval_seconds
            )
        if parsed_cli_args.retain_session_dir is not None:
            settings_kwargs["retain_session_dir"] = parsed_cli_args.retain_session_dir
        if parsed_cli_args.structured_logs is not None:
            settings_kwargs["structured_logs"] = parsed_cli_args.structured_logs

        settings = WorkerSettings(**settings_kwargs)

        if os.name == "posix" and settings.posix_job_user is not None:
            user, group = self._get_user_and_group_from_job_user(settings.posix_job_user)
            self.job_run_as_user_overrides = JobsRunAsUserOverride(
                run_as_agent=settings.run_jobs_as_agent_user,
                job_user=PosixSessionUser(user=user, group=group),
            )
        else:
            self.job_run_as_user_overrides = JobsRunAsUserOverride(
                run_as_agent=settings.run_jobs_as_agent_user
            )

        self.farm_id = settings.farm_id
        self.fleet_id = settings.fleet_id
        self.cleanup_session_user_processes = settings.cleanup_session_user_processes
        self.profile = settings.profile
        self.verbose = settings.verbose
        self.no_shutdown = settings.no_shutdown
        self.allow_instance_profile = settings.allow_instance_profile
        self.worker_persistence_dir = settings.worker_persistence_dir
        self.worker_credentials_dir = (
            self.worker_persistence_dir / DEFAULT_WORKER_CREDENTIALS_RELDIR
        )
        self.worker_state_file = self.worker_persistence_dir / DEFAULT_WORKER_STATE_FILE
        self.capabilities = settings.capabilities
        self.worker_logs_dir = settings.worker_logs_dir
        self.local_session_logs = settings.local_session_logs
        self.host_metrics_logging = settings.host_metrics_logging
        self.host_metrics_logging_interval_seconds = settings.host_metrics_logging_interval_seconds
        self.retain_session_dir = settings.retain_session_dir
        self.structured_logs = settings.structured_logs

        self._validate()

    def _get_user_and_group_from_job_user(self, job_user: str) -> Tuple[str, str]:
        try:
            user, group = job_user.split(":")
        except ValueError:
            raise ConfigurationError(
                f"The job user must be of the form: <user>:<group>. Got: {repr(job_user)}"
            )
        return user, group

    def _validate(self) -> None:
        if not self.farm_id:
            raise ConfigurationError(f"Farm ID must be specified, but got {repr(self.farm_id)})")
        if not self.fleet_id:
            raise ConfigurationError(f"Fleet ID must be specified, but got {repr(self.fleet_id)})")

        if (
            self.job_run_as_user_overrides.run_as_agent
            and self.job_run_as_user_overrides.job_user is not None
        ):
            raise ConfigurationError(
                "Cannot specify a POSIX job user when the option to running Jobs as the agent user is enabled."
            )

        if self.host_metrics_logging_interval_seconds <= 0:
            raise ConfigurationError(
                f"Host metrics logging interval must be a positive number, but got: {repr(self.host_metrics_logging_interval_seconds)}"
            )

    def log(self, logger: Optional[_logging.Logger] = None, level: int = _logging.DEBUG) -> None:
        """Emit logs that represent the effective Configuration.

        Arguments:
            logger: logging.Logger
                An optional logger to log the configuration to. If not specified, this uses
                the `deadline_worker_agent.startup.config` logger.
            level: int
                The logging level to use. This defaults to `DEBUG`.
        """
        if not logger:
            logger = _logger

        if logger.isEnabledFor(level):
            sep = "=" * 80
            logger.log(level, sep)
            logger.log(level, "Configuration".center(80))
            logger.log(level, sep)
            for key in Configuration.__slots__:
                value = getattr(self, key)
                logger.log(level, f"{key}={value}")
            logger.log(level, sep)

    @classmethod
    def load(
        cls,
        cli_args: Optional[Sequence[str]] = None,
    ) -> Configuration:
        """Loads the AWS Deadline Cloud Worker Agent configuration.

        Arguments:
            cli_args: Sequence[str]
                The command-line arguments. If not specified, this defaults to
                using `sys.argv[1:]`.

        Returns:
            A `Configuration` object
        """
        arg_parser = get_argument_parser()
        parsed_cli_args = cast(
            ParsedCommandLineArguments,
            arg_parser.parse_args(cli_args, namespace=ParsedCommandLineArguments()),
        )

        try:
            return Configuration(
                parsed_cli_args=parsed_cli_args,
            )
        except ValidationError as validation_error:
            from itertools import groupby

            msg = "Configuration is not valid. Validation errors:\n\n"
            for loc, entries in groupby(validation_error.errors(), lambda err: err["loc"]):
                loc_str = ".".join(str(component) for component in loc)
                msg += f"{loc_str}: "
                msg += ", ".join(entry["msg"] for entry in entries)
                msg += "\n"
            raise ConfigurationError(msg) from validation_error
