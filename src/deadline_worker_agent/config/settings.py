# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from typing import Optional, Tuple
from pathlib import Path

from pydantic.v1 import BaseSettings, Field
from pydantic.v1.env_settings import SettingsSourceCallable

from ..capabilities import Capabilities
from .config_file import ConfigFile

import os


# Default path for the worker's logs.
DEFAULT_POSIX_WORKER_LOGS_DIR = Path("/var/log/amazon/deadline")
DEFAULT_WINDOWS_WORKER_LOGS_DIR = Path(os.path.expandvars(r"%PROGRAMDATA%/Amazon/Deadline/Logs"))
# Default path for the worker persistence directory.
# The persistence directory is expected to be located on a file-system that is local to the Worker
# Node. The Worker's ID and credentials are persisted and these should not be accessible by other
# Worker Nodes.
DEFAULT_POSIX_WORKER_PERSISTENCE_DIR = Path("/var/lib/deadline")
DEFAULT_WINDOWS_WORKER_PERSISTENCE_DIR = Path(
    os.path.expandvars(r"%PROGRAMDATA%/Amazon/Deadline/Cache")
)

DEFAULT_POSIX_SESSION_ROOT_DIR = Path("/sessions")
DEFAULT_WINDOWS_SESSION_ROOT_DIR: Path = (
    Path(os.getenv("PROGRAMDATA", "C:\\ProgramData")) / "Amazon" / "OpenJD"
)


class WorkerSettings(BaseSettings):
    """Model class for the worker settings. This defines all of the fields and their validation as
    well as the settings sources and their priority order of:

    1. command-line arguments
    2. environment variables
    3. config file

    Parameters
    ----------
    farm_id : str
        The unique identifier of the worker's farm
    fleet_id: str
        The unique identifier of the worker's fleet
    cleanup_session_user_processes: bool
        Whether session user processes should be cleaned up when the session user is not being used
        in any active sessions anymore.
    profile : str
        An AWS profile used to bootstrap the worker
    verbose : bool
        Whether to emit more verbose logging
    no_shutdown : bool
        If true, then the Worker will not shut down when the service tells the worker to stop
    run_jobs_as_agent_user : bool
        If true, then all jobs run as the same user as the agent.
    posix_job_user : str
        Which 'user:group' to use instead of the Queue user when turned on.
    windows_job_user : str
        Which username to use instead of the Queue user when turned on.
    windows_job_user_password_arn : str
        The ARN of an AWS Secrets Manager secret containing the password of the job user for Windows.
    allow_instance_profile : bool
        If false and the worker is running on an EC2 instance with IMDS, then the
        worker will wait until the instance profile is disassociated before running worker sessions.
        This will repeatedly attempt to make requests to IMDS. If the instance profile is still
        associated after some threshold, the worker agent program will log the error and exit.
        Default is true.
    capabilities : deadline_worker_agent.startup.Capabilities
        A set of capabilities that will be declared when the worker starts. These capabilities
        can be used by the service to determine if the worker is eligible to run sessions for a
        given job/step/task and whether the worker is compliant with its fleet's configured minimum
        capabilities.
    worker_logs_dir : Path
        The path to the directory where the Worker Agent writes its logs.
    worker_persistence_dir : Path
        The path to the directory where the Worker Agent persists its state.
    local_session_logs : bool
        Whether to write session logs to the local filesystem
    host_metrics_logging : bool
        Whether to log host metrics
    host_metrics_logging_interval_seconds : float
        The interval between host metrics log messages
    retain_session_dir : bool
        If true, then the OpenJD's session directory will not be removed after the job is finished.
    structured_logs: bool
        If true, then the Worker Agent's logs are structured.
    """

    farm_id: str = Field(regex=r"^farm-[a-z0-9]{32}$")
    fleet_id: str = Field(regex=r"^fleet-[a-z0-9]{32}$")
    cleanup_session_user_processes: bool = True
    profile: Optional[str] = Field(min_length=1, max_length=64, default=None)
    verbose: bool = False
    no_shutdown: bool = False
    run_jobs_as_agent_user: bool = False
    posix_job_user: Optional[str] = Field(
        regex=r"^[a-zA-Z0-9_.][^:]{0,31}:[a-zA-Z0-9_.][^:]{0,31}$"
    )
    windows_job_user: Optional[str] = Field(regex=r"^.{1,512}$")
    windows_job_user_password_arn: Optional[str] = Field(
        regex=r"^arn:aws:secretsmanager:[a-z0-9\-]+:\d{12}:secret\/[a-zA-Z0-9/_+=.@-]+$"
    )
    allow_instance_profile: bool = True
    capabilities: Capabilities = Field(
        default_factory=lambda: Capabilities(amounts={}, attributes={})
    )
    worker_logs_dir: Path = (
        DEFAULT_WINDOWS_WORKER_LOGS_DIR if os.name == "nt" else DEFAULT_POSIX_WORKER_LOGS_DIR
    )
    worker_persistence_dir: Path = (
        DEFAULT_WINDOWS_WORKER_PERSISTENCE_DIR
        if os.name == "nt"
        else DEFAULT_POSIX_WORKER_PERSISTENCE_DIR
    )
    local_session_logs: bool = True
    host_metrics_logging: bool = True
    host_metrics_logging_interval_seconds: float = 60
    retain_session_dir: bool = False
    structured_logs: bool = False
    session_root_dir: Path = (
        DEFAULT_WINDOWS_SESSION_ROOT_DIR if os.name == "nt" else DEFAULT_POSIX_SESSION_ROOT_DIR
    )

    class Config:
        fields = {
            "farm_id": {"env": "DEADLINE_WORKER_FARM_ID"},
            "fleet_id": {"env": "DEADLINE_WORKER_FLEET_ID"},
            "cleanup_session_user_processes": {
                "env": "DEADLINE_WORKER_CLEANUP_SESSION_USER_PROCESSES"
            },
            "profile": {"env": "DEADLINE_WORKER_PROFILE"},
            "verbose": {"env": "DEADLINE_WORKER_VERBOSE"},
            "no_shutdown": {"env": "DEADLINE_WORKER_NO_SHUTDOWN"},
            "run_jobs_as_agent_user": {"env": "DEADLINE_WORKER_RUN_JOBS_AS_AGENT_USER"},
            "posix_job_user": {"env": "DEADLINE_WORKER_POSIX_JOB_USER"},
            "windows_job_user": {"env": "DEADLINE_WORKER_WINDOWS_JOB_USER"},
            "windows_job_user_password_arn": {
                "env": "DEADLINE_WORKER_WINDOWS_JOB_USER_PASSWORD_ARN"
            },
            "allow_instance_profile": {"env": "DEADLINE_WORKER_ALLOW_INSTANCE_PROFILE"},
            "capabilities": {"env": "DEADLINE_WORKER_CAPABILITIES"},
            "worker_logs_dir": {"env": "DEADLINE_WORKER_LOGS_DIR"},
            "worker_persistence_dir": {"env": "DEADLINE_WORKER_PERSISTENCE_DIR"},
            "local_session_logs": {"env": "DEADLINE_WORKER_LOCAL_SESSION_LOGS"},
            "host_metrics_logging": {"env": "DEADLINE_WORKER_HOST_METRICS_LOGGING"},
            "host_metrics_logging_interval_seconds": {
                "env": "DEADLINE_WORKER_HOST_METRICS_LOGGING_INTERVAL_SECONDS"
            },
            "retain_session_dir": {"env": "DEADLINE_WORKER_RETAIN_SESSION_DIR"},
            "structured_logs": {"env": "DEADLINE_WORKER_STRUCTURED_LOGS"},
            "session_dir_root": {"env": "DEADLINE_WORKER_SESSION_ROOT_DIR"},
        }

        @classmethod
        def customise_sources(
            cls,
            init_settings: SettingsSourceCallable,
            env_settings: SettingsSourceCallable,
            file_secret_settings: SettingsSourceCallable,
        ) -> Tuple[SettingsSourceCallable, ...]:
            """This function is called by pydantic to determine the settings sources used and their
            priority order.

            Below, we define the order as:

                1. Command-line arguments (passed in via the construct)
                2. Environment variables
                3. Configuration file

            Parameters
            ----------
            init_settings : pydantic.env_settings.SettingsSourceCallable
                The pydantic built-in init arguments settings source
            env_settings : pydantic.env_settings.SettingsSourceCallable
                The pydantic built-in environment variable settings source
            file_secret_settings : pydantic.env_settings.SettingsSourceCallable
                The pydantic built-in (Docker) secret file settings source

            Returns
            -------
            Tuple[pyadntic.env_settings.SettingsSourceCallable, ...]
                The settings sources used when initializing the WorkerSettings instance in priority
                order.
            """
            try:
                config_file = ConfigFile.load()
            except FileNotFoundError:
                return (init_settings, env_settings)

            return (
                init_settings,
                env_settings,
                config_file.as_settings,
            )
