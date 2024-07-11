# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from pathlib import Path
from typing import Any, Optional
import sys
import os

from pydantic import BaseModel, BaseSettings, Field, ValidationError, root_validator, StrictStr

try:
    from tomllib import load as load_toml, TOMLDecodeError
except ModuleNotFoundError:
    from tomli import load as load_toml, TOMLDecodeError

from ..errors import ConfigurationError
from .capabilities import Capabilities


# Default path for the Worker configuration file keyed on the value of sys.platform
DEFAULT_CONFIG_PATH: dict[str, Path] = {
    "darwin": Path("/etc/amazon/deadline/worker.toml"),
    "linux": Path("/etc/amazon/deadline/worker.toml"),
    "win32": Path(os.path.expandvars(r"%PROGRAMDATA%/Amazon/Deadline/Config/worker.toml")),
}


class WorkerConfigSection(BaseModel):
    farm_id: Optional[str] = Field(regex=r"^farm-[a-z0-9]{32}$", default=None)
    fleet_id: Optional[str] = Field(regex=r"^fleet-[a-z0-9]{32}$", default=None)
    cleanup_session_user_processes: bool = True
    worker_persistence_dir: Optional[Path] = None


class AwsConfigSection(BaseModel):
    profile: Optional[str] = Field(min_length=1, max_length=64, default=None)
    allow_ec2_instance_profile: Optional[bool] = None


class LoggingConfigSection(BaseModel):
    verbose: Optional[bool] = None
    worker_logs_dir: Optional[Path] = None
    local_session_logs: Optional[bool] = None
    host_metrics_logging: Optional[bool] = None
    host_metrics_logging_interval_seconds: Optional[float] = None
    structured_logs: Optional[bool] = None


class OsConfigSection(BaseModel):
    run_jobs_as_agent_user: Optional[bool] = None
    posix_job_user: Optional[str] = Field(
        regex=r"^[a-zA-Z0-9_.][^:]{0,31}:[a-zA-Z0-9_.][^:]{0,31}$"
    )
    shutdown_on_stop: Optional[bool] = None
    retain_session_dir: Optional[bool] = None
    windows_job_user: Optional[StrictStr] = Field(regex=r"^.{1,512}$")  # defer validation to OS.

    @root_validator(pre=True)
    def _disallow_impersonation(cls, values: dict[str, Any]) -> dict[str, Any]:
        if "impersonation" in values:
            raise ValueError(
                "The 'impersonation' option has been removed. Please use 'run_jobs_as_agent_user' instead."
            )
        return values


class ConfigFile(BaseModel):
    worker: WorkerConfigSection
    aws: AwsConfigSection
    logging: LoggingConfigSection
    os: OsConfigSection
    capabilities: Capabilities

    @classmethod
    def load(cls, config_path: Optional[Path] = None) -> ConfigFile:
        if not config_path:
            config_path = cls.get_config_path()

        try:
            # File must be open in binary mode for tomli to ensure the file is utf-8
            with config_path.open(mode="rb") as fh:
                toml_doc = load_toml(fh)
        except TOMLDecodeError as toml_error:
            raise ConfigurationError(
                f"Configuration file ({config_path}) is not valid TOML: {toml_error}"
            ) from toml_error

        try:
            return cls.parse_obj(toml_doc)
        except ValidationError as pydantic_error:
            raise ConfigurationError(
                f"Parsing errors loading configuration file ({config_path}):\n{str(pydantic_error)}"
            ) from pydantic_error

    @classmethod
    def get_config_path(cls) -> Path:
        try:
            return DEFAULT_CONFIG_PATH[sys.platform]
        except KeyError:
            raise NotImplementedError(f"Unsupported platform {sys.platform}") from None

    def as_settings(
        self,
        settings: BaseSettings,
    ) -> dict[str, Any]:
        """
        A simple settings source that loads variables from a JSON file
        at the project's root.

        Here we happen to choose to use the `env_file_encoding` from Config
        when reading `config.json`
        """
        output_settings: dict[str, Any] = {
            "cleanup_session_user_processes": self.worker.cleanup_session_user_processes,
        }
        if self.worker.farm_id is not None:
            output_settings["farm_id"] = self.worker.farm_id
        if self.worker.fleet_id is not None:
            output_settings["fleet_id"] = self.worker.fleet_id
        if self.worker.worker_persistence_dir is not None:
            output_settings["worker_persistence_dir"] = self.worker.worker_persistence_dir
        if self.aws.profile is not None:
            output_settings["profile"] = self.aws.profile
        if self.aws.allow_ec2_instance_profile is not None:
            output_settings["allow_instance_profile"] = self.aws.allow_ec2_instance_profile
        if self.logging.verbose is not None:
            output_settings["verbose"] = self.logging.verbose
        if self.logging.worker_logs_dir is not None:
            output_settings["worker_logs_dir"] = self.logging.worker_logs_dir
        if self.logging.local_session_logs is not None:
            output_settings["local_session_logs"] = self.logging.local_session_logs
        if self.logging.host_metrics_logging is not None:
            output_settings["host_metrics_logging"] = self.logging.host_metrics_logging
        if self.logging.host_metrics_logging_interval_seconds is not None:
            output_settings["host_metrics_logging_interval_seconds"] = (
                self.logging.host_metrics_logging_interval_seconds
            )
        if self.logging.structured_logs is not None:
            output_settings["structured_logs"] = self.logging.structured_logs
        if self.os.shutdown_on_stop is not None:
            output_settings["no_shutdown"] = not self.os.shutdown_on_stop
        if self.os.run_jobs_as_agent_user is not None:
            output_settings["run_jobs_as_agent_user"] = self.os.run_jobs_as_agent_user
        if self.os.posix_job_user is not None:
            output_settings["posix_job_user"] = self.os.posix_job_user
        if self.os.windows_job_user is not None:
            output_settings["windows_job_user"] = self.os.windows_job_user
        if self.os.retain_session_dir is not None:
            output_settings["retain_session_dir"] = self.os.retain_session_dir
        if self.capabilities is not None:
            output_settings["capabilities"] = self.capabilities

        return output_settings
