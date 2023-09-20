# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from pathlib import Path
from typing import Any, Optional
import sys
import os

from pydantic import BaseModel, BaseSettings, Field

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


class OsConfigSection(BaseModel):
    impersonation: Optional[bool] = None
    posix_job_user: Optional[str] = Field(
        regex=r"^[a-zA-Z0-9_.][^:]{0,31}:[a-zA-Z0-9_.][^:]{0,31}$"
    )
    shutdown_on_stop: Optional[bool] = None


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

        # File must be open in binary mode for tomli to ensure the file is utf-8
        with config_path.open(mode="rb") as fh:
            toml_doc = load_toml(fh)

        try:
            return cls.parse_obj(toml_doc)
        except TOMLDecodeError as toml_error:
            raise ConfigurationError(
                f"Configuration file ({config_path}) is not valid TOML: {toml_error}"
            ) from toml_error

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
        if self.logging.verbose is not None:
            output_settings["verbose"] = self.logging.verbose
        if self.logging.worker_logs_dir is not None:
            output_settings["worker_logs_dir"] = self.logging.worker_logs_dir
        if self.logging.local_session_logs is not None:
            output_settings["local_session_logs"] = self.logging.local_session_logs
        if self.os.shutdown_on_stop is not None:
            output_settings["no_shutdown"] = self.os.shutdown_on_stop
        if self.os.impersonation is not None:
            output_settings["impersonation"] = self.os.impersonation
        if self.os.posix_job_user is not None:
            output_settings["posix_job_user"] = self.os.posix_job_user
        if self.aws.allow_ec2_instance_profile is not None:
            output_settings["allow_instance_profile"] = self.aws.allow_ec2_instance_profile
        if self.capabilities is not None:
            output_settings["capabilities"] = self.capabilities

        return output_settings
