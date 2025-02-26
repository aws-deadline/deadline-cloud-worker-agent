# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, NamedTuple, Optional, cast
import shutil
import sys
import os

import tomlkit
from pydantic.v1 import BaseModel, BaseSettings, Field, ValidationError, root_validator, StrictStr
from tomlkit.container import Container
from tomlkit.items import Bool, Comment, SingleKey, String, Table, Trivia, Whitespace

try:
    from tomllib import load as load_toml, TOMLDecodeError
except ModuleNotFoundError:
    from tomli import load as load_toml, TOMLDecodeError

from ..capabilities import Capabilities
from .errors import ConfigurationError
from . import toml_comment


@dataclass(frozen=True)
class ModifiableSettingData:
    table_name: str
    """The name of the TOML table where the setting is located"""
    setting_name: str
    """The key for the setting in the TOML table"""
    preceding_comment: str
    """The comment to be prepended to the setting in the TOML file if the setting did not exist
    previously"""


class ModifiableSetting(Enum):
    ALLOW_EC2_INSTANCE_PROFILE = ModifiableSettingData(
        setting_name="allow_ec2_instance_profile",
        table_name="aws",
        preceding_comment="""
The "allow_ec2_instance_profile" setting controls whether the Worker will run with an EC2 instance
profile associated to the host instance. This value is overridden when the
DEADLINE_WORKER_ALLOW_INSTANCE_PROFILE environment variable is set using one of the following
case-insensitive values:

    '0', 'off', 'f', 'false', 'n', 'no', '1', 'on', 't', 'true', 'y', 'yes'.

or if the --disallow-instance-profile command-line flag is specified.

By default, this value is true and the worker agent will run with or without an instance profile
if the worker is on an EC2 host. If this value is false, the worker host will query the EC2
instance meta-data service (IMDS) to check for an instance profile. If an instance profile is
detected, the worker agent will stop and exit.

 ***************************************** WARNING *****************************************
 *                                                                                         *
 *     IF THIS IS TRUE, THEN ANY SESSIONS RUNNING ON THE WORKER CAN ASSUME THE INSTANCE    *
 *                  PROFILE AND USE ITS ASSOCIATED PRIVILEGES                              *
 *                                                                                         *
 *******************************************************************************************

To turn on this feature and have the worker agent not run with an EC2 instance profile,
uncomment the line below:
""".lstrip(),
    )
    FARM_ID = ModifiableSettingData(
        setting_name="farm_id",
        table_name="worker",
        preceding_comment="""
The unique identifier of the AWS Deadline Cloud farm that the Worker belongs to. This value is overridden
when the DEADLINE_WORKER_FARM_ID environment variable is set or the --farm-id command-line argument
is specified.

The following is an example for setting the farm ID in this configuration file:

farm_id = "farm-aabbccddeeff11223344556677889900"

Uncomment the line below and replace the value with your AWS Deadline Cloud farm ID:
""".lstrip(),
    )
    FLEET_ID = ModifiableSettingData(
        setting_name="fleet_id",
        table_name="worker",
        preceding_comment="""
The unique identifier of the AWS Deadline Cloud fleet that the Worker belongs to. This value is overridden
when the DEADLINE_WORKER_FLEET_ID environment variable is set or the --fleet-id command-line
argument is specified.

The following is an example for setting the fleet ID in this configuration file:

fleet_id = "fleet-aabbccddeeff11223344556677889900"

Uncomment the line below and replace the value with your AWS Deadline Cloud fleet ID:
""".lstrip(),
    )
    SHUTDOWN_ON_STOP = ModifiableSettingData(
        setting_name="shutdown_on_stop",
        table_name="os",
        preceding_comment="""
AWS Deadline Cloud may tell the worker to stop. If the "shutdown_on_stop" setting below is true, then the
Worker will attempt to shutdown the host system after the Worker has been stopped.

This value is overridden when the DEADLINE_WORKER_NO_SHUTDOWN environment variable is set using
one of the following case-insensitive values:

    '0', 'off', 'f', 'false', 'n', 'no', '1', 'on', 't', 'true', 'y', 'yes'.

or if the --no-shutdown command-line flag is specified.

To prevent the worker agent from shutting down the host when being told to stop, uncomment the
line below:
""".lstrip(),
    )
    SESSION_ROOT_DIR = ModifiableSettingData(
        setting_name="session_root_dir",
        table_name="worker",
        preceding_comment="""
The session root directory is a parent directory where worker agent creates per-session
subdirectories under. This value is overridden when the DEADLINE_WORKER_SESSION_ROOT_DIR environment
variable is set or the --session-root-dir command-line argument is specified.

The default session root directory on POSIX systems is "/sessions" and on Windows systems is
"C:\ProgramData\Amazon\OpenJD".

Uncomment the line below and replace the value with your desired session root directory:
""".lstrip(),
    )
    WINDOWS_JOB_USER = ModifiableSettingData(
        setting_name="windows_job_user",
        table_name="os",
        preceding_comment="""
AWS Deadline Cloud may specify a Windows OS user to run a Job's session actions as. Setting
"windows_job_user" will override the OS user and the session actions will be run as
the user given in the value of "windows_job_user" instead. It is important to note that by specifying 
this value, the password for the Windows OS user specified will be reset to a random, unstored value.
This setting also requires that the worker agent is run with administrator privileges. This setting is 
incompatible the setting "run_jobs_as_agent_user" set to true.

To have a specific Windows OS user used when running jobs, uncomment the line below and
replace the username as desired. This value is overridden when the DEADLINE_WORKER_WINDOWS_JOB_USER
environment variable or if the --windows-job-user command-line flag is specified.
""".lstrip(),
    )


class SettingModification(NamedTuple):
    setting: ModifiableSetting
    value: str | bool | None


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
    session_root_dir: Optional[Path] = None


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

    @classmethod
    def backup(
        cls,
        *,
        config_path: Optional[Path] = None,
    ) -> None:
        if config_path is None:
            config_path = cls.get_config_path()

        backup_path = config_path.with_suffix(f"{config_path.suffix}.bak")
        shutil.copyfile(config_path, backup_path)

    @classmethod
    def modify_settings(
        cls,
        *,
        settings_to_modify: list[SettingModification],
        document: tomlkit.TOMLDocument,
    ) -> None:
        for setting_to_modify in settings_to_modify:
            # Lookup the supplementary setting data (section and comment help)
            setting_data = setting_to_modify.setting.value

            # Modify the setting, preserving the TOML markup formatting (spaces, tabs, newlines,
            # comments, etc..). Also has logic to first attempt to detect commented-out settings,
            # if those are found, it uncomments them and sets their value. If a setting is being
            # unset, it comments out any existing setting if it existed.
            ConfigFile._modify_setting(
                document=document,
                setting_data=setting_data,
                setting_value=setting_to_modify.value,
            )

    @classmethod
    def modify_config_file_settings(
        cls,
        *,
        settings_to_modify: list[SettingModification],
        backup: bool = False,
        config_path: Optional[Path] = None,
    ) -> None:
        if config_path is None:
            config_path = ConfigFile.get_config_path()

        # Load the config document
        with config_path.open("r") as f:
            document = tomlkit.load(f)

        ConfigFile.modify_settings(
            document=document,
            settings_to_modify=settings_to_modify,
        )

        if backup:
            # Backup the config
            ConfigFile.backup(config_path=config_path)

        # Write the modified config
        with config_path.open("w") as f:
            tomlkit.dump(document, f)

    @staticmethod
    def _modify_setting(
        *,
        document: tomlkit.TOMLDocument,
        setting_data: ModifiableSettingData,
        setting_value: str | bool | None,
    ) -> None:
        """Modifies a TOML document to apply the worker agent settings specified in the arguments.
        This uses tomlkit to do this while preserving semantics, structure, and style including
        spaces, tabs, newlines, and comments).

        If setting_value is None, it assumes the setting should not be specified in the document.
        The algorithm is as follows:

            1.  If the document has no setting applied, no action
            1.  If the document has an existing setting, it is commented out

        If the setting_value is not None, the algorithm is as follows:

            1.  If the document has an existing setting, the value is changed in place
            2.  If the document has a previously commented-out setting, uncomments the last
                occurrence and sets its value.
            3.  If the document has no prior setting or commented-out setting, a new setting
                is added to the bottom of the table with a preceding comment documenting the
                setting.
        """

        table_name = setting_data.table_name
        setting_name = setting_data.setting_name
        preceding_comment = setting_data.preceding_comment

        if table_name not in document:
            if setting_value is None:
                return
            document[table_name] = tomlkit.table()

        target_table = cast(Table, document[table_name])
        key = SingleKey(setting_name)
        table_container: Container = target_table.value

        # If there is an existing applied setting in the TOML document
        if key in target_table:
            # If we are unsetting the value, comment it out. This is to preserve any comments in the
            # TOML file (e.g. those originating from the example config file)
            if setting_value is None:
                toml_comment.comment_out(
                    table_container=table_container,
                    key=key,
                )
            else:
                target_table[setting_name] = setting_value
            return
        elif setting_value is None:
            return

        # Create tomlkit value wrapper object for the value we want to apply.
        toml_setting_value: Bool | String
        if isinstance(setting_value, str):
            toml_setting_value = tomlkit.string(setting_value)
        elif isinstance(setting_value, bool):
            toml_setting_value = tomlkit.boolean(str(setting_value).lower())
        else:
            raise NotImplementedError(f"Unexpected type for setting_value ({type(setting_value)})")

        # Case: There is a commented-out setting (e.g. from the example file)
        # We uncomment and set the value. This replicates the original bash implementation behaviour
        # of install-deadline-worker
        try:
            toml_comment.uncomment(
                table_container=table_container,
                key=key,
                value=toml_setting_value,
                # We replace the last occurrence since some of the documentation comments include an
                # example TOML line before the commented-out line that is supposed to be uncommented
                occurrence="last",
            )
        except toml_comment.CommentNotFoundError:
            pass
        else:
            return

        # Case: There is no commented-out setting, for example a previous worker agent install where
        # the setting did not exist. or a manually created config file
        if len(table_container.body) > 0 and not isinstance(
            table_container.body[-1][1], Whitespace
        ):
            target_table.add(tomlkit.nl())
            target_table.add(tomlkit.nl())

        for line in preceding_comment.splitlines():
            # tomlkit.comment always adds a space after the #
            # We remove the space for empty lines
            target_table.add(Comment(Trivia(comment_ws="  ", comment=f"# {line}" if line else "#")))
        target_table.add(setting_name, toml_setting_value)

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
        if self.worker.session_root_dir is not None:
            output_settings["session_root_dir"] = self.worker.session_root_dir
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
