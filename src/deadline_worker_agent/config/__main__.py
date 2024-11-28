# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
import argparse
from logging import basicConfig, getLogger, INFO
from pathlib import Path

from .config_file import (
    ConfigFile,
    SettingModification,
    ModifiableSetting,
)


logger = getLogger(__name__)


class ParsedArguments(argparse.Namespace):
    backup: bool
    """Whether to backup the existing worker agent configuration file. The backup is created
    along-side the worker agent configuration file with a .bak extension added"""

    config_path: Path | None = None
    """The path to the worker agent configuration file to be modified"""

    farm_id: str | None = None
    """The unique identifier for the Deadline Cloud farm that the worker belongs to"""

    fleet_id: str | None = None
    """The unique identifier for the Deadline Cloud fleet that the worker belongs to"""

    shutdown_on_stop: bool | None = None
    """Whether the worker agent will attempt to shutdown the worker host when the service instructs
    the worker to stop"""

    allow_ec2_instance_profile: bool | None = None
    """Whether or not the worker agent will allow being started if an EC2 instance profile is
    detected"""

    windows_job_user: str | bool | None = None
    """A Windows username to override the queue jobRunAs configuration. If False, then no
    modification is made. If None, then the setting is unset."""

    session_root_dir: str | None = None
    """Path to the parent directory where the worker agent creates per-session subdirectories under"""


def create_argument_parser() -> argparse.ArgumentParser:
    """Creates the argparse ArgumentParser for the deadline_worker_agent.config module"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config-path",
        type=lambda x: Path(x) if x is not None else None,
        help=argparse.SUPPRESS,
    )
    worker_group = parser.add_argument_group("Worker", "Settings about the Deadline Cloud Worker")

    worker_group.add_argument(
        "--farm-id",
        help="The unique identifier for the Deadline Cloud farm",
        required=False,
    )
    worker_group.add_argument(
        "--fleet-id",
        help="The unique identifier for the Deadline Cloud fleet",
        required=False,
    )
    worker_group.add_argument(
        "--session-root-dir",
        help="The parent directory that worker agent creates per-session subdirectories under",
        required=False,
        type=str,
    )

    aws_group = parser.add_argument_group("AWS", "Settings related to AWS and EC2 hosts")
    parser.set_defaults(allow_ec2_instance_profile=None)
    allow_instance_profile_group = aws_group.add_mutually_exclusive_group()
    allow_instance_profile_group.add_argument(
        "--allow-ec2-instance-profile",
        help=(
            "The worker agent will be allowed to run with an EC2 instance profile associated "
            "to the host instance."
        ),
        action="store_const",
        const=True,
        required=False,
    )
    allow_instance_profile_group.add_argument(
        "--no-allow-ec2-instance-profile",
        help=(
            "The worker agent will fail to run with an EC2 instance profile associated to the "
            "host instance."
        ),
        action="store_const",
        dest="allow_ec2_instance_profile",
        const=False,
        required=False,
    )

    os_group = parser.add_argument_group(
        "Operating System", "Settings related to the host operating system"
    )

    parser.set_defaults(shutdown_on_stop=None)
    shutdown_on_stop_group = os_group.add_mutually_exclusive_group()
    shutdown_on_stop_group.add_argument(
        "--shutdown-on-stop",
        help=(
            "The worker agent will attempt to shutdown the host machine when the service tells "
            "the worker to stop"
        ),
        action="store_const",
        const=True,
        required=False,
    )
    shutdown_on_stop_group.add_argument(
        "--no-shutdown-on-stop",
        help=(
            "The worker agent will not attempt to shutdown the host machine when the service "
            "tells the worker to stop"
        ),
        action="store_const",
        dest="shutdown_on_stop",
        required=False,
        const=False,
    )

    parser.set_defaults(windows_job_user=False)
    windows_job_user_group = parser.add_mutually_exclusive_group()
    windows_job_user_group.add_argument(
        "--windows-job-user",
        help=(
            "On Windows, this refers to the username of a local account that will be used "
            "to run jobs. This overrides the queue jobRunAsUser configuration."
        ),
        required=False,
    )
    windows_job_user_group.add_argument(
        "--no-windows-job-user",
        help=(
            "On Windows, this turns off a previous job user override. Jobs will then run "
            "using the queue jobRunAsUser configuration."
        ),
        required=False,
        dest="windows_job_user",
        action="store_const",
        const=None,
    )

    parser.add_argument(
        "--no-backup",
        help=(
            "Do not create a backup. The default behavior is to create a backup of the "
            'configuration file to a path an added ".bak" extension.'
        ),
        action="store_false",
        dest="backup",
    )
    return parser


def args_to_setting_modifications(parsed_args: ParsedArguments) -> list[SettingModification]:
    settings_to_modify = list[SettingModification]()

    if parsed_args.farm_id is not None:
        settings_to_modify.append(
            SettingModification(
                setting=ModifiableSetting.FARM_ID,
                value=parsed_args.farm_id,
            )
        )
    if parsed_args.fleet_id is not None:
        settings_to_modify.append(
            SettingModification(
                setting=ModifiableSetting.FLEET_ID,
                value=parsed_args.fleet_id,
            )
        )
    if parsed_args.allow_ec2_instance_profile is not None:
        settings_to_modify.append(
            SettingModification(
                setting=ModifiableSetting.ALLOW_EC2_INSTANCE_PROFILE,
                value=parsed_args.allow_ec2_instance_profile,
            )
        )
    if parsed_args.shutdown_on_stop is not None:
        settings_to_modify.append(
            SettingModification(
                setting=ModifiableSetting.SHUTDOWN_ON_STOP,
                value=parsed_args.shutdown_on_stop,
            )
        )
    if parsed_args.windows_job_user is not False:
        if isinstance(parsed_args.windows_job_user, str) or parsed_args.windows_job_user is None:
            settings_to_modify.append(
                SettingModification(
                    setting=ModifiableSetting.WINDOWS_JOB_USER,
                    value=parsed_args.windows_job_user,
                )
            )
        else:
            raise NotImplementedError(
                f"Unexpected value for windows_job_user: {parsed_args.windows_job_user}"
            )
    if parsed_args.session_root_dir is not None:
        settings_to_modify.append(
            SettingModification(
                setting=ModifiableSetting.SESSION_ROOT_DIR,
                value=parsed_args.session_root_dir,
            )
        )

    return settings_to_modify


def main() -> None:
    basicConfig(format="%(msg)s", level=INFO)
    parser = create_argument_parser()
    args = parser.parse_args(namespace=ParsedArguments())

    if (config_path := args.config_path) is None:
        config_path = ConfigFile.get_config_path()

    settings_to_modify = args_to_setting_modifications(args)

    if not settings_to_modify:
        parser.error("No settings to modify")
    else:
        logger.info("The following settings will be modified:")
        for setting_to_modify in settings_to_modify:
            logger.info(
                f"  {setting_to_modify.setting.value.setting_name} = {setting_to_modify.value}"
            )

    ConfigFile.modify_config_file_settings(
        settings_to_modify=settings_to_modify,
        backup=args.backup,
        config_path=config_path,
    )


if __name__ == "__main__":
    main()
