# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from typing import Optional, Any
from argparse import ArgumentParser, Namespace
from pathlib import Path
from subprocess import CalledProcessError, run
import sys
import sysconfig


if sys.platform == "win32":
    from deadline_worker_agent.installer.win_installer import start_windows_installer


INSTALLER_PATH = {
    "linux": Path(__file__).parent / "install.sh",
}


def install() -> None:
    """Installer entrypoint for the AWS Deadline Cloud Worker Agent"""

    if sys.platform not in ["linux", "win32"]:
        print(f"ERROR: Unsupported platform {sys.platform}")
        sys.exit(1)

    arg_parser = get_argument_parser()
    args = arg_parser.parse_args(namespace=ParsedCommandLineArguments)
    scripts_path = Path(sysconfig.get_path("scripts"))
    if sys.platform == "win32":
        installer_args: dict[str, Any] = dict(
            farm_id=args.farm_id,
            fleet_id=args.fleet_id,
            region=args.region,
            worker_agent_program=scripts_path,
            no_install_service=not args.install_service,
            start=args.service_start,
            confirm=args.confirmed,
            allow_shutdown=args.allow_shutdown,
            parser=arg_parser,
        )
        if args.user:
            installer_args.update(user_name=args.user)
        if args.group:
            installer_args.update(group_name=args.group)
        if args.password:
            installer_args.update(password=args.password)
        if args.telemetry_opt_out:
            installer_args.update(telemetry_opt_out=args.telemetry_opt_out)

        start_windows_installer(**installer_args)
    else:
        cmd = [
            "sudo",
            str(INSTALLER_PATH[sys.platform]),
            "--farm-id",
            args.farm_id,
            "--fleet-id",
            args.fleet_id,
            "--region",
            args.region,
            "--user",
            args.user,
            "--scripts-path",
            str(scripts_path),
        ]
        if args.vfs_install_path:
            cmd += ["--vfs-install-path", args.vfs_install_path]
        if args.group:
            cmd += ["--group", args.group]
        if args.confirmed:
            cmd.append("-y")
        if args.service_start:
            cmd.append("--start")
        if args.allow_shutdown:
            cmd.append("--allow-shutdown")
        if not args.install_service:
            cmd.append("--no-install-service")
        if args.telemetry_opt_out:
            cmd.append("--telemetry-opt-out")

        try:
            run(
                cmd,
                check=True,
            )
        except CalledProcessError as error:
            sys.exit(error.returncode)


class ParsedCommandLineArguments(Namespace):
    """Represents the parsed installer command-line arguments"""

    farm_id: str
    fleet_id: str
    region: str
    user: str
    password: Optional[str]
    group: Optional[str]
    confirmed: bool
    service_start: bool
    allow_shutdown: bool
    install_service: bool
    telemetry_opt_out: bool
    vfs_install_path: str


def get_argument_parser() -> ArgumentParser:  # pragma: no cover
    """Returns a command-line argument parser for the AWS Deadline Cloud Worker Agent"""

    parser = ArgumentParser(
        prog="install-deadline-worker",
        description="Installer for the AWS Deadline Cloud Worker Agent",
    )
    parser.add_argument(
        "--farm-id",
        help="The AWS Deadline Cloud Farm ID that the Worker belongs to.",
        required=True,
    )
    parser.add_argument(
        "--fleet-id",
        help="The AWS Deadline Cloud Fleet ID that the Worker belongs to.",
        required=True,
    )
    parser.add_argument(
        "--region",
        help='The AWS region of the AWS Deadline Cloud farm. Defaults to "us-west-2".',
        default="us-west-2",
    )

    # Windows local usernames are restricted to 20 characters in length.
    default_username = "deadline-worker-agent" if sys.platform != "win32" else "deadline-worker"
    parser.add_argument(
        "--user",
        help=f'The username of the AWS Deadline Cloud Worker Agent user. Defaults to "{default_username}".',
        default=default_username,
    )

    parser.add_argument(
        "--group",
        help='The group that is shared between the Agent user and the user(s) that jobs run as. Defaults to "deadline-job-users".',
    )
    parser.add_argument(
        "--start",
        help="Starts the service immediately. Defaults to start on system boot. This option is ignored if --no-install-service is used.",
        action="store_true",
        dest="service_start",
    )

    if sys.platform == "win32":
        help = "Controls whether to grant the worker agent OS user the privilege to shutdown the system"
    else:
        help = "Controls whether to create/delete a sudoers rule allowing the worker agent OS user to shutdown the system"
    parser.add_argument(
        "--allow-shutdown",
        help=help,
        action="store_true",
    )

    parser.add_argument(
        "--no-install-service",
        help="Skips the worker agent service installation",
        action="store_false",
        dest="install_service",
    )
    parser.add_argument(
        "--telemetry-opt-out",
        help="Opts out of telemetry data collection",
        action="store_true",
    )
    parser.add_argument(
        "--yes",
        "-y",
        help="Confirms the installation and skips the interactive confirmation prompt.",
        action="store_true",
        dest="confirmed",
    )
    parser.add_argument(
        "--vfs-install-path",
        help="Absolute path for the install location of the deadline vfs.",
    )

    if sys.platform == "win32":
        parser.add_argument(
            "--password",
            help="The password for the AWS Deadline Cloud Worker Agent user. Defaults to generating a password.",
            required=False,
            default=None,
        )

    return parser
