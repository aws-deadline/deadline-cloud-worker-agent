# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from typing import Optional
from argparse import ArgumentParser, Namespace
from pathlib import Path
from subprocess import CalledProcessError, run
import sys
import sysconfig


INSTALLER_PATH = {
    "linux": Path(__file__).parent / "install.sh",
    "win32": Path(__file__).parent / "install.ps1",
}


def install() -> None:
    """Installer entrypoint for the Amazon Deadline Cloud Worker Agent"""

    if sys.platform not in ["linux", "win32"]:
        print(f"ERROR: Unsupported platform {sys.platform}")
        sys.exit(1)

    arg_parser = get_argument_parser()
    args = arg_parser.parse_args(namespace=ParsedCommandLineArguments)
    scripts_path = Path(sysconfig.get_path("scripts"))

    cmd = [
        "sudo" if sys.platform == "linux" else "",
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
        # Non-zero exit code
        sys.exit(error.returncode)


class ParsedCommandLineArguments(Namespace):
    """Represents the parsed installer command-line arguments"""

    farm_id: str
    fleet_id: str
    region: str
    user: str
    group: Optional[str]
    confirmed: bool
    service_start: bool
    allow_shutdown: bool
    install_service: bool
    telemetry_opt_out: bool
    vfs_install_path: str


def get_argument_parser() -> ArgumentParser:  # pragma: no cover
    """Returns a command-line argument parser for the Amazon Deadline Cloud Worker Agent"""
    parser = ArgumentParser(
        prog="install-deadline-worker",
        description="Installer for the Amazon Deadline Cloud Worker Agent",
    )
    parser.add_argument(
        "--farm-id",
        help="The Amazon Deadline Cloud Farm ID that the Worker belongs to.",
        required=True,
    )
    parser.add_argument(
        "--fleet-id",
        help="The Amazon Deadline Cloud Fleet ID that the Worker belongs to.",
        required=True,
    )
    parser.add_argument(
        "--region",
        help='The AWS region of the Amazon Deadline Cloud farm. Defaults to "us-west-2".',
        default="us-west-2",
    )
    parser.add_argument(
        "--user",
        help='The username of the Amazon Deadline Cloud Worker Agent user. Defaults to "deadline-worker-agent".',
        default="deadline-worker-agent",
    )
    parser.add_argument(
        "--group",
        help='The POSIX group that is shared between the Agent user and the user(s) that jobs run as. Defaults to "deadline-job-users".',
    )
    parser.add_argument(
        "--start",
        help="Starts the systemd service immediately. Defaults to start on system boot. This option is ignored if --no-install-service is used.",
        action="store_true",
        dest="service_start",
    )
    parser.add_argument(
        "--allow-shutdown",
        help="Controls whether to create/delete a sudoers rule allowing the worker agent OS user to"
        "shutdown the system",
        action="store_true",
    )
    parser.add_argument(
        "--no-install-service",
        help="Skips the worker agent systemd service installation",
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

    return parser
