# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from typing import Optional, Any
from argparse import ArgumentParser, Namespace
from pathlib import Path
from subprocess import CalledProcessError, run
import re
import requests
import sys
import sysconfig

from deadline_worker_agent.config.settings import (
    DEFAULT_POSIX_SESSION_ROOT_DIR,
    DEFAULT_WINDOWS_SESSION_ROOT_DIR,
)


if sys.platform == "win32":
    from deadline_worker_agent.installer.win_installer import (
        start_windows_installer,
        InstallerFailedException,
    )


INSTALLER_PATH = {
    "linux": Path(__file__).parent / "install.sh",
}


def _get_ec2_region() -> Optional[str]:
    """
    Gets the AWS region if running on EC2 by querying IMDS.
    Returns None if region could not be detected.
    """
    try:
        # Create IMDSv2 token
        token_response = requests.put(
            url="http://169.254.169.254/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "10"},  # 10 second expiry
        )
        token = token_response.text
        if not token:
            raise RuntimeError("Received empty IMDSv2 token")

        # Get AZ
        az_response = requests.get(
            url="http://169.254.169.254/latest/meta-data/placement/availability-zone",
            headers={"X-aws-ec2-metadata-token": token},
        )
        az = az_response.text
    except Exception as e:
        print(f"Failed to detect AWS region: {e}")
        return None
    else:
        if not az:
            print("AWS region could not be detected, received empty response from IMDS")
            return None

        match = re.match(r"^([a-z-]+-[0-9])([a-z])?$", az)
        if not match:
            print(
                f"AWS region could not be detected, got unexpected availability zone from IMDS: {az}"
            )
            return None

        return match.group(1)


def install() -> None:
    """Installer entrypoint for the AWS Deadline Cloud Worker Agent"""

    if sys.platform not in ["linux", "win32"]:
        print(f"ERROR: Unsupported platform {sys.platform}")
        sys.exit(1)

    arg_parser = get_argument_parser()
    args = arg_parser.parse_args(namespace=ParsedCommandLineArguments())
    scripts_path = Path(sysconfig.get_path("scripts"))

    if args.region is None:
        args.region = _get_ec2_region()
        if args.region is None:
            print("ERROR: Unable to detect AWS region. Please provide a value for --region.")
            sys.exit(1)

    if sys.platform == "win32":
        installer_args: dict[str, Any] = dict(
            farm_id=args.farm_id,
            fleet_id=args.fleet_id,
            region=args.region,
            install_service=args.install_service,
            start_service=args.service_start,
            confirm=args.confirmed,
            allow_shutdown=args.allow_shutdown,
            parser=arg_parser,
            grant_required_access=args.grant_required_access,
            allow_ec2_instance_profile=not args.disallow_instance_profile,
            session_root_dir=args.session_root_dir,
        )
        if args.user:
            installer_args.update(user_name=args.user)
        if args.group:
            installer_args.update(group_name=args.group)
        if args.password:
            installer_args.update(password=args.password)
        if args.telemetry_opt_out:
            installer_args.update(telemetry_opt_out=args.telemetry_opt_out)
        if args.windows_job_user:
            installer_args.update(windows_job_user=args.windows_job_user)

        try:
            start_windows_installer(**installer_args)
        except InstallerFailedException as e:
            print(f"ERROR: {e}")
            sys.exit(1)
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
            "--python-interpreter-path",
            sys.executable,
            "--session-root-dir",
            str(args.session_root_dir),
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
        if args.disallow_instance_profile:
            cmd.append("--disallow-instance-profile")

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
    region: Optional[str] = None
    user: str
    password: Optional[str] = None
    group: Optional[str] = None
    confirmed: bool
    service_start: bool
    allow_shutdown: bool
    install_service: bool
    telemetry_opt_out: bool
    vfs_install_path: str
    grant_required_access: bool
    disallow_instance_profile: bool
    windows_job_user: Optional[str] = None
    session_root_dir: Path


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
        help=(
            "The AWS region of the AWS Deadline Cloud farm. "
            "If on EC2, this is optional and the region will be automatically detected. Otherwise, this option is required."
        ),
        default=None,
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
    parser.add_argument(
        "--disallow-instance-profile",
        help=(
            "Disallow running the worker agent with an EC2 instance profile. When this is provided, the worker "
            "agent makes requests to the EC2 instance meta-data service (IMDS) to check for an instance profile. "
            "If an instance profile is detected, the worker agent will stop and exit. When this is not provided, "
            "the worker agent no longer performs these checks, allowing it to run with an EC2 instance profile."
        ),
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--session-root-dir",
        help="The root directory under which the worker agent creates session directories",
        type=Path,
        default=(
            str(DEFAULT_WINDOWS_SESSION_ROOT_DIR)
            if sys.platform == "win32"
            else str(DEFAULT_POSIX_SESSION_ROOT_DIR)
        ),  # pragma: nocover
    )

    if sys.platform == "win32":
        parser.add_argument(
            "--password",
            help=(
                "The password for the AWS Deadline Cloud Worker Agent user. Defaults to generating a password "
                "if the user does not exist or prompting for the password if the user pre-exists."
            ),
            required=False,
            default=None,
        )
        parser.add_argument(
            "--grant-required-access",
            help=(
                "Allows the installer to modify an existing user so that it can successfully run the worker agent. This will allow "
                "the installer to add the user to the Administrators group and grant any missing user rights which are required to "
                "run the worker agent. This option has no effect if a new user is created by the installer."
            ),
            action="store_true",
            required=False,
            default=False,
        )
        parser.add_argument(
            "--windows-job-user",
            help=(
                "The username of the Windows user that jobs run as. The password for this user account is reset during worker startup."
            ),
            required=False,
            default=None,
        )

    return parser
