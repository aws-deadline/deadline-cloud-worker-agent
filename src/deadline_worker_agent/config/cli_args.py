# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from argparse import ArgumentParser, Namespace
from pathlib import Path
import os


class ParsedCommandLineArguments(Namespace):
    """Represents the parsed AWS Deadline Cloud Worker Agent command-line arguments"""

    farm_id: str | None = None
    fleet_id: str | None = None
    cleanup_session_user_processes: bool | None = None
    profile: str | None = None
    verbose: bool | None = None
    no_shutdown: bool | None = None
    run_jobs_as_agent_user: bool | None = None
    posix_job_user: str | None = None
    windows_job_user: str | None = None
    disallow_instance_profile: bool | None = None
    logs_dir: Path | None = None
    local_session_logs: bool | None = None
    persistence_dir: Path | None = None
    retain_session_dir: bool | None = None
    host_metrics_logging: bool | None = None
    host_metrics_logging_interval_seconds: float | None = None
    structured_logs: bool | None = None
    session_root_dir: Path | None = None


def get_argument_parser() -> ArgumentParser:
    """Returns a command-line argument parser for the AWS Deadline Cloud Worker Agent"""
    parser = ArgumentParser(
        prog="deadline-worker-agent", description="AWS Deadline Cloud Worker Agent"
    )
    parser.add_argument(
        "--farm-id",
        help="The AWS Deadline Cloud Farm identifier that the Worker should register to",
        default=None,
    )
    parser.add_argument(
        "--fleet-id",
        help="The AWS Deadline Cloud Fleet identifier that the Worker should register to",
        default=None,
    )
    parser.add_argument(
        "--no-cleanup-session-user-processes",
        help="Whether to cleanup leftover processes running as a session user when that user is no longer being used in any active session",
        dest="cleanup_session_user_processes",
        action="store_const",
        const=False,
        default=None,
    )
    parser.add_argument(
        "--profile",
        help="The AWS profile to use",
        default=None,
    )
    parser.add_argument(
        "--no-shutdown",
        help="Does not shutdown the instance during scale-in event.",
        action="store_const",
        const=True,
        default=None,
    )
    parser.add_argument(
        "--run-jobs-as-agent-user",
        help="If set, then all Jobs' session actions will run as the same user as the agent. WARNING: this is insecure - for development use only.",
        action="store_const",
        const=True,
        dest="run_jobs_as_agent_user",
        default=None,
    )
    if os.name == "posix":
        parser.add_argument(
            "--posix-job-user",
            help="Overrides the posix user that the Worker Agent impersonates. Format: 'user:group'. "
            "If not set, defaults to what the service sets.",
            default=None,
        )
    elif os.name == "nt":
        parser.add_argument(
            "--windows-job-user",
            help="Overrides the windows user that the Worker Agent impersonates. In doing so, resets the specified user's password to a cryptographically random, unstored value during worker startup. "
            "If not set, impersonation behavior defers to what the service sets.",
            default=None,
        )

    parser.add_argument(
        "--logs-dir",
        help="Overrides the directory where the Worker Agent writes its logs.",
        default=None,
        type=Path,
    )
    parser.add_argument(
        "--no-local-session-logs",
        help="Turns off writing of session logs to the local filesystem",
        dest="local_session_logs",
        action="store_const",
        const=False,
        default=None,
    )
    parser.add_argument(
        "--persistence-dir",
        help="Overrides the directory where the Worker Agent persists files across restarts.",
        default=None,
        type=Path,
    )
    parser.add_argument(
        "--disallow-instance-profile",
        help="Turns on validation that the host EC2 instance profile is disassociated before starting",
        action="store_const",
        const=True,
        dest="disallow_instance_profile",
        default=None,
    )
    parser.add_argument(
        "--host-metrics-logging-interval-seconds",
        help="The interval between host metrics log messages. Default is 60.",
        default=None,
        type=float,
    )
    parser.add_argument(
        "--no-host-metrics-logging",
        help="Turn off host metrics logging. Default is on.",
        action="store_const",
        dest="host_metrics_logging",
        const=False,
        default=None,
    )
    parser.add_argument(
        "--verbose",
        "-v",
        help="Use verbose console logging",
        action="store_const",
        const=True,
        default=None,
    )
    parser.add_argument(
        "--retain-session-dir",
        help="Retain the session directory on completion",
        dest="retain_session_dir",
        action="store_const",
        const=True,
        default=None,
    )
    parser.add_argument(
        "--structured-logs",
        help="Enable structured logging for the Agent's stdout and local file logs.",
        dest="structured_logs",
        action="store_const",
        const=True,
        default=None,
    )
    parser.add_argument(
        "--session-root-dir",
        help="Path to the directory where session directories are created under.",
        default=None,
        type=Path,
    )
    return parser
