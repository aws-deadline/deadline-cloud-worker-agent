# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for the Worker Agent configuration"""

from __future__ import annotations
from argparse import ArgumentParser

import pytest
import os

from deadline_worker_agent.config import cli_args as cli_args_mod


class TestArgumentParser:
    """Tests for the get_argument_parser"""

    @pytest.fixture
    def arg_parser(self):
        return cli_args_mod.get_argument_parser()

    def test_defaults(self, arg_parser: ArgumentParser) -> None:
        """Tests that when no arguments are supplied, the defaults values returned"""
        # GIVEN
        args: list[str] = []

        # WHEN
        result = arg_parser.parse_args(args, namespace=cli_args_mod.ParsedCommandLineArguments())

        # THEN
        assert result.disallow_instance_profile is None
        assert result.farm_id is None
        assert result.fleet_id is None
        assert result.host_metrics_logging is None
        assert result.host_metrics_logging_interval_seconds is None
        assert result.local_session_logs is None
        assert result.logs_dir is None
        assert result.no_shutdown is None
        assert result.persistence_dir is None
        assert result.profile is None
        assert result.run_jobs_as_agent_user is None
        assert result.retain_session_dir is None
        assert result.structured_logs is None
        assert result.verbose is None
        assert result.posix_job_user is None
        assert result.windows_job_user is None

    @pytest.mark.parametrize(
        ["farm_id"],
        (
            ("abc",),
            ("def",),
        ),
    )
    def test_farm_id(self, arg_parser: ArgumentParser, farm_id: str) -> None:
        """Asserts that the --farm-id command-line argument is parsed"""
        # GIVEN
        args = ["--farm-id", farm_id]

        # WHEN
        result = arg_parser.parse_args(args, namespace=cli_args_mod.ParsedCommandLineArguments())

        # THEN
        assert result.farm_id == farm_id

    @pytest.mark.parametrize(
        ["fleet_id"],
        (
            ("abc",),
            ("def",),
        ),
    )
    def test_fleet_id(self, arg_parser: ArgumentParser, fleet_id: str) -> None:
        """Asserts that the --fleet-id command-line argument is parsed"""
        # GIVEN
        args = ["--fleet-id", fleet_id]

        # WHEN
        result = arg_parser.parse_args(args, namespace=cli_args_mod.ParsedCommandLineArguments())

        # THEN
        assert result.fleet_id == fleet_id

    @pytest.mark.parametrize(
        ["profile"],
        (
            ("a",),
            ("b",),
        ),
    )
    def test_profile(self, arg_parser: ArgumentParser, profile: str) -> None:
        """Asserts that the --profile command-line argument is parsed"""
        # GIVEN
        args = ["--profile", profile]

        # WHEN
        result = arg_parser.parse_args(args, namespace=cli_args_mod.ParsedCommandLineArguments())

        # THEN
        assert result.profile == profile

    @pytest.mark.parametrize(
        ("verbose",),
        (
            ("-v",),
            ("--verbose",),
        ),
    )
    def test_verbose(self, arg_parser: ArgumentParser, verbose: str) -> None:
        """Asserts that the -v / --verbose command-line argument is parsed"""
        # GIVEN
        args = [verbose]

        # WHEN
        result = arg_parser.parse_args(args, namespace=cli_args_mod.ParsedCommandLineArguments())

        # THEN
        assert result.verbose is True

    @pytest.mark.parametrize(
        ("args", "expected"),
        (
            pytest.param(["--no-shutdown"], True, id="NoShutdownPresent"),
            pytest.param([], None, id="NoShutdownAbsent"),
        ),
    )
    def test_no_shutdown(
        self, arg_parser: ArgumentParser, args: list[str], expected: bool | None
    ) -> None:
        """Asserts that the --no-shutdown command-line argument is parsed"""
        # WHEN
        result = arg_parser.parse_args(args, namespace=cli_args_mod.ParsedCommandLineArguments())

        # THEN
        assert result.no_shutdown == expected

    @pytest.mark.parametrize(
        ("args", "expected_cleanup_session_user_processes"),
        (
            pytest.param(
                ["--no-cleanup-session-user-processes"],
                False,
                id="NoCleanupSessionUserProcessPresent",
            ),
            pytest.param([], None, id="NoCleanupSessionUserProcessesAbsent"),
        ),
    )
    def test_no_cleanup_session_user_processes(
        self,
        arg_parser: ArgumentParser,
        args: list[str],
        expected_cleanup_session_user_processes: bool | None,
    ) -> None:
        """Asserts that the --no-cleanup-session-user-processes command-line argument is parsed"""
        # WHEN
        result = arg_parser.parse_args(args, namespace=cli_args_mod.ParsedCommandLineArguments())

        # THEN
        assert result.cleanup_session_user_processes == expected_cleanup_session_user_processes

    @pytest.mark.skipif(os.name != "nt", reason="Windows only test")
    @pytest.mark.parametrize(
        ["windows_job_user"],
        (
            ("job user a",),
            ("job user b",),
        ),
    )
    def test_windows_job_user(self, arg_parser: ArgumentParser, windows_job_user: str) -> None:
        """Asserts that the --windows-job-user command-line argument is parsed"""
        # GIVEN
        args = ["--windows-job-user", windows_job_user]

        # WHEN
        result = arg_parser.parse_args(args, namespace=cli_args_mod.ParsedCommandLineArguments())

        # THEN
        assert result.windows_job_user == windows_job_user
