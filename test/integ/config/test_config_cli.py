# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
This test module contains end-to-end tests that cover using deadline_worker_agent.config module
using its command-line interface
"""

from __future__ import annotations
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Callable, Literal

import pytest

from deadline_worker_agent.config import config_file


def cli_args_for_allow_ec2_instance_profile(value: str | bool | None) -> list[str]:
    if value is None:
        return []
    elif isinstance(value, bool):
        return ["--allow-ec2-instance-profile" if value else "--no-allow-ec2-instance-profile"]
    else:
        raise NotImplementedError(f"Unexpected value: {value}")


def cli_args_for_farm_id(value: str | bool | None) -> list[str]:
    if value is None:
        return []
    elif isinstance(value, str):
        return ["--farm-id", value]
    else:
        raise NotImplementedError(f"Unexpected value: {value}")


def cli_args_for_fleet_id(value: str | bool | None) -> list[str]:
    if value is None:
        return []
    elif isinstance(value, str):
        return ["--fleet-id", value]
    else:
        raise NotImplementedError(f"Unexpected value: {value}")


def cli_args_for_windows_job_user(value: str | bool | None) -> list[str]:
    if value is None:
        return ["--no-windows-job-user"]
    elif isinstance(value, str):
        return ["--windows-job-user", value]
    else:
        raise NotImplementedError(f"Unexpected value: {value}")


def cli_args_for_shutdown_on_stop(value: str | bool | None) -> list[str]:
    if value is None:
        return []
    elif value is True:
        return ["--shutdown-on-stop"]
    elif value is False:
        return ["--no-shutdown-on-stop"]
    else:
        raise NotImplementedError(f"Unexpected value: {value}")


SETTING_TO_CLI_ARGS: dict[
    config_file.ModifiableSetting, Callable[[str | bool | None], list[str]]
] = {
    config_file.ModifiableSetting.ALLOW_EC2_INSTANCE_PROFILE: cli_args_for_allow_ec2_instance_profile,
    config_file.ModifiableSetting.FARM_ID: cli_args_for_farm_id,
    config_file.ModifiableSetting.FLEET_ID: cli_args_for_fleet_id,
    config_file.ModifiableSetting.WINDOWS_JOB_USER: cli_args_for_windows_job_user,
    config_file.ModifiableSetting.SHUTDOWN_ON_STOP: cli_args_for_shutdown_on_stop,
}


@pytest.fixture
def value_to_set_cli_args(
    modifiable_setting: config_file.ModifiableSetting,
    value_to_set: str | bool | None,
) -> list[str]:
    try:
        setting_to_cli_args = SETTING_TO_CLI_ARGS[modifiable_setting]
    except KeyError:
        raise NotImplementedError(f"Unhandled setting: {modifiable_setting.name}") from None

    return setting_to_cli_args(value_to_set)


class TestMissingExistingCommented:
    @pytest.fixture(params=["missing", "existing", "commented"])
    def scenario(self, request: pytest.FixtureRequest) -> str:
        """We constrain these tests to the missing / existing / commented tests. The "unset"
        scenario is a special case and handled in the class below.
        """
        return request.param

    def test(
        self,
        input_path: str,
        output_path: str,
        worker_config_path: Path,
        value_to_set_cli_args: list[str],
    ) -> None:
        """Tests that when using deadline_worker_agent.config as a CLI module that the setting is
        updated correctly.
        """
        # GIVEN
        with open(output_path, "r") as f:
            expected_output = f.read()
        shutil.copyfile(input_path, worker_config_path)
        cmd = [
            sys.executable,
            "-m",
            "deadline_worker_agent.config",
            "--config-path",
            str(worker_config_path),
            *value_to_set_cli_args,
        ]

        # WHEN
        subprocess.run(cmd, check=True)

        # THEN
        written_config = worker_config_path.read_text()
        assert expected_output == written_config


class TestUnsetCommentsOutInputSetting:
    """Tests that when an there is an existing active setting in the TOML file that is being
    unset, that the setting becomes commented out and all TOML comments and blank space are
    preserved.

    This relies on conventional directory structure relative to the parent directory of this
    test module:

    data/
        unset/
            <SETTING_NAME>/
                input.toml
                output.toml

    Where <SETTING_NAME> is the lower-case match of each enum value in
    deadline_worker_agent.config.config_file.ModifiableSetting.

    The input.toml file serves as the existing input config file. The output.toml is the
    expected output file to be generated with the setting denoted by <SETTING_NAME> commented
    out.
    """

    @pytest.fixture
    def modifiable_setting(self) -> config_file.ModifiableSetting:
        return config_file.ModifiableSetting.WINDOWS_JOB_USER

    @pytest.fixture
    def value_to_set_cli_args(self) -> list[str]:
        return ["--no-windows-job-user"]

    @pytest.fixture
    def scenario(self) -> Literal["commented", "existing", "missing", "unset"]:
        return "unset"

    def test(
        self,
        input_path: str,
        output_path: str,
        setting_name: str,
        worker_config_path: Path,
        value_to_set_cli_args: list[str],
    ) -> None:
        """The functional test. See class docstring"""
        # GIVEN
        with open(output_path, "r") as f:
            expected_output = f.read()
        shutil.copyfile(input_path, worker_config_path)
        cmd = [
            sys.executable,
            "-m",
            "deadline_worker_agent.config",
            "--config-path",
            str(worker_config_path),
            *value_to_set_cli_args,
        ]

        # WHEN
        subprocess.run(cmd, check=True)

        # THEN
        written_config = worker_config_path.read_text()
        assert expected_output == written_config
