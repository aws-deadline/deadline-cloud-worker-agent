# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for the WorkerSettings class"""

from __future__ import annotations
from unittest.mock import MagicMock, Mock, patch
from typing import Any, Generator, NamedTuple, Type
import pytest
import os
from pathlib import Path

from pydantic import ConstrainedStr

import deadline_worker_agent.config.settings as settings_mod
from deadline_worker_agent.capabilities import Capabilities
from deadline_worker_agent.config.settings import WorkerSettings


@pytest.fixture(autouse=True)
def mock_config_file_cls() -> Generator[MagicMock, None, None]:
    with patch.object(settings_mod, "ConfigFile", autospec=True) as mock_config_file_cls:
        yield mock_config_file_cls


class FieldTestCaseParams(NamedTuple):
    field_name: str
    expected_type: Type
    expected_required: bool
    expected_default: Any
    expected_default_factory_return_value: Any


FIELD_TEST_CASES: list[FieldTestCaseParams] = [
    FieldTestCaseParams(
        field_name="farm_id",
        expected_type=ConstrainedStr,
        expected_required=True,
        expected_default=None,
        expected_default_factory_return_value=None,
    ),
    FieldTestCaseParams(
        field_name="fleet_id",
        expected_type=ConstrainedStr,
        expected_required=True,
        expected_default=None,
        expected_default_factory_return_value=None,
    ),
    FieldTestCaseParams(
        field_name="cleanup_session_user_processes",
        expected_type=bool,
        expected_required=False,
        expected_default=True,
        expected_default_factory_return_value=None,
    ),
    FieldTestCaseParams(
        field_name="profile",
        expected_type=ConstrainedStr,
        expected_required=False,
        expected_default=None,
        expected_default_factory_return_value=None,
    ),
    FieldTestCaseParams(
        field_name="verbose",
        expected_type=bool,
        expected_required=False,
        expected_default=False,
        expected_default_factory_return_value=None,
    ),
    FieldTestCaseParams(
        field_name="no_shutdown",
        expected_type=bool,
        expected_required=False,
        expected_default=False,
        expected_default_factory_return_value=None,
    ),
    FieldTestCaseParams(
        field_name="run_jobs_as_agent_user",
        expected_type=bool,
        expected_required=False,
        expected_default=False,
        expected_default_factory_return_value=None,
    ),
    FieldTestCaseParams(
        field_name="posix_job_user",
        expected_type=ConstrainedStr,
        expected_required=False,
        expected_default=None,
        expected_default_factory_return_value=None,
    ),
    FieldTestCaseParams(
        field_name="windows_job_user",
        expected_type=ConstrainedStr,
        expected_required=False,
        expected_default=None,
        expected_default_factory_return_value=None,
    ),
    FieldTestCaseParams(
        field_name="windows_job_user_password_arn",
        expected_type=ConstrainedStr,
        expected_required=False,
        expected_default=None,
        expected_default_factory_return_value=None,
    ),
    FieldTestCaseParams(
        field_name="allow_instance_profile",
        expected_type=bool,
        expected_required=False,
        expected_default=True,
        expected_default_factory_return_value=None,
    ),
    FieldTestCaseParams(
        field_name="capabilities",
        expected_type=Capabilities,
        expected_required=False,
        expected_default=None,
        expected_default_factory_return_value=Capabilities(amounts={}, attributes={}),
    ),
    FieldTestCaseParams(
        field_name="worker_logs_dir",
        expected_type=Path,
        expected_required=False,
        expected_default=(
            Path("/var/log/amazon/deadline")
            if os.name == "posix"
            else Path(os.path.expandvars(r"%PROGRAMDATA%/Amazon/Deadline/Logs"))
        ),
        expected_default_factory_return_value=None,
    ),
    FieldTestCaseParams(
        field_name="worker_persistence_dir",
        expected_type=Path,
        expected_required=False,
        expected_default=(
            Path("/var/lib/deadline")
            if os.name == "posix"
            else Path(os.path.expandvars(r"%PROGRAMDATA%/Amazon/Deadline/Cache"))
        ),
        expected_default_factory_return_value=None,
    ),
    FieldTestCaseParams(
        field_name="local_session_logs",
        expected_type=bool,
        expected_required=False,
        expected_default=True,
        expected_default_factory_return_value=None,
    ),
    FieldTestCaseParams(
        field_name="structured_logs",
        expected_type=bool,
        expected_required=False,
        expected_default=False,
        expected_default_factory_return_value=None,
    ),
    FieldTestCaseParams(
        field_name="host_metrics_logging",
        expected_type=bool,
        expected_required=False,
        expected_default=True,
        expected_default_factory_return_value=None,
    ),
    FieldTestCaseParams(
        field_name="host_metrics_logging_interval_seconds",
        expected_type=float,
        expected_required=False,
        expected_default=60,
        expected_default_factory_return_value=None,
    ),
    FieldTestCaseParams(
        field_name="retain_session_dir",
        expected_type=bool,
        expected_required=False,
        expected_default=False,
        expected_default_factory_return_value=None,
    ),
]


@pytest.mark.parametrize(
    argnames="test_case_params",
    argvalues=(
        pytest.param(
            field_test_case_params,
            id=field_test_case_params.field_name,
        )
        for field_test_case_params in FIELD_TEST_CASES
    ),
)
def test_settings_field(test_case_params: FieldTestCaseParams) -> None:
    """Tests that all of the WorkerSettings model fields exist and have the correct configuration
    for:

    1.  type
    2.  default value / default factory return value
    3.  required/optional
    """

    # THEN
    assert test_case_params.field_name in WorkerSettings.__fields__
    field = WorkerSettings.__fields__[test_case_params.field_name]
    # subclass to handle ConstrainedStr vs ConstrainedStrValue
    assert issubclass(field.type_, test_case_params.expected_type)
    assert field.required is test_case_params.expected_required
    assert field.default == test_case_params.expected_default
    if field.default_factory:
        default_factory_return_value = field.default_factory()
        assert (
            default_factory_return_value == test_case_params.expected_default_factory_return_value
        )
    else:
        assert (
            test_case_params.expected_default_factory_return_value is None
        ), f"no default factory for {test_case_params.field_name} but expected one"


def test_settings_field_coverage() -> None:
    # GIVEN
    model_field_test_cases = {test_case.field_name for test_case in FIELD_TEST_CASES}
    model_fields = set(WorkerSettings.__fields__.keys())

    # THEN
    assert model_field_test_cases == model_fields, "Test cases mismatch from model fields"


def test_customize_sources_config_file_exists(
    mock_config_file_cls: MagicMock,
) -> None:
    """Tests that given an existing worker config file, that WorkerSettings is configured to use the
    following sources in order of higher to lower priority:

    1.  kwargs passed to __init__ (cli arguments)
    2.  environment variables
    3.  worker config file
    """

    # GIVEN
    # Mock settings sources passed to pydantic the Config.customize_sources()
    init_settings = Mock()
    env_settings = Mock()
    file_secret_settings = Mock()
    # The config file settings as returned by ConfigFile.load().as_settings()
    config_file_settings: MagicMock = mock_config_file_cls.load().as_settings

    # WHEN
    customized_sources = WorkerSettings.Config.customise_sources(
        init_settings,
        env_settings,
        file_secret_settings,
    )

    # THEN
    assert customized_sources == (
        init_settings,
        env_settings,
        config_file_settings,
    )


def test_customize_sources_config_file_missing(
    mock_config_file_cls: MagicMock,
) -> None:
    """Tests that given a non-existent worker config file, that WorkerSettings is configured to use
    the following sources in order of higher to lower priority:

    1.  kwargs passed to __init__ (cli arguments)
    2.  environment variables
    """

    # GIVEN
    # Mock settings sources passed to pydantic the Config.customize_sources()
    init_settings = Mock()
    env_settings = Mock()
    file_secret_settings = Mock()
    # Mock a FileNotFound error when calling ConfigFile.load()
    config_file_load: MagicMock = mock_config_file_cls.load
    config_file_load.side_effect = FileNotFoundError()

    # WHEN
    customized_sources = WorkerSettings.Config.customise_sources(
        init_settings,
        env_settings,
        file_secret_settings,
    )

    # THEN
    assert customized_sources == (
        init_settings,
        env_settings,
    )
