# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from pathlib import Path
from typing import Literal

import pytest

try:
    from tomllib import load as load_toml
except ModuleNotFoundError:
    from tomli import load as load_toml

from deadline_worker_agent.config.config_file import ModifiableSetting


TEST_CASE_DATA_BASE_DIR = Path(__file__).parent / "data"
INPUT_CONFIG_FILENAME = "input.toml"
EXPECTED_OUTPUT_CONFIG_FILENAME = "output.toml"


@pytest.fixture
def worker_config_path(tmp_path: Path) -> Path:
    return tmp_path / "input.toml"


@pytest.fixture(params=[setting for setting in ModifiableSetting])
def modifiable_setting(request: pytest.FixtureRequest) -> ModifiableSetting:
    return request.param


@pytest.fixture(
    params=[
        "commented",
        "existing",
        "missing",
        "unset",
    ],
)
def scenario(
    request: pytest.FixtureRequest,
) -> Literal["commented", "existing", "missing", "unset"]:
    return request.param


@pytest.fixture
def input_path(
    setting_name: str,
    scenario: Literal["commented", "existing", "missing", "unset"],
) -> str:
    path = TEST_CASE_DATA_BASE_DIR / scenario / setting_name / INPUT_CONFIG_FILENAME
    if not path.is_file():
        raise NotImplementedError(f"No input file at expected path {path}")
    return str(path)


@pytest.fixture
def setting_name(modifiable_setting: ModifiableSetting) -> str:
    return modifiable_setting.value.setting_name


@pytest.fixture
def output_path(
    setting_name: str,
    scenario: Literal["commented", "existing", "missing", "unset"],
) -> str:
    output_path = (
        TEST_CASE_DATA_BASE_DIR / scenario / setting_name / EXPECTED_OUTPUT_CONFIG_FILENAME
    )
    if not output_path.is_file():
        raise NotImplementedError(f"No expected output file path at {output_path}")
    return str(output_path)


@pytest.fixture
def value_to_set(
    setting_name: str,
    output_path: str,
    scenario: Literal["commented", "existing", "missing", "unset"],
) -> str | bool | None:
    if scenario == "unset":
        return None
    with open(output_path, "rb") as f:
        doc = load_toml(f)
        assert len(doc) == 1, f"Only a single section expected, but got {doc}"
        for table in doc.values():
            assert isinstance(table, dict)
            assert setting_name in table
            return table[setting_name]
    return None
