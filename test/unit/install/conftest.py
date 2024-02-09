# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from pathlib import Path
from typing import Optional

import pytest

from deadline_worker_agent.installer import (
    ParsedCommandLineArguments,
)
from deadline_worker_agent import installer as installer_mod


VFS_DEFAULT_INSTALL_PATH = "/opt/deadline_vfs"


def test_installer_path(platform: str) -> None:
    """Tests the value of deadline_worker_agent.installer.INSTALLER_PATH"""
    # GIVEN
    if platform == "linux":
        expected_value = Path(installer_mod.__file__).parent / "install.sh"
    else:
        assert False, f"No expected path for platform {platform}"

    # THEN
    assert installer_mod.INSTALLER_PATH[platform] == expected_value


@pytest.fixture
def farm_id() -> str:
    return "farm-123e4567e89b12d3a456426655441234"


@pytest.fixture
def fleet_id() -> str:
    return "fleet-123e4567e89b12d3a456426655444321"


@pytest.fixture
def region() -> str:
    return "us-west-2"


@pytest.fixture
def user() -> str:
    return "wa_user"


# @pytest.fixture(params=("wa_group", None))
@pytest.fixture(params=("wa_group",))
def group(request: pytest.FixtureRequest) -> Optional[str]:
    return request.param


@pytest.fixture
def service_start() -> bool:
    return False


@pytest.fixture
def confirmed() -> bool:
    return True


@pytest.fixture
def allow_shutdown() -> bool:
    return False


@pytest.fixture
def telemetry_opt_out() -> bool:
    return True


@pytest.fixture
def install_service() -> bool:
    return True


@pytest.fixture
def vfs_install_path() -> str:
    return VFS_DEFAULT_INSTALL_PATH


@pytest.fixture
def parsed_args(
    farm_id: str,
    fleet_id: str,
    region: str,
    user: str,
    group: Optional[str],
    service_start: bool,
    confirmed: bool,
    allow_shutdown: bool,
    install_service: bool,
    telemetry_opt_out: bool,
    vfs_install_path: str,
) -> ParsedCommandLineArguments:
    parsed_args = ParsedCommandLineArguments()
    parsed_args.farm_id = farm_id
    parsed_args.fleet_id = fleet_id
    parsed_args.user = user
    parsed_args.group = group
    parsed_args.region = region
    parsed_args.service_start = service_start
    parsed_args.confirmed = confirmed
    parsed_args.allow_shutdown = allow_shutdown
    parsed_args.install_service = install_service
    parsed_args.telemetry_opt_out = telemetry_opt_out
    parsed_args.vfs_install_path = vfs_install_path
    return parsed_args


@pytest.fixture(
    params=("linux",),
)
def platform(request: pytest.FixtureRequest) -> str:
    return request.param
