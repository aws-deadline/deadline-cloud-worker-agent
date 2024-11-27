# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for the deadline_worker_agent.capabilities module"""

from typing import Any
from unittest.mock import MagicMock, patch
import pytest
import subprocess

from pydantic import ValidationError

from deadline_worker_agent.capabilities import Capabilities
from deadline_worker_agent import capabilities as capabilities_mod


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(
            {"amounts": {}, "attributes": {}},
            id="empty fields",
        ),
        pytest.param(
            {
                "amounts": {
                    "amount.slots": 20,
                    "deadline:amount.pets": 99,
                },
                "attributes": {
                    "attr.groups": ["simulation"],
                    "acmewidgetsco:attr.admins": ["bob", "alice"],
                },
            },
            id="full fields",
        ),
    ],
)
def test_input_validation_success(data: dict[str, Any]) -> None:
    """Asserts that a valid input dictionary passes Capabilities model validation"""
    Capabilities.parse_obj(data)


@pytest.mark.parametrize(
    "data",
    [
        pytest.param({}, id="missing amounts and attributes"),
        pytest.param({"attributes": {}}, id="missing amounts"),
        pytest.param({"amounts": {}}, id="missing attributes"),
        pytest.param(
            {"amounts": {"amount": 20}, "attributes": {}},
            id="nonvalid amounts - a dictionary key is nonvalid (no segment)",
        ),
        pytest.param(
            {"amounts": {"amount.0seg": 20}, "attributes": {}},
            id="nonvalid amounts - a dictionary key is nonvalid (nonvalid segment)",
        ),
        pytest.param(
            {"amounts": {"not_amount.slots": 20}, "attributes": {}},
            id="nonvalid amounts - a dictionary key is nonvalid (nonvalid capability name)",
        ),
        pytest.param(
            {"amounts": {"amount.slots": -20}, "attributes": {}},
            id="nonvalid amounts - a dictionary value is not NonNegativeFloat",
        ),
        pytest.param(
            {"amounts": {}, "attributes": {"attr": ["a", "b"]}},
            id="nonvalid attributes  - a dictionary key is nonvalid (no segment)",
        ),
        pytest.param(
            {"amounts": {}, "attributes": {"attr.(seg)": ["a", "b"]}},
            id="nonvalid attributes  - a dictionary key is nonvalid (nonvalid segment)",
        ),
        pytest.param(
            {"amounts": {}, "attributes": {"not_attr.groups": ["a", "b"]}},
            id="nonvalid attributes  - a dictionary key is nonvalid (nonvalid capability name)",
        ),
        pytest.param(
            {"amounts": {}, "attributes": {"attr.groups": "a"}},
            id="nonvalid attributes  - a dictionary value is not list[str]",
        ),
    ],
)
def test_input_validation_failure(data: dict[str, Any]) -> None:
    """Tests that an nonvalid input dictionary fails Capabilities model validation"""
    # WHEN
    with pytest.raises(ValidationError) as excinfo:
        Capabilities.parse_obj(data)

    # THEN
    assert len(excinfo.value.errors()) > 0


def test_for_update_worker() -> None:
    """Ensures that Capabilities.for_update_worker() returns a dictionary representation of
    the capabilities in the format expected in the UpdateWorkerState API request, for example:

    {
        "amounts": [
            {
                "name": "amount.cap1",
                "value": 1
            },
            // ...
        ],
        "attributes": [
            {
                "name": "attr.cap2",
                "values": [
                    "a",
                    // ...
                ]
            },
            // ...
        ]
    }
    """
    # GIVEN
    capabilities = Capabilities(
        amounts={
            "amount.first": 12,
            "vendora:amount.second": 1,
        },
        attributes={
            "attr.first": ["a", "b"],
            "vendorb:attr.second": ["g"],
        },
    )

    # WHEN
    result = capabilities.for_update_worker()

    # THEN
    assert result == {
        "amounts": [
            {
                "name": "amount.first",
                "value": 12,
            },
            {
                "name": "vendora:amount.second",
                "value": 1,
            },
        ],
        "attributes": [
            {
                "name": "attr.first",
                "values": ["a", "b"],
            },
            {
                "name": "vendorb:attr.second",
                "values": ["g"],
            },
        ],
    }


@pytest.mark.parametrize(
    argnames=("lhs", "rhs", "expected_result"),
    argvalues=(
        pytest.param(
            Capabilities(amounts={"amount.a": 1}, attributes={"attr.b": ["a", "b"]}),
            Capabilities(amounts={"amount.b": 2}, attributes={"attr.a": ["c"]}),
            Capabilities(
                amounts={"amount.a": 1, "amount.b": 2},
                attributes={"attr.b": ["a", "b"], "attr.a": ["c"]},
            ),
            id="disjoint",
        ),
        pytest.param(
            Capabilities(amounts={"amount.a": 1}, attributes={"attr.b": ["a", "b"]}),
            Capabilities(amounts={"amount.a": 2}, attributes={"attr.b": ["c"]}),
            Capabilities(amounts={"amount.a": 2}, attributes={"attr.b": ["c"]}),
            id="overlapping",
        ),
        pytest.param(
            Capabilities(
                amounts={"amount.a": 1, "amount.b": 99},
                attributes={"attr.a": ["z"], "attr.b": ["a", "b"]},
            ),
            Capabilities(amounts={"amount.a": 2}, attributes={"attr.b": ["c"]}),
            Capabilities(
                amounts={"amount.a": 2, "amount.b": 99},
                attributes={"attr.a": ["z"], "attr.b": ["c"]},
            ),
            id="partially-overlapping",
        ),
    ),
)
def test_merge(
    lhs: Capabilities,
    rhs: Capabilities,
    expected_result: Capabilities,
) -> None:
    """Tests that Capabilities.merge(...) correctly merges two Capabilities instances. This should
    return a new Capabilities instance and values from the LHS should be replaced (if existing) with
    values from the RHS."""
    # WHEN
    result = lhs.merge(rhs)

    # THEN
    assert result == expected_result


@pytest.mark.parametrize(
    argnames=("platform_machine", "expected_arch"),
    argvalues=(
        pytest.param("x86_64", "x86_64", id="intel-x86-64bit"),
        pytest.param("amd64", "x86_64", id="amd-x86-64bit"),
        pytest.param("arm64", "arm64", id="macos-arm"),
        pytest.param("aarch64", "arm64", id="macos-arm"),
    ),
)
def test_get_arch(
    platform_machine: str,
    expected_arch: str,
) -> None:
    """Tests that the _get_arch() function returns the correctly mapped value from
    platform.machine()"""

    # GIVEN
    with patch.object(capabilities_mod.platform, "machine", return_value=platform_machine):
        # WHEN
        arch = capabilities_mod._get_arch()

    # THEN
    assert arch == expected_arch


class TestGetGPUCount:
    @patch.object(capabilities_mod.subprocess, "check_output")
    def test_get_gpu_count(
        self,
        check_output_mock: MagicMock,
    ) -> None:
        """
        Tests that the _get_gpu_count function returns the correct number of GPUs
        """
        # GIVEN
        check_output_mock.return_value = b"2"

        # WHEN
        result = capabilities_mod._get_gpu_count()

        # THEN
        check_output_mock.assert_called_once_with(
            ["nvidia-smi", "--query-gpu=count", "-i=0", "--format=csv,noheader"]
        )
        assert result == 2

    @pytest.mark.parametrize(
        ("exception", "expected_result"),
        (
            pytest.param(FileNotFoundError("nvidia-smi not found"), 0, id="FileNotFoundError"),
            pytest.param(subprocess.CalledProcessError(1, "command"), 0, id="CalledProcessError"),
            pytest.param(PermissionError("Permission denied"), 0, id="PermissionError"),
            pytest.param(Exception("something went wrong"), 0, id="OSError"),
        ),
    )
    @patch.object(capabilities_mod.subprocess, "check_output")
    def test_get_gpu_count_nvidia_smi_error(
        self,
        check_output_mock: MagicMock,
        exception: Exception,
        expected_result: int,
    ) -> None:
        """
        Tests that the _get_gpu_count function returns 0 when nvidia-smi is not found or fails
        """
        # GIVEN
        check_output_mock.side_effect = exception

        # WHEN
        result = capabilities_mod._get_gpu_count()

        # THEN
        check_output_mock.assert_called_once_with(
            ["nvidia-smi", "--query-gpu=count", "-i=0", "--format=csv,noheader"]
        )

        assert result == expected_result


class TestGetGPUMemory:
    @patch.object(capabilities_mod.subprocess, "check_output")
    def test_get_gpu_memory(
        self,
        check_output_mock: MagicMock,
    ) -> None:
        """
        Tests that the _get_gpu_memory function returns total memory
        """
        # GIVEN
        check_output_mock.return_value = b"6800 MiB"

        # WHEN
        result = capabilities_mod._get_gpu_memory()

        # THEN
        check_output_mock.assert_called_once_with(
            ["nvidia-smi", "--query-gpu=memory.total", "--format=csv,noheader"]
        )
        assert result == 6800

    @patch.object(capabilities_mod.subprocess, "check_output")
    def test_get_multi_gpu_memory(
        self,
        check_output_mock: MagicMock,
    ) -> None:
        """
        Tests that the _get_gpu_memory function returns the minimum total memory among all GPUs
        reported by nvidia-smi.
        """
        # GIVEN
        check_output_mock.return_value = b"6800 MiB\n1200MiB"

        # WHEN
        result = capabilities_mod._get_gpu_memory()

        # THEN
        check_output_mock.assert_called_once_with(
            ["nvidia-smi", "--query-gpu=memory.total", "--format=csv,noheader"]
        )
        assert result == 1200

    @pytest.mark.parametrize(
        ("exception", "expected_result"),
        (
            pytest.param(FileNotFoundError("nvidia-smi not found"), 0, id="FileNotFoundError"),
            pytest.param(subprocess.CalledProcessError(1, "command"), 0, id="CalledProcessError"),
            pytest.param(PermissionError("Permission denied"), 0, id="PermissionError"),
            pytest.param(Exception("something went wrong"), 0, id="OSError"),
        ),
    )
    @patch.object(capabilities_mod.subprocess, "check_output")
    def test_get_gpu_memory_nvidia_smi_error(
        self,
        check_output_mock: MagicMock,
        exception: Exception,
        expected_result: int,
    ) -> None:
        """
        Tests that the _get_gpu_memory function returns 0 when nvidia-smi is not found or fails
        """
        # GIVEN
        check_output_mock.side_effect = exception

        # WHEN
        result = capabilities_mod._get_gpu_memory()

        # THEN
        check_output_mock.assert_called_once_with(
            ["nvidia-smi", "--query-gpu=memory.total", "--format=csv,noheader"]
        )

        assert result == expected_result
