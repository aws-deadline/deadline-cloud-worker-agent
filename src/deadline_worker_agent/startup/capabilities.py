# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from copy import deepcopy
from typing import Any, Literal, TYPE_CHECKING
from openjd.model import validate_attribute_capability_name, validate_amount_capability_name
from openjd.model.v2023_09 import STANDARD_ATTRIBUTE_CAPABILITIES, STANDARD_AMOUNT_CAPABILITIES
import logging
import platform
import shutil
import subprocess

from pydantic import BaseModel, NonNegativeFloat, PositiveFloat
import psutil

from ..errors import ConfigurationError

if TYPE_CHECKING:
    from pydantic.typing import CallableGenerator


_logger = logging.getLogger(__name__)


def detect_system_capabilities() -> Capabilities:
    amounts: dict[AmountCapabilityName, PositiveFloat] = {}
    attributes: dict[AttributeCapabilityName, list[str]] = {}

    # Determine OpenJobDescription OS
    platform_system = platform.system().lower()
    python_system_to_openjd_os_family = {
        "darwin": "macos",
        "linux": "linux",
        "windows": "windows",
    }
    if openjd_os_family := python_system_to_openjd_os_family.get(platform_system):
        attributes[AttributeCapabilityName("attr.worker.os.family")] = [openjd_os_family]

    attributes[AttributeCapabilityName("attr.worker.cpu.arch")] = [_get_arch()]

    amounts[AmountCapabilityName("amount.worker.vcpu")] = float(psutil.cpu_count())
    amounts[AmountCapabilityName("amount.worker.memory")] = float(psutil.virtual_memory().total) / (
        1024.0**2
    )
    amounts[AmountCapabilityName("amount.worker.disk.scratch")] = int(
        shutil.disk_usage("/").free // 1024 // 1024
    )
    amounts[AmountCapabilityName("amount.worker.gpu")] = _get_gpu_count()
    amounts[AmountCapabilityName("amount.worker.gpu.memory")] = _get_gpu_memory()

    return Capabilities(amounts=amounts, attributes=attributes)


def _get_arch() -> str:
    # Determine OpenJobDescription architecture
    python_machine_to_openjd_arch = {
        "x86_64": "x86_64",
        "aarch64": "arm64",
        "amd64": "x86_64",
    }
    platform_machine = platform.machine().lower()
    return python_machine_to_openjd_arch.get(platform_machine, platform_machine)


def _get_gpu_count(*, verbose: bool = True) -> int:
    """
    Get the number of GPUs available on the machine.

    Returns
    -------
    int
        The number of GPUs available on the machine.
    """
    try:
        output = subprocess.check_output(
            ["nvidia-smi", "--query-gpu=count", "--format=csv,noheader"]
        )
    except FileNotFoundError:
        if verbose:
            _logger.warning("Could not detect GPU count, nvidia-smi not found")
        return 0
    except subprocess.CalledProcessError:
        if verbose:
            _logger.warning("Could not detect GPU count, error running nvidia-smi")
        return 0
    except PermissionError:
        if verbose:
            _logger.warning(
                "Could not detect GPU count, permission denied trying to run nvidia-smi"
            )
        return 0
    except Exception:
        if verbose:
            _logger.warning("Could not detect GPU count, unexpected error running nvidia-smi")
        return 0
    else:
        if verbose:
            _logger.info("Number of GPUs: %s", output.decode().strip())
        return int(output.decode().strip())


def _get_gpu_memory(*, verbose: bool = True) -> int:
    """
    Get the total GPU memory available on the machine.

    Returns
    -------
    int
        The total GPU memory available on the machine.
    """
    try:
        output = subprocess.check_output(
            ["nvidia-smi", "--query-gpu=memory.total", "--format=csv,noheader"]
        )
    except FileNotFoundError:
        if verbose:
            _logger.warning("Could not detect GPU memory, nvidia-smi not found")
        return 0
    except subprocess.CalledProcessError:
        if verbose:
            _logger.warning("Could not detect GPU memory, error running nvidia-smi")
        return 0
    except PermissionError:
        if verbose:
            _logger.warning(
                "Could not detect GPU memory, permission denied trying to run nvidia-smi"
            )
        return 0
    except Exception:
        if verbose:
            _logger.warning("Could not detect GPU memory, unexpected error running nvidia-smi")
        return 0
    else:
        if verbose:
            _logger.info("Total GPU Memory: %s", output.decode().strip())
        return int(output.decode().strip().replace("MiB", ""))


def capability_type(capability_name_str: str) -> Literal["amount", "attr"]:
    no_prefix_capability_name_str = capability_name_str
    if ":" in capability_name_str:
        _, _, no_prefix_capability_name_str = capability_name_str.partition(":")
    if no_prefix_capability_name_str.startswith("amount."):
        return "amount"
    elif no_prefix_capability_name_str.startswith("attr."):
        return "attr"
    else:
        raise ConfigurationError(
            f"Capability names must begin with 'amount.' or 'attr.', but got '{capability_name_str}]"
        )


class CapabilityName(str):
    @classmethod
    def __get_validators__(cls) -> CallableGenerator:
        yield cls._validate_min_length
        yield cls._validate_max_length

    @classmethod
    def _validate_min_length(cls, value: Any) -> str:
        if not isinstance(value, str):
            raise ValueError(f"Capability names must be strings. -- {value}")
        if not value:
            raise ValueError("Capability names cannot be the empty string.")
        return value

    @classmethod
    def _validate_max_length(cls, value: Any) -> str:
        if not isinstance(value, str):
            raise ValueError(f"Capability names must be strings. -- {value}")
        if len(value) > 100:
            raise ValueError(
                f"Capability names must not exceed 100 characters in length. '{value}' is {len(value)} characters long."
            )
        return value


class AmountCapabilityName(CapabilityName):
    @classmethod
    def __get_validators__(cls) -> CallableGenerator:
        yield from super().__get_validators__()
        yield cls._validate_amount_capability_name

    @classmethod
    def _validate_amount_capability_name(cls, value: Any) -> str:
        if not isinstance(value, str):
            raise ValueError("Capability names must be strings.")
        validate_amount_capability_name(
            capability_name=value, standard_capabilities=list(STANDARD_AMOUNT_CAPABILITIES.keys())
        )
        return value


class AttributeCapabilityName(CapabilityName):
    @classmethod
    def __get_validators__(cls) -> CallableGenerator:
        yield from super().__get_validators__()
        yield cls._validate_attribute_capability_name

    @classmethod
    def _validate_attribute_capability_name(cls, value: Any) -> str:
        if not isinstance(value, str):
            raise ValueError("Capability names must be strings.")
        validate_attribute_capability_name(
            capability_name=value,
            standard_capabilities=list(STANDARD_ATTRIBUTE_CAPABILITIES.keys()),
        )
        return value


class Capabilities(BaseModel):
    """The Worker capabilities"""

    amounts: dict[AmountCapabilityName, NonNegativeFloat]
    attributes: dict[AttributeCapabilityName, list[str]]

    def for_update_worker(self) -> dict[str, Any]:
        """Returns a dict representation of the capabilities in the format of the "capabilities
        field expected required for calling UpdateWorker API.

        Returns
        -------
        dict[str, dict[str, float | list[str]]]
            A dict as expected in the "capabilities" field of the UpdateWorker request.
        """

        return {
            "amounts": [{"name": name, "value": value} for name, value in self.amounts.items()],
            "attributes": [
                {"name": name, "values": values} for name, values in self.attributes.items()
            ],
        }

    def merge(self, other: Capabilities) -> Capabilities:
        amounts = self.amounts.copy()
        attributes = deepcopy(self.attributes)

        amounts.update(other.amounts)
        attributes.update(other.attributes)

        return Capabilities(
            amounts=amounts,
            attributes=attributes,
        )
