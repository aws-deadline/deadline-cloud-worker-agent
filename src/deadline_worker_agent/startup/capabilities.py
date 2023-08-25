# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from copy import deepcopy
from typing import Any, Literal, TYPE_CHECKING
from openjd.model import validate_attribute_capability_name, validate_amount_capability_name
from openjd.model.v2023_09 import STANDARD_ATTRIBUTE_CAPABILITIES, STANDARD_AMOUNT_CAPABILITIES

from pydantic import BaseModel, NonNegativeFloat

from ..errors import ConfigurationError

if TYPE_CHECKING:
    from pydantic.typing import CallableGenerator


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
