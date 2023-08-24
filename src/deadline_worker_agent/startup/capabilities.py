# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from copy import deepcopy
from typing import Any, Literal, TYPE_CHECKING
from openjobio.model.v2022_09_01 import CapabilityName

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


class AmountCapabilityName(CapabilityName):
    @classmethod
    def __get_validators__(cls) -> CallableGenerator:
        yield from super().__get_validators__()
        yield cls._validate_amount_capability_name

    @classmethod
    def _validate_amount_capability_name(cls, value: str) -> AmountCapabilityName:
        cap_type = capability_type(value)

        if not cap_type == "amount":
            raise ValueError(
                f"Capability name ({value}) is not an amount capability type ({cap_type})"
            )

        return AmountCapabilityName(value)


class AttributeCapabilityName(CapabilityName):
    @classmethod
    def __get_validators__(cls) -> CallableGenerator:
        yield from super().__get_validators__()
        yield cls._validate_attribute_capability_name

    @classmethod
    def _validate_attribute_capability_name(cls, value: str) -> AttributeCapabilityName:
        cap_type = capability_type(value)

        if not cap_type == "attr":
            raise ValueError(
                f"Capability name ({value}) is not an attribute capability type ({cap_type})"
            )

        return AttributeCapabilityName(value)


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
