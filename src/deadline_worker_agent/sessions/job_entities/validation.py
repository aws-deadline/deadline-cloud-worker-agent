# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from typing import Any, NamedTuple, Sequence, Type


class Field(NamedTuple):
    """Class holding field-level validation meta-data"""

    key: str
    """The JSON object key of the field"""
    expected_type: Type
    """The expected runtime type of the field's value"""
    required: bool
    """Whether the field is required to be present"""
    fields: Sequence[Field] | None = None
    """Sub-fields for object types"""


def validate_object(
    *,
    data: dict[str, Any],
    fields: Sequence[Field],
) -> None:
    expected_field_names = {field.key for field in fields}
    for obj_field in fields:
        if (value := data.get(obj_field.key, None)) is None:
            if obj_field.required:
                raise ValueError(f'Required field "{obj_field.key}" not found')
        elif not isinstance(value, obj_field.expected_type):
            raise ValueError(
                f'Expected {obj_field.expected_type} for field "{obj_field.key}" but got {type(value)}'
            )
        elif obj_field.fields:
            if obj_field.expected_type is dict:
                validate_object(data=value, fields=obj_field.fields)
            elif obj_field.expected_type is list:
                for element in value:
                    if isinstance(element, dict):
                        validate_object(data=element, fields=obj_field.fields)

    unexpected_fields = data.keys() - expected_field_names
    if unexpected_fields:
        unexpected_fields_str = ", ".join(
            f'"{unexpected_field}"' for unexpected_field in sorted(unexpected_fields)
        )
        raise ValueError(f"Unexpected fields: {unexpected_fields_str}")
