# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Transitional producer-side conversions from worker (v0) values to the
SessionRuntime interface's native (_v1 / wire-format) vocabulary.

The worker's job-entity layer still decodes and stores OpenJD v0 types. Until
it produces interface-native values directly, these helpers convert at the
point where a ``SessionRuntimeConfig`` is built. Delete this module when the
job-entity layer moves off the v0 library.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from openjd.expr import PathFormat, PathMappingRule

if TYPE_CHECKING:
    from openjd.model import ParameterValue
    from openjd.sessions import PathMappingRule as V0PathMappingRule

__all__ = [
    "to_interface_parameter_values",
    "to_interface_path_mapping_rule",
    "to_interface_path_mapping_rules",
]


# openjd.expr.PathFormat is a non-constructable Rust enum, so map the v0
# rule's format string to the member by its value.
_PATH_FORMATS: dict[str, PathFormat] = {
    "POSIX": PathFormat.POSIX,
    "WINDOWS": PathFormat.WINDOWS,
}


def to_interface_parameter_values(
    parameter_values: dict[str, ParameterValue],
) -> dict[str, dict[str, Any]]:
    """Convert v0 ParameterValue objects into wire-format ``{"type", "value"}`` dicts.

    The wire dict form is total over the v0 type domain (including types like
    CHUNK_INT that the _v1 binding's parameter-type enums do not define), so no
    parameter that works today changes behavior. Each runtime decodes the dict
    with its own library.
    """
    return {
        name: {"type": value.type.value, "value": value.value}
        for name, value in parameter_values.items()
    }


def to_interface_path_mapping_rule(rule: V0PathMappingRule) -> PathMappingRule:
    """Convert one v0 PathMappingRule into the interface's _v1 type. Paths are
    coerced to strings, which the _v1 rule stores."""
    try:
        source_path_format = _PATH_FORMATS[rule.source_path_format.value]
    except KeyError:
        raise ValueError(
            f"Unrecognized v0 path format: {rule.source_path_format.value!r}"
        ) from None
    return PathMappingRule(
        source_path_format=source_path_format,
        source_path=str(rule.source_path),
        destination_path=str(rule.destination_path),
    )


def to_interface_path_mapping_rules(
    rules: Optional[list[V0PathMappingRule]],
) -> Optional[list[PathMappingRule]]:
    """Convert v0 PathMappingRule lists into the interface's _v1 type, passing
    None through."""
    if rules is None:
        return None
    return [to_interface_path_mapping_rule(rule) for rule in rules]


def _exit_code_to_i32(exitcode: int) -> int:
    """Reinterpret an exit code as a 32-bit signed integer.

    On Windows, subprocess exit codes are commonly unsigned 32-bit values
    (e.g. 0xC0000005 for access violation).  The v1 ActionStatus (backed by
    Rust i32) and the UpdateWorkerSchedule API both require a signed 32-bit
    value, so we truncate to the low 32 bits and reinterpret as signed.
    """
    as_uint32_bytes = (exitcode & 0xFFFFFFFF).to_bytes(4, "big", signed=False)
    return int.from_bytes(as_uint32_bytes, "big", signed=True)
