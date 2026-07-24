# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Optional

if TYPE_CHECKING:
    from openjd.expr import PathMappingRule
    from openjd.sessions import SessionUser
    from openjd.sessions._v1 import ActionStatus

# Matches the _v1 session callback shape: Callable[[session_id, ActionStatus], None]
ActionCallback = Callable[[str, "ActionStatus"], None]


@dataclass(frozen=True)
class SessionRuntimeConfig:
    """Construction arguments for a SessionRuntime.

    The interface is expressed in OpenJD _v1 (native) types and wire-format
    values: the Rust runtime consumes them directly, and the Python runtime
    bridges them to the v0 library types at its boundary. When the Python
    runtime is retired, the bridge is deleted with it and no conversion layer
    remains.

    ``job_parameter_values`` carries wire-format ``{"type", "value"}`` dicts
    rather than _v1 parameter-value objects: the wire form is total over the
    v0 type domain (e.g. CHUNK_INT, which the _v1 parameter-type enums do not
    define), so no parameter that works today changes behavior.

    ``user`` is the one deliberate v0 hold-over and still carries
    ``openjd.sessions.SessionUser``: the worker's OS-user machinery is
    v0-native and the _v1 ``WindowsSessionUser`` validates the logon at
    construction, so converting at the producer and back in the Python adapter
    would round-trip live credentials through a third type system for no
    benefit. It moves to the _v1 type when the worker's user handling does.
    """

    session_id: str
    job_parameter_values: dict[str, dict[str, Any]]
    path_mapping_rules: Optional[list[PathMappingRule]]
    retain_working_dir: bool
    user: Optional[SessionUser]
    action_callback: ActionCallback
    os_env_vars: Optional[dict[str, str]]
    session_root_directory: Path
    spec_revision: str = "2023-09"
    supported_extensions: tuple[str, ...] = ()
