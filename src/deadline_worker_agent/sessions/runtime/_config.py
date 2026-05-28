# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Optional

if TYPE_CHECKING:
    from openjd.sessions import (
        ActionStatus,
        PathMappingRule,
        SessionUser,
    )

# Matches openjd.sessions.SessionCallbackType: Callable[[session_id, ActionStatus], None]
ActionCallback = Callable[[str, "ActionStatus"], None]


@dataclass(frozen=True)
class SessionRuntimeConfig:
    """Construction arguments for a SessionRuntime.

    Mirrors ``openjd.sessions.Session``'s constructor surface but expressed in
    primitives that any backend (Python, Rust) can translate at its boundary.
    ``spec_revision`` and ``supported_extensions`` carry OpenJD revision info
    that each adapter converts into its native types.
    """

    session_id: str
    job_parameter_values: dict[str, Any]
    path_mapping_rules: Optional[list[PathMappingRule]]
    retain_working_dir: bool
    user: Optional[SessionUser]
    action_callback: ActionCallback
    os_env_vars: Optional[dict[str, str]]
    session_root_directory: Path
    spec_revision: str = "2023-09"
    supported_extensions: tuple[str, ...] = ()
