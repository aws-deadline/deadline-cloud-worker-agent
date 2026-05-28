# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""SessionRuntime abstraction: ABC, kinds, config, and factory.

Defines the seam between the worker agent and the underlying OpenJD session
implementation. Concrete adapters (PythonSessionRuntime, RustSessionRuntime)
implement this ABC; ``create_session_runtime`` selects between them based on
the configured ``RuntimeKind``.

This module is import-light by design: adapter modules are imported lazily
inside the factory so callers that never request a particular adapter don't
pay the cost (or risk the failure) of importing it.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Optional

if TYPE_CHECKING:
    from openjd.sessions import (
        ActionStatus,
        EnvironmentIdentifier,
        EnvironmentModel,
        PathMappingRule,
        SessionUser,
        StepScriptModel,
    )

__all__ = [
    "ActionCallback",
    "RuntimeKind",
    "SessionRuntime",
    "SessionRuntimeConfig",
    "create_session_runtime",
]


# Matches openjd.sessions.SessionCallbackType: Callable[[session_id, ActionStatus], None]
ActionCallback = Callable[[str, "ActionStatus"], None]


class RuntimeKind(str, Enum):
    """Identifies a SessionRuntime implementation.

    The string values are user-facing (CLI flags, config files) and must
    remain stable. Subclassing ``str`` lets these values flow through
    config layers without manual conversion.
    """

    PYTHON = "python"
    RUST = "rust"
    SERVICE_SELECTED = "service-selected"


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


class SessionRuntime(ABC):
    """Abstract surface every session backend implements.

    Mirrors ``openjd.sessions.Session``'s public surface 1:1.
    """

    @abstractmethod
    def enter_environment(
        self,
        *,
        environment: EnvironmentModel,
        identifier: Optional[EnvironmentIdentifier] = None,
        os_env_vars: Optional[dict[str, str]] = None,
    ) -> EnvironmentIdentifier:
        """Enter an environment; returns its identifier."""
        ...

    @abstractmethod
    def exit_environment(
        self,
        *,
        identifier: EnvironmentIdentifier,
        os_env_vars: Optional[dict[str, str]] = None,
        keep_session_running: bool = False,
    ) -> None:
        """Exit a previously entered environment."""
        ...

    @abstractmethod
    def run_task(
        self,
        *,
        step_script: StepScriptModel,
        task_parameter_values: dict[str, Any],
        os_env_vars: Optional[dict[str, str]] = None,
        log_task_banner: bool = True,
    ) -> None:
        """Run a task within the session's active environment(s)."""
        ...

    @abstractmethod
    def run_task_without_session_env(
        self,
        *,
        step_script: StepScriptModel,
        task_parameter_values: dict[str, Any],
        os_env_vars: Optional[dict[str, str]] = None,
        log_task_banner: bool = True,
    ) -> None:
        """Run a task without entering a session environment (attachment-sync path).

        Maps to upstream's private ``_run_task_without_session_env``.
        """
        ...

    @abstractmethod
    def cancel_action(
        self,
        *,
        time_limit: Optional[timedelta] = None,
        mark_action_failed: bool = False,
    ) -> None:
        """Cancel the currently running action (env enter/exit, run_task, etc.)."""
        ...

    @abstractmethod
    def cleanup(self) -> None:
        """Tear down the runtime and release any external resources."""
        ...

    @property
    @abstractmethod
    def working_directory(self) -> Path:
        """The session's working directory on disk."""
        ...

    @property
    @abstractmethod
    def action_status(self) -> Optional[ActionStatus]:
        """Status of the most recent action; ``None`` if no action has run yet."""
        ...


def create_session_runtime(kind: RuntimeKind, config: SessionRuntimeConfig) -> SessionRuntime:
    """Construct a SessionRuntime for the given kind.

    Args:
        kind: Which runtime backend to construct.
        config: Construction arguments forwarded to the adapter.

    Raises:
        ValueError: ``kind`` is not a recognised RuntimeKind.
        NotImplementedError: the adapter module for ``kind`` cannot be imported
            (e.g. the Rust binding is unavailable on this platform). Callers
            that want graceful degradation should catch this and fall back.
    """
    if not isinstance(kind, RuntimeKind):
        raise ValueError(f"Unknown RuntimeKind: {kind!r}")

    if kind is RuntimeKind.SERVICE_SELECTED:
        raise ValueError(
            "SERVICE_SELECTED must be resolved to PYTHON or RUST via "
            "select_runtime() before calling create_session_runtime()"
        )

    if kind is RuntimeKind.PYTHON:
        try:
            from .python import PythonSessionRuntime  # type: ignore[import-not-found]
        except ImportError as exc:
            raise NotImplementedError(
                f"PythonSessionRuntime adapter is not available: {exc}"
            ) from exc
        return PythonSessionRuntime(config)

    if kind is RuntimeKind.RUST:
        try:
            from .rust import RustSessionRuntime  # type: ignore[import-not-found]
        except ImportError as exc:
            raise NotImplementedError(
                f"RustSessionRuntime adapter is not available: {exc}"
            ) from exc
        return RustSessionRuntime(config)

    # Defensive: enum membership checked above, but new variants without a
    # branch should fail loudly rather than silently fall through.
    raise ValueError(f"Unhandled RuntimeKind: {kind!r}")
