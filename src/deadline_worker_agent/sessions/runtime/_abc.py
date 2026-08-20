# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import timedelta
from functools import wraps
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Optional, TypeVar

if TYPE_CHECKING:
    from openjd.sessions import (
        ActionStatus,
        EnvironmentIdentifier,
        EnvironmentModel,
        PathMappingRule,
        StepScriptModel,
    )

_F = TypeVar("_F", bound=Callable[..., Any])


class SessionRuntimeCrashError(Exception):
    """A non-Exception error from a session runtime (e.g. a Rust panic crossing
    the PyO3 boundary as a BaseException) converted to a regular exception so
    the session's existing failure handling engages (report FAILED + cleanup)
    instead of the session thread dying silently."""


def convert_runtime_crashes(method: _F) -> _F:
    """Converts BaseException escapes from a runtime adapter method into
    SessionRuntimeCrashError. Regular Exceptions and interpreter control-flow
    exceptions (KeyboardInterrupt, SystemExit) propagate unchanged."""

    @wraps(method)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return method(*args, **kwargs)
        except (Exception, KeyboardInterrupt, SystemExit):
            raise
        except BaseException as e:
            raise SessionRuntimeCrashError(f"session runtime crashed: {type(e).__name__}") from e

    return wrapper  # type: ignore[return-value]


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
        resolved_symbol_table_json: str | None = None,
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
        resolved_symbol_table_json: str | None = None,
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
        step_name: str | None = None,
        resolved_symbol_table_json: str | None = None,
    ) -> None:
        """Run a task within the session's active environment(s)."""
        ...

    @abstractmethod
    def _run_task_without_session_env(
        self,
        *,
        step_script: StepScriptModel,
        task_parameter_values: dict[str, Any],
        os_env_vars: Optional[dict[str, str]] = None,
        log_task_banner: bool = True,
        step_name: str | None = None,
    ) -> None:
        """Run a task without entering a session environment (attachment-sync path)."""
        ...

    @abstractmethod
    def extend_path_mapping_rules(self, rules: list[PathMappingRule]) -> None:
        """Add path mapping rules to the session mid-flight.

        The Rust runtime (v1) exposes this as a public API. The Python runtime
        (v0) lacks it, so the adapter encapsulates the direct attribute access.
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
