# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from openjd.expr import PathMappingRule
    from openjd.sessions import EnvironmentIdentifier
    from openjd.sessions._v1 import ActionStatus


class SessionRuntime(ABC):
    """Abstract surface every session backend implements.

    Environments and step scripts cross this interface as OpenJD wire-format
    JSON dicts (the service's own vocabulary) rather than as decoded models:
    each backend decodes with its native library, so no decoded object ever
    needs translating between the v0 and _v1 type systems. Scalar values
    (parameters, path-mapping rules, action statuses) are the _v1 native types.
    """

    @abstractmethod
    def enter_environment(
        self,
        *,
        environment: dict[str, Any],
        identifier: Optional[EnvironmentIdentifier] = None,
        os_env_vars: Optional[dict[str, str]] = None,
    ) -> EnvironmentIdentifier:
        """Enter an environment; returns its identifier.

        ``environment`` is the wire-format JSON dict of the OpenJD environment
        (the value of a template's ``environment`` key).
        """
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
        step_script: dict[str, Any],
        task_parameter_values: dict[str, dict[str, Any]],
        os_env_vars: Optional[dict[str, str]] = None,
        log_task_banner: bool = True,
    ) -> None:
        """Run a task within the session's active environment(s).

        ``step_script`` is the wire-format JSON dict of the OpenJD step script
        (the value of a step template's ``script`` key).
        """
        ...

    @abstractmethod
    def _run_task_without_session_env(
        self,
        *,
        step_script: dict[str, Any],
        task_parameter_values: dict[str, dict[str, Any]],
        os_env_vars: Optional[dict[str, str]] = None,
        log_task_banner: bool = True,
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


class SessionRuntimeDecodeError(Exception):
    """A wire-format JSON document failed to decode at a runtime boundary.

    Raised by adapters when the environment or step-script JSON handed across
    the SessionRuntime interface is rejected by the backend's decoder. Names
    the boundary so the failure is diagnosable from a session log without a
    traceback (decode failures otherwise surface in customer-visible progress
    messages with nothing pointing at the conversion hop).
    """

    def __init__(self, *, boundary: str, cause: Exception) -> None:
        self.boundary = boundary
        super().__init__(f"Failed to decode OpenJD document at {boundary}: {cause}")
