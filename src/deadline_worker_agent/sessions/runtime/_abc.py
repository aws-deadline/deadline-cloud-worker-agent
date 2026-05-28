# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from openjd.sessions import (
        ActionStatus,
        EnvironmentIdentifier,
        EnvironmentModel,
        StepScriptModel,
    )


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
    def _run_task_without_session_env(
        self,
        *,
        step_script: StepScriptModel,
        task_parameter_values: dict[str, Any],
        os_env_vars: Optional[dict[str, str]] = None,
        log_task_banner: bool = True,
    ) -> None:
        """Run a task without entering a session environment (attachment-sync path)."""
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
