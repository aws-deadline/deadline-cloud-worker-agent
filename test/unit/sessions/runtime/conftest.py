# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any, Optional

import pytest

from deadline_worker_agent.sessions.runtime import SessionRuntime, SessionRuntimeConfig


@pytest.fixture()
def runtime_config() -> SessionRuntimeConfig:
    """Minimal valid SessionRuntimeConfig for testing."""
    return SessionRuntimeConfig(
        session_id="session-1",
        job_parameter_values={},
        path_mapping_rules=None,
        retain_working_dir=False,
        user=None,
        action_callback=lambda session_id, status: None,
        os_env_vars=None,
        session_root_directory=Path("/tmp/sessions/session-1"),
    )


def _make_stub_runtime_class() -> type[SessionRuntime]:
    """Return a concrete SessionRuntime subclass with all abstracts stubbed."""

    class _StubRuntime(SessionRuntime):
        def __init__(self, config: SessionRuntimeConfig) -> None:
            self._config = config

        def enter_environment(
            self,
            *,
            environment: Any = None,
            identifier: Any = None,
            os_env_vars: Optional[dict[str, str]] = None,
        ) -> str:
            return "env-id"

        def exit_environment(
            self,
            *,
            identifier: Any = None,
            os_env_vars: Optional[dict[str, str]] = None,
            keep_session_running: bool = False,
        ) -> None:
            return None

        def run_task(
            self,
            *,
            step_script: Any = None,
            task_parameter_values: dict[str, Any] | None = None,
            os_env_vars: Optional[dict[str, str]] = None,
            log_task_banner: bool = True,
            step_name: str | None = None,
        ) -> None:
            return None

        def _run_task_without_session_env(
            self,
            *,
            step_script: Any = None,
            task_parameter_values: dict[str, Any] | None = None,
            os_env_vars: Optional[dict[str, str]] = None,
            log_task_banner: bool = True,
            step_name: str | None = None,
        ) -> None:
            return None

        def extend_path_mapping_rules(self, rules: list[Any]) -> None:
            return None

        def cancel_action(
            self,
            *,
            time_limit: Optional[timedelta] = None,
            mark_action_failed: bool = False,
        ) -> None:
            return None

        def cleanup(self) -> None:
            return None

        @property
        def working_directory(self) -> Path:
            return Path("/tmp")

        @property
        def action_status(self) -> None:
            return None

    return _StubRuntime


@pytest.fixture()
def stub_runtime_cls() -> type[SessionRuntime]:
    """A complete SessionRuntime subclass for testing."""
    return _make_stub_runtime_class()
