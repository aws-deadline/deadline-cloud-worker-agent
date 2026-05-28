# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path
from types import ModuleType
from typing import Any, Optional

import pytest

from deadline_worker_agent.sessions.runtime import (
    RuntimeKind,
    SessionRuntime,
    SessionRuntimeConfig,
    create_session_runtime,
)


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


def _make_complete_runtime_subclass() -> type[SessionRuntime]:
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
        ) -> None:
            return None

        def run_task_without_session_env(
            self,
            *,
            step_script: Any = None,
            task_parameter_values: dict[str, Any] | None = None,
            os_env_vars: Optional[dict[str, str]] = None,
            log_task_banner: bool = True,
        ) -> None:
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


class TestRuntimeKind:
    def test_runtime_kind_values(self) -> None:
        assert RuntimeKind.PYTHON.value == "python"
        assert RuntimeKind.RUST.value == "rust"
        # str subclass — values compare equal to plain strings
        assert isinstance(RuntimeKind.PYTHON, str)
        assert isinstance(RuntimeKind.RUST, str)


class TestSessionRuntimeConfig:
    def test_session_runtime_config_defaults(self, runtime_config: SessionRuntimeConfig) -> None:
        assert runtime_config.spec_revision == "2023-09"
        assert runtime_config.supported_extensions == ()

    def test_session_runtime_config_is_frozen(self, runtime_config: SessionRuntimeConfig) -> None:
        with pytest.raises(AttributeError):
            runtime_config.session_id = "mutated"  # type: ignore[misc]


class TestSessionRuntimeABC:
    def test_session_runtime_is_abstract(self) -> None:
        with pytest.raises(TypeError, match="abstract"):
            SessionRuntime()  # type: ignore[abstract]

    @pytest.mark.parametrize(
        "missing_method",
        [
            "enter_environment",
            "exit_environment",
            "run_task",
            "run_task_without_session_env",
            "cancel_action",
            "cleanup",
            "working_directory",
            "action_status",
        ],
    )
    def test_session_runtime_subclass_must_implement_all_methods(self, missing_method: str) -> None:
        """A subclass missing any single abstract cannot be instantiated."""
        base_cls = _make_complete_runtime_subclass()

        # Build a class that inherits from SessionRuntime but copies all methods
        # from the stub EXCEPT the missing one
        namespace: dict[str, Any] = {
            k: v
            for k, v in base_cls.__dict__.items()
            if k != missing_method and not k.startswith("__")
        }
        incomplete_cls = type("Incomplete", (SessionRuntime,), namespace)

        with pytest.raises(TypeError, match="abstract"):
            incomplete_cls()  # type: ignore[abstract]

    def test_session_runtime_complete_subclass_can_be_instantiated(
        self, runtime_config: SessionRuntimeConfig
    ) -> None:
        cls = _make_complete_runtime_subclass()
        instance = cls(runtime_config)  # type: ignore[call-arg]
        assert isinstance(instance, SessionRuntime)


class TestCreateSessionRuntime:
    def test_create_session_runtime_invalid_kind_raises_value_error(
        self, runtime_config: SessionRuntimeConfig
    ) -> None:
        with pytest.raises(ValueError, match="Unknown RuntimeKind"):
            create_session_runtime("python", runtime_config)  # type: ignore[arg-type]

    def test_create_session_runtime_service_selected_raises_value_error(
        self, runtime_config: SessionRuntimeConfig
    ) -> None:
        with pytest.raises(ValueError, match="SERVICE_SELECTED must be resolved"):
            create_session_runtime(RuntimeKind.SERVICE_SELECTED, runtime_config)

    def test_create_session_runtime_python_kind_not_implemented_when_module_missing(
        self, runtime_config: SessionRuntimeConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setitem(sys.modules, "deadline_worker_agent.sessions.runtime.python", None)
        with pytest.raises(NotImplementedError, match="PythonSessionRuntime"):
            create_session_runtime(RuntimeKind.PYTHON, runtime_config)

    def test_create_session_runtime_rust_not_implemented_when_module_missing(
        self, runtime_config: SessionRuntimeConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setitem(sys.modules, "deadline_worker_agent.sessions.runtime.rust", None)
        with pytest.raises(NotImplementedError, match="RustSessionRuntime"):
            create_session_runtime(RuntimeKind.RUST, runtime_config)

    def test_create_session_runtime_python_kind_returns_adapter(
        self, runtime_config: SessionRuntimeConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        stub_cls = _make_complete_runtime_subclass()
        fake_module = ModuleType("deadline_worker_agent.sessions.runtime.python")
        fake_module.PythonSessionRuntime = stub_cls  # type: ignore[attr-defined]
        monkeypatch.setitem(
            sys.modules, "deadline_worker_agent.sessions.runtime.python", fake_module
        )

        result = create_session_runtime(RuntimeKind.PYTHON, runtime_config)
        assert isinstance(result, SessionRuntime)
        assert isinstance(result, stub_cls)
        assert result._config is runtime_config  # type: ignore[attr-defined]

    def test_create_session_runtime_rust_kind_returns_adapter(
        self, runtime_config: SessionRuntimeConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        stub_cls = _make_complete_runtime_subclass()
        fake_module = ModuleType("deadline_worker_agent.sessions.runtime.rust")
        fake_module.RustSessionRuntime = stub_cls  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "deadline_worker_agent.sessions.runtime.rust", fake_module)

        result = create_session_runtime(RuntimeKind.RUST, runtime_config)
        assert isinstance(result, SessionRuntime)
        assert isinstance(result, stub_cls)
        assert result._config is runtime_config  # type: ignore[attr-defined]

