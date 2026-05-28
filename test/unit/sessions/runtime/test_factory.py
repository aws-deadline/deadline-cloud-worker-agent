# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import sys
from types import ModuleType

import pytest

from deadline_worker_agent.sessions.runtime import (
    SessionRuntimeKind,
    SessionRuntime,
    SessionRuntimeConfig,
    create_session_runtime,
)


class TestCreateSessionRuntime:
    def test_service_selected_raises_value_error(
        self, runtime_config: SessionRuntimeConfig
    ) -> None:
        with pytest.raises(ValueError, match="SERVICE_SELECTED must be resolved"):
            create_session_runtime(SessionRuntimeKind.SERVICE_SELECTED, runtime_config)

    def test_python_not_implemented_when_module_missing(
        self, runtime_config: SessionRuntimeConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setitem(sys.modules, "deadline_worker_agent.sessions.runtime.python", None)
        with pytest.raises(NotImplementedError, match="PythonSessionRuntime"):
            create_session_runtime(SessionRuntimeKind.PYTHON, runtime_config)

    def test_rust_not_implemented_when_module_missing(
        self, runtime_config: SessionRuntimeConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setitem(sys.modules, "deadline_worker_agent.sessions.runtime.rust", None)
        with pytest.raises(NotImplementedError, match="RustSessionRuntime"):
            create_session_runtime(SessionRuntimeKind.RUST, runtime_config)

    def test_python_returns_adapter(
        self,
        runtime_config: SessionRuntimeConfig,
        stub_runtime_cls: type[SessionRuntime],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake_module = ModuleType("deadline_worker_agent.sessions.runtime.python")
        fake_module.PythonSessionRuntime = stub_runtime_cls  # type: ignore[attr-defined]
        monkeypatch.setitem(
            sys.modules, "deadline_worker_agent.sessions.runtime.python", fake_module
        )

        result = create_session_runtime(SessionRuntimeKind.PYTHON, runtime_config)
        assert isinstance(result, SessionRuntime)
        assert isinstance(result, stub_runtime_cls)
        assert result._config is runtime_config  # type: ignore[attr-defined]

    def test_rust_returns_adapter(
        self,
        runtime_config: SessionRuntimeConfig,
        stub_runtime_cls: type[SessionRuntime],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake_module = ModuleType("deadline_worker_agent.sessions.runtime.rust")
        fake_module.RustSessionRuntime = stub_runtime_cls  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "deadline_worker_agent.sessions.runtime.rust", fake_module)

        result = create_session_runtime(SessionRuntimeKind.RUST, runtime_config)
        assert isinstance(result, SessionRuntime)
        assert isinstance(result, stub_runtime_cls)
        assert result._config is runtime_config  # type: ignore[attr-defined]
