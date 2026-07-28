# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from typing import Any

import pytest

from deadline_worker_agent.sessions.runtime import SessionRuntime, SessionRuntimeConfig
from deadline_worker_agent.sessions.runtime._abc import (
    SessionRuntimeCrashError,
    convert_runtime_crashes,
)


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
            "_run_task_without_session_env",
            "cancel_action",
            "cleanup",
            "working_directory",
            "action_status",
        ],
    )
    def test_session_runtime_subclass_must_implement_all_methods(
        self, missing_method: str, stub_runtime_cls: type[SessionRuntime]
    ) -> None:
        """A subclass missing any single abstract cannot be instantiated."""
        namespace: dict[str, Any] = {
            k: v
            for k, v in stub_runtime_cls.__dict__.items()
            if k != missing_method and not k.startswith("__")
        }
        incomplete_cls = type("Incomplete", (SessionRuntime,), namespace)

        with pytest.raises(TypeError, match="abstract"):
            incomplete_cls()  # type: ignore[abstract]

    def test_session_runtime_complete_subclass_can_be_instantiated(
        self,
        runtime_config: SessionRuntimeConfig,
        stub_runtime_cls: type[SessionRuntime],
    ) -> None:
        instance = stub_runtime_cls(runtime_config)  # type: ignore[call-arg]
        assert isinstance(instance, SessionRuntime)


class _FakePanic(BaseException):
    """Stand-in for pyo3_runtime.PanicException (a BaseException subclass)."""


class TestConvertRuntimeCrashes:
    """Tests for the convert_runtime_crashes adapter-boundary decorator."""

    def test_converts_base_exception_to_crash_error(self) -> None:
        """A BaseException (e.g. a Rust panic) is converted, with the original
        preserved as the cause."""

        @convert_runtime_crashes
        def method() -> None:
            raise _FakePanic("panicked at 'index out of bounds'")

        with pytest.raises(SessionRuntimeCrashError, match="_FakePanic") as exc_info:
            method()
        assert isinstance(exc_info.value.__cause__, _FakePanic)

    @pytest.mark.parametrize(
        "exc",
        [
            pytest.param(ValueError("regular error"), id="exception"),
            pytest.param(KeyboardInterrupt(), id="keyboard_interrupt"),
            pytest.param(SystemExit(1), id="system_exit"),
        ],
    )
    def test_other_exceptions_propagate_unchanged(self, exc: BaseException) -> None:
        """Regular Exceptions and interpreter control-flow exceptions are untouched."""

        @convert_runtime_crashes
        def method() -> None:
            raise exc

        with pytest.raises(type(exc)) as exc_info:
            method()
        assert exc_info.value is exc

    def test_return_value_passes_through(self) -> None:
        @convert_runtime_crashes
        def method() -> str:
            return "ok"

        assert method() == "ok"
