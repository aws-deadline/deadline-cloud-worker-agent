# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from typing import Any

import pytest

from deadline_worker_agent.sessions.runtime import SessionRuntime, SessionRuntimeConfig


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
