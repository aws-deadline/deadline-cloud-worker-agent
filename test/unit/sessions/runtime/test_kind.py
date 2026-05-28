# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from deadline_worker_agent.sessions.runtime import RuntimeKind


class TestRuntimeKind:
    def test_runtime_kind_values(self) -> None:
        assert RuntimeKind.PYTHON.value == "python"
        assert RuntimeKind.RUST.value == "rust"
        assert isinstance(RuntimeKind.PYTHON, str)
        assert isinstance(RuntimeKind.RUST, str)
