# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from deadline_worker_agent.sessions.runtime import SessionRuntimeKind


class TestSessionRuntimeKind:
    def test_runtime_kind_values(self) -> None:
        assert SessionRuntimeKind.PYTHON.value == "python"
        assert SessionRuntimeKind.RUST.value == "rust"
        assert isinstance(SessionRuntimeKind.PYTHON, str)
        assert isinstance(SessionRuntimeKind.RUST, str)
