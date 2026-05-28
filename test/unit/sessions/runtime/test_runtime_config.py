# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import pytest

from deadline_worker_agent.sessions.runtime import SessionRuntimeConfig


class TestSessionRuntimeConfig:
    def test_session_runtime_config_defaults(self, runtime_config: SessionRuntimeConfig) -> None:
        assert runtime_config.spec_revision == "2023-09"
        assert runtime_config.supported_extensions == ()

    def test_session_runtime_config_is_frozen(self, runtime_config: SessionRuntimeConfig) -> None:
        with pytest.raises(AttributeError):
            runtime_config.session_id = "mutated"  # type: ignore[misc]
