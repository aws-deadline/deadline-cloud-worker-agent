# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from typing import Any

import pytest
from openjd.model.v2023_09 import ExtensionName

from deadline_worker_agent.sessions._extensions import (
    RUNTIME_CAPABILITY_EXTENSIONS,
    resolve_supported_extensions,
    session_extensions,
)


class TestResolveSupportedExtensions:
    """Tests for resolve_supported_extensions."""

    def test_key_absent_returns_empty(self) -> None:
        """When 'extensions' key is absent, returns empty tuple."""
        entity_data: dict[str, Any] = {"template": {}, "schemaVersion": "jobtemplate-2023-09"}
        result = resolve_supported_extensions(entity_data)
        assert result == ()

    def test_key_none_returns_empty(self) -> None:
        """When 'extensions' key is explicitly None, returns empty tuple."""
        entity_data: dict[str, Any] = {"template": {}, "extensions": None}
        result = resolve_supported_extensions(entity_data)
        assert result == ()

    def test_key_present_with_values(self) -> None:
        """When 'extensions' is a non-empty list, returns exactly those values."""
        entity_data: dict[str, Any] = {"template": {}, "extensions": ["EXPR", "WRAP_ACTIONS"]}
        result = resolve_supported_extensions(entity_data)
        assert result == ("EXPR", "WRAP_ACTIONS")

    def test_key_present_empty_list(self) -> None:
        """When 'extensions' is an empty list, returns empty tuple."""
        entity_data: dict[str, Any] = {"template": {}, "extensions": []}
        result = resolve_supported_extensions(entity_data)
        assert result == ()

    def test_unknown_extension_passes_through(self) -> None:
        """Extension names unknown to ExtensionName pass through unchanged."""
        entity_data: dict[str, Any] = {"template": {}, "extensions": ["EXPR", "FUTURE_UNKNOWN_EXT"]}
        result = resolve_supported_extensions(entity_data)
        assert result == ("EXPR", "FUTURE_UNKNOWN_EXT")


class TestSessionExtensions:
    """Tests for session_extensions — derives session-level enablement from the job declaration."""

    def test_none_returns_all_known_extensions(self) -> None:
        """When extensions is None (field absent / legacy), all known extensions are enabled."""
        result = session_extensions(None)
        assert result == RUNTIME_CAPABILITY_EXTENSIONS
        for ext in ExtensionName:
            assert ext.value in result

    def test_empty_list_returns_only_redacted_env_vars(self) -> None:
        """When job declared no extensions, only the always-on set is enabled."""
        result = session_extensions([])
        assert "REDACTED_ENV_VARS" in result
        assert len(result) == 1

    def test_list_includes_declared_plus_redacted_env_vars(self) -> None:
        """When job declared specific extensions, those plus REDACTED_ENV_VARS are returned."""
        result = session_extensions(["EXPR", "WRAP_ACTIONS"])
        assert "EXPR" in result
        assert "WRAP_ACTIONS" in result
        assert "REDACTED_ENV_VARS" in result
        assert len(result) == 3

    def test_redacted_env_vars_not_duplicated(self) -> None:
        """When job explicitly declares REDACTED_ENV_VARS, it appears only once."""
        result = session_extensions(["EXPR", "REDACTED_ENV_VARS"])
        assert result.count("REDACTED_ENV_VARS") == 1
        assert "EXPR" in result
        assert len(result) == 2

    @pytest.mark.parametrize(
        "job_extensions",
        [
            pytest.param([], id="empty-list"),
            pytest.param(["EXPR"], id="single-extension"),
            pytest.param(["EXPR", "WRAP_ACTIONS", "FEATURE_BUNDLE_1"], id="multiple-extensions"),
        ],
    )
    def test_redacted_env_vars_always_present(self, job_extensions: list[str]) -> None:
        """REDACTED_ENV_VARS is unconditionally present regardless of the job's declaration."""
        result = session_extensions(job_extensions)
        assert "REDACTED_ENV_VARS" in result
