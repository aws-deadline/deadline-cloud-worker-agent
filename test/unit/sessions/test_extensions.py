# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from typing import Any

from deadline_worker_agent.sessions._extensions import (
    resolve_supported_extensions,
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
