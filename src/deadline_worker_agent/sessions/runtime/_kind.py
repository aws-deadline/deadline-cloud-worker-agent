# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from enum import Enum


class RuntimeKind(str, Enum):
    """Identifies a SessionRuntime implementation.

    The string values are user-facing (CLI flags, config files) and must
    remain stable. Subclassing ``str`` lets these values flow through
    config layers without manual conversion.
    """

    PYTHON = "python"
    RUST = "rust"
    SERVICE_SELECTED = "service-selected"
