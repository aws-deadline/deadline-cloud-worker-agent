# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from openjd.model.v2023_09 import ExtensionName

__all__ = ["INTERIM_SUPPORTED_EXTENSIONS"]

# Interim value: allow all known extensions for simplicity. This should be replaced with the
# list of extensions actually requested for the job once that information is returned by
# BatchGetJobEntity.
INTERIM_SUPPORTED_EXTENSIONS: tuple[str, ...] = tuple(v.value for v in ExtensionName)
