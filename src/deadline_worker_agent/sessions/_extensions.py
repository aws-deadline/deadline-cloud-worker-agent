# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from typing import Any, Mapping

from openjd.model.v2023_09 import ExtensionName

__all__ = ["RUNTIME_CAPABILITY_EXTENSIONS", "resolve_supported_extensions"]

# The full set of known extension names, passed to the session runtime at
# construction time. This is NOT a parse-site fallback — it is a runtime
# CAPABILITY declaration. The session-scoped extensions list drives runtime
# behavior: for example, openjd/sessions/_action_filter.py gates env-var
# propagation via REDACTED_ENV_VARS on whether the extension name appears in the
# session's RevisionExtensions. Omitting an extension from the session-level
# list disables the runtime feature, so all known extensions must be declared.
RUNTIME_CAPABILITY_EXTENSIONS: tuple[str, ...] = tuple(v.value for v in ExtensionName)


def resolve_supported_extensions(entity_data: Mapping[str, Any]) -> tuple[str, ...]:
    """Resolve the supported extensions list from a BatchGetJobEntity entity payload.

    The ``extensions`` field is additive-optional: absent from responses until the service
    deploys the feature.

    Rules:
    - Key absent or value is None → return empty (no extensions enabled for validation).
    - Key present with a list (including empty) → use verbatim, no filtering.

    The empty default is safe because this value only gates template validation
    (which extensions the template is allowed to use). Runtime behavior is
    governed by the session-scoped extensions list (RUNTIME_CAPABILITY_EXTENSIONS),
    not by the per-entity parse-site value.
    """
    extensions = entity_data.get("extensions")
    if extensions is None:
        return ()
    return tuple(extensions)
