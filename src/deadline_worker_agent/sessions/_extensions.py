# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from typing import Any, Mapping

from openjd.model.v2023_09 import ExtensionName

__all__ = ["RUNTIME_CAPABILITY_EXTENSIONS", "resolve_supported_extensions", "session_extensions"]

# The full set of known extension names, passed to the session runtime at
# construction time. This is NOT a parse-site fallback — it is a runtime
# CAPABILITY declaration. The session-scoped extensions list drives runtime
# behavior: for example, openjd/sessions/_action_filter.py gates env-var
# propagation via REDACTED_ENV_VARS on whether the extension name appears in the
# session's RevisionExtensions. Omitting an extension from the session-level
# list disables the runtime feature, so this tuple represents the ceiling of
# extensions the worker runtime implements. Per-entity template validation is
# governed separately by resolve_supported_extensions.
RUNTIME_CAPABILITY_EXTENSIONS: tuple[str, ...] = tuple(v.value for v in ExtensionName)

# REDACTED_ENV_VARS controls env-var redaction in session logs. It must always
# be enabled regardless of the job's extension declaration — disabling it would
# silently leak sensitive environment variables into session output.
_ALWAYS_ON_EXTENSIONS: frozenset[str] = frozenset({"REDACTED_ENV_VARS"})


def session_extensions(job_extensions: list[str] | None) -> tuple[str, ...]:
    """Derive the session-level supported extensions from the job's declaration.

    Rules:
    - None (field absent): the service has not yet shipped the field, or this is
      a legacy job. Fall back to advertising all known extensions to preserve
      today's behavior.
    - [] (empty list): the job declared no extensions. Enable only the always-on
      set (REDACTED_ENV_VARS).
    - Non-empty list: enable exactly those extensions plus the always-on set.

    REDACTED_ENV_VARS is unconditionally included because it gates env-var
    redaction in session logs — turning it off would be a security regression
    for jobs that did not explicitly declare it.
    """
    if job_extensions is None:
        return RUNTIME_CAPABILITY_EXTENSIONS
    return tuple(set(job_extensions) | _ALWAYS_ON_EXTENSIONS)


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
