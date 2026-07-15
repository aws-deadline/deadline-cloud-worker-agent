# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

from typing import Optional

from ..._session_runtime_kind import SessionRuntimeKind

# Valid values that the service may send as a runtimeHint. The hint is a raw
# string from the UpdateWorkerSchedule response, so we map it explicitly rather
# than trusting SessionRuntimeKind(...) — "service-selected" is a valid enum
# value but NOT a valid hint.
_RUNTIME_HINT_MAP: dict[str, SessionRuntimeKind] = {
    SessionRuntimeKind.PYTHON.value: SessionRuntimeKind.PYTHON,
    SessionRuntimeKind.RUST.value: SessionRuntimeKind.RUST,
}


def select_runtime(
    configured_kind: SessionRuntimeKind,
    *,
    runtime_hint: Optional[str] = None,
) -> SessionRuntimeKind:
    """Resolve the worker's configured runtime mode to a concrete runtime kind.

    The worker configuration determines the mode. PYTHON and RUST modes pin the
    runtime unconditionally and ignore any service-provided hint. SERVICE_SELECTED
    mode defers to the service's runtimeHint when present, and defaults to PYTHON
    when absent.

    There is intentionally no capability check, escalation, or fallback here. If
    the selected runtime cannot support the job's required extensions, runtime
    construction fails — that is openjd's responsibility, not this function's.

    This function has no callers yet; reading runtimeHint from the
    UpdateWorkerSchedule response and wiring it into session construction
    lands in a follow-up change.
    """
    if configured_kind == SessionRuntimeKind.PYTHON:
        return SessionRuntimeKind.PYTHON
    if configured_kind == SessionRuntimeKind.RUST:
        return SessionRuntimeKind.RUST

    # SERVICE_SELECTED: the hint decides; no hint means PYTHON.
    if runtime_hint is None:
        return SessionRuntimeKind.PYTHON
    try:
        return _RUNTIME_HINT_MAP[runtime_hint]
    except KeyError:
        raise ValueError(
            f"Invalid runtimeHint value {runtime_hint!r}; expected one of {sorted(_RUNTIME_HINT_MAP)}"
        ) from None
