# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

from typing import Optional

from ..._session_runtime_kind import SessionRuntimeKind

# Valid values that the service may send as a runtimeHint in the
# UpdateWorkerSchedule response's session metadata. These are the service's
# RuntimeMode wire values and intentionally differ from SessionRuntimeKind's
# config strings: "pythonexpr" denotes the Python session runtime (with the
# Rust expression engine), "rust" denotes the full Rust session runtime.
# Unknown values raise ValueError — the map is an explicit allowlist.
#
# This map is the single translation point from the service's wire
# vocabulary to SessionRuntimeKind. Callers pass the raw metadata string
# through unmodified — no pre-translation — so that the wire contract is
# defined (and validated) in exactly one place.
_RUNTIME_HINT_MAP: dict[str, SessionRuntimeKind] = {
    "pythonexpr": SessionRuntimeKind.PYTHON,
    "rust": SessionRuntimeKind.RUST,
}


def select_runtime(
    configured_kind: SessionRuntimeKind,
    *,
    runtime_hint: Optional[str] = None,
) -> SessionRuntimeKind:
    """Resolve the worker's configured runtime mode to a concrete runtime kind.

    The worker configuration determines the mode. PYTHON and RUST modes pin the
    runtime unconditionally and ignore any service-provided hint. SERVICE_SELECTED
    mode defers to the service's runtimeHint ("pythonexpr" or "rust") when present,
    and defaults to PYTHON when absent.

    There is intentionally no capability check, escalation, or fallback here. If
    the selected runtime cannot support the job's required extensions, runtime
    construction fails — that is openjd's responsibility, not this function's.
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
