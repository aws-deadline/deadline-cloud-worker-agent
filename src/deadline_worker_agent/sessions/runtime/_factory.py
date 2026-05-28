# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from ._abc import SessionRuntime
from ._config import SessionRuntimeConfig
from ._kind import RuntimeKind


def create_session_runtime(kind: RuntimeKind, config: SessionRuntimeConfig) -> SessionRuntime:
    """Construct a SessionRuntime for the given kind.

    Adapter modules are imported lazily so callers that never request a
    particular adapter don't pay the cost (or risk the failure) of importing it.

    Raises:
        ValueError: ``kind`` is not a recognised RuntimeKind.
        NotImplementedError: the adapter module for ``kind`` cannot be imported
            (e.g. the Rust binding is unavailable on this platform).
    """
    if kind is RuntimeKind.SERVICE_SELECTED:
        raise ValueError(
            "SERVICE_SELECTED must be resolved to PYTHON or RUST via "
            "select_runtime() before calling create_session_runtime()"
        )

    if kind is RuntimeKind.PYTHON:
        try:
            from .python import PythonSessionRuntime  # type: ignore[import-not-found]
        except ImportError as exc:
            raise NotImplementedError(
                f"PythonSessionRuntime adapter is not available: {exc}"
            ) from exc
        return PythonSessionRuntime(config)

    if kind is RuntimeKind.RUST:
        try:
            from .rust import RustSessionRuntime  # type: ignore[import-not-found]
        except ImportError as exc:
            raise NotImplementedError(
                f"RustSessionRuntime adapter is not available: {exc}"
            ) from exc
        return RustSessionRuntime(config)

    # Defensive: new variants without a branch should fail loudly.
    raise ValueError(f"Unhandled RuntimeKind: {kind!r}")
