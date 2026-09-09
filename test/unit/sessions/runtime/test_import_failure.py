# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import sys
from types import ModuleType

import pytest

from deadline_worker_agent.sessions.runtime import (
    SessionRuntimeKind,
    SessionRuntime,
    SessionRuntimeConfig,
    create_session_runtime,
)

# Fully-qualified adapter module names. Setting either to ``None`` in
# ``sys.modules`` makes the factory's lazy ``from .rust import ...`` /
# ``from .python import ...`` raise ImportError, simulating a missing
# binding without uninstalling anything.
_RUST_MODULE = "deadline_worker_agent.sessions.runtime.rust"
_PYTHON_MODULE = "deadline_worker_agent.sessions.runtime.python"


class TestRustImportFailureContract:
    """The full import-failure contract for the RUST adapter.

    The basic 'RUST import fails -> NotImplementedError' assertion already
    lives in test_factory.py (test_rust_not_implemented_when_module_missing).
    This suite proves the stronger guarantees the design depends on: the
    original ImportError is chained, there is NO silent downgrade to the
    Python adapter, and the Python runtime remains constructible so the agent
    stays alive for Python sessions when the Rust binding is unavailable.
    """

    def test_rust_import_failure_chains_original_import_error(
        self, runtime_config: SessionRuntimeConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setitem(sys.modules, _RUST_MODULE, None)
        with pytest.raises(
            NotImplementedError,
            match="RustSessionRuntime adapter is not available",
        ) as excinfo:
            create_session_runtime(SessionRuntimeKind.RUST, runtime_config)

        # The underlying ImportError must be preserved as the cause so
        # operators can see *why* the binding could not be loaded.
        assert isinstance(excinfo.value.__cause__, ImportError)

    def test_rust_import_failure_does_not_fall_back_to_python(
        self,
        runtime_config: SessionRuntimeConfig,
        stub_runtime_cls: type[SessionRuntime],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        constructed: list[object] = []

        class _TrackingPythonRuntime(stub_runtime_cls):  # type: ignore[misc, valid-type]
            def __init__(self, config: SessionRuntimeConfig) -> None:
                constructed.append(self)
                super().__init__(config)

        py_module = ModuleType(_PYTHON_MODULE)
        py_module.PythonSessionRuntime = _TrackingPythonRuntime  # type: ignore[attr-defined]

        # Rust import is broken, and a working Python adapter is available.
        # A silent-fallback bug would construct the Python adapter here; the
        # correct behaviour is to raise and construct nothing.
        monkeypatch.setitem(sys.modules, _RUST_MODULE, None)
        monkeypatch.setitem(sys.modules, _PYTHON_MODULE, py_module)
        with pytest.raises(
            NotImplementedError,
            match="RustSessionRuntime adapter is not available",
        ):
            create_session_runtime(SessionRuntimeKind.RUST, runtime_config)

        assert constructed == [], "RUST import failure must not construct a Python adapter"

    def test_python_runtime_still_constructs_when_rust_import_broken(
        self,
        runtime_config: SessionRuntimeConfig,
        stub_runtime_cls: type[SessionRuntime],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # The Python adapter module is stubbed (as test_factory.py does) so no
        # real openjd Session is built.
        py_module = ModuleType(_PYTHON_MODULE)
        py_module.PythonSessionRuntime = stub_runtime_cls  # type: ignore[attr-defined]

        monkeypatch.setitem(sys.modules, _RUST_MODULE, None)
        monkeypatch.setitem(sys.modules, _PYTHON_MODULE, py_module)
        result = create_session_runtime(SessionRuntimeKind.PYTHON, runtime_config)

        assert isinstance(result, SessionRuntime)
        assert isinstance(result, stub_runtime_cls)
