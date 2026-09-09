# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""The Python runtime adapter must not load the native EXPR extension at import.

``openjd.expr`` is a thin facade over the native ``openjd._openjd_rs``
extension. python.py needs ``SerializedSymbolTable`` from it only when a
session actually receives a resolved symbol table, so the import is
function-local and behind a None guard. A module-level import would make the
extension a load-time requirement of every worker process, including ones
that only run non-EXPR templates.

Each probe runs in a fresh interpreter, because by the time any given test
runs the extension has usually been loaded by another test in the same worker
(test_rust.py imports it at module level). Modeled on
test/openjd/test_import_purity.py in openjd-sessions-for-python.

Note for coverage readers: these probes run in child processes with no
``COVERAGE_PROCESS_START``, so the lazy imports they exercise still report as
uncovered. Do not "fix" that by making the imports module-level -- they are
what these tests exist to protect.
"""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap
from pathlib import Path

# Kept below the suite-wide timeout so a hung probe is reported as a probe
# timeout with its output rather than being killed silently.
_PROBE_TIMEOUT_SECONDS = 20

# Injected ahead of every probe body. sys.path is handed over as a JSON
# argument -- not embedded in the source -- so that a path which is not
# encodable as UTF-8 source cannot break the probe.
_PROBE_PREAMBLE = """\
import json
import sys

sys.path[:] = json.loads(sys.argv[1])

RS = "openjd._openjd_rs"
"""


def _probe_python() -> str:
    """The interpreter to run a probe with.

    Under Windows Session 0 ``sys.executable`` is ``pythonservice.exe``, which
    cannot run a script.
    """
    if sys.platform == "win32" and "pythonservice.exe" in sys.executable.lower():
        return sys.executable.lower().replace("pythonservice.exe", "python.exe")
    return sys.executable


def _run_probe(tmp_path: Path, body: str) -> str:
    """Run ``body`` in a fresh interpreter and return its stdout, stripped."""
    script = tmp_path / "probe.py"
    script.write_text(_PROBE_PREAMBLE + textwrap.dedent(body), encoding="utf-8")
    completed = subprocess.run(
        [_probe_python(), str(script), json.dumps(sys.path)],
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=_PROBE_TIMEOUT_SECONDS,
    )
    assert completed.returncode == 0, (
        f"probe exited {completed.returncode}\n"
        f"--- script ---\n{script.read_text(encoding='utf-8')}\n"
        f"--- stdout ---\n{completed.stdout}\n--- stderr ---\n{completed.stderr}"
    )
    return completed.stdout.strip()


def test_importing_python_runtime_does_not_load_native_extension(tmp_path: Path) -> None:
    """Importing the Python runtime adapter must not load the extension.

    Before this fix, python.py imported SerializedSymbolTable at module level,
    which loaded the native extension in every worker process unconditionally.
    """
    # WHEN
    loaded = _run_probe(
        tmp_path,
        """
        import deadline_worker_agent.sessions.runtime.python

        print(RS in sys.modules)
        """,
    )

    # THEN
    assert loaded == "False", (
        "importing deadline_worker_agent.sessions.runtime.python loaded the "
        "native extension. Something on its import path imports openjd.expr "
        "(or openjd._openjd_rs) at module level; it must be imported lazily, "
        "behind the None guards in _extract_job_name/_parse_resolved_symtab."
    )


def test_importing_python_runtime_does_not_resolve_openjd_expr(tmp_path: Path) -> None:
    """``openjd.expr`` is the facade the extension gets pulled in through, so
    it must not appear either -- this still fails on a module-level import if
    some future build made the extension itself lazy."""
    # WHEN
    resolved = _run_probe(
        tmp_path,
        """
        import deadline_worker_agent.sessions.runtime.python

        print("openjd.expr" in sys.modules)
        """,
    )

    # THEN
    assert resolved == "False", (
        "importing the Python runtime adapter resolved openjd.expr at import "
        "time; it must only be imported from inside a function on the "
        "resolved-table path."
    )


def test_constructing_runtime_without_table_does_not_load_native_extension(
    tmp_path: Path,
) -> None:
    """Building a runtime with no resolved table must not load the extension.

    The two probes above pin import time only. This one pins construction: the
    ctor runs ``_extract_job_name(config.resolved_symbol_table_json)``, and the
    None guard there is the thing that keeps the extension out of a worker
    process that only ever runs non-EXPR templates. ``_parse_resolved_symtab``
    is exercised too, since the same guard protects it on the action paths.

    ``path_mapping_rules=None`` is load-bearing: real path mapping rules load
    the extension themselves (a known open limitation in the sessions repo), so
    a probe that passed rules would fail for a reason this test is not about.
    """
    # WHEN
    loaded = _run_probe(
        tmp_path,
        """
        import tempfile
        from pathlib import Path

        from deadline_worker_agent.sessions.runtime import SessionRuntimeConfig
        from deadline_worker_agent.sessions.runtime.python import (
            PythonSessionRuntime,
            _parse_resolved_symtab,
        )

        with tempfile.TemporaryDirectory() as td:
            runtime = PythonSessionRuntime(
                SessionRuntimeConfig(
                    session_id="purity-probe",
                    job_parameter_values={},
                    path_mapping_rules=None,
                    retain_working_dir=False,
                    user=None,
                    action_callback=lambda session_id, status: None,
                    os_env_vars=None,
                    session_root_directory=Path(td),
                    resolved_symbol_table_json=None,
                )
            )
            # Release the working directory before the tempdir is removed --
            # Windows refuses to delete a directory that is still in use.
            runtime.cleanup()
            _parse_resolved_symtab(None)

        print(RS in sys.modules)
        """,
    )

    # THEN
    assert loaded == "False", (
        "constructing PythonSessionRuntime with resolved_symbol_table_json=None "
        "loaded the native extension. The imports in _extract_job_name and "
        "_parse_resolved_symtab must stay inside their functions, after the "
        "None guard -- a session that never receives a resolved table must not "
        "pay for the extension."
    )


def test_native_extension_is_available(tmp_path: Path) -> None:
    """Positive control: the extension really is installed here. Without this,
    every "must not be loaded" test above would pass trivially in an
    environment where it simply is not present."""
    # WHEN
    loaded = _run_probe(
        tmp_path,
        """
        import openjd.expr

        print(RS in sys.modules)
        """,
    )

    # THEN
    assert loaded == "True", (
        "openjd.expr did not load the native extension, so the purity tests in "
        "this file prove nothing. Check the openjd-model install."
    )
