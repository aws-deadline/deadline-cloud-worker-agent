# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""End-to-end coverage for step-template-scope ``let`` delivery (RFC 0005 §3.6).

This drives the worker's real chain for a task run --

    BatchGetJobEntity payload
      -> StepDetails.from_boto        (real parse, real extension gating)
      -> RunStepTaskAction.start      (real action)
      -> Session.run_task             (stood in for; see _RunTaskOnlySession)
      -> PythonSessionRuntime.run_task(real adapter)
      -> openjd.sessions.Session      (real v0 session, real subprocess)

-- and asserts on the *actual* text the task's command emitted, not on mock
call arguments. The plumbing-level assertions live in test_python.py,
test_rust.py, test_session.py and actions/test_run_step_task.py; this file
exists to prove the whole path resolves a real step-scope name.

Step-scope ``let`` resolves at job instantiation, and the worker never
instantiates a job: it is handed an *un-instantiated* ``StepTemplate`` whose
``let`` and ``script.let`` are separate fields. The service resolves that scope
and serves the result in the entity's ``resolvedSymbolTable``, which is the
single authoritative channel for those values -- the worker reads the table and
does not re-evaluate ``StepTemplate.let``. These tests therefore seed step-scope
names through the served table, exactly as production does.
"""

from __future__ import annotations

import json
import logging
import sys
import time
import uuid
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest
from openjd.sessions import ActionState

from deadline_worker_agent.api_models import StepDetailsData
from deadline_worker_agent.sessions.actions.run_step_task import RunStepTaskAction
from deadline_worker_agent.sessions.job_entities.step_details import StepDetails
from deadline_worker_agent.sessions.runtime import SessionRuntimeConfig
from deadline_worker_agent.sessions.runtime.python import PythonSessionRuntime

# Bound on how long a single `echo` task may take before the test gives up.
# Generous: this waits on a real subprocess, and a hang is a test bug worth
# failing loudly rather than blocking the suite forever.
_ACTION_TIMEOUT = 30.0


class _RunTaskOnlySession:
    """Stands in for ``deadline_worker_agent.sessions.Session``.

    The real class is a scheduler-facing orchestrator (queue, asset sync,
    telemetry, action reporting) whose construction pulls in far more than this
    test needs. Its ``run_task`` is a verbatim pass-through to the runtime, and
    that pass-through -- including ``resolved_symbol_table_json`` -- is pinned
    independently by ``TestRunTask`` in test_session.py. So this reproduces just
    that one hop and lets the real runtime and the real openjd session do the
    work.
    """

    def __init__(self, runtime: PythonSessionRuntime) -> None:
        self._runtime = runtime

    def run_task(self, **kwargs: Any) -> None:
        self._runtime.run_task(**kwargs)


def _step_details(
    *,
    step_let: list[str] | None,
    script_let: list[str] | None,
    args: list[str],
    resolved_symbol_table: list[dict[str, str]] | None = None,
) -> StepDetails:
    """Build a StepDetails from the shape the service actually serves.

    Note ``let`` and ``script.let`` are siblings here, never folded together --
    that is the served shape, and the reason this defect exists.

    ``resolved_symbol_table`` is a list of ``{"name", "type", "value"}`` entries
    serialized into the payload's ``resolvedSymbolTable`` field -- the JSON
    string ``create_job`` pre-resolves and ``BatchGetJobEntity`` serves.
    """
    template: dict[str, Any] = {
        "name": "MyStep",
        "script": {"actions": {"onRun": {"command": "echo", "args": args}}},
    }
    if step_let is not None:
        template["let"] = step_let
    if script_let is not None:
        template["script"]["let"] = script_let

    payload: StepDetailsData = {
        "jobId": "job-123",
        "stepId": "step-123",
        "schemaVersion": "jobtemplate-2023-09",
        "dependencies": [],
        "extensions": ["EXPR"],
        "template": template,
    }
    if resolved_symbol_table is not None:
        payload["resolvedSymbolTable"] = json.dumps(resolved_symbol_table)

    return StepDetails.from_boto(payload)


def _bash_sugar_step_details(
    *,
    step_let: list[str] | None,
    sugar_let: list[str] | None,
    script_body: str,
    resolved_symbol_table: list[dict[str, str]] | None = None,
) -> StepDetails:
    """Build a StepDetails for a FEATURE_BUNDLE_1 ``bash:`` simple action.

    A sibling of ``_step_details`` rather than a parameter on it: the served
    shape is genuinely different. There is no ``script`` field at all, the
    command and embedded file do not exist yet, and the simple action carries
    its own ``let`` alongside the step's. ``FEATURE_BUNDLE_1`` has to be in the
    payload's ``extensions`` or the parse rejects the sugar.
    """
    bash: dict[str, Any] = {"script": script_body}
    if sugar_let is not None:
        bash["let"] = sugar_let

    template: dict[str, Any] = {"name": "MyStep", "bash": bash}
    if step_let is not None:
        template["let"] = step_let

    payload: StepDetailsData = {
        "jobId": "job-123",
        "stepId": "step-123",
        "schemaVersion": "jobtemplate-2023-09",
        "dependencies": [],
        "extensions": ["FEATURE_BUNDLE_1", "EXPR"],
        "template": template,
    }
    if resolved_symbol_table is not None:
        payload["resolvedSymbolTable"] = json.dumps(resolved_symbol_table)

    return StepDetails.from_boto(payload)


def _run_action_to_completion(
    details: StepDetails, session_root: Path
) -> tuple[ActionState, PythonSessionRuntime]:
    """Run the step's task through the real chain and return its final state."""
    runtime = PythonSessionRuntime(
        SessionRuntimeConfig(
            session_id=f"session-{uuid.uuid4().hex}",
            job_parameter_values={},
            path_mapping_rules=None,
            retain_working_dir=False,
            user=None,
            action_callback=lambda session_id, status: None,
            os_env_vars=None,
            session_root_directory=session_root,
            supported_extensions=("EXPR",),
        )
    )
    try:
        action = RunStepTaskAction(
            id="sessionaction-123",
            details=details,
            task_id="task-456",
            task_parameter_values={},
        )

        action.start(session=_RunTaskOnlySession(runtime), executor=Mock())  # type: ignore[arg-type]

        # run_task is asynchronous -- it spawns the subprocess on its own
        # thread. Poll the real status rather than sleeping a fixed amount.
        deadline = time.monotonic() + _ACTION_TIMEOUT
        while True:
            status = runtime.action_status
            if status is not None and status.state != ActionState.RUNNING:
                return status.state, runtime
            if time.monotonic() > deadline:
                pytest.fail(
                    f"task action did not finish within {_ACTION_TIMEOUT}s (last status: {status})"
                )
            time.sleep(0.05)
    finally:
        runtime.cleanup()


class TestStepScopeLetEndToEnd:
    def test_step_scope_let_resolves_in_the_task_command(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A step-scope ``let`` name resolves in the command the task runs.

        The served shape production sends: the un-instantiated template still
        carries ``let``, and the table carries that scope already resolved. The
        table is authoritative, so ``{{ from_step }}`` resolves from it.
        """
        caplog.set_level(logging.INFO)
        details = _step_details(
            step_let=["from_step = 'step value'"],
            script_let=None,
            args=["STEP:{{ from_step }}"],
            resolved_symbol_table=[{"name": "from_step", "type": "string", "value": "step value"}],
        )

        state, _ = _run_action_to_completion(details, tmp_path)

        assert state == ActionState.SUCCESS
        assert any("STEP:step value" in m for m in caplog.messages)

    def test_both_scopes_resolve_and_script_scope_shadows_step_scope(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Both scopes reach the task, and script scope wins on a name clash.

        RFC 0005 orders step-scope bindings before script-scope ones, so a
        script-scope binding of the same name shadows the step's, and a
        script-scope binding may reference a step-scope one. The step's scope
        arrives resolved in the served table; the script's is evaluated on top
        of it by the session.
        """
        caplog.set_level(logging.INFO)
        details = _step_details(
            step_let=["shared = 'from step'", "base = 'step base'"],
            script_let=["shared = 'from script'", "derived = base + ' + script'"],
            args=["SHARED:{{ shared }} DERIVED:{{ derived }}"],
            resolved_symbol_table=[
                {"name": "shared", "type": "string", "value": "from step"},
                {"name": "base", "type": "string", "value": "step base"},
            ],
        )

        state, _ = _run_action_to_completion(details, tmp_path)

        assert state == ActionState.SUCCESS
        assert any("SHARED:from script" in m for m in caplog.messages)
        assert any("DERIVED:step base + script" in m for m in caplog.messages)

    def test_script_scope_let_can_reference_step_name(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Script-scope bindings evaluate after ``Step.Name`` is seeded.

        Pins the seeding order through the whole worker chain: the action passes
        ``step_name``, and the session must place ``Step.Name`` in the table
        before evaluating the script's own bindings.
        """
        caplog.set_level(logging.INFO)
        details = _step_details(
            step_let=None,
            script_let=["msg = 'step is ' + Step.Name"],
            args=["NAME:{{ msg }}"],
        )

        state, _ = _run_action_to_completion(details, tmp_path)

        assert state == ActionState.SUCCESS
        assert any("NAME:step is MyStep" in m for m in caplog.messages)

    def test_a_step_without_let_bindings_still_runs(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Negative control: the common no-``let`` step is unaffected.

        Guards against over-correction -- a fix that only works when bindings
        are present, or that breaks when ``StepTemplate.let`` is None.
        """
        caplog.set_level(logging.INFO)
        details = _step_details(step_let=None, script_let=None, args=["PLAIN:ok"])
        assert details.step_template.let is None

        state, _ = _run_action_to_completion(details, tmp_path)

        assert state == ActionState.SUCCESS
        assert any("PLAIN:ok" in m for m in caplog.messages)

    def test_script_scope_let_alone_still_resolves(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Negative control: script-scope ``let`` resolves on its own.

        It travels inside the StepScript the action forwards, with no served
        table involved. If a change to the table channel were to disturb it,
        this fails.
        """
        caplog.set_level(logging.INFO)
        details = _step_details(
            step_let=None,
            script_let=["from_script = 'script value'"],
            args=["SCRIPT:{{ from_script }}"],
        )

        state, _ = _run_action_to_completion(details, tmp_path)

        assert state == ActionState.SUCCESS
        assert any("SCRIPT:script value" in m for m in caplog.messages)

    def test_a_broken_script_scope_binding_fails_the_action_cleanly(self, tmp_path: Path) -> None:
        """An unresolvable binding fails the action, not the worker.

        The binding references an undefined symbol. The session must fail the
        action before starting the subprocess rather than raising out through
        the worker's action layer.
        """
        details = _step_details(
            step_let=None,
            script_let=["msg = NoSuchSymbol"],
            args=["NEVER:{{ msg }}"],
        )

        state, _ = _run_action_to_completion(details, tmp_path)

        assert state == ActionState.FAILED


class TestResolvedSymbolTableEndToEnd:
    """The served ``resolvedSymbolTable`` reaches the v0 session's symbol table.

    Same real chain as TestStepScopeLetEndToEnd, but the symbol under test
    arrives via the entity's pre-resolved symbol table rather than the
    template's ``let`` -- the channel this fix adds for the Python runtime.
    """

    def test_symbol_from_resolved_table_resolves_in_the_task_command(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A symbol the template's ``let`` does NOT define resolves from the
        served table.

        This is the regression. Before the fix the Python runtime dropped
        ``resolvedSymbolTable``, so ``{{ from_base }}`` had no definition and
        the action failed to resolve it.
        """
        caplog.set_level(logging.INFO)
        details = _step_details(
            step_let=None,
            script_let=None,
            args=["BASE:{{ from_base }}"],
            resolved_symbol_table=[
                {"name": "from_base", "type": "string", "value": "served value"}
            ],
        )

        state, _ = _run_action_to_completion(details, tmp_path)

        assert state == ActionState.SUCCESS
        assert any("BASE:served value" in m for m in caplog.messages)

    def test_a_step_without_resolved_table_still_runs(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Negative control: no table and no ``let`` is today's common case.

        Guards against over-correction -- a fix that only works when a table is
        served, or that breaks when ``resolvedSymbolTable`` is absent.
        """
        caplog.set_level(logging.INFO)
        details = _step_details(step_let=None, script_let=None, args=["NOTABLE:ok"])
        assert details.resolved_symbol_table_json is None

        state, _ = _run_action_to_completion(details, tmp_path)

        assert state == ActionState.SUCCESS
        assert any("NOTABLE:ok" in m for m in caplog.messages)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="the de-sugared bash: action spawns `bash`, which stock Windows lacks. "
    "The de-sugar branch itself is pinned on every platform by "
    "TestRunStepTaskActionSimpleActionSugar in actions/test_run_step_task.py.",
)
class TestSimpleActionSugarEndToEnd:
    """A served FEATURE_BUNDLE_1 simple action runs, and its ``let`` resolves.

    Gap 25: the service serves simple-action sugar as authored, so
    ``StepTemplate.script`` is None and ``StepTemplate.bash`` holds the body.
    ``create_job`` de-sugars during instantiation, so a caller that instantiates
    a job locally (openjd-cli) never sees this -- the worker is handed the
    un-instantiated template and has to de-sugar it itself.
    """

    def test_a_bash_sugar_step_runs_and_resolves_its_own_let(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """This is the regression.

        Before the fix the action forwarded ``step_template.script`` -- None for
        a sugar template -- under a comment claiming the service had already
        resolved the sugar, and the task failed before its command ran.
        """
        caplog.set_level(logging.INFO)
        details = _bash_sugar_step_details(
            step_let=None,
            sugar_let=["msg = 'hello from bash'"],
            script_body='echo "BASH:{{ msg }}"',
        )
        assert details.step_template.script is None

        state, _ = _run_action_to_completion(details, tmp_path)

        assert state == ActionState.SUCCESS
        assert any("BASH:hello from bash" in m for m in caplog.messages)

    def test_step_and_sugar_scope_let_both_resolve_through_the_fold(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Both scopes of a sugar template resolve, via the fold alone.

        ``resolve_syntax_sugar()`` folds the step's ``let`` into the script's own
        as ``[*step lets, *simple-action lets]``, so the de-sugared script
        carries both scopes in RFC 0005 order and a sugar-scope binding can
        reference a step-scope one. This is the only thing that resolves the
        step's scope on this path -- nothing else re-applies it.
        """
        caplog.set_level(logging.INFO)
        details = _bash_sugar_step_details(
            step_let=["base = 'from step'"],
            sugar_let=["out = base + '|sugar'"],
            script_body='echo "OUT:{{ out }}"',
        )
        assert details.step_template.script is None

        state, _ = _run_action_to_completion(details, tmp_path)

        assert state == ActionState.SUCCESS
        assert any("OUT:from step|sugar" in m for m in caplog.messages)
