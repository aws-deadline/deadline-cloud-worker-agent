# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Generator, Optional

import backoff
import pytest

from openjd.model import decode_environment_template, decode_job_template
from openjd.sessions import ActionStatus

from deadline_worker_agent.sessions.runtime import (
    SessionRuntime,
    SessionRuntimeConfig,
    SessionRuntimeKind,
    create_session_runtime,
)

if TYPE_CHECKING:
    from openjd.sessions import EnvironmentModel, StepScriptModel

# These tests run the SAME scenarios through BOTH the Python (v0) and Rust (v1)
# SessionRuntime adapters against real openjd sessions executing real local
# subprocesses. They require no AWS/farm resources: every scenario is driven by
# in-process pydantic models and a session_root_directory under pytest's
# tmp_path, so they are collected and pass on any host with the openjd _v1
# binding installed.
#
# Observable-equivalence assertions normalize on ``ActionStatus.state.name``
# rather than enum identity: the Python runtime surfaces
# ``openjd.sessions._types.ActionState`` while the Rust runtime surfaces the
# distinct ``openjd.sessions._v1.ActionState`` enum. The two enums share member
# names (RUNNING/SUCCESS/FAILED/CANCELED/TIMEOUT) but are not ``==`` to each
# other, so comparing ``.name`` is the correct runtime-agnostic normalization.

_RUNTIME_KINDS = [
    pytest.param(SessionRuntimeKind.PYTHON, id="python"),
    pytest.param(SessionRuntimeKind.RUST, id="rust"),
]


class _StatusRecorder:
    """Captures the ActionStatus updates delivered to the runtime callback.

    The callback signature mirrors ``openjd.sessions.SessionCallbackType``:
    ``(session_id, ActionStatus) -> None``.
    """

    def __init__(self) -> None:
        self.statuses: list[ActionStatus] = []

    def __call__(self, session_id: str, status: ActionStatus) -> None:
        self.statuses.append(status)

    @property
    def state_names(self) -> list[str]:
        return [status.state.name for status in self.statuses]


RuntimeFactory = Callable[[SessionRuntimeKind], "tuple[SessionRuntime, _StatusRecorder, Path]"]


@pytest.fixture
def make_runtime(
    tmp_path: Path,
) -> Generator[RuntimeFactory, None, None]:
    """Builds real SessionRuntime adapters and tears them down afterwards.

    Returns a factory that constructs a runtime of the requested kind with a
    real ``SessionRuntimeConfig`` (real session_root_directory under tmp_path,
    a real status-capturing callback). Every runtime created through the
    factory is cleaned up on teardown, best-effort, so a scenario that leaves
    an action mid-flight (e.g. the Rust cancel divergence) never leaks a
    subprocess or polling thread.
    """
    created: list[SessionRuntime] = []
    counter = {"n": 0}

    def _factory(kind: SessionRuntimeKind) -> tuple[SessionRuntime, _StatusRecorder, Path]:
        counter["n"] += 1
        root = tmp_path / f"session-{kind.name.lower()}-{counter['n']}"
        root.mkdir(parents=True, exist_ok=True)
        recorder = _StatusRecorder()
        config = SessionRuntimeConfig(
            session_id=f"session-{kind.name.lower()}-{counter['n']}",
            job_parameter_values={},
            path_mapping_rules=None,
            retain_working_dir=False,
            user=None,
            action_callback=recorder,
            os_env_vars=None,
            session_root_directory=root,
        )
        runtime = create_session_runtime(kind, config)
        created.append(runtime)
        return runtime, recorder, root

    yield _factory

    for runtime in created:
        try:
            runtime.cleanup()
        except Exception:
            # Best-effort teardown: a runtime may already be cleaned up by the
            # test, or left in a busy state by a divergence scenario.
            pass


def _build_step_script(
    command: str,
    args: list[str],
    embedded_files: Optional[list[dict[str, str]]] = None,
) -> StepScriptModel:
    """Build a real pydantic v2023_09 StepScript via the openjd decoder.

    Decoding a full job template (rather than constructing the model directly)
    supplies the parsing context the v2023_09 constrained-string fields
    (e.g. embedded-file ``data``) require, and yields exactly the pydantic
    object the worker's action layer hands to the adapters at runtime.
    """
    script: dict[str, object] = {"actions": {"onRun": {"command": command, "args": args}}}
    if embedded_files is not None:
        script["embeddedFiles"] = embedded_files
    job_template = decode_job_template(
        template={
            "specificationVersion": "jobtemplate-2023-09",
            "name": "DifferentialRuntimeJob",
            "steps": [{"name": "DifferentialStep", "script": script}],
        }
    )
    step_script = job_template.steps[0].script
    assert step_script is not None
    return step_script


def _build_environment(name: str, variables: Optional[dict[str, str]] = None) -> EnvironmentModel:
    """Build a real pydantic v2023_09 Environment via the openjd decoder."""
    environment: dict[str, object] = {"name": name}
    if variables is not None:
        environment["variables"] = variables
    env_template = decode_environment_template(
        template={
            "specificationVersion": "environment-2023-09",
            "environment": environment,
        }
    )
    return env_template.environment


def _state_name(runtime: SessionRuntime) -> Optional[str]:
    """Normalized state name of the runtime's latest action, or None."""
    status = runtime.action_status
    return None if status is None else status.state.name


def _wait_for_state(runtime: SessionRuntime, target: str, timeout: float = 15.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _state_name(runtime) == target:
            return True
        time.sleep(0.02)
    return False


def _wait_for_new_action(recorder: _StatusRecorder, prev_count: int, timeout: float = 15.0) -> bool:
    """Wait until the callback has fired at least once more than ``prev_count``.

    This detects that a new action has started (or completed instantly) without
    relying on the state value — which avoids the SUCCESS→SUCCESS ambiguity for
    back-to-back instant actions (e.g. variables-only environments).
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if len(recorder.statuses) > prev_count:
            return True
        time.sleep(0.02)
    return False


@backoff.on_exception(backoff.constant, RuntimeError, max_time=5, interval=1, jitter=None)
def _cancel_when_registered(runtime: SessionRuntime) -> None:
    """Cancel the running action, retrying while the runtime reports none running.

    The Rust runtime publishes the RUNNING state a few instructions before it
    registers the action's cancel state, so a cancel issued the instant RUNNING
    becomes observable can be rejected with "no action is running". The window
    is sub-millisecond and unreachable through the worker agent's own cancel
    paths (service-observed cancels and grace-time timeouts both arrive far
    later), but a test that polls for RUNNING and cancels immediately lands in
    it every run. Retrying until the cancel is accepted keeps this scenario
    about cancel *delivery* rather than about that ordering.
    """
    runtime.cancel_action()


def _wait_for_terminal(runtime: SessionRuntime, timeout: float = 30.0) -> Optional[str]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        name = _state_name(runtime)
        if name is not None and name != "RUNNING":
            return name
        time.sleep(0.05)
    return _state_name(runtime)


class TestDifferentialSessionRuntime:
    """Differential behavior tests: Python (v0) vs Rust (v1) adapters.

    Each scenario runs identically against both runtimes and asserts the same
    observable outcome, proving the two adapters are behaviorally equivalent.
    """

    @pytest.mark.timeout(60)
    @pytest.mark.parametrize("runtime_kind", _RUNTIME_KINDS)
    def test_happy_path_enter_run_exit_succeeds(
        self, runtime_kind: SessionRuntimeKind, make_runtime: RuntimeFactory
    ) -> None:
        runtime, recorder, root = make_runtime(runtime_kind)
        marker = root / "ran.txt"

        environment = _build_environment("HappyEnv", variables={"HAPPY_PATH": "1"})
        prev = len(recorder.statuses)
        identifier = runtime.enter_environment(environment=environment)
        assert _wait_for_new_action(recorder, prev)
        assert _wait_for_terminal(runtime) == "SUCCESS"

        step_script = _build_step_script(
            sys.executable,
            ["-c", f"open(r'{marker}', 'w').write('ok')"],
        )
        prev = len(recorder.statuses)
        runtime.run_task(step_script=step_script, task_parameter_values={})
        assert _wait_for_new_action(recorder, prev)
        assert _wait_for_terminal(runtime) == "SUCCESS"
        assert marker.read_text() == "ok"

        prev = len(recorder.statuses)
        runtime.exit_environment(identifier=identifier)
        assert _wait_for_new_action(recorder, prev)
        assert _wait_for_terminal(runtime) == "SUCCESS"

        # The callback observed at least one terminal SUCCESS transition.
        assert "SUCCESS" in recorder.state_names

    @pytest.mark.timeout(60)
    @pytest.mark.parametrize("runtime_kind", _RUNTIME_KINDS)
    def test_env_var_mutation_surfaces_in_task(
        self, runtime_kind: SessionRuntimeKind, make_runtime: RuntimeFactory
    ) -> None:
        runtime, recorder, root = make_runtime(runtime_kind)
        captured = root / "env_value.txt"

        # An environment that defines a session variable the subsequent task
        # should observe in its process environment.
        environment = _build_environment("VarEnv", variables={"DIFF_RUNTIME_VAR": "mutated-value"})
        prev = len(recorder.statuses)
        identifier = runtime.enter_environment(environment=environment)
        assert _wait_for_new_action(recorder, prev)
        assert _wait_for_terminal(runtime) == "SUCCESS"

        step_script = _build_step_script(
            sys.executable,
            [
                "-c",
                f"import os; open(r'{captured}', 'w').write(os.environ.get('DIFF_RUNTIME_VAR', 'MISSING'))",
            ],
        )
        prev = len(recorder.statuses)
        runtime.run_task(step_script=step_script, task_parameter_values={})
        assert _wait_for_new_action(recorder, prev)
        assert _wait_for_terminal(runtime) == "SUCCESS"

        # Both runtimes surface the environment-defined variable identically.
        assert captured.read_text() == "mutated-value"

        prev = len(recorder.statuses)
        runtime.exit_environment(identifier=identifier)
        assert _wait_for_new_action(recorder, prev)
        assert _wait_for_terminal(runtime) == "SUCCESS"

    @pytest.mark.timeout(60)
    @pytest.mark.parametrize("runtime_kind", _RUNTIME_KINDS)
    def test_cleanup_after_failed_task(
        self, runtime_kind: SessionRuntimeKind, make_runtime: RuntimeFactory
    ) -> None:
        runtime, recorder, _root = make_runtime(runtime_kind)

        step_script = _build_step_script(
            sys.executable,
            ["-c", "import sys; sys.exit(3)"],
        )
        runtime.run_task(step_script=step_script, task_parameter_values={})
        assert _wait_for_new_action(recorder, 0)

        assert _wait_for_terminal(runtime) == "FAILED"
        status = runtime.action_status
        assert status is not None
        assert status.exit_code == 3

        # cleanup() after a failed action must succeed without raising for both
        # runtimes.
        runtime.cleanup()

    @pytest.mark.timeout(60)
    @pytest.mark.parametrize("runtime_kind", _RUNTIME_KINDS)
    def test_attachment_sync_materializes_embedded_file(
        self, runtime_kind: SessionRuntimeKind, make_runtime: RuntimeFactory
    ) -> None:
        # Attachment-sync runs for BOTH runtimes — the Rust adapter mirrors the
        # prototype's embedded-file materialization + Task.File resolution
        # recipe, so there is no Rust skip here.
        runtime, _recorder, root = make_runtime(runtime_kind)
        output = root / "sync_output.txt"

        step_script = _build_step_script(
            sys.executable,
            [
                "-c",
                f"import sys; open(r'{output}', 'w').write(open(sys.argv[1]).read())",
                "{{ Task.File.Payload }}",
            ],
            embedded_files=[{"name": "Payload", "type": "TEXT", "data": "embedded-payload-123"}],
        )
        runtime._run_task_without_session_env(step_script=step_script, task_parameter_values={})

        assert _wait_for_terminal(runtime) == "SUCCESS"
        # The embedded file was materialized and its Task.File reference resolved
        # to the materialized path, so the task read back the embedded contents.
        assert output.read_text() == "embedded-payload-123"

    @pytest.mark.timeout(60)
    @pytest.mark.parametrize("runtime_kind", _RUNTIME_KINDS)
    def test_embedded_file_path_resolves_to_materialized_path(
        self, runtime_kind: SessionRuntimeKind, make_runtime: RuntimeFactory
    ) -> None:
        runtime, _recorder, root = make_runtime(runtime_kind)
        output = root / "resolved_path.txt"

        # The task records the resolved Task.File path it received and the file
        # contents at that path, letting us assert the reference resolved to a
        # real, readable materialized file identically for both runtimes.
        code = (
            "import sys;"
            "path = sys.argv[1];"
            "open(sys.argv[2], 'w').write(path + '\\n' + open(path).read())"
        )
        step_script = _build_step_script(
            sys.executable,
            ["-c", code, "{{ Task.File.Payload }}", str(output)],
            embedded_files=[{"name": "Payload", "type": "TEXT", "data": "resolve-me"}],
        )
        runtime._run_task_without_session_env(step_script=step_script, task_parameter_values={})

        assert _wait_for_terminal(runtime) == "SUCCESS"
        resolved_path, _, contents = output.read_text().partition("\n")
        # The reference resolved to a concrete path (not the literal template
        # token) that exists on disk and carries the embedded data.
        assert "{{" not in resolved_path
        assert Path(resolved_path).is_file()
        assert contents == "resolve-me"

    @pytest.mark.timeout(60)
    @pytest.mark.parametrize("runtime_kind", _RUNTIME_KINDS)
    def test_cancel_mid_action_ends_canceled(
        self, runtime_kind: SessionRuntimeKind, make_runtime: RuntimeFactory
    ) -> None:
        runtime, _recorder, _root = make_runtime(runtime_kind)

        # A long-running task we can interrupt mid-flight.
        step_script = _build_step_script(
            sys.executable,
            ["-c", "import time; time.sleep(30)"],
        )
        runtime.run_task(step_script=step_script, task_parameter_values={})
        assert _wait_for_state(runtime, "RUNNING")

        _cancel_when_registered(runtime)

        assert _wait_for_terminal(runtime) == "CANCELED"
