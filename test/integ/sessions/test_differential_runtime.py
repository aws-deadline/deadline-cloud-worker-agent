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

from deadline_worker_agent.sessions.job_entities.job_details import (
    parameters_from_api_response,
)
from deadline_worker_agent.sessions.runtime import (
    SessionRuntime,
    SessionRuntimeConfig,
    SessionRuntimeKind,
    create_session_runtime,
)

if TYPE_CHECKING:
    from openjd.model import ParameterValue
    from openjd.sessions import EnvironmentModel, StepScriptModel

    from deadline_worker_agent.api_models import BoolParameter

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


def _resolve_job_param_reference(
    runtime_kind: SessionRuntimeKind,
    *,
    job_parameter_values: dict[str, ParameterValue],
    param_name: str,
    param_type: str,
    root: Path,
) -> str:
    """Resolve a single ``{{Param.<param_name>}}`` reference through a real session.

    Builds a runtime of the requested kind with the EXPR extension enabled --
    the boolean parameter types (BOOL, LIST[BOOL]) are EXPR-extension types and
    the decoder rejects them without it -- then enters an environment whose
    onEnter writes the resolved reference to a file and returns the captured
    text. The reference is spliced into the ``-c`` body as a single-quoted
    Python string literal, so a LIST[BOOL] value is observed as one rendered
    string (e.g. ``[true, false]``) rather than expanded into separate process
    arguments the way a bare list reference in ``args`` would be.

    The environment template both declares ``extensions: ["EXPR"]`` and is
    decoded with ``supported_extensions=["EXPR"]``; both are required -- the
    template field opts the template in, and the decode argument is the
    implementation allowlist the field is intersected against.
    """
    output = root / "resolved_param.txt"
    reference = f"{{{{Param.{param_name}}}}}"
    recorder = _StatusRecorder()
    config = SessionRuntimeConfig(
        session_id=f"session-boolparam-{runtime_kind.name.lower()}",
        job_parameter_values=job_parameter_values,
        path_mapping_rules=None,
        retain_working_dir=False,
        user=None,
        action_callback=recorder,
        os_env_vars=None,
        session_root_directory=root,
        supported_extensions=("EXPR",),
    )
    runtime = create_session_runtime(runtime_kind, config)
    try:
        env_template = decode_environment_template(
            template={
                "specificationVersion": "environment-2023-09",
                "extensions": ["EXPR"],
                "environment": {
                    "name": "BoolParamEnv",
                    "script": {
                        "actions": {
                            "onEnter": {
                                "command": sys.executable,
                                "args": [
                                    "-c",
                                    f"import sys; open(sys.argv[1], 'w').write('{reference}')",
                                    str(output),
                                ],
                            }
                        }
                    },
                },
                "parameterDefinitions": [{"name": param_name, "type": param_type}],
            },
            supported_extensions=["EXPR"],
        )

        prev = len(recorder.statuses)
        identifier = runtime.enter_environment(environment=env_template.environment)
        assert _wait_for_new_action(recorder, prev)
        terminal = _wait_for_terminal(runtime)
        assert terminal == "SUCCESS", (
            f"{runtime_kind.name}: expected SUCCESS resolving Param.{param_name} "
            f"but got {terminal}; statuses={recorder.state_names}"
        )
        resolved = output.read_text()

        prev = len(recorder.statuses)
        runtime.exit_environment(identifier=identifier)
        assert _wait_for_new_action(recorder, prev)
        assert _wait_for_terminal(runtime) == "SUCCESS"
        return resolved
    finally:
        try:
            runtime.cleanup()
        except Exception:
            pass


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

    @pytest.mark.timeout(60)
    @pytest.mark.parametrize("runtime_kind", _RUNTIME_KINDS)
    def test_enter_environment_when_job_has_typed_params_resolves_param_reference(
        self, runtime_kind: SessionRuntimeKind, tmp_path: Path
    ) -> None:
        """An environment referencing {{Param.X}} succeeds when the runtime is
        constructed with non-empty job_parameter_values including non-STRING
        types. This exercises the REAL decoder through the adapter — unit tests
        mock it and cannot validate payload shape."""
        from openjd.model._types import ParameterValue, ParameterValueType

        root = tmp_path / f"session-params-{runtime_kind.name.lower()}"
        root.mkdir(parents=True, exist_ok=True)
        recorder = _StatusRecorder()
        config = SessionRuntimeConfig(
            session_id=f"session-params-{runtime_kind.name.lower()}",
            job_parameter_values={
                "Message": ParameterValue(type=ParameterValueType.STRING, value="hello-world"),
                "Count": ParameterValue(type=ParameterValueType.INT, value="42"),
            },
            path_mapping_rules=None,
            retain_working_dir=False,
            user=None,
            action_callback=recorder,
            os_env_vars=None,
            session_root_directory=root,
        )
        runtime = create_session_runtime(runtime_kind, config)
        try:
            # The environment's onEnter script references {{Param.Message}} — the
            # decoder must resolve it using the parameterDefinitions we supply.
            env_template = decode_environment_template(
                template={
                    "specificationVersion": "environment-2023-09",
                    "environment": {
                        "name": "ParamEnv",
                        "script": {
                            "actions": {
                                "onEnter": {
                                    "command": sys.executable,
                                    "args": ["-c", "print('{{Param.Message}}')"],
                                }
                            }
                        },
                    },
                    "parameterDefinitions": [
                        {"name": "Message", "type": "STRING"},
                        {"name": "Count", "type": "INT"},
                    ],
                }
            )
            environment = env_template.environment

            prev = len(recorder.statuses)
            identifier = runtime.enter_environment(environment=environment)
            assert _wait_for_new_action(recorder, prev)
            terminal = _wait_for_terminal(runtime)
            assert terminal == "SUCCESS", (
                f"Expected SUCCESS but got {terminal}; statuses={recorder.state_names}"
            )

            prev = len(recorder.statuses)
            runtime.exit_environment(identifier=identifier)
            assert _wait_for_new_action(recorder, prev)
            assert _wait_for_terminal(runtime) == "SUCCESS"
        finally:
            try:
                runtime.cleanup()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Boolean wire-format parameter resolution.
    #
    # The worker decodes boolean task/job parameters at a single wire-decode
    # choke point (``parameters_from_api_response``), coercing the two wire
    # forms the service may send -- the native JSON boolean ``{"bool": true}``
    # (sent today) and the string ``{"bool": "true"}`` (sent after the model
    # change) -- into a native Python bool. These tests exercise that decode
    # path end to end: a WIRE-FORMAT dict goes through
    # ``parameters_from_api_response`` and the decoded value is fed to a real
    # session that references it via ``{{Param.X}}``, so the resolved value is
    # observed in output rather than merely type-checked at the decode boundary.
    #
    # Both wire forms, and every non-canonical-but-legal token, must resolve to
    # the canonical lowercase ``true``/``false`` OpenJD renders for a bool, and
    # must do so identically on the Python (v0) and Rust (v1) runtimes. The
    # expected strings below are spec-derived literals, not values recomputed
    # from the decoder, and each parametrized runtime asserts against the same
    # literal -- so a runtime that rendered a bool differently (e.g. ``True`` or
    # ``1``) would fail its own case rather than be averaged away.

    @pytest.mark.timeout(60)
    @pytest.mark.parametrize("runtime_kind", _RUNTIME_KINDS)
    @pytest.mark.parametrize(
        "wire_value, expected",
        [
            pytest.param({"bool": True}, "true", id="native-true"),
            pytest.param({"bool": "true"}, "true", id="string-true"),
            pytest.param({"bool": "yes"}, "true", id="string-yes"),
            pytest.param({"bool": "1"}, "true", id="string-1"),
            pytest.param({"bool": False}, "false", id="native-false"),
            pytest.param({"bool": "0"}, "false", id="string-0"),
        ],
    )
    def test_bool_job_param_wire_form_resolves_to_canonical_string(
        self,
        runtime_kind: SessionRuntimeKind,
        wire_value: BoolParameter,
        expected: str,
        tmp_path: Path,
    ) -> None:
        root = tmp_path / f"boolparam-{runtime_kind.name.lower()}"
        root.mkdir(parents=True, exist_ok=True)

        # The API-shaped dict is decoded through the real wire path -- the same
        # code the worker runs on a BatchGetJobEntity response -- not by
        # hand-building a ParameterValue.
        job_parameter_values = parameters_from_api_response({"MyBool": wire_value})

        resolved = _resolve_job_param_reference(
            runtime_kind,
            job_parameter_values=job_parameter_values,
            param_name="MyBool",
            param_type="BOOL",
            root=root,
        )

        assert resolved == expected

    @pytest.mark.timeout(60)
    @pytest.mark.parametrize("runtime_kind", _RUNTIME_KINDS)
    def test_bool_list_job_param_wire_form_resolves_to_canonical_strings(
        self, runtime_kind: SessionRuntimeKind, tmp_path: Path
    ) -> None:
        root = tmp_path / f"boollist-{runtime_kind.name.lower()}"
        root.mkdir(parents=True, exist_ok=True)

        # A LIST[BOOL] mixing the native form with non-canonical legal tokens;
        # every element must decode and resolve to its canonical rendering.
        job_parameter_values = parameters_from_api_response(
            {"MyBools": {"boolList": [True, "false", "yes", "0"]}}
        )

        resolved = _resolve_job_param_reference(
            runtime_kind,
            job_parameter_values=job_parameter_values,
            param_name="MyBools",
            param_type="LIST[BOOL]",
            root=root,
        )

        assert resolved == "[true, false, true, false]"

    @pytest.mark.timeout(90)
    def test_bool_param_resolution_is_identical_across_runtimes(self, tmp_path: Path) -> None:
        """The same wire-form bool must resolve to the same concrete text on both
        runtimes. This compares them directly rather than relying on each
        asserting a shared literal, so a silent divergence -- one runtime
        rendering ``True`` or ``1`` while the other renders ``true`` -- fails
        here. A non-canonical token (``"yes"``) is used so the assertion also
        pins that the decoder's vocabulary is applied consistently on both."""
        wire_value: BoolParameter = {"bool": "yes"}
        outputs: dict[str, str] = {}
        for runtime_kind in (SessionRuntimeKind.PYTHON, SessionRuntimeKind.RUST):
            root = tmp_path / f"cross-{runtime_kind.name.lower()}"
            root.mkdir(parents=True, exist_ok=True)
            job_parameter_values = parameters_from_api_response({"MyBool": wire_value})
            outputs[runtime_kind.name] = _resolve_job_param_reference(
                runtime_kind,
                job_parameter_values=job_parameter_values,
                param_name="MyBool",
                param_type="BOOL",
                root=root,
            )

        assert outputs["PYTHON"] == outputs["RUST"]
        # And the shared value is the canonical rendering, not merely equal-but-wrong.
        assert outputs["PYTHON"] == "true"
