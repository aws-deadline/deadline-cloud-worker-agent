# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import json
from typing import Any
from unittest.mock import Mock
import pytest

from deadline_worker_agent.sessions.actions.run_step_task import RunStepTaskAction
from deadline_worker_agent.sessions.job_entities.step_details import StepDetails


@pytest.fixture
def mock_step_details():
    mock = Mock(spec=StepDetails)
    mock.step_id = "step-123"
    mock.step_template = Mock()
    mock.step_template.name = "step-name"
    mock.step_template.script = Mock()
    return mock


@pytest.fixture
def mock_session():
    session = Mock()
    session.run_task = Mock()
    return session


@pytest.fixture
def mock_executor():
    return Mock()


class TestRunStepTaskAction:
    """Tests for RunStepTaskAction with optional task_id."""

    def test_init_with_task_id(self, mock_step_details):
        """Test creating RunStepTaskAction with task_id."""
        action = RunStepTaskAction(
            id="action-123",
            details=mock_step_details,
            task_id="task-456",
            task_parameter_values={},
        )

        assert action.task_id == "task-456"
        assert action._id == "action-123"

    def test_init_without_task_id(self, mock_step_details):
        """Test creating RunStepTaskAction without task_id."""
        action = RunStepTaskAction(
            id="action-123",
            details=mock_step_details,
            task_parameter_values={},
        )

        assert action.task_id is None
        assert action._id == "action-123"

    def test_start_with_task_id(self, mock_step_details, mock_session, mock_executor):
        """Test start() includes DEADLINE_TASK_ID when task_id is provided."""
        action = RunStepTaskAction(
            id="action-123",
            details=mock_step_details,
            task_id="task-456",
            task_parameter_values={},
        )

        action.start(session=mock_session, executor=mock_executor)

        mock_session.run_task.assert_called_once()
        call_args = mock_session.run_task.call_args[1]

        assert "os_env_vars" in call_args
        env_vars = call_args["os_env_vars"]
        assert env_vars["DEADLINE_STEP_ID"] == "step-123"
        assert env_vars["DEADLINE_TASK_ID"] == "task-456"
        assert env_vars["DEADLINE_SESSIONACTION_ID"] == "action-123"

    def test_start_without_task_id(self, mock_step_details, mock_session, mock_executor):
        """Test start() excludes DEADLINE_TASK_ID when task_id is None."""
        action = RunStepTaskAction(
            id="action-123",
            details=mock_step_details,
            task_parameter_values={},
        )

        action.start(session=mock_session, executor=mock_executor)

        mock_session.run_task.assert_called_once()
        call_args = mock_session.run_task.call_args[1]

        assert "os_env_vars" in call_args
        env_vars = call_args["os_env_vars"]
        assert env_vars["DEADLINE_STEP_ID"] == "step-123"
        assert env_vars["DEADLINE_SESSIONACTION_ID"] == "action-123"
        assert "DEADLINE_TASK_ID" not in env_vars


def _step_details_from_template(
    template: dict[str, Any],
    extensions: list[str],
    resolved_symbol_table: str | None = None,
) -> StepDetails:
    """Parse a served step template the way BatchGetJobEntity delivers it."""
    payload: Any = {
        "jobId": "job-123",
        "stepId": "step-123",
        "schemaVersion": "jobtemplate-2023-09",
        "dependencies": [],
        "extensions": extensions,
        "template": template,
    }
    if resolved_symbol_table is not None:
        payload["resolvedSymbolTable"] = resolved_symbol_table
    return StepDetails.from_boto(payload)


def _warned(mock_session: Mock) -> bool:
    """Whether the action warned on the *session* logger.

    Asserted through `session.logger` rather than caplog on purpose: routing this
    to the session log is the point -- the agent log is on the host, and a job owner
    on a service-managed fleet cannot read it. A caplog assertion would pass just as
    happily against the module logger and so would not pin that.
    """
    return any(
        "served no resolved symbol table" in str(call.args[0])
        for call in mock_session.logger.warning.call_args_list
    )


class TestStepLetWithoutAServedTable:
    """A step declaring template-scope ``let`` with no table served is reported.

    The table is the only channel for those names -- the worker does not evaluate
    the step's ``let`` itself -- so this combination leaves them undefined. The
    service can produce it: it drops a table over a size cap, and gates serving
    one on a minimum worker version.

    The action warns rather than raising. A step may declare bindings its script
    never references, and such a step runs correctly with no table; failing it
    would turn a dropped table into a job failure for work that would otherwise
    succeed. The warning exists so the log names the missing channel instead of
    leaving an undefined-symbol error to point at a binding that is plainly
    declared in the template.
    """

    def test_warns_when_a_step_let_has_no_table(self, mock_session, mock_executor) -> None:
        details = _step_details_from_template(
            {
                "name": "MyStep",
                "let": ["base = 'from step'"],
                "script": {"actions": {"onRun": {"command": "echo", "args": ["hi"]}}},
            },
            ["EXPR"],
        )
        assert details.resolved_symbol_table_json is None
        action = RunStepTaskAction(id="action-123", details=details, task_parameter_values={})

        action.start(session=mock_session, executor=mock_executor)

        assert _warned(mock_session)
        # Warned, not raised: the task still goes out.
        mock_session.run_task.assert_called_once()

    def test_warns_for_a_sugar_step_too(self, mock_session, mock_executor) -> None:
        """The sugar path reaches the same check.

        Worth pinning separately: before openjd-model 0.11.9 a sugar step in this
        state still resolved, because ``resolve_syntax_sugar()`` re-declared the
        step's ``let`` in ``script.let`` and the session re-evaluated it. That
        fold is gone, so the sugar path now depends on the table exactly as the
        ``script:`` path does.
        """
        details = _step_details_from_template(
            {
                "name": "MyStep",
                "let": ["base = 'from step'"],
                "bash": {"let": ["msg = base"], "script": "echo hi"},
            },
            ["FEATURE_BUNDLE_1", "EXPR"],
        )
        action = RunStepTaskAction(id="action-123", details=details, task_parameter_values={})

        action.start(session=mock_session, executor=mock_executor)

        assert _warned(mock_session)

    def test_is_silent_when_a_table_is_served(self, mock_session, mock_executor) -> None:
        """Control: the normal case must not warn."""
        details = _step_details_from_template(
            {
                "name": "MyStep",
                "let": ["base = 'from step'"],
                "script": {"actions": {"onRun": {"command": "echo", "args": ["hi"]}}},
            },
            ["EXPR"],
            resolved_symbol_table=json.dumps(
                [{"name": "base", "type": "string", "value": "from step"}]
            ),
        )
        action = RunStepTaskAction(id="action-123", details=details, task_parameter_values={})

        action.start(session=mock_session, executor=mock_executor)

        assert not _warned(mock_session)

    def test_is_silent_for_a_step_with_no_let(self, mock_session, mock_executor) -> None:
        """Control: no ``let`` and no table is the common case, and is fine."""
        details = _step_details_from_template(
            {
                "name": "MyStep",
                "script": {"actions": {"onRun": {"command": "echo", "args": ["hi"]}}},
            },
            ["EXPR"],
        )
        action = RunStepTaskAction(id="action-123", details=details, task_parameter_values={})

        action.start(session=mock_session, executor=mock_executor)

        assert not _warned(mock_session)


class TestRunStepTaskActionSimpleActionSugar:
    """A served FEATURE_BUNDLE_1 simple action has no ``script`` to forward.

    Gap 25: the service serves the sugar as authored, so
    ``StepTemplate.script`` is None. The action de-sugars it here, because
    nothing else on the worker's path does -- ``create_job`` only de-sugars for
    callers that instantiate a job.

    Real templates rather than Mocks: the whole point is what the model's
    ``resolve_syntax_sugar()`` produces, which a Mock cannot tell us.
    """

    def test_start_de_sugars_a_bash_step(self, mock_session, mock_executor):
        """The folded script goes out, carrying the simple action's own `let`.

        ``resolve_syntax_sugar()`` folds the simple action's ``let`` into the script
        it produces. Without that fold the action would have no script at all to
        send.

        The step's template-scope ``let`` is deliberately absent from that list. As
        of openjd-model 0.11.9 the model resolves template scope once at job
        creation rather than folding it here, and this package receives those
        bindings by a different route anyway: the service-resolved symtab it passes
        to ``openjd-sessions``. Asserting them here would test a shape no worker
        sees, since the worker de-sugars a template the service already created.
        """
        details = _step_details_from_template(
            {
                "name": "MyStep",
                "let": ["base = 'from step'"],
                "bash": {"let": ["msg = base"], "script": "echo hi"},
            },
            ["FEATURE_BUNDLE_1", "EXPR"],
        )
        assert details.step_template.script is None
        action = RunStepTaskAction(
            id="action-123",
            details=details,
            task_id="task-456",
            task_parameter_values={},
        )

        action.start(session=mock_session, executor=mock_executor)

        call_kwargs = mock_session.run_task.call_args.kwargs
        step_script = call_kwargs["step_script"]
        assert step_script is not None
        # Simple-action scope only; template scope arrives via the resolved symtab.
        assert step_script.let == ["msg = base"]
        assert step_script.actions.onRun.command == "bash"

    def test_start_does_not_mutate_the_served_template(self, mock_session, mock_executor):
        """De-sugaring returns a new template; the entity's own is untouched.

        StepDetails is cached and re-used across the tasks of a step, so a
        de-sugar that mutated in place would leave later reads of the same
        entity looking at a different shape than the service sent.
        """
        details = _step_details_from_template(
            {"name": "MyStep", "bash": {"script": "echo hi"}},
            ["FEATURE_BUNDLE_1", "EXPR"],
        )
        action = RunStepTaskAction(
            id="action-123",
            details=details,
            task_parameter_values={},
        )

        action.start(session=mock_session, executor=mock_executor)

        assert details.step_template.script is None
        assert details.step_template.bash is not None

    def test_start_forwards_a_plain_script_unchanged(self, mock_session, mock_executor):
        """Control: a ``script:`` template sends its own script object untouched.

        Guards the other direction -- de-sugaring unconditionally would send a
        rebuilt script in place of the served one, so a plain step's script must
        come through by identity. The step's own ``let`` stays out of
        ``script.let`` on this path as it does on the sugar path, since neither
        re-declares template scope.
        """
        details = _step_details_from_template(
            {
                "name": "MyStep",
                "let": ["region = 'us-west-2'"],
                "script": {"actions": {"onRun": {"command": "echo", "args": ["hi"]}}},
            },
            ["EXPR"],
        )
        action = RunStepTaskAction(
            id="action-123",
            details=details,
            task_parameter_values={},
        )

        action.start(session=mock_session, executor=mock_executor)

        call_kwargs = mock_session.run_task.call_args.kwargs
        script = details.step_template.script
        assert script is not None
        assert call_kwargs["step_script"] is script
        # No fold happened: the step's own `let` stayed out of script.let.
        assert script.let is None
