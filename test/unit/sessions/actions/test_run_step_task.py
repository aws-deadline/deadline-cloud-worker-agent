# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

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


def _step_details_from_template(template: dict[str, Any], extensions: list[str]) -> StepDetails:
    """Parse a served step template the way BatchGetJobEntity delivers it."""
    payload: Any = {
        "jobId": "job-123",
        "stepId": "step-123",
        "schemaVersion": "jobtemplate-2023-09",
        "dependencies": [],
        "extensions": extensions,
        "template": template,
    }
    return StepDetails.from_boto(payload)


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
        """The folded script goes out, carrying both `let` scopes in order.

        ``resolve_syntax_sugar()`` folds step-scope ``let`` into the script's own
        ``let`` as ``[*step lets, *simple-action lets]``. Without that fold the
        action would have no script at all to send.
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
        # Both scopes present exactly once, step bindings first.
        assert step_script.let == ["base = 'from step'", "msg = base"]
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

        Guards the other direction -- de-sugaring unconditionally would fold
        the step's ``let`` into ``script.let`` and send a rebuilt script, so a
        plain step's script must come through by identity.
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
