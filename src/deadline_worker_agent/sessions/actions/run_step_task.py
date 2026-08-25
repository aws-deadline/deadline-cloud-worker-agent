# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from concurrent.futures import Executor
from typing import Any, Optional, TYPE_CHECKING, cast

from openjd.model import TaskParameterSet

from ...log_messages import SessionActionLogKind
from .openjd_action import OpenjdAction

if TYPE_CHECKING:
    from openjd.model.v2023_09 import StepScript, StepTemplate

    from ..job_entities import StepDetails
    from ..session import Session


def _resolve_step_script(
    step_template: StepTemplate,
) -> tuple[StepScript, Optional[list[str]]]:
    """Pick the StepScript to run, and the step-scope `let` to send with it.

    The step template arrives un-instantiated, in either of two shapes.

    A `script:` template has step-scope `let` (StepTemplate.let) and
    script-scope `let` (StepTemplate.script.let) as separate fields. Nothing
    folds the former into the latter on this path, so it goes out as
    `extra_let_bindings` or step-scope names are missing from the session's
    symbol table.

    A FEATURE_BUNDLE_1 simple-action template (`bash:`, `cmd:`, `node:`,
    `powershell:`, `python:`) has no `script` at all. The service serves the
    sugar as authored and the worker never instantiates a job, so nothing
    de-sugars it. `resolve_syntax_sugar()` does that here, returning a new
    template whose script carries `[*step lets, *simple-action lets]`.

    That fold is why the de-sugared path sends `extra_let_bindings=None`: the
    step-scope bindings are already inside `script.let`, and applying them
    twice is not harmless. A literal binding is idempotent, but a
    self-referential one is not, and `n = n + 1` applied twice yields 3.
    """
    script = step_template.script
    if script is not None:
        return script, step_template.let

    # The model rejects a StepTemplate carrying neither `script` nor a simple
    # action, so the fold always produces a script. The cast records that
    # invariant for the type checker; it is not a runtime conversion.
    folded = step_template.resolve_syntax_sugar()
    return cast("StepScript", folded.script), None


class RunStepTaskAction(OpenjdAction):
    """Action to run a step's task within a Worker session

    Parameters
    ----------
    id : str
        A unique identifier for the session action
    step_id : str
        The unique step identifier
    details : StepDetails
        The environment details
    task_id : Optional[str]
        The unique task identifier
    task_parameter_values : TaskParameterSet
        The task parameter values
    """

    task_id: Optional[str]
    _details: StepDetails
    _task_parameter_values: TaskParameterSet

    def __init__(
        self,
        *,
        id: str,
        details: StepDetails,
        task_id: Optional[str] = None,
        task_parameter_values: TaskParameterSet,
    ) -> None:
        super(RunStepTaskAction, self).__init__(
            id=id, action_log_kind=SessionActionLogKind.TASK_RUN, step_id=details.step_id
        )
        self._details = details
        self.task_id = task_id
        self._task_parameter_values = task_parameter_values

    def __eq__(self, other: Any) -> bool:
        return (
            type(self) is type(other)
            and self._id == other._id
            and self.step_id == other.step_id
            and self.task_id == other.task_id
            and self._details == other._details
            and self._task_parameter_values == other._task_parameter_values
        )

    def start(self, *, session: Session, executor: Executor) -> None:
        """Initiates the running of a step's task in the session

        Parameters
        ----------
        session : Session
            The Session that is the target of the action
        executor : Executor
            An executor for running futures
        """
        env_vars = {
            "DEADLINE_STEP_ID": self._details.step_id,
            "DEADLINE_SESSIONACTION_ID": self._id,
        }
        if self.task_id is not None:
            env_vars["DEADLINE_TASK_ID"] = self.task_id

        step_template = self._details.step_template
        step_script, extra_let_bindings = _resolve_step_script(step_template)

        session.run_task(
            step_script=step_script,
            task_parameter_values=self._task_parameter_values,
            os_env_vars=env_vars,
            step_name=step_template.name,
            resolved_symbol_table_json=self._details.resolved_symbol_table_json,
            extra_let_bindings=extra_let_bindings,
        )
