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


def _resolve_step_script(step_template: StepTemplate) -> StepScript:
    """Pick the StepScript to run.

    The step template arrives un-instantiated, in either of two shapes.

    A `script:` template already carries the script to run, in
    `StepTemplate.script`.

    A FEATURE_BUNDLE_1 simple-action template (`bash:`, `cmd:`, `node:`,
    `powershell:`, `python:`) has no `script` at all. The service serves the
    sugar as authored and the worker never instantiates a job, so nothing
    de-sugars it. `resolve_syntax_sugar()` does that here, returning a new
    template whose script carries the simple action's own `let` and nothing
    else.

    Step-scope `let` values reach the session through the resolved symbol table
    the service serves, on both shapes alike. That is the only channel: the
    step's own `let` is evaluated once in template scope at job creation and its
    values travel in `create_job_with_symbol_tables().step_symbol_tables[name]`,
    which the service serves as the entity's `resolvedSymbolTable`. Neither
    branch above re-declares those names, so their source expressions are never
    re-evaluated on the worker.

    Deliberately so, as of the `openjd-model` floor in `pyproject.toml`: folding
    step scope into `script.let` had the session re-evaluate those bindings in
    host scope, re-rendering PATH values over the correctly formatted seeded
    value.

    A step whose `let` the service serves no table for therefore has no binding
    for those names -- a service-side condition, from the table being dropped over
    a size cap or gated on a minimum worker version.
    `_warn_if_step_let_has_no_table` reports it so the log names that cause rather
    than only the symbol.

    On the sugar path that is a change in outcome, and worth being explicit about.
    Before the model stopped folding, a tableless sugar step still resolved: the
    fold re-declared the step's `let` in `script.let` and `apply_let_bindings`
    evaluated it. A tableless `script:` step never had that and failed then as it
    fails now. So the sugar path lost a route that did work, and the size cap in
    particular is not monotone in worker version -- it fires on payload size, so it
    can reach a job that ran yesterday.

    Not restored here, and the reason is not that it is impossible -- a fold
    conditional on no table being served would work. It is that the value it
    produces is the wrong kind of wrong. Host-scope evaluation is what the model
    withdrew the fold to stop: it re-renders a PATH binding in the host's format
    rather than the format job creation resolved it to. With no table there is no
    seeded value to clobber, so nothing detects the difference, and the step runs
    with a plausible but differently-formatted path -- writing output somewhere
    slightly wrong instead of failing. A conditional fold would also make the same
    template resolve or not depending on whether it was authored as `bash:` or
    `script:`, and would mean rebuilding the script here, since the model exposes no
    "fold anyway" mode.

    A clear failure is the better outcome, which is what an undefined symbol
    produces, with the warning above naming the missing table as its cause.
    """
    script = step_template.script
    if script is not None:
        return script

    # The model rejects a StepTemplate carrying neither `script` nor a simple
    # action, so the fold always produces a script. The cast records that
    # invariant for the type checker; it is not a runtime conversion.
    folded = step_template.resolve_syntax_sugar()
    return cast("StepScript", folded.script)


def _warn_if_step_let_has_no_table(session: Session, details: StepDetails) -> None:
    """Log when a step declares a template-scope `let` but no table was served.

    Those two facts are only visible together here, which is why this sits on the
    action rather than in `_resolve_step_script`.

    Emitted on `session.logger`, the session log, rather than the module logger.
    The reader this is written for is whoever opens the failed task's log and sees
    an undefined-symbol error; the agent log is on the host and is not theirs to
    read at all on a service-managed fleet. Same reason the attachment actions
    build their own adapter over OPENJD_LOG.

    Once per task, deliberately. The condition belongs to the step rather than the
    task, so this repeats across a step's tasks -- but so does the failure it
    explains, and each task's log is read on its own. A reader who opens one failed
    task finds exactly one warning; suppressing all but the first would leave most
    failed tasks unexplained.

    A warning rather than a raise. `_parse_resolved_symtab` raises on a table that
    is present but unparseable, because there the served bytes are evidence the
    service meant to supply symbols and something corrupted them. An absent table
    is not that: a step may declare `let` bindings its script never references, and
    such a step runs correctly with no table at all. Failing it would turn a
    dropped table into a job failure for work that would otherwise succeed.

    Scope: this detects an absent table, not a served table that lacks the step's
    names. Checking the latter would mean parsing the table here and deciding which
    names the script needs -- work the session already does, and the place its
    result is already reported. So the message says what was observed, a table that
    was not served, rather than claiming every route to an undefined symbol.
    """
    if details.step_template.let and details.resolved_symbol_table_json is None:
        session.logger.warning(
            "Step %s declares template-scope `let` bindings but the service served no "
            "resolved symbol table, so those names are undefined for this task. The "
            "table is the only channel for them; the worker does not evaluate the "
            "step's `let` itself. Expect an undefined-symbol failure if the script "
            "references one.",
            details.step_id,
        )


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
        _warn_if_step_let_has_no_table(session, self._details)
        step_script = _resolve_step_script(step_template)

        session.run_task(
            step_script=step_script,
            task_parameter_values=self._task_parameter_values,
            os_env_vars=env_vars,
            step_name=step_template.name,
            resolved_symbol_table_json=self._details.resolved_symbol_table_json,
        )
