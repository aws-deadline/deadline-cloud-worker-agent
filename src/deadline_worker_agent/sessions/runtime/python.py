# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from datetime import timedelta
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import TYPE_CHECKING, Any, Optional

from openjd.expr import PathFormat as V1PathFormat, PathMappingRule as V1PathMappingRule
from openjd.model import (
    DecodeValidationError,
    ParameterValue,
    ParameterValueType,
    RevisionExtensions,
    SpecificationRevision,
    parse_model,
)
from openjd.model.v2023_09 import (
    Environment as Environment_2023_09,
    StepScript as StepScript_2023_09,
)
from openjd.sessions import (
    ActionState,
    ActionStatus,
    PathFormat,
    PathMappingRule,
    Session as OpenJDSession,
)
from openjd.sessions._v1 import (
    ActionState as V1ActionState,
    ActionStatus as V1ActionStatus,
)

from . import SessionRuntime, SessionRuntimeConfig, SessionRuntimeDecodeError
from ._v0_compat import _exit_code_to_i32

if TYPE_CHECKING:
    from openjd.sessions import EnvironmentIdentifier

__all__ = ["PythonSessionRuntime"]


# This adapter is the v1->v0 bridge: the SessionRuntime interface speaks the
# _v1 native types, and everything below converts them to the v0 library's
# types (and back, for statuses). When the Python runtime is retired, this
# module is deleted and no v0 conversion code remains in the worker.


# The v0 ActionState -> _v1 ActionState translation. Keyed off the real v0
# members so a rename on either side surfaces here. A state added on the v0
# side must be mapped deliberately -- failing loud beats silently
# mis-reporting an action's state to the service.
_V1_ACTION_STATES: dict[ActionState, V1ActionState] = {
    ActionState.RUNNING: V1ActionState.RUNNING,
    ActionState.CANCELED: V1ActionState.CANCELED,
    ActionState.TIMEOUT: V1ActionState.TIMEOUT,
    ActionState.FAILED: V1ActionState.FAILED,
    ActionState.SUCCESS: V1ActionState.SUCCESS,
}

# openjd.expr.PathFormat (the _v1 rule's format) mapped to the v0 enum. The
# _v1 enum is non-iterable, so keys are computed from its members at import.
_V0_PATH_FORMATS: dict[str, PathFormat] = {
    str(V1PathFormat.POSIX): PathFormat.POSIX,
    str(V1PathFormat.WINDOWS): PathFormat.WINDOWS,
}


def _to_v1_action_status(status: ActionStatus) -> V1ActionStatus:
    """Convert a v0 ActionStatus into the _v1 type the interface reports."""
    try:
        state = _V1_ACTION_STATES[status.state]
    except KeyError:
        raise ValueError(f"Unrecognized v0 ActionState: {status.state!r}") from None
    return V1ActionStatus(
        state=state,
        progress=status.progress,
        status_message=status.status_message,
        fail_message=status.fail_message,
        exit_code=_exit_code_to_i32(status.exit_code) if status.exit_code is not None else None,
    )


def _to_v0_parameter_values(values: dict[str, dict[str, Any]]) -> dict[str, ParameterValue]:
    """Convert wire-format ``{"type", "value"}`` dicts into v0 ParameterValues."""
    converted: dict[str, ParameterValue] = {}
    for name, value in values.items():
        try:
            value_type = ParameterValueType(value["type"])
        except ValueError:
            raise ValueError(
                f"Parameter {name!r} has type {value['type']!r}, which the "
                "v0 ParameterValueType enum does not define."
            ) from None
        converted[name] = ParameterValue(type=value_type, value=value["value"])
    return converted


def _to_v0_path_mapping_rule(rule: V1PathMappingRule) -> PathMappingRule:
    """Convert one _v1 (openjd.expr) PathMappingRule into the v0 type.

    The source path is rebuilt as the pure-path class matching its declared
    format, mirroring how the worker's entity layer constructs v0 rules.
    """
    try:
        source_path_format = _V0_PATH_FORMATS[str(rule.source_path_format)]
    except KeyError:
        raise ValueError(f"Unrecognized _v1 path format: {rule.source_path_format!r}") from None
    source_path = (
        PureWindowsPath(rule.source_path)
        if source_path_format == PathFormat.WINDOWS
        else PurePosixPath(rule.source_path)
    )
    return PathMappingRule(
        source_path_format=source_path_format,
        source_path=source_path,
        destination_path=Path(rule.destination_path),
    )


def _to_v0_path_mapping_rules(
    rules: Optional[list[V1PathMappingRule]],
) -> Optional[list[PathMappingRule]]:
    """Convert _v1 PathMappingRule lists into the v0 type, passing None through."""
    if rules is None:
        return None
    return [_to_v0_path_mapping_rule(rule) for rule in rules]


class PythonSessionRuntime(SessionRuntime):
    """SessionRuntime backed by openjd.sessions (v0 Python implementation).

    Bridges the interface's _v1/wire-format vocabulary to the v0 library:
    wire-JSON documents are decoded with the v0 pydantic models, and scalar
    values are converted at the boundary.
    """

    _session: OpenJDSession

    def __init__(self, config: SessionRuntimeConfig) -> None:
        # The v0 session reports v0 ActionStatus; the interface's callback
        # expects the _v1 type, so translate on the way out.
        v1_callback = config.action_callback

        def _action_callback(session_id: str, status: ActionStatus) -> None:
            v1_callback(session_id, _to_v1_action_status(status))

        self._session = OpenJDSession(
            session_id=config.session_id,
            job_parameter_values=_to_v0_parameter_values(config.job_parameter_values),
            path_mapping_rules=_to_v0_path_mapping_rules(config.path_mapping_rules),
            retain_working_dir=config.retain_working_dir,
            user=config.user,
            callback=_action_callback,
            os_env_vars=config.os_env_vars,
            session_root_directory=config.session_root_directory,
            revision_extensions=RevisionExtensions(
                # Currently for simplicity request that our session allow all extensions.
                # This does not obey the spec. It should be changed at a later date to the
                # list of requested extensions once those are returned by BatchGetJobEntity.
                spec_rev=SpecificationRevision(config.spec_revision),
                supported_extensions=list(config.supported_extensions),
            ),
        )

    def enter_environment(
        self,
        *,
        environment: dict[str, Any],
        identifier: Optional[EnvironmentIdentifier] = None,
        os_env_vars: Optional[dict[str, str]] = None,
    ) -> EnvironmentIdentifier:
        try:
            environment_model = parse_model(model=Environment_2023_09, obj=environment)
        except DecodeValidationError as exc:
            raise SessionRuntimeDecodeError(
                boundary="PythonSessionRuntime.enter_environment (v0 environment decode)",
                cause=exc,
            ) from exc
        return self._session.enter_environment(
            environment=environment_model,
            identifier=identifier,
            os_env_vars=os_env_vars,
        )

    def exit_environment(
        self,
        *,
        identifier: EnvironmentIdentifier,
        os_env_vars: Optional[dict[str, str]] = None,
        keep_session_running: bool = False,
    ) -> None:
        self._session.exit_environment(
            identifier=identifier,
            os_env_vars=os_env_vars,
            keep_session_running=keep_session_running,
        )

    def _decode_step_script(self, step_script: dict[str, Any]) -> StepScript_2023_09:
        try:
            return parse_model(model=StepScript_2023_09, obj=step_script)
        except DecodeValidationError as exc:
            raise SessionRuntimeDecodeError(
                boundary="PythonSessionRuntime.run_task (v0 step script decode)",
                cause=exc,
            ) from exc

    def run_task(
        self,
        *,
        step_script: dict[str, Any],
        task_parameter_values: dict[str, dict[str, Any]],
        os_env_vars: Optional[dict[str, str]] = None,
        log_task_banner: bool = True,
    ) -> None:
        self._session.run_task(
            step_script=self._decode_step_script(step_script),
            task_parameter_values=_to_v0_parameter_values(task_parameter_values),
            os_env_vars=os_env_vars,
            log_task_banner=log_task_banner,
        )

    def _run_task_without_session_env(
        self,
        *,
        step_script: dict[str, Any],
        task_parameter_values: dict[str, dict[str, Any]],
        os_env_vars: Optional[dict[str, str]] = None,
        log_task_banner: bool = True,
    ) -> None:
        self._session._run_task_without_session_env(
            step_script=self._decode_step_script(step_script),
            task_parameter_values=_to_v0_parameter_values(task_parameter_values),
            os_env_vars=os_env_vars,
            log_task_banner=log_task_banner,
        )

    def extend_path_mapping_rules(self, rules: list[V1PathMappingRule]) -> None:
        v0_rules = [_to_v0_path_mapping_rule(rule) for rule in rules]
        # bisect.insort only supports the 'key' arg in 3.10 or later, so
        # we first extend the list and sort it afterwards.
        if self._session._path_mapping_rules:
            self._session._path_mapping_rules.extend(v0_rules)
        else:
            self._session._path_mapping_rules = list(v0_rules)
        # openjd sorts path mapping rules by descending source path length so that
        # rules that are subsets of each other match in a predictable (most-specific-first) manner.
        self._session._path_mapping_rules.sort(key=lambda rule: -len(rule.source_path.parts))

    def cancel_action(
        self,
        *,
        time_limit: Optional[timedelta] = None,
        mark_action_failed: bool = False,
    ) -> None:
        self._session.cancel_action(time_limit=time_limit, mark_action_failed=mark_action_failed)

    def cleanup(self) -> None:
        self._session.cleanup()

    @property
    def working_directory(self) -> Path:
        return self._session.working_directory

    @property
    def action_status(self) -> Optional[V1ActionStatus]:
        status = self._session.action_status
        return _to_v1_action_status(status) if status is not None else None
