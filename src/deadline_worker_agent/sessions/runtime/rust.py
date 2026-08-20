# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import os
import re
import stat
import tempfile
from datetime import timedelta
from logging import getLogger
from pathlib import Path
from shutil import chown
from typing import TYPE_CHECKING, Any, Optional

from openjd._openjd_rs import create_environment, deserialize_step
from openjd.expr import PathFormat, PathMappingRule as RustPathMappingRule
from openjd.model._v1 import decode_environment_template
from openjd.model._v1.types import (
    JobParameterType,
    JobParameterValue,
    ModelExtension,
    ModelProfile,
    SpecificationRevision,
    TaskParameterType,
    TaskParameterValue,
)
from openjd.sessions import (
    ActionState,
    ActionStatus,
    PathMappingRule,
    PosixSessionUser,
    WindowsSessionUser,
)
from openjd.sessions._v1 import (
    ActionState as RustActionState,
    ActionStatus as RustActionStatus,
    PosixSessionUser as RustPosixSessionUser,
    Session as OpenJDRustSession,
    WindowsSessionUser as RustWindowsSessionUser,
)

from deadline_worker_agent.file_system_operations import (
    FileSystemPermissionEnum,
    set_permissions,
)

from . import SessionRuntime, SessionRuntimeConfig
from ._abc import convert_runtime_crashes

if TYPE_CHECKING:
    from openjd.sessions import (
        EnvironmentIdentifier,
        EnvironmentModel,
        SessionUser,
        StepScriptModel,
    )

__all__ = ["RustSessionRuntime"]

logger = getLogger(__name__)


# Deliberate allowlist of the OpenJD specification revisions this adapter
# supports. Adding a revision is not just adding a map entry: the adapter
# hardcodes 2023-09 wire strings elsewhere (e.g. enter_environment's
# "environment-2023-09" envelope), which must be audited for a new revision.
_SPEC_REVISIONS: dict[str, SpecificationRevision] = {
    SpecificationRevision.v2023_09.value: SpecificationRevision.v2023_09,
}


# openjd.expr.PathFormat is a non-constructable Rust enum, so map the v0
# rule's format string to the member by its value.
_PATH_FORMATS: dict[str, PathFormat] = {
    "POSIX": PathFormat.POSIX,
    "WINDOWS": PathFormat.WINDOWS,
}


# The _v1 ActionState is a non-iterable pyo3 enum, so map each member to the
# v0 enum explicitly (same pattern as _SPEC_REVISIONS/_PATH_FORMATS). A state
# added on the Rust side must be mapped here deliberately — failing loud beats
# silently mis-reporting an action's state to the service.
_ACTION_STATES: dict[str, ActionState] = {
    str(RustActionState.RUNNING): ActionState.RUNNING,
    str(RustActionState.CANCELED): ActionState.CANCELED,
    str(RustActionState.TIMEOUT): ActionState.TIMEOUT,
    str(RustActionState.FAILED): ActionState.FAILED,
    str(RustActionState.SUCCESS): ActionState.SUCCESS,
}


def _to_v0_action_state(state: RustActionState) -> ActionState:
    """Convert a _v1 ActionState into the v0 enum member, failing loud on
    states this adapter does not recognize."""
    try:
        return _ACTION_STATES[str(state)]
    except KeyError:
        raise ValueError(f"Unrecognized _v1 ActionState: {state!r}") from None


def _to_rust_session_user(
    user: Optional[SessionUser],
) -> Optional[RustPosixSessionUser | RustWindowsSessionUser]:
    """Convert a worker (v0) SessionUser into the equivalent _v1 type.

    The worker builds v0 ``openjd.sessions`` user objects, but the Rust session
    only accepts its own ``openjd.sessions._v1`` user classes — structurally
    identical but distinct at the type level. None passes through unchanged
    (the session runs as the agent's own user in that case).
    """
    if user is None:
        return None
    if isinstance(user, PosixSessionUser):
        return RustPosixSessionUser(user.user, group=user.group)
    if isinstance(user, WindowsSessionUser):
        return RustWindowsSessionUser(
            user.user, password=user.password, logon_token=user.logon_token
        )
    raise TypeError(f"Unsupported SessionUser type: {type(user).__name__}")


def _to_rust_parameter_values(
    values: dict[str, Any],
    *,
    value_cls: type,
    type_cls: type,
) -> dict[str, Any]:
    """Convert worker (v0) parameter values into native _v1 parameter values.

    The worker supplies ``openjd.model`` ParameterValue objects; the Rust
    session accepts the native pyo3 value types exported from
    ``openjd.model._v1.types``, which are constructable from Python. Values
    already in the ``{"type", "value"}`` dict form pass through unchanged.
    Parameter types the binding does not define fail loud rather than being
    silently re-encoded.
    """
    converted: dict[str, Any] = {}
    for name, value in values.items():
        if isinstance(value, dict):
            converted[name] = value
            continue
        native_type = getattr(type_cls, value.type.value, None)
        if native_type is None:
            raise ValueError(
                f"Parameter {name!r} has type {value.type.value!r}, which the "
                f"_v1 binding's {type_cls.__name__} does not define."
            )
        converted[name] = value_cls(type=native_type, value=value.value)
    return converted


def _to_rust_job_parameter_values(values: dict[str, Any]) -> dict[str, Any]:
    """Convert worker (v0) job parameter values into native _v1 JobParameterValues."""
    return _to_rust_parameter_values(values, value_cls=JobParameterValue, type_cls=JobParameterType)


def _to_rust_task_parameter_values(values: dict[str, Any]) -> dict[str, Any]:
    """Convert worker (v0) task parameter values into native _v1 TaskParameterValues."""
    return _to_rust_parameter_values(
        values, value_cls=TaskParameterValue, type_cls=TaskParameterType
    )


def _to_environment_parameter_definitions(values: dict[str, Any]) -> list[dict[str, str]]:
    """Synthesize parameterDefinitions entries from job parameter values.

    The environment template is decoded standalone (lifted out of its job
    context), so the job's parameter declarations must be re-supplied here for
    ``{{Param.X}}`` references to resolve. Every ``JobParameterType`` member is a
    valid environment-template parameter type — only task-parameter-space types
    like ``CHUNK[INT]`` are rejected, and those cannot reach here:
    ``JobDetails._validate_job_parameters`` restricts jobDetails parameters to
    the job-parameter-type subset (string/path/int/float), and raw dicts
    originate from the wire which only carries job parameter types.

    No type filter is applied. All values are declared unconditionally.

    Known limitation: only parameters that have *values* are declared. A job
    parameter with no value supplied would still fail validation at decode time.
    In practice everything reaching the worker has a default or submitted value.
    """
    definitions: list[dict[str, str]] = []
    for name, value in values.items():
        if isinstance(value, dict):
            type_str = value.get("type")
            if type_str is None:
                # Skip malformed entries rather than raising — the same raw dict
                # is passed to _to_rust_job_parameter_values moments later, so a
                # genuinely invalid value still fails at Rust session construction
                # with the session's own clear error. This matches the sibling
                # helper's convention of deferring dict validation to the Rust session.
                continue
        else:
            type_str = value.type.value
        definitions.append({"name": name, "type": type_str})
    return definitions


def _to_rust_path_mapping_rule(rule: PathMappingRule) -> RustPathMappingRule:
    """Convert one worker (v0) PathMappingRule into the _v1 (openjd.expr) type.

    The worker builds ``openjd.sessions`` path-mapping rules, but the Rust
    session only accepts ``openjd.expr.PathMappingRule`` — a distinct class with
    the same shape. Paths are coerced to strings, which the _v1 rule stores.
    """
    return RustPathMappingRule(
        source_path_format=_PATH_FORMATS[rule.source_path_format.value],
        source_path=str(rule.source_path),
        destination_path=str(rule.destination_path),
    )


def _to_rust_path_mapping_rules(
    rules: Optional[list[PathMappingRule]],
) -> Optional[list[RustPathMappingRule]]:
    """Convert worker (v0) PathMappingRule lists into the _v1 type, passing None through."""
    if rules is None:
        return None
    return [_to_rust_path_mapping_rule(rule) for rule in rules]


def _to_v0_action_status(status: RustActionStatus) -> ActionStatus:
    """Convert a _v1 ActionStatus back into the v0 type the worker consumes.

    The Rust session reports status with its own ``openjd.sessions._v1``
    ActionStatus/ActionState, but the worker's scheduler compares against the v0
    ActionState enum. The state is remapped through the explicit _ACTION_STATES
    table and the remaining fields are copied across.
    """
    return ActionStatus(
        state=_to_v0_action_state(status.state),
        progress=status.progress,
        status_message=status.status_message,
        fail_message=status.fail_message,
        exit_code=status.exit_code,
    )


class RustSessionRuntime(SessionRuntime):
    """SessionRuntime backed by openjd.sessions._v1 (Rust implementation)."""

    _session: OpenJDRustSession
    _user: Optional[SessionUser]
    _environment_parameter_definitions: list[dict[str, str]]
    _supported_extensions: tuple[str, ...]

    def __init__(self, config: SessionRuntimeConfig) -> None:
        try:
            revision = _SPEC_REVISIONS[config.spec_revision]
        except KeyError:
            raise ValueError(
                f"Unsupported OpenJD specification revision: {config.spec_revision!r}"
            ) from None

        # The extensions to enable are supplied by the config as strings
        # (sourced from the OpenJD model library). The v1 API needs native
        # ModelExtension enums, so convert each name to its enum member.
        #
        # The Python openjd.model library may know extensions the Rust crate
        # doesn't yet (e.g. WRAP_ACTIONS). Skip those rather than fail: a job
        # template that actually requires a skipped extension is rejected at
        # decode time with a clear error, matching how the v0 session
        # tolerates extension names it doesn't recognize.
        extensions: list[ModelExtension] = []
        # Kept in sync with `extensions` — only names that convert successfully
        # are retained, so both lists always describe the same set.
        supported_extension_names: list[str] = []
        for name in config.supported_extensions:
            extension = ModelExtension.from_str(name)
            if extension is None:
                logger.warning(
                    "OpenJD model extension %r is not supported by the Rust session runtime; ignoring it.",
                    name,
                )
                continue
            extensions.append(extension)
            supported_extension_names.append(name)
        self._supported_extensions: tuple[str, ...] = tuple(supported_extension_names)

        # Kept for the attachment-sync path: on POSIX it grants the files it
        # writes group-read access for the session user's group, so the job
        # (which runs as that user) can read them.
        self._user = config.user

        # Job parameter values are fixed for the session's lifetime, so the
        # declarations are synthesized once here. See
        # _to_environment_parameter_definitions for why they are needed at all.
        self._environment_parameter_definitions = _to_environment_parameter_definitions(
            config.job_parameter_values
        )

        # The _v1 session reports status with _v1 ActionStatus/ActionState, but
        # the worker's callback expects the v0 types, so translate on the way out.
        v0_callback = config.action_callback

        def _rust_action_callback(session_id: str, status: RustActionStatus) -> None:
            v0_callback(session_id, _to_v0_action_status(status))

        self._session = OpenJDRustSession(
            session_id=config.session_id,
            job_parameter_values=_to_rust_job_parameter_values(config.job_parameter_values),
            path_mapping_rules=_to_rust_path_mapping_rules(config.path_mapping_rules),
            retain_working_dir=config.retain_working_dir,
            user=_to_rust_session_user(config.user),
            callback=_rust_action_callback,
            os_env_vars=config.os_env_vars,
            session_root_directory=config.session_root_directory,
            profile=ModelProfile(revision=revision, extensions=extensions),
        )

    @convert_runtime_crashes
    def enter_environment(
        self,
        *,
        environment: EnvironmentModel,
        identifier: Optional[EnvironmentIdentifier] = None,
        os_env_vars: Optional[dict[str, str]] = None,
    ) -> EnvironmentIdentifier:
        # The shared action layer hands a pydantic v2023_09 environment, but the
        # Rust session needs a native _v1 environment. Serialize to the OpenJD
        # wire shape and rebuild it natively. exclude_none=True is required, not
        # cosmetic: the Rust decoder rejects explicit nulls (OpenJD treats
        # absent and null as equivalent).
        template: dict[str, Any] = {
            "specificationVersion": "environment-2023-09",
            "environment": environment.model_dump(mode="json", by_alias=True, exclude_none=True),
        }
        # The job's parameter declarations must accompany the environment or its
        # {{Param.X}} references cannot resolve. The key is omitted entirely when
        # empty, because the schema rejects "parameterDefinitions": [].
        if self._environment_parameter_definitions:
            template["parameterDefinitions"] = self._environment_parameter_definitions
        if self._supported_extensions:
            template["extensions"] = list(self._supported_extensions)
        native_environment = create_environment(
            decode_environment_template(
                template,
                supported_extensions=list(self._supported_extensions) or None,
            )
        )
        return self._session.enter_environment(
            environment=native_environment,
            identifier=identifier,
            os_env_vars=os_env_vars,
        )

    @convert_runtime_crashes
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

    @convert_runtime_crashes
    def run_task(
        self,
        *,
        step_script: StepScriptModel,
        task_parameter_values: dict[str, Any],
        os_env_vars: Optional[dict[str, str]] = None,
        log_task_banner: bool = True,
        step_name: str | None = None,
    ) -> None:
        # step_name: forwarded to the _v1 session so RFC 0008's WrappedStep.Name
        # resolves correctly inside onWrapTaskRun hooks.
        #
        # The shared action layer hands a pydantic v2023_09 StepScript, but the
        # Rust session needs a native _v1 step. Serialize to the OpenJD wire
        # shape and rebuild it natively. deserialize_step requires a named step
        # and the binding has no bare step-script deserializer (tracked in
        # OpenJobDescription/openjd-sessions-for-python#334), so wrap the script
        # as ``{"name": ..., "script": ...}`` and unwrap it after decoding.
        # exclude_none=True is required, not cosmetic: the Rust decoder rejects
        # explicit nulls (OpenJD treats absent and null as equivalent).
        native_step_script = deserialize_step(
            {
                "name": step_name or "Placeholder",
                "script": step_script.model_dump(mode="json", by_alias=True, exclude_none=True),
            }
        ).script
        self._session.run_task(
            step_script=native_step_script,
            task_parameter_values=_to_rust_task_parameter_values(task_parameter_values),
            os_env_vars=os_env_vars,
            log_task_banner=log_task_banner,
            step_name=step_name,
        )

    @convert_runtime_crashes
    def _run_task_without_session_env(
        self,
        *,
        step_script: StepScriptModel,
        task_parameter_values: dict[str, Any],
        os_env_vars: Optional[dict[str, str]] = None,
        log_task_banner: bool = True,
        step_name: str | None = None,
    ) -> None:
        # Attachment-sync path: the native run_task's embedded-file handling is
        # unavailable here, so materialize the script's embedded files to the
        # session's files directory ourselves, resolve ``Task.File.*`` argument
        # references to those paths, and run the command as a bare subprocess.
        #
        # STOPGAP — tracked in OpenJobDescription/openjd-sessions-for-python#332.
        # Remove this block and delegate once the _v1 wrapper exposes a
        # ``run_task(..., use_session_env_vars=False)``-style API (which would
        # also restore the library's cross-platform file permission handling).
        file_paths: dict[str, str] = {}
        if step_script.embeddedFiles:
            for embedded_file in step_script.embeddedFiles:
                if embedded_file.data is None:
                    continue
                fd, path = tempfile.mkstemp(
                    dir=str(self._session.files_directory),
                    prefix=f"{embedded_file.name}_",
                )
                # UTF-8 explicitly: the platform default (e.g. cp1252 on
                # Windows) can corrupt or reject non-ASCII paths in the
                # manifest JSON, which the reader script decodes as UTF-8.
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    f.write(str(embedded_file.data))
                # Owner read/write. On POSIX, grant the session user's group read
                # access so the job user can read the materialized file. On Windows,
                # set an explicit ACL (agent=full control, job user=read) rather than
                # relying on NTFS inheritance which may be absent or misconfigured.
                mode = stat.S_IRUSR | stat.S_IWUSR
                if self._user is not None and os.name == "posix":
                    group = getattr(self._user, "group", None)
                    if group is not None:
                        chown(path, group=group)
                        mode |= stat.S_IRGRP
                    os.chmod(path, mode)
                elif self._user is not None and os.name == "nt":
                    set_permissions(
                        file_path=Path(path),
                        agent_user_permission=FileSystemPermissionEnum.READ_WRITE,
                        user_permission=FileSystemPermissionEnum.READ,
                        permitted_user=self._user,
                    )
                else:
                    os.chmod(path, mode)
                file_paths[embedded_file.name] = path

        command = str(step_script.actions.onRun.command)
        args: list[str] = []
        if step_script.actions.onRun.args:
            for arg in step_script.actions.onRun.args:
                # Resolve {{ Task.File.<name> }} references with any interior
                # whitespace, matching the format-string parser's tolerance.
                # References to files that were not materialized (unknown name
                # or no data) are left untouched.
                resolved = re.sub(
                    r"\{\{\s*Task\.File\.(\w+)\s*\}\}",
                    lambda m: file_paths.get(m.group(1), m.group(0)),
                    str(arg),
                )
                args.append(resolved)

        # use_session_env_vars=False bypasses the session environment (e.g. a
        # conda env) so attachment-sync scripts run in a clean environment.
        subprocess_env = {"PYTHONUNBUFFERED": "1", **(os_env_vars or {})}
        self._session.run_subprocess(
            command=command,
            args=args,
            os_env_vars=subprocess_env,
            use_session_env_vars=False,
            log_banner_message="Running Task" if log_task_banner else None,
        )

    @convert_runtime_crashes
    def extend_path_mapping_rules(self, rules: list[PathMappingRule]) -> None:
        # The Rust session exposes this as a public method and sorts the rules
        # by source-path length internally, so no pre-sorting is needed here.
        self._session.extend_path_mapping_rules(
            [_to_rust_path_mapping_rule(rule) for rule in rules]
        )

    @convert_runtime_crashes
    def cancel_action(
        self,
        *,
        time_limit: Optional[timedelta] = None,
        mark_action_failed: bool = False,
    ) -> None:
        self._session.cancel_action(time_limit=time_limit, mark_action_failed=mark_action_failed)

    @convert_runtime_crashes
    def cleanup(self) -> None:
        self._session.cleanup()

    @property
    @convert_runtime_crashes
    def working_directory(self) -> Path:
        return self._session.working_directory

    @property
    @convert_runtime_crashes
    def action_status(self) -> Optional[ActionStatus]:
        status = self._session.action_status
        return _to_v0_action_status(status) if status is not None else None
