# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from datetime import timedelta
from logging import getLogger
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from openjd.model import RevisionExtensions, SpecificationRevision
from openjd.sessions import Session as OpenJDSession

from . import ResolvedSymbolTableError, SessionRuntime, SessionRuntimeConfig

if TYPE_CHECKING:
    from openjd.expr import SerializedSymbolTable
    from openjd.sessions import (
        ActionStatus,
        EnvironmentIdentifier,
        EnvironmentModel,
        PathMappingRule,
        StepScriptModel,
    )

__all__ = ["PythonSessionRuntime"]

logger = getLogger(__name__)


def _extract_job_name(json_str: str | None) -> str | None:
    """Extract the Job.Name value from a resolved symbol table JSON string.

    Returns None when the input is None, the table lacks a Job.Name entry, the
    entry's value is not a string, or parsing fails (graceful degradation —
    mirrors _parse_resolved_symtab in the Rust adapter).
    """
    if json_str is None:
        return None
    try:
        # Imported lazily, behind the None guard above: SerializedSymbolTable
        # lives in openjd.expr, a facade over the native extension. A session
        # that never receives a resolved table must not load the extension.
        # Inside the try so an extension-load ImportError is handled here rather
        # than escaping raw from a lazy import.
        from openjd.expr import SerializedSymbolTable

        symtab = SerializedSymbolTable.from_json_str(json_str).to_symtab()
        entry = symtab.get("Job.Name")
        if entry is None:
            return None
        value = entry.item()
        return value if isinstance(value, str) else None
    except Exception as e:
        logger.warning("Failed to extract Job.Name from resolvedSymbolTable: %s", e)
        return None


def _parse_resolved_symtab(json_str: str | None) -> Optional["SerializedSymbolTable"]:
    """Parse a resolved symbol table JSON string into a SerializedSymbolTable.

    Returns None only when the input is None -- the common, benign case of the
    service serving no table. A table that is present but unparseable raises
    ResolvedSymbolTableError instead of degrading to None, because the table is
    the only channel for step-scope `let` values: proceeding without it would
    run the action with those symbols undefined and report a downstream
    "undefined symbol" error naming the symbol rather than the real cause. It
    would also make a partially-serviceable table (schema skew, truncation)
    indistinguishable from "no table served".

    Callers on the enter and run paths let that propagate. exit_environment
    deliberately catches it and degrades to None so teardown is unconditional --
    see the comment there.

    Mirrors _parse_resolved_symtab in the Rust adapter. Note the deliberate
    asymmetry with _extract_job_name above, which also degrades: it runs during
    session construction rather than action start, and the job name it recovers
    only labels log output.
    """
    if json_str is None:
        return None
    try:
        # Imported lazily, behind the None guard above: see _extract_job_name.
        # Deliberately inside the try. Moving this import from module scope to
        # function scope moved where an unloadable native extension is
        # discovered: it used to fail at adapter construction, which
        # _factory.create_session_runtime turns into a clean NotImplementedError
        # ("adapter is not available"). Left outside the try it would instead
        # surface mid-session as a raw ImportError reported as the task's fail
        # message. Catching it here gives it the same handling as any other
        # reason the table cannot be turned into a symbol table.
        from openjd.expr import SerializedSymbolTable

        return SerializedSymbolTable.from_json_str(json_str)
    # Broad on purpose. Empirically the decoder only raises ValueError, and only
    # for malformed JSON: openjd-model 0.11.6 accepts a well-formed table with an
    # unknown symbol type, an unknown extra field, or a missing required field,
    # so an additive service-side change does not land here. The breadth is to
    # cover the ImportError above and anything the native extension surprises us
    # with, not to paper over version skew.
    except Exception as e:
        # Full detail to the agent log; the raised message reaches the service
        # as the action's fail message, so it must not echo payload contents.
        logger.error("Failed to parse resolvedSymbolTable: %s", e)
        raise ResolvedSymbolTableError(
            "The service served a resolvedSymbolTable this worker could not parse "
            f"({type(e).__name__}); step-scope `let` values cannot be resolved"
        ) from e


class PythonSessionRuntime(SessionRuntime):
    """SessionRuntime backed by openjd.sessions (v0 Python implementation)."""

    _session: OpenJDSession

    def __init__(self, config: SessionRuntimeConfig) -> None:
        self._session = OpenJDSession(
            session_id=config.session_id,
            job_parameter_values=config.job_parameter_values,
            path_mapping_rules=config.path_mapping_rules,
            retain_working_dir=config.retain_working_dir,
            user=config.user,
            callback=config.action_callback,
            os_env_vars=config.os_env_vars,
            session_root_directory=config.session_root_directory,
            job_name=_extract_job_name(config.resolved_symbol_table_json),
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
        environment: EnvironmentModel,
        identifier: Optional[EnvironmentIdentifier] = None,
        os_env_vars: Optional[dict[str, str]] = None,
        resolved_symbol_table_json: str | None = None,
        step_name: str | None = None,
    ) -> EnvironmentIdentifier:
        # Parse the pre-resolved symbol table if the service provided one. The
        # v0 session seeds it as the base of its per-action symbol table, the
        # same layering the _v1 (Rust) session applies.
        return self._session.enter_environment(
            environment=environment,
            identifier=identifier,
            os_env_vars=os_env_vars,
            step_name=step_name,
            resolved_symtab=_parse_resolved_symtab(resolved_symbol_table_json),
        )

    def exit_environment(
        self,
        *,
        identifier: EnvironmentIdentifier,
        os_env_vars: Optional[dict[str, str]] = None,
        keep_session_running: bool = False,
        resolved_symbol_table_json: str | None = None,
    ) -> None:
        # Parse the pre-resolved symbol table if the service provided one.
        #
        # Unlike enter_environment and run_task, an unparseable table must NOT
        # fail this call. Session.exit_environment pops the environment off
        # _active_envs only after this returns, so raising would leave it active
        # and Session._cleanup would retry the exit with the same stored table
        # inside a `except Exception: warning` -- same payload, same failure,
        # swallowed. onExit would never run: no license released, no daemon
        # stopped, no teardown of whatever the environment set up, and only a
        # warning line to show for it.
        #
        # The trade differs from the enter/run paths. There, degrading means an
        # action runs with step-scope symbols undefined and reports a confusing
        # downstream error instead of the real cause. Here it means onExit runs
        # with some symbols possibly undefined -- recoverable, and visible in the
        # session log -- rather than not running at all and leaking state the
        # worker cannot reclaim later. Teardown stays unconditional.
        #
        # _parse_resolved_symtab has already logged the cause at error level.
        try:
            resolved_symtab = _parse_resolved_symtab(resolved_symbol_table_json)
        except ResolvedSymbolTableError:
            resolved_symtab = None

        self._session.exit_environment(
            identifier=identifier,
            os_env_vars=os_env_vars,
            keep_session_running=keep_session_running,
            resolved_symtab=resolved_symtab,
        )

    def run_task(
        self,
        *,
        step_script: StepScriptModel,
        task_parameter_values: dict[str, Any],
        os_env_vars: Optional[dict[str, str]] = None,
        log_task_banner: bool = True,
        step_name: str | None = None,
        resolved_symbol_table_json: str | None = None,
    ) -> None:
        # Parse the pre-resolved symbol table if the service provided one. The
        # v0 session seeds it first and layers Session.*/Task.* values on top,
        # matching the _v1 (Rust) session. It is the only channel for
        # step-scope `let` values.
        self._session.run_task(
            step_script=step_script,
            task_parameter_values=task_parameter_values,
            os_env_vars=os_env_vars,
            log_task_banner=log_task_banner,
            step_name=step_name,
            resolved_symtab=_parse_resolved_symtab(resolved_symbol_table_json),
        )

    def _run_task_without_session_env(
        self,
        *,
        step_script: StepScriptModel,
        task_parameter_values: dict[str, Any],
        os_env_vars: Optional[dict[str, str]] = None,
        log_task_banner: bool = True,
        step_name: str | None = None,
    ) -> None:
        # step_name intentionally not forwarded: openjd-sessions'
        # _run_task_without_session_env does not accept it, and the
        # attachment-sync path has no wrap environment to thread through.
        self._session._run_task_without_session_env(
            step_script=step_script,
            task_parameter_values=task_parameter_values,
            os_env_vars=os_env_vars,
            log_task_banner=log_task_banner,
        )

    def extend_path_mapping_rules(self, rules: list[PathMappingRule]) -> None:
        # bisect.insort only supports the 'key' arg in 3.10 or later, so
        # we first extend the list and sort it afterwards.
        if self._session._path_mapping_rules:
            self._session._path_mapping_rules.extend(rules)
        else:
            self._session._path_mapping_rules = list(rules)
        # openjd sorts path mapping rules by descending source path length so that
        # rules that are subsets of each other match in a predictable (most-specific-first) manner.
        # Reuse openjd's own component count: a URI rule's source_path is a plain
        # string (not a PurePath), so counting `.parts` would not work for it.
        self._session._path_mapping_rules.sort(
            key=lambda rule: -rule._source_path_component_count()
        )

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
    def action_status(self) -> Optional[ActionStatus]:
        return self._session.action_status
