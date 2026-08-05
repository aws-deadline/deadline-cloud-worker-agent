# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from openjd.model import RevisionExtensions, SpecificationRevision
from openjd.sessions import Session as OpenJDSession

from . import SessionRuntime, SessionRuntimeConfig

if TYPE_CHECKING:
    from openjd.sessions import (
        ActionStatus,
        EnvironmentIdentifier,
        EnvironmentModel,
        PathMappingRule,
        StepScriptModel,
    )

__all__ = ["PythonSessionRuntime"]


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
    ) -> EnvironmentIdentifier:
        return self._session.enter_environment(
            environment=environment,
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

    def run_task(
        self,
        *,
        step_script: StepScriptModel,
        task_parameter_values: dict[str, Any],
        os_env_vars: Optional[dict[str, str]] = None,
        log_task_banner: bool = True,
        step_name: str | None = None,
    ) -> None:
        self._session.run_task(
            step_script=step_script,
            task_parameter_values=task_parameter_values,
            os_env_vars=os_env_vars,
            log_task_banner=log_task_banner,
            step_name=step_name,
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
