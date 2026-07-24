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

from openjd._openjd_rs import StepScript as V1StepScript, create_environment, deserialize_step
from openjd.model._v1 import decode_environment_template
from openjd.model._v1.errors import DecodeValidationError
from openjd.model._v1.types import (
    ModelExtension,
    ModelProfile,
    SpecificationRevision,
)
from openjd.sessions import (
    PosixSessionUser,
    WindowsSessionUser,
)
from openjd.sessions._v1 import (
    PosixSessionUser as V1PosixSessionUser,
    Session as OpenJDRustSession,
    WindowsSessionUser as V1WindowsSessionUser,
)

from . import SessionRuntime, SessionRuntimeConfig, SessionRuntimeDecodeError

if TYPE_CHECKING:
    from openjd.expr import PathMappingRule
    from openjd.sessions import EnvironmentIdentifier, SessionUser
    from openjd.sessions._v1 import ActionStatus

__all__ = ["RustSessionRuntime"]

logger = getLogger(__name__)


# Deliberate allowlist of the OpenJD specification revisions this adapter
# supports. Adding a revision is not just adding a map entry: the adapter
# hardcodes 2023-09 wire strings elsewhere (e.g. enter_environment's
# "environment-2023-09" envelope), which must be audited for a new revision.
_SPEC_REVISIONS: dict[str, SpecificationRevision] = {
    SpecificationRevision.v2023_09.value: SpecificationRevision.v2023_09,
}


def _to_v1_session_user(
    user: Optional[SessionUser],
) -> Optional[V1PosixSessionUser | V1WindowsSessionUser]:
    """Convert a worker (v0) SessionUser into the equivalent _v1 type.

    The session user is the interface's one deliberate v0 hold-over (see
    SessionRuntimeConfig): the worker's OS-user machinery is v0-native, so this
    adapter converts at its boundary. None passes through unchanged (the
    session runs as the agent's own user in that case).
    """
    if user is None:
        return None
    if isinstance(user, PosixSessionUser):
        return V1PosixSessionUser(user.user, group=user.group)
    if isinstance(user, WindowsSessionUser):
        return V1WindowsSessionUser(user.user, password=user.password, logon_token=user.logon_token)
    raise TypeError(f"Unsupported SessionUser type: {type(user).__name__}")


class RustSessionRuntime(SessionRuntime):
    """SessionRuntime backed by openjd.sessions._v1 (Rust implementation).

    The interface speaks the _v1 native types and wire-format JSON, so this
    adapter passes scalar values straight through and decodes the JSON
    documents with the _v1 decoders directly.
    """

    _session: OpenJDRustSession
    _user: Optional[SessionUser]

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
        for name in config.supported_extensions:
            extension = ModelExtension.from_str(name)
            if extension is None:
                logger.warning(
                    "OpenJD model extension %r is not supported by the Rust session runtime; ignoring it.",
                    name,
                )
                continue
            extensions.append(extension)

        # Kept for the attachment-sync path: on POSIX it grants the files it
        # writes group-read access for the session user's group, so the job
        # (which runs as that user) can read them.
        self._user = config.user

        self._session = OpenJDRustSession(
            session_id=config.session_id,
            job_parameter_values=config.job_parameter_values,
            path_mapping_rules=config.path_mapping_rules,
            retain_working_dir=config.retain_working_dir,
            user=_to_v1_session_user(config.user),
            callback=config.action_callback,
            os_env_vars=config.os_env_vars,
            session_root_directory=config.session_root_directory,
            profile=ModelProfile(revision=revision, extensions=extensions),
        )

    def enter_environment(
        self,
        *,
        environment: dict[str, Any],
        identifier: Optional[EnvironmentIdentifier] = None,
        os_env_vars: Optional[dict[str, str]] = None,
    ) -> EnvironmentIdentifier:
        # The interface hands the wire-format environment JSON; wrap it in the
        # template envelope the _v1 decoder expects and decode natively.
        try:
            native_environment = create_environment(
                decode_environment_template(
                    {
                        "specificationVersion": "environment-2023-09",
                        "environment": environment,
                    }
                )
            )
        except DecodeValidationError as exc:
            raise SessionRuntimeDecodeError(
                boundary="RustSessionRuntime.enter_environment (_v1 environment decode)",
                cause=exc,
            ) from exc
        return self._session.enter_environment(
            environment=native_environment,
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

    def _decode_step_script(self, step_script: dict[str, Any]) -> V1StepScript:
        # deserialize_step requires a named step and the binding has no bare
        # step-script deserializer (tracked in
        # OpenJobDescription/openjd-sessions-for-python#334), so wrap the script
        # as ``{"name": ..., "script": ...}`` and unwrap it after decoding.
        try:
            return deserialize_step({"name": "Placeholder", "script": step_script}).script
        except (DecodeValidationError, ValueError) as exc:
            raise SessionRuntimeDecodeError(
                boundary="RustSessionRuntime.run_task (_v1 step script decode)",
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
            task_parameter_values=task_parameter_values,
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
        # Attachment-sync path: the native run_task's embedded-file handling is
        # unavailable here, so materialize the script's embedded files to the
        # session's files directory ourselves, resolve ``Task.File.*``
        # references to those paths, and run the command as a bare subprocess.
        #
        # STOPGAP — tracked in OpenJobDescription/openjd-sessions-for-python#332.
        # Remove this block and delegate once the _v1 wrapper exposes a
        # ``run_task(..., use_session_env_vars=False)``-style API (which would
        # also restore the library's cross-platform file permission handling).
        script = self._decode_step_script(step_script)

        file_paths: dict[str, str] = {}
        for embedded_file in script.embeddedFiles or []:
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
            # Owner read/write. On POSIX, also grant the session user's group
            # read access so the job user can read the materialized file.
            mode = stat.S_IRUSR | stat.S_IWUSR
            if self._user is not None and os.name == "posix":
                group = getattr(self._user, "group", None)
                if group is not None:
                    chown(path, group=group)
                    mode |= stat.S_IRGRP
            os.chmod(path, mode)
            file_paths[embedded_file.name] = path

        def _resolve_task_file_refs(value: str) -> str:
            # Resolve {{ Task.File.<name> }} references with any interior
            # whitespace, matching the format-string parser's tolerance.
            # References to files that were not materialized (unknown name
            # or no data) are left untouched.
            return re.sub(
                r"\{\{\s*Task\.File\.(\w+)\s*\}\}",
                lambda m: file_paths.get(m.group(1), m.group(0)),
                value,
            )

        # The command gets the same Task.File resolution as the args: the v0
        # session and the Rust engine's own runner resolve both identically,
        # and attachment scripts may reference an embedded file as the command
        # itself (e.g. ``command: "{{ Task.File.syncScript }}"``).
        command = _resolve_task_file_refs(str(script.actions.onRun.command))
        args = [_resolve_task_file_refs(str(arg)) for arg in script.actions.onRun.args or []]

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

    def extend_path_mapping_rules(self, rules: list[PathMappingRule]) -> None:
        # The Rust session exposes this as a public method and sorts the rules
        # by source-path length internally, so no pre-sorting is needed here.
        self._session.extend_path_mapping_rules(rules)

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
