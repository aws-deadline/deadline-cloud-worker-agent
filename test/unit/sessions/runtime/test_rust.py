# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import os
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any, Generator
from unittest.mock import MagicMock, call, patch

import pytest

from openjd.model._v1.types import ModelProfile, SpecificationRevision

from deadline_worker_agent.sessions.runtime import (
    SessionRuntime,
    SessionRuntimeConfig,
    SessionRuntimeDecodeError,
)
from deadline_worker_agent.sessions.runtime import rust as rust_module
from deadline_worker_agent.sessions.runtime.rust import RustSessionRuntime


@pytest.fixture()
def runtime_config() -> SessionRuntimeConfig:
    """Minimal valid SessionRuntimeConfig for testing."""
    return SessionRuntimeConfig(
        session_id="session-1",
        job_parameter_values={"Param1": {"type": "STRING", "value": "value1"}},
        path_mapping_rules=None,
        retain_working_dir=False,
        user=None,
        action_callback=lambda session_id, status: None,
        os_env_vars=None,
        session_root_directory=Path("/tmp/sessions/session-1"),
    )


@pytest.fixture()
def mock_rust_session() -> Generator[MagicMock, None, None]:
    with patch.object(rust_module, "OpenJDRustSession") as mock_cls:
        yield mock_cls


class TestRustSessionRuntimeConstruction:
    def test_construction_when_default_config_maps_kwargs_to_v1_session(
        self, runtime_config: SessionRuntimeConfig, mock_rust_session: MagicMock
    ) -> None:
        RustSessionRuntime(runtime_config)

        mock_rust_session.assert_called_once()
        call_kwargs = mock_rust_session.call_args.kwargs
        assert call_kwargs["session_id"] == "session-1"
        # Wire-format dicts pass straight through without conversion.
        assert call_kwargs["job_parameter_values"] == {
            "Param1": {"type": "STRING", "value": "value1"}
        }
        assert call_kwargs["path_mapping_rules"] is None
        assert call_kwargs["retain_working_dir"] is False
        assert call_kwargs["user"] is None
        assert callable(call_kwargs["callback"])
        assert call_kwargs["os_env_vars"] is None
        assert call_kwargs["session_root_directory"] == Path("/tmp/sessions/session-1")
        profile = call_kwargs["profile"]
        assert isinstance(profile, ModelProfile)

    def test_construction_when_no_extensions_configured_wires_empty_list(
        self, runtime_config: SessionRuntimeConfig, mock_rust_session: MagicMock
    ) -> None:
        # Proves the extensions are NOT hardcoded: an empty config yields an
        # empty extensions list rather than a fixed set.
        with (
            patch.object(rust_module, "ModelProfile") as mock_profile,
            patch.object(rust_module, "ModelExtension") as mock_extension,
        ):
            RustSessionRuntime(runtime_config)

        mock_extension.from_str.assert_not_called()
        profile_kwargs = mock_profile.call_args.kwargs
        assert profile_kwargs["extensions"] == []
        assert profile_kwargs["revision"] == SpecificationRevision.v2023_09

    def test_construction_when_extensions_configured_wires_them_from_config(
        self, mock_rust_session: MagicMock
    ) -> None:
        config = SessionRuntimeConfig(
            session_id="session-2",
            job_parameter_values={},
            path_mapping_rules=None,
            retain_working_dir=False,
            user=None,
            action_callback=lambda session_id, status: None,
            os_env_vars=None,
            session_root_directory=Path("/tmp/sessions/session-2"),
            supported_extensions=("EXPR", "TASK_CHUNKING"),
        )

        with (
            patch.object(rust_module, "ModelProfile") as mock_profile,
            patch.object(rust_module, "ModelExtension") as mock_extension,
        ):
            mock_extension.from_str.side_effect = lambda name: f"EXT::{name}"
            RustSessionRuntime(config)

        # Each configured extension identifier is coerced via from_str, in order.
        assert mock_extension.from_str.call_args_list == [call("EXPR"), call("TASK_CHUNKING")]
        profile_kwargs = mock_profile.call_args.kwargs
        assert profile_kwargs["extensions"] == ["EXT::EXPR", "EXT::TASK_CHUNKING"]

    def test_construction_when_unknown_extension_warns_and_skips(
        self, mock_rust_session: MagicMock
    ) -> None:
        config = SessionRuntimeConfig(
            session_id="session-3",
            job_parameter_values={},
            path_mapping_rules=None,
            retain_working_dir=False,
            user=None,
            action_callback=lambda session_id, status: None,
            os_env_vars=None,
            session_root_directory=Path("/tmp/sessions/session-3"),
            supported_extensions=("NOT_A_REAL_EXTENSION",),
        )

        with patch.object(rust_module, "logger") as mock_logger:
            RustSessionRuntime(config)

        mock_logger.warning.assert_called_once()
        assert "NOT_A_REAL_EXTENSION" in mock_logger.warning.call_args[0][1]
        profile = mock_rust_session.call_args.kwargs["profile"]
        assert profile.extensions == []

    def test_construction_when_unknown_spec_revision_raises_value_error(
        self, mock_rust_session: MagicMock
    ) -> None:
        config = SessionRuntimeConfig(
            session_id="session-4",
            job_parameter_values={},
            path_mapping_rules=None,
            retain_working_dir=False,
            user=None,
            action_callback=lambda session_id, status: None,
            os_env_vars=None,
            session_root_directory=Path("/tmp/sessions/session-4"),
            spec_revision="9999-99",
        )

        with pytest.raises(ValueError, match="Unsupported OpenJD specification revision"):
            RustSessionRuntime(config)

    @pytest.mark.skipif(os.name == "nt", reason="PosixSessionUser is only constructible on POSIX")
    def test_construction_when_posix_user_converts_v0_to_v1(
        self, runtime_config: SessionRuntimeConfig, mock_rust_session: MagicMock
    ) -> None:
        """A real v0 PosixSessionUser must be converted to the _v1 type before
        passing it to the Rust session — they are distinct classes with the same
        fields.  Without the conversion the Rust binding raises TypeError."""
        from openjd.sessions import PosixSessionUser
        from openjd.sessions._v1 import PosixSessionUser as V1PosixSessionUser

        v0_user = PosixSessionUser(user="job-user", group="job-group")
        config = replace(runtime_config, user=v0_user)
        RustSessionRuntime(config)

        passed_user = mock_rust_session.call_args.kwargs["user"]
        assert isinstance(passed_user, V1PosixSessionUser)
        assert passed_user.user == "job-user"
        assert passed_user.group == "job-group"

    @pytest.mark.skipif(os.name != "nt", reason="WindowsSessionUser only available on Windows")
    def test_construction_when_windows_user_converts_v0_to_v1(
        self, runtime_config: SessionRuntimeConfig, mock_rust_session: MagicMock
    ) -> None:
        """Same as the POSIX test but for WindowsSessionUser.

        The _v1 WindowsSessionUser constructor performs a real Win32 logon to
        validate the credentials, so it is mocked out — the adapter's
        responsibility under test is only that it converts the v0 user to the
        _v1 type and forwards the fields.
        """
        from openjd.sessions import WindowsSessionUser

        v0_user = WindowsSessionUser(user="job-user", password="secret")
        config = replace(runtime_config, user=v0_user)
        with patch.object(rust_module, "V1WindowsSessionUser") as mock_v1_user_cls:
            RustSessionRuntime(config)

        mock_v1_user_cls.assert_called_once_with(
            "job-user", password="secret", logon_token=v0_user.logon_token
        )
        assert mock_rust_session.call_args.kwargs["user"] is mock_v1_user_cls.return_value

    def test_construction_when_user_is_none_passes_none_through(
        self, runtime_config: SessionRuntimeConfig, mock_rust_session: MagicMock
    ) -> None:
        """None means run-as-agent-user — it passes through without conversion."""
        config = replace(runtime_config, user=None)
        RustSessionRuntime(config)

        assert mock_rust_session.call_args.kwargs["user"] is None

    def test_construction_when_unsupported_user_type_raises_type_error(
        self, runtime_config: SessionRuntimeConfig, mock_rust_session: MagicMock
    ) -> None:
        """An unrecognized SessionUser subtype should fail loud."""
        fake_user = MagicMock()
        fake_user.__class__.__name__ = "AlienUser"
        config = replace(runtime_config, user=fake_user)

        with pytest.raises(TypeError, match="Unsupported SessionUser type"):
            RustSessionRuntime(config)

    def test_construction_when_callback_is_passed_through_directly(
        self, runtime_config: SessionRuntimeConfig, mock_rust_session: MagicMock
    ) -> None:
        """The callback passes through without wrapping — no v1→v0 conversion
        exists in the Rust adapter after the interface moved to _v1 types."""
        RustSessionRuntime(runtime_config)

        assert mock_rust_session.call_args.kwargs["callback"] is runtime_config.action_callback

    def test_construction_when_path_mapping_rules_passes_through_directly(
        self, mock_rust_session: MagicMock
    ) -> None:
        """_v1 path mapping rules pass straight through without conversion."""
        from openjd.expr import PathFormat as ExprPathFormat
        from openjd.expr import PathMappingRule as ExprPathMappingRule

        rule = ExprPathMappingRule(
            source_path_format=ExprPathFormat.POSIX,
            source_path="/source",
            destination_path="/dest",
        )
        config = SessionRuntimeConfig(
            session_id="session-pass",
            job_parameter_values={},
            path_mapping_rules=[rule],
            retain_working_dir=False,
            user=None,
            action_callback=lambda sid, s: None,
            os_env_vars=None,
            session_root_directory=Path("/tmp/sessions/session-pass"),
        )

        RustSessionRuntime(config)

        passed = mock_rust_session.call_args.kwargs["path_mapping_rules"]
        assert passed == [rule]


class TestRustSessionRuntimeDelegation:
    @pytest.fixture()
    def adapter(
        self, runtime_config: SessionRuntimeConfig, mock_rust_session: MagicMock
    ) -> RustSessionRuntime:
        return RustSessionRuntime(runtime_config)

    @pytest.fixture()
    def mock_session_instance(self, mock_rust_session: MagicMock) -> MagicMock:
        return mock_rust_session.return_value

    def test_enter_environment_when_called_decodes_wire_json_and_delegates(
        self, adapter: RustSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        environment = {"name": "TestEnv", "script": {"actions": {"onEnter": {"command": "echo"}}}}
        identifier = "job-env-1"
        os_env = {"KEY": "VAL"}

        with (
            patch.object(rust_module, "decode_environment_template") as mock_decode,
            patch.object(rust_module, "create_environment") as mock_create,
        ):
            result = adapter.enter_environment(
                environment=environment, identifier=identifier, os_env_vars=os_env
            )

        # The wire-format dict is wrapped in the template envelope and decoded.
        mock_decode.assert_called_once_with(
            {
                "specificationVersion": "environment-2023-09",
                "environment": environment,
            }
        )
        mock_create.assert_called_once_with(mock_decode.return_value)
        mock_session_instance.enter_environment.assert_called_once_with(
            environment=mock_create.return_value,
            identifier=identifier,
            os_env_vars=os_env,
        )
        assert result is mock_session_instance.enter_environment.return_value

    def test_enter_environment_when_decode_fails_raises_session_runtime_decode_error(
        self, adapter: RustSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        """F3: DecodeValidationError from the _v1 decoder is wrapped in
        SessionRuntimeDecodeError with the boundary named."""
        from openjd.model._v1.errors import DecodeValidationError

        with patch.object(rust_module, "decode_environment_template") as mock_decode:
            mock_decode.side_effect = DecodeValidationError("bad template")
            with pytest.raises(
                SessionRuntimeDecodeError, match="RustSessionRuntime.enter_environment"
            ):
                adapter.enter_environment(environment={"bad": "data"})

    def test_exit_environment_when_called_delegates_to_wrapped_session(
        self, adapter: RustSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        adapter.exit_environment(
            identifier="job-env-1", os_env_vars={"A": "B"}, keep_session_running=True
        )

        mock_session_instance.exit_environment.assert_called_once_with(
            identifier="job-env-1", os_env_vars={"A": "B"}, keep_session_running=True
        )

    def test_run_task_when_called_decodes_step_script_and_delegates(
        self, adapter: RustSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        step_script: dict[str, Any] = {"actions": {"onRun": {"command": "echo", "args": ["hi"]}}}
        task_params: dict[str, dict[str, Any]] = {
            "TaskParam": {"type": "STRING", "value": "val"},
        }

        with patch.object(rust_module, "deserialize_step") as mock_deserialize:
            adapter.run_task(
                step_script=step_script,
                task_parameter_values=task_params,
                os_env_vars={"X": "Y"},
                log_task_banner=False,
            )

        # The wire-format step script dict is wrapped as a named step and decoded.
        mock_deserialize.assert_called_once_with({"name": "Placeholder", "script": step_script})
        mock_session_instance.run_task.assert_called_once()
        run_kwargs = mock_session_instance.run_task.call_args.kwargs
        assert run_kwargs["step_script"] is mock_deserialize.return_value.script
        # Wire-format task params pass through unchanged.
        assert run_kwargs["task_parameter_values"] == task_params
        assert run_kwargs["os_env_vars"] == {"X": "Y"}
        assert run_kwargs["log_task_banner"] is False

    def test_run_task_when_decode_fails_raises_session_runtime_decode_error(
        self, adapter: RustSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        """F3: DecodeValidationError from deserialize_step is wrapped in
        SessionRuntimeDecodeError with the boundary named."""
        from openjd.model._v1.errors import DecodeValidationError

        with patch.object(rust_module, "deserialize_step") as mock_deserialize:
            mock_deserialize.side_effect = DecodeValidationError("bad script")
            with pytest.raises(SessionRuntimeDecodeError, match="RustSessionRuntime.run_task"):
                adapter.run_task(
                    step_script={"bad": "script"},
                    task_parameter_values={},
                )

    def test_run_task_when_value_error_raises_session_runtime_decode_error(
        self, adapter: RustSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        """F3: ValueError from deserialize_step is also wrapped."""
        with patch.object(rust_module, "deserialize_step") as mock_deserialize:
            mock_deserialize.side_effect = ValueError("invalid step")
            with pytest.raises(SessionRuntimeDecodeError, match="RustSessionRuntime.run_task"):
                adapter.run_task(
                    step_script={"bad": "script"},
                    task_parameter_values={},
                )

    def test_run_task_without_session_env_materializes_files_and_runs_subprocess(
        self, adapter: RustSessionRuntime, mock_session_instance: MagicMock, tmp_path: Path
    ) -> None:
        mock_session_instance.files_directory = tmp_path

        embedded_file = MagicMock()
        embedded_file.name = "WorkerManifest"
        embedded_file.data = "file-contents"

        mock_script = MagicMock()
        mock_script.embeddedFiles = [embedded_file]
        mock_script.actions.onRun.command = "python"
        mock_script.actions.onRun.args = [
            "{{ Task.File.WorkerManifest }}",
            "{{Task.File.WorkerManifest}}",
            "{{ Task.File.WorkerManifest}}",
            "{{Task.File.WorkerManifest }}",
            "{{  Task.File.WorkerManifest  }}",
            "literal-arg",
        ]

        step_script: dict[str, Any] = {"actions": {"onRun": {"command": "python"}}}

        with patch.object(rust_module, "deserialize_step") as mock_deserialize:
            mock_deserialize.return_value.script = mock_script
            adapter._run_task_without_session_env(
                step_script=step_script,
                task_parameter_values={},
                os_env_vars={"EXTRA": "1"},
                log_task_banner=True,
            )

        mock_session_instance.run_subprocess.assert_called_once()
        run_kwargs = mock_session_instance.run_subprocess.call_args.kwargs
        assert run_kwargs["command"] == "python"
        # All whitespace spellings of the Task.File reference resolve to the
        # same materialized path; the literal arg is untouched.
        materialized_path = run_kwargs["args"][0]
        assert run_kwargs["args"] == [*([materialized_path] * 5), "literal-arg"]
        assert os.path.dirname(materialized_path) == str(tmp_path)
        with open(materialized_path) as f:
            assert f.read() == "file-contents"
        # Attachment-sync bypasses the session environment and forces unbuffered output.
        assert run_kwargs["use_session_env_vars"] is False
        assert run_kwargs["os_env_vars"] == {"PYTHONUNBUFFERED": "1", "EXTRA": "1"}
        assert run_kwargs["log_banner_message"] == "Running Task"

    def test_run_task_without_session_env_when_command_has_task_file_ref_resolves_it(
        self, adapter: RustSessionRuntime, mock_session_instance: MagicMock, tmp_path: Path
    ) -> None:
        """F2: Task.File references in the command itself are resolved, not only args."""
        mock_session_instance.files_directory = tmp_path

        embedded_file = MagicMock()
        embedded_file.name = "syncScript"
        embedded_file.data = "#!/bin/bash\necho sync"

        mock_script = MagicMock()
        mock_script.embeddedFiles = [embedded_file]
        mock_script.actions.onRun.command = "{{ Task.File.syncScript }}"
        mock_script.actions.onRun.args = None

        with patch.object(rust_module, "deserialize_step") as mock_deserialize:
            mock_deserialize.return_value.script = mock_script
            adapter._run_task_without_session_env(
                step_script={"actions": {"onRun": {"command": "placeholder"}}},
                task_parameter_values={},
            )

        run_kwargs = mock_session_instance.run_subprocess.call_args.kwargs
        # The command was resolved from the Task.File reference.
        assert "syncScript_" in run_kwargs["command"]
        assert os.path.exists(run_kwargs["command"])

    def test_run_task_without_session_env_when_no_banner_passes_none(
        self, adapter: RustSessionRuntime, mock_session_instance: MagicMock, tmp_path: Path
    ) -> None:
        mock_session_instance.files_directory = tmp_path

        mock_script = MagicMock()
        mock_script.embeddedFiles = None
        mock_script.actions.onRun.command = "echo"
        mock_script.actions.onRun.args = None

        with patch.object(rust_module, "deserialize_step") as mock_deserialize:
            mock_deserialize.return_value.script = mock_script
            adapter._run_task_without_session_env(
                step_script={"actions": {"onRun": {"command": "echo"}}},
                task_parameter_values={},
                log_task_banner=False,
            )

        run_kwargs = mock_session_instance.run_subprocess.call_args.kwargs
        assert run_kwargs["args"] == []
        assert run_kwargs["log_banner_message"] is None

    @pytest.mark.skipif(os.name != "posix", reason="chown group semantics are POSIX-only")
    def test_run_task_without_session_env_when_posix_user_chowns_group(
        self, runtime_config: SessionRuntimeConfig, mock_rust_session: MagicMock, tmp_path: Path
    ) -> None:
        from openjd.sessions import PosixSessionUser

        user = PosixSessionUser(user="job-user", group="job-group")
        config = replace(runtime_config, session_id="session-5", user=user)
        adapter = RustSessionRuntime(config)
        mock_rust_session.return_value.files_directory = tmp_path

        embedded_file = MagicMock()
        embedded_file.name = "Manifest"
        embedded_file.data = "data"

        mock_script = MagicMock()
        mock_script.embeddedFiles = [embedded_file]
        mock_script.actions.onRun.command = "cmd"
        mock_script.actions.onRun.args = None

        with (
            patch.object(rust_module, "deserialize_step") as mock_deserialize,
            patch.object(rust_module, "chown") as mock_chown,
            patch.object(rust_module.os, "chmod") as mock_chmod,
        ):
            mock_deserialize.return_value.script = mock_script
            adapter._run_task_without_session_env(
                step_script={"actions": {"onRun": {"command": "cmd"}}},
                task_parameter_values={},
            )

        mock_chown.assert_called_once()
        assert mock_chown.call_args.kwargs["group"] == "job-group"
        # With a group present, the chmod grants owner rw + group-read (0o640).
        mock_chmod.assert_called_once()
        assert mock_chmod.call_args.args[1] == 0o640

    def test_run_task_without_session_env_when_embedded_file_data_none_is_skipped(
        self, adapter: RustSessionRuntime, mock_session_instance: MagicMock, tmp_path: Path
    ) -> None:
        mock_session_instance.files_directory = tmp_path

        none_file = MagicMock()
        none_file.name = "EmptyFile"
        none_file.data = None
        real_file = MagicMock()
        real_file.name = "RealFile"
        real_file.data = "real-contents"

        mock_script = MagicMock()
        mock_script.embeddedFiles = [none_file, real_file]
        mock_script.actions.onRun.command = "python"
        mock_script.actions.onRun.args = [
            "{{ Task.File.EmptyFile }}",
            "{{ Task.File.RealFile }}",
        ]

        with patch.object(rust_module, "deserialize_step") as mock_deserialize:
            mock_deserialize.return_value.script = mock_script
            adapter._run_task_without_session_env(
                step_script={"actions": {"onRun": {"command": "python"}}},
                task_parameter_values={},
            )

        # The None-data embedded file is skipped: only the real-data file is
        # materialized into the session files directory.
        materialized = list(tmp_path.iterdir())
        assert len(materialized) == 1
        materialized_path = str(materialized[0])

        mock_session_instance.run_subprocess.assert_called_once()
        run_kwargs = mock_session_instance.run_subprocess.call_args.kwargs
        # The skipped file's reference is never added to file_paths, so it is
        # left unresolved; only the real file's reference resolves to a path.
        assert run_kwargs["args"] == ["{{ Task.File.EmptyFile }}", materialized_path]
        with open(materialized_path) as f:
            assert f.read() == "real-contents"

    @pytest.mark.skipif(os.name != "posix", reason="chown group semantics are POSIX-only")
    def test_run_task_without_session_env_when_posix_user_without_group_skips_chown(
        self, mock_rust_session: MagicMock, tmp_path: Path
    ) -> None:
        # When no user is configured (run-as-agent-user path), self._user is
        # None and no group ownership or group-read is granted; the
        # materialized file stays owner rw only.
        config = SessionRuntimeConfig(
            session_id="session-6",
            job_parameter_values={},
            path_mapping_rules=None,
            retain_working_dir=False,
            user=None,
            action_callback=lambda session_id, status: None,
            os_env_vars=None,
            session_root_directory=Path("/tmp/sessions/session-6"),
        )
        adapter = RustSessionRuntime(config)
        mock_rust_session.return_value.files_directory = tmp_path

        embedded_file = MagicMock()
        embedded_file.name = "Manifest"
        embedded_file.data = "data"

        mock_script = MagicMock()
        mock_script.embeddedFiles = [embedded_file]
        mock_script.actions.onRun.command = "cmd"
        mock_script.actions.onRun.args = None

        with (
            patch.object(rust_module, "deserialize_step") as mock_deserialize,
            patch.object(rust_module, "chown") as mock_chown,
            patch.object(rust_module.os, "chmod") as mock_chmod,
        ):
            mock_deserialize.return_value.script = mock_script
            adapter._run_task_without_session_env(
                step_script={"actions": {"onRun": {"command": "cmd"}}},
                task_parameter_values={},
            )

        # No group → no chown, and the chmod mode carries no group-read bit
        # (0o600), leaving the file owner read/write only.
        mock_chown.assert_not_called()
        mock_chmod.assert_called_once()
        assert mock_chmod.call_args.args[1] == 0o600
        mock_rust_session.return_value.run_subprocess.assert_called_once()

    def test_run_task_without_session_env_when_decode_fails_raises_session_runtime_decode_error(
        self, adapter: RustSessionRuntime, mock_session_instance: MagicMock, tmp_path: Path
    ) -> None:
        """F3: Decode failure in _run_task_without_session_env is wrapped."""
        mock_session_instance.files_directory = tmp_path
        from openjd.model._v1.errors import DecodeValidationError

        with patch.object(rust_module, "deserialize_step") as mock_deserialize:
            mock_deserialize.side_effect = DecodeValidationError("bad")
            with pytest.raises(SessionRuntimeDecodeError, match="RustSessionRuntime.run_task"):
                adapter._run_task_without_session_env(
                    step_script={"bad": "script"},
                    task_parameter_values={},
                )

    def test_extend_path_mapping_rules_delegates_without_conversion(
        self, adapter: RustSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        from openjd.expr import PathFormat as ExprPathFormat
        from openjd.expr import PathMappingRule as ExprPathMappingRule

        rule_short = ExprPathMappingRule(
            source_path_format=ExprPathFormat.POSIX,
            source_path="/a",
            destination_path="/b",
        )
        rule_long = ExprPathMappingRule(
            source_path_format=ExprPathFormat.POSIX,
            source_path="/longer/path",
            destination_path="/dest/path",
        )
        rules = [rule_short, rule_long]

        adapter.extend_path_mapping_rules(rules)

        # The public method is called with the rules in their original order —
        # the session sorts internally, so the adapter must not pre-sort.
        mock_session_instance.extend_path_mapping_rules.assert_called_once_with(rules)

    def test_cancel_action_when_called_delegates_to_wrapped_session(
        self, adapter: RustSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        limit = timedelta(seconds=30)

        adapter.cancel_action(time_limit=limit, mark_action_failed=True)

        mock_session_instance.cancel_action.assert_called_once_with(
            time_limit=limit, mark_action_failed=True
        )

    def test_cancel_action_when_defaults_passes_none_and_false(
        self, adapter: RustSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        """Default kwargs are None time_limit and mark_action_failed=False."""
        adapter.cancel_action()

        mock_session_instance.cancel_action.assert_called_once_with(
            time_limit=None, mark_action_failed=False
        )

    def test_cancel_action_when_rust_session_raises_runtime_error_propagates(
        self, adapter: RustSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        """RuntimeError from the Rust session (e.g. 'Cannot cancel: session is
        busy with an action') must propagate to the caller — the openjd_action
        layer catches it and wraps as CancelationError."""
        mock_session_instance.cancel_action.side_effect = RuntimeError(
            "Cannot cancel: session is busy with an action"
        )

        with pytest.raises(RuntimeError, match="Cannot cancel"):
            adapter.cancel_action(time_limit=timedelta(seconds=5))

    def test_cleanup_when_called_delegates_to_wrapped_session(
        self, adapter: RustSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        adapter.cleanup()

        mock_session_instance.cleanup.assert_called_once_with()


class TestRustSessionRuntimeProperties:
    @pytest.fixture()
    def adapter(
        self, runtime_config: SessionRuntimeConfig, mock_rust_session: MagicMock
    ) -> RustSessionRuntime:
        return RustSessionRuntime(runtime_config)

    @pytest.fixture()
    def mock_session_instance(self, mock_rust_session: MagicMock) -> MagicMock:
        return mock_rust_session.return_value

    def test_working_directory_when_accessed_returns_wrapped_session_value(
        self, adapter: RustSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        mock_session_instance.working_directory = Path("/tmp/work")

        assert adapter.working_directory == Path("/tmp/work")

    def test_action_status_when_accessed_returns_session_value_directly(
        self, adapter: RustSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        """action_status is a pass-through — no conversion needed since both
        the interface and the Rust session speak the _v1 ActionStatus type."""
        fake_status = MagicMock()
        mock_session_instance.action_status = fake_status

        assert adapter.action_status is fake_status

    def test_action_status_when_none_returns_none(
        self, adapter: RustSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        mock_session_instance.action_status = None

        assert adapter.action_status is None


def test_rust_session_runtime_is_session_runtime_subclass() -> None:
    # Smoke test: the module imports cleanly against the installed _v1 binding
    # and the adapter satisfies the ABC contract.
    assert issubclass(RustSessionRuntime, SessionRuntime)
