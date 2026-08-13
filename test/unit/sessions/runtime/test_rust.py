# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import os
from dataclasses import replace
from datetime import timedelta
from pathlib import Path, PurePosixPath
from typing import Any, Generator
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

from openjd.model._types import ParameterValue, ParameterValueType
from openjd.model._v1.types import ModelProfile, SpecificationRevision

from deadline_worker_agent.sessions.runtime import SessionRuntime, SessionRuntimeConfig
from deadline_worker_agent.sessions.runtime._abc import SessionRuntimeCrashError
from deadline_worker_agent.sessions.runtime import rust as rust_module
from deadline_worker_agent.sessions.runtime.rust import RustSessionRuntime
from deadline_worker_agent.sessions.runtime.rust import _to_rust_task_parameter_values
from deadline_worker_agent.sessions.runtime.rust import _to_environment_parameter_definitions


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
        from openjd.sessions._v1 import PosixSessionUser as RustPosixSessionUser

        v0_user = PosixSessionUser(user="job-user", group="job-group")
        config = replace(runtime_config, user=v0_user)
        RustSessionRuntime(config)

        passed_user = mock_rust_session.call_args.kwargs["user"]
        assert isinstance(passed_user, RustPosixSessionUser)
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
        with patch.object(rust_module, "RustWindowsSessionUser") as mock_v1_user_cls:
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


class TestRustSessionRuntimeDelegation:
    @pytest.fixture()
    def adapter(
        self, runtime_config: SessionRuntimeConfig, mock_rust_session: MagicMock
    ) -> RustSessionRuntime:
        return RustSessionRuntime(runtime_config)

    @pytest.fixture()
    def mock_session_instance(self, mock_rust_session: MagicMock) -> MagicMock:
        return mock_rust_session.return_value

    def test_enter_environment_when_called_converts_env_and_delegates(
        self, adapter: RustSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        environment = MagicMock()
        identifier = "job-env-1"
        os_env = {"KEY": "VAL"}

        with (
            patch.object(rust_module, "decode_environment_template") as mock_decode,
            patch.object(rust_module, "create_environment") as mock_create,
        ):
            result = adapter.enter_environment(
                environment=environment, identifier=identifier, os_env_vars=os_env
            )

        # The pydantic environment is serialized and rebuilt natively before
        # being handed to the session. The fixture's job_parameter_values
        # contains one STRING param in dict form, which must appear as a
        # parameterDefinitions entry.
        mock_decode.assert_called_once_with(
            {
                "specificationVersion": "environment-2023-09",
                "environment": environment.model_dump.return_value,
                "parameterDefinitions": [{"name": "Param1", "type": "STRING"}],
            }
        )
        environment.model_dump.assert_called_once_with(
            mode="json", by_alias=True, exclude_none=True
        )
        mock_create.assert_called_once_with(mock_decode.return_value)
        mock_session_instance.enter_environment.assert_called_once_with(
            environment=mock_create.return_value,
            identifier=identifier,
            os_env_vars=os_env,
        )
        assert result is mock_session_instance.enter_environment.return_value

    def test_enter_environment_includes_all_job_parameter_types(
        self, mock_rust_session: MagicMock
    ) -> None:
        """All JobParameterType members are declared in parameterDefinitions."""
        config = SessionRuntimeConfig(
            session_id="session-env-multi",
            job_parameter_values={
                "A": ParameterValue(type=ParameterValueType.STRING, value="hello"),
                "B": ParameterValue(type=ParameterValueType.INT, value="42"),
                "C": ParameterValue(type=ParameterValueType.PATH, value="/tmp"),
                "D": ParameterValue(type=ParameterValueType.FLOAT, value="3.14"),
                "E": ParameterValue(type=ParameterValueType.BOOL, value="true"),
            },
            path_mapping_rules=None,
            retain_working_dir=False,
            user=None,
            action_callback=lambda sid, s: None,
            os_env_vars=None,
            session_root_directory=Path("/tmp/sessions/session-env-multi"),
        )
        adapter = RustSessionRuntime(config)

        environment = MagicMock()
        with (
            patch.object(rust_module, "decode_environment_template") as mock_decode,
            patch.object(rust_module, "create_environment"),
        ):
            adapter.enter_environment(environment=environment, identifier="env-1")

        template = mock_decode.call_args.args[0]
        assert template["parameterDefinitions"] == [
            {"name": "A", "type": "STRING"},
            {"name": "B", "type": "INT"},
            {"name": "C", "type": "PATH"},
            {"name": "D", "type": "FLOAT"},
            {"name": "E", "type": "BOOL"},
        ]

    def test_enter_environment_when_empty_params_omits_parameter_definitions_key(
        self, mock_rust_session: MagicMock
    ) -> None:
        """Empty job_parameter_values means parameterDefinitions must be absent."""
        config = SessionRuntimeConfig(
            session_id="session-env-empty",
            job_parameter_values={},
            path_mapping_rules=None,
            retain_working_dir=False,
            user=None,
            action_callback=lambda sid, s: None,
            os_env_vars=None,
            session_root_directory=Path("/tmp/sessions/session-env-empty"),
        )
        adapter = RustSessionRuntime(config)

        environment = MagicMock()
        with (
            patch.object(rust_module, "decode_environment_template") as mock_decode,
            patch.object(rust_module, "create_environment"),
        ):
            adapter.enter_environment(environment=environment, identifier="env-1")

        template = mock_decode.call_args.args[0]
        assert template == {
            "specificationVersion": "environment-2023-09",
            "environment": environment.model_dump.return_value,
        }
        assert "parameterDefinitions" not in template

    def test_enter_environment_when_dict_param_type_invalid_is_still_declared(
        self, mock_rust_session: MagicMock
    ) -> None:
        """A raw dict with an invalid type (e.g. CHUNK[INT]) is still declared.

        ParameterValue objects cannot carry CHUNK_INT (JobParameterType has no
        such member), but raw dicts bypass that constraint. No type filter is
        applied — the decoder itself will reject invalid types at template
        decode time with a clear error, which is the correct behavior.
        """
        config = SessionRuntimeConfig(
            session_id="session-env-chunk",
            job_parameter_values={
                "Good": ParameterValue(type=ParameterValueType.STRING, value="ok"),
                "Bad": {"type": "CHUNK[INT]", "value": "1-5"},
            },
            path_mapping_rules=None,
            retain_working_dir=False,
            user=None,
            action_callback=lambda sid, s: None,
            os_env_vars=None,
            session_root_directory=Path("/tmp/sessions/session-env-chunk"),
        )
        adapter = RustSessionRuntime(config)

        environment = MagicMock()
        with (
            patch.object(rust_module, "decode_environment_template") as mock_decode,
            patch.object(rust_module, "create_environment"),
        ):
            adapter.enter_environment(environment=environment, identifier="env-1")

        template = mock_decode.call_args.args[0]
        # Both params are declared — no type filter. The decoder rejects
        # CHUNK[INT] at decode time with a clear error.
        assert template["parameterDefinitions"] == [
            {"name": "Good", "type": "STRING"},
            {"name": "Bad", "type": "CHUNK[INT]"},
        ]

    def test_enter_environment_when_mixed_dict_and_object_params_both_appear(
        self, mock_rust_session: MagicMock
    ) -> None:
        """Both ParameterValue objects and raw dicts are handled correctly."""
        config = SessionRuntimeConfig(
            session_id="session-env-mixed",
            job_parameter_values={
                "Obj": ParameterValue(type=ParameterValueType.INT, value="7"),
                "Dict": {"type": "PATH", "value": "/out"},
            },
            path_mapping_rules=None,
            retain_working_dir=False,
            user=None,
            action_callback=lambda sid, s: None,
            os_env_vars=None,
            session_root_directory=Path("/tmp/sessions/session-env-mixed"),
        )
        adapter = RustSessionRuntime(config)

        environment = MagicMock()
        with (
            patch.object(rust_module, "decode_environment_template") as mock_decode,
            patch.object(rust_module, "create_environment"),
        ):
            adapter.enter_environment(environment=environment, identifier="env-1")

        template = mock_decode.call_args.args[0]
        assert template["parameterDefinitions"] == [
            {"name": "Obj", "type": "INT"},
            {"name": "Dict", "type": "PATH"},
        ]

    def test_enter_environment_when_dict_param_missing_type_key_is_skipped(
        self, mock_rust_session: MagicMock
    ) -> None:
        """A dict-shaped value with no "type" key is skipped rather than raising.

        The same raw dict reaches _to_rust_job_parameter_values moments later, so
        a genuinely malformed value still fails at Rust session construction with
        the session's own error rather than a KeyError from __init__.
        """
        config = SessionRuntimeConfig(
            session_id="session-env-missing-type",
            job_parameter_values={
                "Good": {"type": "PATH", "value": "/tmp"},
                "Bad": {"value": "oops"},  # no "type" key
                "AlsoGood": ParameterValue(type=ParameterValueType.FLOAT, value="1.5"),
            },
            path_mapping_rules=None,
            retain_working_dir=False,
            user=None,
            action_callback=lambda sid, s: None,
            os_env_vars=None,
            session_root_directory=Path("/tmp/sessions/session-env-missing-type"),
        )
        adapter = RustSessionRuntime(config)

        environment = MagicMock()
        with (
            patch.object(rust_module, "decode_environment_template") as mock_decode,
            patch.object(rust_module, "create_environment"),
        ):
            adapter.enter_environment(environment=environment, identifier="env-1")

        template = mock_decode.call_args.args[0]
        assert template["parameterDefinitions"] == [
            {"name": "Good", "type": "PATH"},
            {"name": "AlsoGood", "type": "FLOAT"},
        ]

    def test_exit_environment_when_called_delegates_to_wrapped_session(
        self, adapter: RustSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        adapter.exit_environment(
            identifier="job-env-1", os_env_vars={"A": "B"}, keep_session_running=True
        )

        mock_session_instance.exit_environment.assert_called_once_with(
            identifier="job-env-1", os_env_vars={"A": "B"}, keep_session_running=True
        )

    def test_run_task_when_called_converts_step_script_and_delegates(
        self, adapter: RustSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        step_script = MagicMock()
        task_params: dict[str, Any] = {
            "TaskParam": ParameterValue(type=ParameterValueType.STRING, value="val"),
        }

        with patch.object(rust_module, "deserialize_step") as mock_deserialize:
            adapter.run_task(
                step_script=step_script,
                task_parameter_values=task_params,
                os_env_vars={"X": "Y"},
                log_task_banner=False,
            )

        # The pydantic step script is serialized, wrapped as a named step, and
        # rebuilt natively; the native ``.script`` is forwarded to the session.
        step_script.model_dump.assert_called_once_with(
            mode="json", by_alias=True, exclude_none=True
        )
        mock_deserialize.assert_called_once_with(
            {"name": "Placeholder", "script": step_script.model_dump.return_value}
        )
        mock_session_instance.run_task.assert_called_once()
        run_kwargs = mock_session_instance.run_task.call_args.kwargs
        assert run_kwargs["step_script"] is mock_deserialize.return_value.script
        # The v0 ParameterValue is rebuilt as a native _v1 TaskParameterValue.
        passed_param = run_kwargs["task_parameter_values"]["TaskParam"]
        assert isinstance(passed_param, rust_module.TaskParameterValue)
        assert passed_param.value == "val"
        assert run_kwargs["os_env_vars"] == {"X": "Y"}
        assert run_kwargs["log_task_banner"] is False

    def test_run_task_without_session_env_materializes_files_and_runs_subprocess(
        self, adapter: RustSessionRuntime, mock_session_instance: MagicMock, tmp_path: Path
    ) -> None:
        mock_session_instance.files_directory = tmp_path

        embedded_file = MagicMock()
        embedded_file.name = "WorkerManifest"
        embedded_file.data = "file-contents"

        step_script = MagicMock()
        step_script.embeddedFiles = [embedded_file]
        step_script.actions.onRun.command = "python"
        step_script.actions.onRun.args = [
            "{{ Task.File.WorkerManifest }}",
            "{{Task.File.WorkerManifest}}",
            "{{ Task.File.WorkerManifest}}",
            "{{Task.File.WorkerManifest }}",
            "{{  Task.File.WorkerManifest  }}",
            "literal-arg",
        ]

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

    def test_run_task_without_session_env_when_no_banner_passes_none(
        self, adapter: RustSessionRuntime, mock_session_instance: MagicMock, tmp_path: Path
    ) -> None:
        mock_session_instance.files_directory = tmp_path
        step_script = MagicMock()
        step_script.embeddedFiles = None
        step_script.actions.onRun.command = "echo"
        step_script.actions.onRun.args = None

        adapter._run_task_without_session_env(
            step_script=step_script,
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
        step_script = MagicMock()
        step_script.embeddedFiles = [embedded_file]
        step_script.actions.onRun.command = "cmd"
        step_script.actions.onRun.args = None

        with (
            patch.object(rust_module, "chown") as mock_chown,
            patch.object(rust_module.os, "chmod") as mock_chmod,
        ):
            adapter._run_task_without_session_env(step_script=step_script, task_parameter_values={})

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

        step_script = MagicMock()
        step_script.embeddedFiles = [none_file, real_file]
        step_script.actions.onRun.command = "python"
        step_script.actions.onRun.args = [
            "{{ Task.File.EmptyFile }}",
            "{{ Task.File.RealFile }}",
        ]

        adapter._run_task_without_session_env(
            step_script=step_script,
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
        step_script = MagicMock()
        step_script.embeddedFiles = [embedded_file]
        step_script.actions.onRun.command = "cmd"
        step_script.actions.onRun.args = None

        with (
            patch.object(rust_module, "chown") as mock_chown,
            patch.object(rust_module.os, "chmod") as mock_chmod,
        ):
            adapter._run_task_without_session_env(step_script=step_script, task_parameter_values={})

        # No group → no chown, and the chmod mode carries no group-read bit
        # (0o600), leaving the file owner read/write only.
        mock_chown.assert_not_called()
        mock_chmod.assert_called_once()
        assert mock_chmod.call_args.args[1] == 0o600
        mock_rust_session.return_value.run_subprocess.assert_called_once()

    def test_extend_path_mapping_rules_delegates_without_presorting(
        self, adapter: RustSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        from openjd.expr import PathFormat as ExprPathFormat
        from openjd.expr import PathMappingRule as ExprPathMappingRule
        from openjd.sessions import PathFormat, PathMappingRule as V0PathMappingRule

        rule_short = V0PathMappingRule(
            source_path_format=PathFormat.POSIX,
            source_path=PurePosixPath("/a"),
            destination_path=PurePosixPath("/b"),
        )
        rule_long = V0PathMappingRule(
            source_path_format=PathFormat.POSIX,
            source_path=PurePosixPath("/longer/path"),
            destination_path=PurePosixPath("/dest/path"),
        )
        rules: list[V0PathMappingRule] = [rule_short, rule_long]

        adapter.extend_path_mapping_rules(rules)

        # The public method is called with the rules in their original order —
        # the session sorts internally, so the adapter must not pre-sort.
        mock_session_instance.extend_path_mapping_rules.assert_called_once()
        passed = mock_session_instance.extend_path_mapping_rules.call_args.args[0]
        assert len(passed) == 2
        assert isinstance(passed[0], ExprPathMappingRule)
        assert passed[0].source_path_format == ExprPathFormat.POSIX
        assert passed[0].source_path == "/a"
        assert passed[0].destination_path == "/b"
        assert passed[1].source_path == "/longer/path"

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

    def test_action_status_when_accessed_returns_converted_v0_status(
        self, adapter: RustSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        from openjd.sessions import ActionState, ActionStatus

        v1_state = MagicMock(**{"__str__.return_value": "running"})
        v1_status = SimpleNamespace(
            state=v1_state,
            progress=42,
            status_message="working",
            fail_message=None,
            exit_code=None,
        )
        mock_session_instance.action_status = v1_status

        result = adapter.action_status
        assert isinstance(result, ActionStatus)
        assert result.state == ActionState.RUNNING
        assert result.progress == 42
        assert result.status_message == "working"
        assert result.fail_message is None
        assert result.exit_code is None

    def test_action_status_when_none_returns_none(
        self, adapter: RustSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        mock_session_instance.action_status = None

        assert adapter.action_status is None


class TestRustSessionRuntimeTypeConversions:
    """Tests for v0 ↔ v1 type conversion helpers."""

    @pytest.fixture()
    def mock_rust_session(self) -> Generator[MagicMock, None, None]:
        with patch.object(rust_module, "OpenJDRustSession") as mock_cls:
            yield mock_cls

    def test_job_parameter_values_when_v0_parameter_value_converts_to_native(
        self, mock_rust_session: MagicMock
    ) -> None:
        """A v0 openjd.model ParameterValue is rebuilt as a native _v1 JobParameterValue."""
        config = SessionRuntimeConfig(
            session_id="session-conv-1",
            job_parameter_values={
                "P": ParameterValue(type=ParameterValueType.STRING, value="hello"),
            },
            path_mapping_rules=None,
            retain_working_dir=False,
            user=None,
            action_callback=lambda sid, s: None,
            os_env_vars=None,
            session_root_directory=Path("/tmp/sessions/session-conv-1"),
        )

        RustSessionRuntime(config)

        passed = mock_rust_session.call_args.kwargs["job_parameter_values"]["P"]
        assert isinstance(passed, rust_module.JobParameterValue)
        assert passed.value == "hello"

    def test_parameter_values_when_type_unknown_to_binding_raises(self) -> None:
        """A parameter type the _v1 binding does not define fails loud."""
        bogus = SimpleNamespace(type=SimpleNamespace(value="HOLOGRAM"), value="x")
        with pytest.raises(ValueError, match="HOLOGRAM.*JobParameterType does not define"):
            rust_module._to_rust_job_parameter_values({"P": bogus})

    def test_every_v1_action_state_maps_to_v0(self) -> None:
        """Every member of the REAL _v1 ActionState enum maps to a v0 member.

        The pyo3 enum is not iterable, so members are collected via dir(). The
        member-count assertion is the drift tripwire: a state added on the Rust
        side must be added to _ACTION_STATES deliberately.
        """
        from openjd.sessions import ActionState
        from openjd.sessions._v1 import ActionState as RustActionState

        members = [getattr(RustActionState, n) for n in dir(RustActionState) if n.isupper()]
        assert len(members) == 5
        for member in members:
            assert isinstance(rust_module._to_v0_action_state(member), ActionState)

    def test_unrecognized_v1_action_state_raises(self) -> None:
        """A _v1 state missing from _ACTION_STATES fails loud instead of drifting."""

        class _FakeState:
            def __str__(self) -> str:
                return "warp-speed"

        with pytest.raises(ValueError, match="Unrecognized _v1 ActionState"):
            rust_module._to_v0_action_state(_FakeState())  # type: ignore[arg-type]

    def test_job_parameter_values_when_dict_passes_through_unchanged(
        self, mock_rust_session: MagicMock
    ) -> None:
        """A dict value is passed through without modification."""
        config = SessionRuntimeConfig(
            session_id="session-conv-2",
            job_parameter_values={
                "D": {"type": "INT", "value": "42"},
            },
            path_mapping_rules=None,
            retain_working_dir=False,
            user=None,
            action_callback=lambda sid, s: None,
            os_env_vars=None,
            session_root_directory=Path("/tmp/sessions/session-conv-2"),
        )

        RustSessionRuntime(config)

        passed = mock_rust_session.call_args.kwargs["job_parameter_values"]
        assert passed == {"D": {"type": "INT", "value": "42"}}

    def test_path_mapping_rules_when_v0_rules_converts_to_expr_type(
        self, mock_rust_session: MagicMock
    ) -> None:
        """v0 PathMappingRule objects are converted to openjd.expr.PathMappingRule."""
        from openjd.expr import PathFormat as ExprPathFormat
        from openjd.expr import PathMappingRule as ExprPathMappingRule
        from openjd.sessions import PathFormat, PathMappingRule as V0PathMappingRule

        v0_rule = V0PathMappingRule(
            source_path_format=PathFormat.POSIX,
            source_path=PurePosixPath("/source"),
            destination_path=PurePosixPath("/dest"),
        )
        config = SessionRuntimeConfig(
            session_id="session-conv-3",
            job_parameter_values={},
            path_mapping_rules=[v0_rule],
            retain_working_dir=False,
            user=None,
            action_callback=lambda sid, s: None,
            os_env_vars=None,
            session_root_directory=Path("/tmp/sessions/session-conv-3"),
        )

        RustSessionRuntime(config)

        passed = mock_rust_session.call_args.kwargs["path_mapping_rules"]
        assert len(passed) == 1
        assert isinstance(passed[0], ExprPathMappingRule)
        assert passed[0].source_path_format == ExprPathFormat.POSIX
        assert passed[0].source_path == "/source"
        assert passed[0].destination_path == "/dest"

    def test_path_mapping_rules_when_none_passes_none(self, mock_rust_session: MagicMock) -> None:
        config = SessionRuntimeConfig(
            session_id="session-conv-4",
            job_parameter_values={},
            path_mapping_rules=None,
            retain_working_dir=False,
            user=None,
            action_callback=lambda sid, s: None,
            os_env_vars=None,
            session_root_directory=Path("/tmp/sessions/session-conv-4"),
        )

        RustSessionRuntime(config)

        assert mock_rust_session.call_args.kwargs["path_mapping_rules"] is None

    def test_callback_when_invoked_converts_v1_status_to_v0(
        self, mock_rust_session: MagicMock
    ) -> None:
        """The callback wrapper converts _v1 ActionStatus to v0 ActionStatus."""
        from openjd.sessions import ActionState, ActionStatus

        original_callback = MagicMock()
        config = SessionRuntimeConfig(
            session_id="session-conv-5",
            job_parameter_values={},
            path_mapping_rules=None,
            retain_working_dir=False,
            user=None,
            action_callback=original_callback,
            os_env_vars=None,
            session_root_directory=Path("/tmp/sessions/session-conv-5"),
        )

        RustSessionRuntime(config)

        # Grab the wrapped callback that was passed to the Rust session.
        wrapped_callback = mock_rust_session.call_args.kwargs["callback"]

        # Simulate a v1 ActionStatus with a state whose str() == "success"
        v1_state = MagicMock(**{"__str__.return_value": "success"})
        v1_status = SimpleNamespace(
            state=v1_state,
            progress=100,
            status_message="done",
            fail_message=None,
            exit_code=0,
        )

        wrapped_callback("session-conv-5", v1_status)

        original_callback.assert_called_once()
        call_args = original_callback.call_args
        assert call_args[0][0] == "session-conv-5"
        v0_status = call_args[0][1]
        assert isinstance(v0_status, ActionStatus)
        assert v0_status.state == ActionState.SUCCESS
        assert v0_status.progress == 100
        assert v0_status.status_message == "done"
        assert v0_status.fail_message is None
        assert v0_status.exit_code == 0

    def test_action_status_property_when_v1_status_present_returns_v0(
        self, mock_rust_session: MagicMock
    ) -> None:
        """The action_status property converts _v1 status to v0."""
        from openjd.sessions import ActionState, ActionStatus

        config = SessionRuntimeConfig(
            session_id="session-conv-6",
            job_parameter_values={},
            path_mapping_rules=None,
            retain_working_dir=False,
            user=None,
            action_callback=lambda sid, s: None,
            os_env_vars=None,
            session_root_directory=Path("/tmp/sessions/session-conv-6"),
        )
        adapter = RustSessionRuntime(config)

        v1_state = MagicMock(**{"__str__.return_value": "failed"})
        mock_rust_session.return_value.action_status = SimpleNamespace(
            state=v1_state,
            progress=0,
            status_message=None,
            fail_message="something broke",
            exit_code=1,
        )

        result = adapter.action_status
        assert isinstance(result, ActionStatus)
        assert result.state == ActionState.FAILED
        assert result.fail_message == "something broke"
        assert result.exit_code == 1

    def test_action_status_property_when_none_returns_none(
        self, mock_rust_session: MagicMock
    ) -> None:
        config = SessionRuntimeConfig(
            session_id="session-conv-7",
            job_parameter_values={},
            path_mapping_rules=None,
            retain_working_dir=False,
            user=None,
            action_callback=lambda sid, s: None,
            os_env_vars=None,
            session_root_directory=Path("/tmp/sessions/session-conv-7"),
        )
        adapter = RustSessionRuntime(config)
        mock_rust_session.return_value.action_status = None

        assert adapter.action_status is None


class TestToRustTaskParameterValues:
    """Tests for _to_rust_task_parameter_values conversion helper."""

    def test_converts_parameter_value_objects_to_native(self) -> None:
        """ParameterValue objects convert to native _v1 TaskParameterValues of the right type."""
        from openjd.model._v1.types import TaskParameterType

        values: dict[str, Any] = {
            "StringParam": ParameterValue(type=ParameterValueType.STRING, value="hello"),
            "IntParam": ParameterValue(type=ParameterValueType.INT, value="42"),
            "FloatParam": ParameterValue(type=ParameterValueType.FLOAT, value="3.14"),
            "PathParam": ParameterValue(type=ParameterValueType.PATH, value="/tmp/out"),
        }

        result = _to_rust_task_parameter_values(values)

        expected: dict[str, tuple[Any, str]] = {
            "StringParam": (TaskParameterType.STRING, "hello"),
            "IntParam": (TaskParameterType.INT, "42"),
            "FloatParam": (TaskParameterType.FLOAT, "3.14"),
            "PathParam": (TaskParameterType.PATH, "/tmp/out"),
        }
        assert result.keys() == expected.keys()
        for name, (expected_type, expected_value) in expected.items():
            converted = result[name]
            assert isinstance(converted, rust_module.TaskParameterValue), name
            assert str(converted.type) == str(expected_type), name
            assert converted.value == expected_value, name

    def test_dict_values_pass_through_unchanged(self) -> None:
        """Values already in dict form are not modified."""
        values: dict[str, Any] = {
            "AlreadyDict": {"type": "STRING", "value": "existing"},
        }

        result = _to_rust_task_parameter_values(values)

        assert result == {"AlreadyDict": {"type": "STRING", "value": "existing"}}

    def test_empty_dict_returns_empty_dict(self) -> None:
        """An empty input produces an empty output."""
        assert _to_rust_task_parameter_values({}) == {}

    def test_mixed_parameter_values_and_dicts(self) -> None:
        """ParameterValue objects convert to native types; plain dicts pass through."""
        values: dict[str, Any] = {
            "Obj": ParameterValue(type=ParameterValueType.INT, value="7"),
            "Dict": {"type": "FLOAT", "value": "2.5"},
        }

        result = _to_rust_task_parameter_values(values)

        assert isinstance(result["Obj"], rust_module.TaskParameterValue)
        assert result["Obj"].value == "7"
        assert result["Dict"] == {"type": "FLOAT", "value": "2.5"}


def test_rust_session_runtime_is_session_runtime_subclass() -> None:
    # Smoke test: the module imports cleanly against the installed _v1 binding
    # and the adapter satisfies the ABC contract.
    assert issubclass(RustSessionRuntime, SessionRuntime)


class _FakePanic(BaseException):
    """Stand-in for pyo3_runtime.PanicException (a BaseException subclass)."""


class TestRuntimeCrashConversion:
    """A panic escaping the _v1 session is converted at the adapter boundary
    (WA-7): it must surface as SessionRuntimeCrashError, not BaseException."""

    def test_panic_from_v1_call_is_converted(
        self, runtime_config: SessionRuntimeConfig, mock_rust_session: MagicMock
    ) -> None:
        runtime = RustSessionRuntime(runtime_config)
        mock_rust_session.return_value.exit_environment.side_effect = _FakePanic(
            "panicked at 'index out of bounds'"
        )

        with pytest.raises(SessionRuntimeCrashError, match="_FakePanic") as exc_info:
            runtime.exit_environment(identifier=MagicMock())
        assert isinstance(exc_info.value.__cause__, _FakePanic)

    def test_regular_exception_from_v1_call_propagates_unchanged(
        self, runtime_config: SessionRuntimeConfig, mock_rust_session: MagicMock
    ) -> None:
        original = RuntimeError("normal failure")
        runtime = RustSessionRuntime(runtime_config)
        mock_rust_session.return_value.exit_environment.side_effect = original

        with pytest.raises(RuntimeError) as exc_info:
            runtime.exit_environment(identifier=MagicMock())
        assert exc_info.value is original

    def test_panic_from_run_task_without_session_env_is_converted(
        self, runtime_config: SessionRuntimeConfig, mock_rust_session: MagicMock
    ) -> None:
        runtime = RustSessionRuntime(runtime_config)
        mock_rust_session.return_value.run_subprocess.side_effect = _FakePanic(
            "panicked at 'null pointer'"
        )

        step_script = MagicMock()
        step_script.embeddedFiles = None
        step_script.actions.onRun.command = "echo"
        step_script.actions.onRun.args = None

        with pytest.raises(SessionRuntimeCrashError, match="_FakePanic") as exc_info:
            runtime._run_task_without_session_env(step_script=step_script, task_parameter_values={})
        assert isinstance(exc_info.value.__cause__, _FakePanic)

    def test_panic_from_extend_path_mapping_rules_is_converted(
        self, runtime_config: SessionRuntimeConfig, mock_rust_session: MagicMock
    ) -> None:
        runtime = RustSessionRuntime(runtime_config)
        mock_rust_session.return_value.extend_path_mapping_rules.side_effect = _FakePanic(
            "panicked at 'capacity overflow'"
        )

        with pytest.raises(SessionRuntimeCrashError, match="_FakePanic") as exc_info:
            runtime.extend_path_mapping_rules(rules=[])
        assert isinstance(exc_info.value.__cause__, _FakePanic)


class TestToEnvironmentParameterDefinitions:
    """Direct tests for _to_environment_parameter_definitions helper."""

    def test_extracts_type_from_dict_form(self) -> None:
        values: dict[str, Any] = {
            "D": {"type": "STRING", "value": "hello"},
        }
        result = _to_environment_parameter_definitions(values)
        assert result == [{"name": "D", "type": "STRING"}]

    def test_includes_all_parameter_value_types(self) -> None:
        """No type filter is applied — all ParameterValue types are declared."""
        values: dict[str, Any] = {
            "Good": ParameterValue(type=ParameterValueType.STRING, value="ok"),
            "Also": ParameterValue(type=ParameterValueType.CHUNK_INT, value="1-5"),
        }
        result = _to_environment_parameter_definitions(values)
        assert result == [
            {"name": "Good", "type": "STRING"},
            {"name": "Also", "type": "CHUNK[INT]"},
        ]

    def test_empty_input_returns_empty_list(self) -> None:
        assert _to_environment_parameter_definitions({}) == []

    def test_non_primitive_types_are_declared(self) -> None:
        """Types beyond STRING/PATH/INT/FLOAT (e.g. BOOL) are declared unconditionally."""
        values: dict[str, Any] = {
            "Flag": ParameterValue(type=ParameterValueType.BOOL, value="true"),
            "Expr": {"type": "RANGE_EXPR", "value": "1-10"},
        }
        result = _to_environment_parameter_definitions(values)
        assert result == [
            {"name": "Flag", "type": "BOOL"},
            {"name": "Expr", "type": "RANGE_EXPR"},
        ]
