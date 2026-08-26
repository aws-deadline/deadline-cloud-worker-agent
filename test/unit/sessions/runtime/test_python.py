# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import builtins
from datetime import timedelta
from pathlib import Path
from typing import Generator
from unittest.mock import MagicMock, patch

import pytest

from openjd.model import SpecificationRevision

from deadline_worker_agent.sessions.runtime import (
    ResolvedSymbolTableError,
    SessionRuntimeConfig,
)
from deadline_worker_agent.sessions.runtime import python as python_module
from deadline_worker_agent.sessions.runtime.python import PythonSessionRuntime


@pytest.fixture()
def runtime_config() -> SessionRuntimeConfig:
    """Minimal valid SessionRuntimeConfig for testing."""
    return SessionRuntimeConfig(
        session_id="session-1",
        job_parameter_values={"Param1": "value1"},
        path_mapping_rules=None,
        retain_working_dir=False,
        user=None,
        action_callback=lambda session_id, status: None,
        os_env_vars=None,
        session_root_directory=Path("/tmp/sessions/session-1"),
    )


@pytest.fixture()
def mock_openjd_session() -> Generator[MagicMock, None, None]:
    with patch.object(python_module, "OpenJDSession", autospec=True) as mock_cls:
        yield mock_cls


class TestPythonSessionRuntimeConstruction:
    def test_construction_when_default_config_delegates_to_openjd_session(
        self, runtime_config: SessionRuntimeConfig, mock_openjd_session: MagicMock
    ) -> None:
        PythonSessionRuntime(runtime_config)

        mock_openjd_session.assert_called_once()
        call_kwargs = mock_openjd_session.call_args.kwargs
        assert call_kwargs["session_id"] == "session-1"
        assert call_kwargs["job_parameter_values"] == {"Param1": "value1"}
        assert call_kwargs["path_mapping_rules"] is None
        assert call_kwargs["retain_working_dir"] is False
        assert call_kwargs["user"] is None
        assert call_kwargs["callback"] is runtime_config.action_callback
        assert call_kwargs["os_env_vars"] is None
        assert call_kwargs["session_root_directory"] == Path("/tmp/sessions/session-1")
        assert call_kwargs["job_name"] is None
        rev_ext = call_kwargs["revision_extensions"]
        assert rev_ext.spec_rev == SpecificationRevision.v2023_09
        assert rev_ext.extensions == set()

    def test_construction_when_spec_revision_string_converts_to_enum(
        self, mock_openjd_session: MagicMock
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
            spec_revision="2023-09",
            supported_extensions=("WRAP_ACTIONS",),
        )

        PythonSessionRuntime(config)

        call_kwargs = mock_openjd_session.call_args.kwargs
        rev_ext = call_kwargs["revision_extensions"]
        assert rev_ext.spec_rev == SpecificationRevision.v2023_09
        assert rev_ext.extensions == {"WRAP_ACTIONS"}


class TestPythonSessionRuntimeDelegation:
    @pytest.fixture()
    def adapter(
        self, runtime_config: SessionRuntimeConfig, mock_openjd_session: MagicMock
    ) -> PythonSessionRuntime:
        return PythonSessionRuntime(runtime_config)

    @pytest.fixture()
    def mock_session_instance(self, mock_openjd_session: MagicMock) -> MagicMock:
        return mock_openjd_session.return_value

    def test_enter_environment_when_called_delegates_to_wrapped_session(
        self, adapter: PythonSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        env = MagicMock()
        identifier = MagicMock()
        os_env = {"KEY": "VAL"}

        result = adapter.enter_environment(
            environment=env, identifier=identifier, os_env_vars=os_env
        )

        mock_session_instance.enter_environment.assert_called_once_with(
            environment=env,
            identifier=identifier,
            os_env_vars=os_env,
            step_name=None,
            resolved_symtab=None,
        )
        # enter_environment is the only non-void method — verify the return value
        # (EnvironmentIdentifier) flows through the adapter.
        assert result is mock_session_instance.enter_environment.return_value

    def test_enter_environment_forwards_step_context_to_wrapped_session(
        self, adapter: PythonSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        """A non-None step_name is forwarded to the Python session, which uses
        it for wrap-action symbol resolution."""
        env = MagicMock()
        identifier = "env-123"
        os_env = {"K": "V"}

        result = adapter.enter_environment(
            environment=env,
            identifier=identifier,
            os_env_vars=os_env,
            step_name="MyStep",
        )

        mock_session_instance.enter_environment.assert_called_once_with(
            environment=env,
            identifier=identifier,
            os_env_vars=os_env,
            step_name="MyStep",
            resolved_symtab=None,
        )
        assert result is mock_session_instance.enter_environment.return_value

    def test_exit_environment_when_called_delegates_to_wrapped_session(
        self, adapter: PythonSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        identifier = MagicMock()

        adapter.exit_environment(
            identifier=identifier, os_env_vars={"A": "B"}, keep_session_running=True
        )

        mock_session_instance.exit_environment.assert_called_once_with(
            identifier=identifier,
            os_env_vars={"A": "B"},
            keep_session_running=True,
            resolved_symtab=None,
        )

    def test_run_task_when_called_delegates_to_wrapped_session(
        self, adapter: PythonSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        step_script = MagicMock()
        task_params = {"TaskParam": "val"}

        adapter.run_task(
            step_script=step_script,
            task_parameter_values=task_params,
            os_env_vars={"X": "Y"},
            log_task_banner=False,
        )

        mock_session_instance.run_task.assert_called_once_with(
            step_script=step_script,
            task_parameter_values=task_params,
            os_env_vars={"X": "Y"},
            log_task_banner=False,
            step_name=None,
            resolved_symtab=None,
        )

    def test_run_task_without_session_env_when_called_delegates_to_private_method(
        self, adapter: PythonSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        step_script = MagicMock()
        task_params = {"P": "v"}

        adapter._run_task_without_session_env(
            step_script=step_script,
            task_parameter_values=task_params,
            os_env_vars=None,
            log_task_banner=True,
        )

        mock_session_instance._run_task_without_session_env.assert_called_once_with(
            step_script=step_script,
            task_parameter_values=task_params,
            os_env_vars=None,
            log_task_banner=True,
        )

    def test_cancel_action_when_called_delegates_to_wrapped_session(
        self, adapter: PythonSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        limit = timedelta(seconds=30)

        adapter.cancel_action(time_limit=limit, mark_action_failed=True)

        mock_session_instance.cancel_action.assert_called_once_with(
            time_limit=limit, mark_action_failed=True
        )

    def test_cleanup_when_called_delegates_to_wrapped_session(
        self, adapter: PythonSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        adapter.cleanup()

        mock_session_instance.cleanup.assert_called_once_with()


class TestPythonSessionRuntimeProperties:
    @pytest.fixture()
    def adapter(
        self, runtime_config: SessionRuntimeConfig, mock_openjd_session: MagicMock
    ) -> PythonSessionRuntime:
        return PythonSessionRuntime(runtime_config)

    @pytest.fixture()
    def mock_session_instance(self, mock_openjd_session: MagicMock) -> MagicMock:
        return mock_openjd_session.return_value

    def test_working_directory_when_accessed_returns_wrapped_session_value(
        self, adapter: PythonSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        mock_session_instance.working_directory = Path("/tmp/work")

        assert adapter.working_directory == Path("/tmp/work")

    def test_action_status_when_accessed_returns_wrapped_session_value(
        self, adapter: PythonSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        fake_status = MagicMock()
        mock_session_instance.action_status = fake_status

        assert adapter.action_status is fake_status

    def test_action_status_when_none_returns_none(
        self, adapter: PythonSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        mock_session_instance.action_status = None

        assert adapter.action_status is None


class TestPythonSessionRuntimeJobName:
    """Tests for job_name extraction and forwarding at construction time."""

    def test_construction_passes_job_name_when_resolved_table_has_job_name(
        self, mock_openjd_session: MagicMock
    ) -> None:
        config = SessionRuntimeConfig(
            session_id="session-jn-1",
            job_parameter_values={},
            path_mapping_rules=None,
            retain_working_dir=False,
            user=None,
            action_callback=lambda session_id, status: None,
            os_env_vars=None,
            session_root_directory=Path("/tmp/sessions/session-jn-1"),
            resolved_symbol_table_json='[{"name":"Job.Name","type":"string","value":"Example Job"}]',
        )

        PythonSessionRuntime(config)

        call_kwargs = mock_openjd_session.call_args.kwargs
        assert call_kwargs["job_name"] == "Example Job"

    def test_construction_passes_none_when_resolved_table_json_is_none(
        self, mock_openjd_session: MagicMock
    ) -> None:
        config = SessionRuntimeConfig(
            session_id="session-jn-2",
            job_parameter_values={},
            path_mapping_rules=None,
            retain_working_dir=False,
            user=None,
            action_callback=lambda session_id, status: None,
            os_env_vars=None,
            session_root_directory=Path("/tmp/sessions/session-jn-2"),
            resolved_symbol_table_json=None,
        )

        PythonSessionRuntime(config)

        call_kwargs = mock_openjd_session.call_args.kwargs
        assert call_kwargs["job_name"] is None

    def test_construction_passes_none_when_resolved_table_lacks_job_name(
        self, mock_openjd_session: MagicMock
    ) -> None:
        config = SessionRuntimeConfig(
            session_id="session-jn-3",
            job_parameter_values={},
            path_mapping_rules=None,
            retain_working_dir=False,
            user=None,
            action_callback=lambda session_id, status: None,
            os_env_vars=None,
            session_root_directory=Path("/tmp/sessions/session-jn-3"),
            resolved_symbol_table_json="[]",
        )

        PythonSessionRuntime(config)

        call_kwargs = mock_openjd_session.call_args.kwargs
        assert call_kwargs["job_name"] is None

    def test_construction_passes_none_when_resolved_table_is_malformed_json(
        self, mock_openjd_session: MagicMock
    ) -> None:
        config = SessionRuntimeConfig(
            session_id="session-jn-4",
            job_parameter_values={},
            path_mapping_rules=None,
            retain_working_dir=False,
            user=None,
            action_callback=lambda session_id, status: None,
            os_env_vars=None,
            session_root_directory=Path("/tmp/sessions/session-jn-4"),
            resolved_symbol_table_json="{not json",
        )

        with patch.object(python_module, "logger") as mock_logger:
            PythonSessionRuntime(config)

        mock_logger.warning.assert_called_once()
        call_kwargs = mock_openjd_session.call_args.kwargs
        assert call_kwargs["job_name"] is None


class TestResolvedSymbolTableForwarding:
    """Tests for resolved_symbol_table_json parsing and forwarding to the v0 session.

    Mirrors TestResolvedSymbolTableForwarding in test_rust.py. python.py imports
    SerializedSymbolTable lazily (extension purity), so the class is patched at
    its source (openjd.expr) rather than as a module attribute of python.py.
    """

    @pytest.fixture()
    def adapter(
        self, runtime_config: SessionRuntimeConfig, mock_openjd_session: MagicMock
    ) -> PythonSessionRuntime:
        return PythonSessionRuntime(runtime_config)

    @pytest.fixture()
    def mock_session_instance(self, mock_openjd_session: MagicMock) -> MagicMock:
        return mock_openjd_session.return_value

    def test_enter_environment_forwards_resolved_symtab_when_json_present(
        self, adapter: PythonSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        """When resolved_symbol_table_json is provided, it's parsed and forwarded."""
        from openjd.expr import SerializedSymbolTable

        environment = MagicMock()
        symtab_json = '[{"name":"Job.Name","type":"string","value":"TestJob"}]'
        fake_symtab = MagicMock()

        with patch.object(
            SerializedSymbolTable, "from_json_str", return_value=fake_symtab
        ) as mock_from_json:
            adapter.enter_environment(
                environment=environment,
                identifier="env-1",
                resolved_symbol_table_json=symtab_json,
            )

        mock_from_json.assert_called_once_with(symtab_json)
        call_kwargs = mock_session_instance.enter_environment.call_args.kwargs
        assert call_kwargs["resolved_symtab"] is fake_symtab

    def test_run_task_forwards_resolved_symtab_when_json_present(
        self, adapter: PythonSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        """When resolved_symbol_table_json is provided, it's parsed and forwarded."""
        from openjd.expr import SerializedSymbolTable

        step_script = MagicMock()
        symtab_json = '[{"name":"Job.Name","type":"string","value":"TestJob"}]'
        fake_symtab = MagicMock()

        with patch.object(
            SerializedSymbolTable, "from_json_str", return_value=fake_symtab
        ) as mock_from_json:
            adapter.run_task(
                step_script=step_script,
                task_parameter_values={},
                resolved_symbol_table_json=symtab_json,
            )

        mock_from_json.assert_called_once_with(symtab_json)
        call_kwargs = mock_session_instance.run_task.call_args.kwargs
        assert call_kwargs["resolved_symtab"] is fake_symtab

    def test_exit_environment_forwards_resolved_symtab_when_json_present(
        self, adapter: PythonSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        """When resolved_symbol_table_json is provided, it's parsed and forwarded."""
        from openjd.expr import SerializedSymbolTable

        symtab_json = '[{"name":"Job.Name","type":"string","value":"TestJob"}]'
        fake_symtab = MagicMock()

        with patch.object(
            SerializedSymbolTable, "from_json_str", return_value=fake_symtab
        ) as mock_from_json:
            adapter.exit_environment(
                identifier="env-1",
                resolved_symbol_table_json=symtab_json,
            )

        mock_from_json.assert_called_once_with(symtab_json)
        call_kwargs = mock_session_instance.exit_environment.call_args.kwargs
        assert call_kwargs["resolved_symtab"] is fake_symtab

    def test_exit_environment_passes_none_when_json_is_none(
        self, adapter: PythonSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        """When resolved_symbol_table_json is None, the parser is not invoked."""
        from openjd.expr import SerializedSymbolTable

        with patch.object(SerializedSymbolTable, "from_json_str") as mock_from_json:
            adapter.exit_environment(
                identifier="env-1",
                resolved_symbol_table_json=None,
            )

        mock_from_json.assert_not_called()
        call_kwargs = mock_session_instance.exit_environment.call_args.kwargs
        assert call_kwargs["resolved_symtab"] is None

    def test_enter_environment_raises_on_malformed_json(
        self, adapter: PythonSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        """An unparseable table fails the action instead of silently dropping it.

        The table is the only channel for step-scope `let`, so degrading to None
        would enter the environment with those symbols undefined and surface a
        downstream "undefined symbol" error naming the symbol, not the cause.
        """
        environment = MagicMock()

        with patch.object(python_module, "logger") as mock_logger:
            with pytest.raises(ResolvedSymbolTableError):
                adapter.enter_environment(
                    environment=environment,
                    identifier="env-1",
                    resolved_symbol_table_json="not valid json",
                )

        # The environment must not be entered at all -- a half-entered
        # environment would land on Session._active_envs and be exited later.
        mock_session_instance.enter_environment.assert_not_called()
        mock_logger.error.assert_called_once()
        assert "resolvedSymbolTable" in mock_logger.error.call_args[0][0]

    def test_run_task_raises_on_malformed_json(
        self, adapter: PythonSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        """An unparseable table fails the task instead of running it degraded."""
        with patch.object(python_module, "logger") as mock_logger:
            with pytest.raises(ResolvedSymbolTableError):
                adapter.run_task(
                    step_script=MagicMock(),
                    task_parameter_values={},
                    resolved_symbol_table_json="{not json",
                )

        mock_session_instance.run_task.assert_not_called()
        mock_logger.error.assert_called_once()
        assert "resolvedSymbolTable" in mock_logger.error.call_args[0][0]

    def test_exit_environment_degrades_on_malformed_json_so_teardown_runs(
        self, adapter: PythonSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        """Exit must not be blocked by an unparseable table -- onExit still runs.

        Deliberately the opposite of the enter and run paths. Session.exit_environment
        pops _active_envs only after this returns, so raising would leave the
        environment active and Session._cleanup would retry with the same stored
        table and swallow the same failure -- onExit would never run, leaking
        whatever the environment set up (licenses, daemons).
        """
        with patch.object(python_module, "logger") as mock_logger:
            adapter.exit_environment(
                identifier="env-1",
                resolved_symbol_table_json="{not json",
            )

        # Teardown proceeded, with the table dropped rather than the exit skipped.
        mock_session_instance.exit_environment.assert_called_once()
        assert mock_session_instance.exit_environment.call_args.kwargs["resolved_symtab"] is None
        # Still loud in the agent log, just not fatal.
        mock_logger.error.assert_called_once()
        assert "resolvedSymbolTable" in mock_logger.error.call_args[0][0]

    def test_enter_and_run_still_raise_while_exit_degrades(
        self, adapter: PythonSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        """Negative control for the exit carve-out: it must not soften enter/run.

        A single flag or a misplaced try/except that degraded everywhere would
        pass the exit test above; this pins that the carve-out is exit-only.
        """
        bad = "{not json"

        with pytest.raises(ResolvedSymbolTableError):
            adapter.enter_environment(environment=MagicMock(), resolved_symbol_table_json=bad)
        with pytest.raises(ResolvedSymbolTableError):
            adapter.run_task(
                step_script=MagicMock(), task_parameter_values={}, resolved_symbol_table_json=bad
            )

        adapter.exit_environment(identifier="env-1", resolved_symbol_table_json=bad)

        mock_session_instance.enter_environment.assert_not_called()
        mock_session_instance.run_task.assert_not_called()
        mock_session_instance.exit_environment.assert_called_once()

    def test_extension_load_failure_becomes_resolved_symbol_table_error(
        self, adapter: PythonSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        """An unloadable native extension must not escape as a raw ImportError.

        The openjd.expr import is lazy (function-local, to keep the extension off
        the import path for sessions that never receive a table). That moved where
        an unloadable extension is discovered: it no longer fails at adapter
        construction, where _factory.create_session_runtime converts ImportError
        into NotImplementedError. Left uncaught it would surface mid-session as a
        raw ImportError reported as the task's fail message.
        """
        real_import = builtins.__import__

        def fail_openjd_expr(name: str, *args: object, **kwargs: object) -> object:
            if name == "openjd.expr":
                raise ImportError("native extension unavailable on this platform")
            return real_import(name, *args, **kwargs)  # type: ignore[arg-type]

        with patch.object(builtins, "__import__", side_effect=fail_openjd_expr):
            with pytest.raises(ResolvedSymbolTableError):
                adapter.run_task(
                    step_script=MagicMock(),
                    task_parameter_values={},
                    resolved_symbol_table_json='[{"name":"X","type":"string","value":"1"}]',
                )

        mock_session_instance.run_task.assert_not_called()

    def test_malformed_table_error_does_not_leak_payload_and_chains_cause(
        self, adapter: PythonSessionRuntime
    ) -> None:
        """The fail message reaches the service, so it must not echo the payload.

        Session._start_action reports str(e) as the action's fail message, which
        the scheduler forwards as progressMessage. The underlying parse error is
        preserved as __cause__ for the agent-side traceback instead.
        """
        payload = '{"UNIQUE_PAYLOAD_MARKER_9f3a": "should not be echoed"'

        with pytest.raises(ResolvedSymbolTableError) as excinfo:
            adapter.run_task(
                step_script=MagicMock(),
                task_parameter_values={},
                resolved_symbol_table_json=payload,
            )

        message = str(excinfo.value)
        assert "UNIQUE_PAYLOAD_MARKER_9f3a" not in message
        assert "resolvedSymbolTable" in message
        assert excinfo.value.__cause__ is not None
