# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Generator
from unittest.mock import MagicMock, patch

import pytest

from openjd.model import SpecificationRevision

from deadline_worker_agent.sessions.runtime import SessionRuntimeConfig
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
    with patch.object(python_module, "OpenJDSession") as mock_cls:
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
            environment=env, identifier=identifier, os_env_vars=os_env
        )
        # enter_environment is the only non-void method — verify the return value
        # (EnvironmentIdentifier) flows through the adapter.
        assert result is mock_session_instance.enter_environment.return_value

    def test_exit_environment_when_called_delegates_to_wrapped_session(
        self, adapter: PythonSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        identifier = MagicMock()

        adapter.exit_environment(
            identifier=identifier, os_env_vars={"A": "B"}, keep_session_running=True
        )

        mock_session_instance.exit_environment.assert_called_once_with(
            identifier=identifier, os_env_vars={"A": "B"}, keep_session_running=True
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
