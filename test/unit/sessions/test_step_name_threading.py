# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for step_name threading from RunStepTaskAction → Session → SessionRuntime.

Each test asserts that the step_name kwarg is forwarded with the exact literal
value originating from StepDetails.step_template.name.
"""

from __future__ import annotations

from unittest.mock import MagicMock, Mock

import pytest

from deadline_worker_agent.sessions.actions.run_step_task import RunStepTaskAction
from deadline_worker_agent.sessions.job_entities.step_details import StepDetails


STEP_NAME = "MyRenderStep"
"""Literal step name used across all tests — never derived from the implementation."""


@pytest.fixture
def mock_step_details() -> MagicMock:
    mock = MagicMock(spec=StepDetails)
    mock.step_id = "step-abc"
    mock.step_template = MagicMock()
    mock.step_template.name = STEP_NAME
    mock.step_template.script = MagicMock()
    return mock


@pytest.fixture
def mock_session() -> MagicMock:
    session = MagicMock()
    session.run_task = Mock()
    return session


@pytest.fixture
def mock_executor() -> MagicMock:
    return MagicMock()


class TestRunStepTaskActionPassesStepName:
    """RunStepTaskAction.start() must forward step_template.name as step_name."""

    def test_start_when_called_passes_step_name_from_step_template(
        self, mock_step_details: MagicMock, mock_session: MagicMock, mock_executor: MagicMock
    ) -> None:
        action = RunStepTaskAction(
            id="action-1",
            details=mock_step_details,
            task_id="task-1",
            task_parameter_values={},
        )

        action.start(session=mock_session, executor=mock_executor)

        mock_session.run_task.assert_called_once()
        call_kwargs = mock_session.run_task.call_args.kwargs
        assert call_kwargs["step_name"] == STEP_NAME


class TestSessionRunTaskPassesStepName:
    """Session.run_task() must forward step_name to the runtime."""

    def test_run_task_when_step_name_provided_passes_to_runtime(self) -> None:
        from deadline_worker_agent.sessions.session import Session

        mock_runtime = MagicMock()
        session = MagicMock(spec=Session)
        session._runtime = mock_runtime
        # Call the real method via the unbound function
        Session.run_task(
            session,
            step_script=MagicMock(),
            task_parameter_values={},
            os_env_vars=None,
            log_task_banner=True,
            step_name=STEP_NAME,
        )

        mock_runtime.run_task.assert_called_once()
        call_kwargs = mock_runtime.run_task.call_args.kwargs
        assert call_kwargs["step_name"] == STEP_NAME

    def test_run_task_when_step_name_none_passes_none_to_runtime(self) -> None:
        from deadline_worker_agent.sessions.session import Session

        mock_runtime = MagicMock()
        session = MagicMock(spec=Session)
        session._runtime = mock_runtime
        Session.run_task(
            session,
            step_script=MagicMock(),
            task_parameter_values={},
            os_env_vars=None,
            log_task_banner=True,
        )

        mock_runtime.run_task.assert_called_once()
        call_kwargs = mock_runtime.run_task.call_args.kwargs
        assert call_kwargs["step_name"] is None


class TestPythonRuntimeForwardsStepName:
    """PythonSessionRuntime.run_task() must forward step_name to openjd Session."""

    def test_run_task_when_step_name_provided_forwards_to_openjd_session(self) -> None:
        from unittest.mock import patch
        from deadline_worker_agent.sessions.runtime import python as python_module
        from deadline_worker_agent.sessions.runtime.python import PythonSessionRuntime
        from deadline_worker_agent.sessions.runtime import SessionRuntimeConfig
        from pathlib import Path

        config = SessionRuntimeConfig(
            session_id="session-1",
            job_parameter_values={},
            path_mapping_rules=None,
            retain_working_dir=False,
            user=None,
            action_callback=lambda sid, s: None,
            os_env_vars=None,
            session_root_directory=Path("/tmp/sessions/session-1"),
        )

        with patch.object(python_module, "OpenJDSession") as mock_cls:
            adapter = PythonSessionRuntime(config)
            mock_openjd = mock_cls.return_value

            adapter.run_task(
                step_script=MagicMock(),
                task_parameter_values={},
                os_env_vars=None,
                log_task_banner=True,
                step_name=STEP_NAME,
            )

            mock_openjd.run_task.assert_called_once()
            call_kwargs = mock_openjd.run_task.call_args.kwargs
            assert call_kwargs["step_name"] == STEP_NAME
