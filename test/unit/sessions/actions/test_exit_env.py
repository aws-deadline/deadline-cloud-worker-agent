# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from unittest.mock import Mock

import pytest

from deadline_worker_agent.sessions.actions.exit_env import ExitEnvironmentAction
from deadline_worker_agent.sessions.job_entities import EnvironmentDetails


@pytest.fixture
def mock_session() -> Mock:
    session = Mock()
    session.exit_environment = Mock()
    return session


@pytest.fixture
def mock_executor() -> Mock:
    return Mock()


class TestExitEnvironmentAction:
    """Tests for ExitEnvironmentAction with optional details."""

    def test_start_forwards_resolved_symbol_table_json_from_details(
        self, mock_session: Mock, mock_executor: Mock
    ) -> None:
        """start() forwards self._details.resolved_symbol_table_json to session.exit_environment."""
        table = '[{"name":"Job.Name","type":"string","value":"Outer"}]'
        details = Mock(spec=EnvironmentDetails)
        details.resolved_symbol_table_json = table

        action = ExitEnvironmentAction(
            id="action-exit-1",
            environment_id="env-outer",
            details=details,
        )

        action.start(session=mock_session, executor=mock_executor)

        mock_session.exit_environment.assert_called_once_with(
            job_env_id="env-outer",
            os_env_vars={"DEADLINE_SESSIONACTION_ID": "action-exit-1"},
            resolved_symbol_table_json=table,
        )

    def test_start_forwards_none_when_no_details(
        self, mock_session: Mock, mock_executor: Mock
    ) -> None:
        """start() forwards None when constructed without details."""
        action = ExitEnvironmentAction(
            id="action-exit-2",
            environment_id="env-inner",
        )

        action.start(session=mock_session, executor=mock_executor)

        mock_session.exit_environment.assert_called_once_with(
            job_env_id="env-inner",
            os_env_vars={"DEADLINE_SESSIONACTION_ID": "action-exit-2"},
            resolved_symbol_table_json=None,
        )

    def test_eq_includes_details(self) -> None:
        """__eq__ considers details so equality proves the details were threaded."""
        details_a = Mock(spec=EnvironmentDetails)
        details_a.resolved_symbol_table_json = '[{"name":"Job.Name","type":"string","value":"A"}]'
        details_b = Mock(spec=EnvironmentDetails)
        details_b.resolved_symbol_table_json = '[{"name":"Job.Name","type":"string","value":"B"}]'

        action_a = ExitEnvironmentAction(id="action-1", environment_id="env-1", details=details_a)
        action_b = ExitEnvironmentAction(id="action-1", environment_id="env-1", details=details_b)
        action_c = ExitEnvironmentAction(id="action-1", environment_id="env-1", details=details_a)

        assert action_a == action_c
        assert action_a != action_b

    def test_eq_without_details(self) -> None:
        """Two actions without details are equal when id and environment_id match."""
        action_a = ExitEnvironmentAction(id="action-1", environment_id="env-1")
        action_b = ExitEnvironmentAction(id="action-1", environment_id="env-1")

        assert action_a == action_b

    def test_eq_none_details_vs_details(self) -> None:
        """An action without details is not equal to one with details."""
        details = Mock(spec=EnvironmentDetails)
        details.resolved_symbol_table_json = "[]"

        action_no_details = ExitEnvironmentAction(id="action-1", environment_id="env-1")
        action_with_details = ExitEnvironmentAction(
            id="action-1", environment_id="env-1", details=details
        )

        assert action_no_details != action_with_details
