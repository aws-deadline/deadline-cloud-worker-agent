# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from datetime import timedelta
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Generator
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from openjd.model import SpecificationRevision

from deadline_worker_agent.sessions.runtime import (
    SessionRuntimeConfig,
    SessionRuntimeDecodeError,
)
from deadline_worker_agent.sessions.runtime import python as python_module
from deadline_worker_agent.sessions.runtime.python import (
    PythonSessionRuntime,
    _to_v0_parameter_values,
    _to_v0_path_mapping_rule,
    _to_v0_path_mapping_rules,
    _to_v1_action_status,
)
from deadline_worker_agent.sessions.runtime._v0_compat import _exit_code_to_i32


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
        # Wire-format dicts are converted to v0 ParameterValue objects.
        from openjd.model import ParameterValue, ParameterValueType

        expected_param = ParameterValue(type=ParameterValueType.STRING, value="value1")
        assert call_kwargs["job_parameter_values"] == {"Param1": expected_param}
        assert call_kwargs["path_mapping_rules"] is None
        assert call_kwargs["retain_working_dir"] is False
        assert call_kwargs["user"] is None
        # The callback is wrapped (v0→v1 conversion), so it's NOT the same object.
        assert callable(call_kwargs["callback"])
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

    def test_construction_when_path_mapping_rules_converts_v1_to_v0(
        self, mock_openjd_session: MagicMock
    ) -> None:
        """_v1 PathMappingRules from the config are converted to v0 types."""
        from openjd.expr import PathFormat as V1PathFormat, PathMappingRule as V1PathMappingRule
        from openjd.sessions import PathFormat, PathMappingRule as V0PathMappingRule

        v1_rule = V1PathMappingRule(
            source_path_format=V1PathFormat.POSIX,
            source_path="/source",
            destination_path="/dest",
        )
        config = SessionRuntimeConfig(
            session_id="session-pmr",
            job_parameter_values={},
            path_mapping_rules=[v1_rule],
            retain_working_dir=False,
            user=None,
            action_callback=lambda sid, s: None,
            os_env_vars=None,
            session_root_directory=Path("/tmp/sessions/session-pmr"),
        )

        PythonSessionRuntime(config)

        passed = mock_openjd_session.call_args.kwargs["path_mapping_rules"]
        assert len(passed) == 1
        assert isinstance(passed[0], V0PathMappingRule)
        assert passed[0].source_path_format == PathFormat.POSIX
        assert passed[0].source_path == PurePosixPath("/source")
        assert passed[0].destination_path == Path("/dest")


class TestPythonSessionRuntimeDelegation:
    @pytest.fixture()
    def adapter(
        self, runtime_config: SessionRuntimeConfig, mock_openjd_session: MagicMock
    ) -> PythonSessionRuntime:
        return PythonSessionRuntime(runtime_config)

    @pytest.fixture()
    def mock_session_instance(self, mock_openjd_session: MagicMock) -> MagicMock:
        return mock_openjd_session.return_value

    def test_enter_environment_when_called_decodes_and_delegates(
        self, adapter: PythonSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        env: dict[str, Any] = {
            "name": "TestEnv",
            "script": {"actions": {"onEnter": {"command": "echo"}}},
        }
        identifier = MagicMock()
        os_env = {"KEY": "VAL"}

        with patch.object(python_module, "parse_model") as mock_parse:
            result = adapter.enter_environment(
                environment=env, identifier=identifier, os_env_vars=os_env
            )

        # The wire-format dict is decoded via parse_model into a v0 pydantic model.
        mock_parse.assert_called_once()
        assert mock_parse.call_args.kwargs["obj"] == env
        mock_session_instance.enter_environment.assert_called_once_with(
            environment=mock_parse.return_value,
            identifier=identifier,
            os_env_vars=os_env,
        )
        assert result is mock_session_instance.enter_environment.return_value

    def test_enter_environment_when_decode_fails_raises_session_runtime_decode_error(
        self, adapter: PythonSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        """F3: DecodeValidationError from parse_model is wrapped."""
        from openjd.model import DecodeValidationError

        with patch.object(python_module, "parse_model") as mock_parse:
            mock_parse.side_effect = DecodeValidationError("bad env")
            with pytest.raises(
                SessionRuntimeDecodeError, match="PythonSessionRuntime.enter_environment"
            ):
                adapter.enter_environment(environment={"bad": "data"})

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

    def test_run_task_when_called_decodes_and_converts_then_delegates(
        self, adapter: PythonSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        step_script: dict[str, Any] = {"actions": {"onRun": {"command": "echo"}}}
        task_params: dict[str, dict[str, Any]] = {"TaskParam": {"type": "STRING", "value": "val"}}

        with patch.object(python_module, "parse_model") as mock_parse:
            adapter.run_task(
                step_script=step_script,
                task_parameter_values=task_params,
                os_env_vars={"X": "Y"},
                log_task_banner=False,
            )

        mock_parse.assert_called_once()
        assert mock_parse.call_args.kwargs["obj"] == step_script
        mock_session_instance.run_task.assert_called_once()
        run_kwargs = mock_session_instance.run_task.call_args.kwargs
        assert run_kwargs["step_script"] is mock_parse.return_value
        # Wire-format task params are converted to v0 ParameterValue objects.
        from openjd.model import ParameterValue, ParameterValueType

        expected = ParameterValue(type=ParameterValueType.STRING, value="val")
        assert run_kwargs["task_parameter_values"] == {"TaskParam": expected}
        assert run_kwargs["os_env_vars"] == {"X": "Y"}
        assert run_kwargs["log_task_banner"] is False

    def test_run_task_when_decode_fails_raises_session_runtime_decode_error(
        self, adapter: PythonSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        """F3: DecodeValidationError from parse_model is wrapped."""
        from openjd.model import DecodeValidationError

        with patch.object(python_module, "parse_model") as mock_parse:
            mock_parse.side_effect = DecodeValidationError("bad script")
            with pytest.raises(SessionRuntimeDecodeError, match="PythonSessionRuntime.run_task"):
                adapter.run_task(step_script={"bad": "script"}, task_parameter_values={})

    def test_run_task_without_session_env_when_called_decodes_and_delegates(
        self, adapter: PythonSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        step_script: dict[str, Any] = {"actions": {"onRun": {"command": "echo"}}}
        task_params: dict[str, dict[str, Any]] = {"P": {"type": "INT", "value": "7"}}

        with patch.object(python_module, "parse_model") as mock_parse:
            adapter._run_task_without_session_env(
                step_script=step_script,
                task_parameter_values=task_params,
                os_env_vars=None,
                log_task_banner=True,
            )

        mock_parse.assert_called_once()
        assert mock_parse.call_args.kwargs["obj"] == step_script
        mock_session_instance._run_task_without_session_env.assert_called_once()
        run_kwargs = mock_session_instance._run_task_without_session_env.call_args.kwargs
        assert run_kwargs["step_script"] is mock_parse.return_value
        from openjd.model import ParameterValue, ParameterValueType

        expected = ParameterValue(type=ParameterValueType.INT, value="7")
        assert run_kwargs["task_parameter_values"] == {"P": expected}

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

    def test_extend_path_mapping_rules_when_called_converts_and_sorts(
        self, adapter: PythonSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        """_v1 rules are converted to v0 and sorted by descending source path length."""
        from openjd.expr import PathFormat as V1PathFormat, PathMappingRule as V1PathMappingRule

        rule_short = V1PathMappingRule(
            source_path_format=V1PathFormat.POSIX,
            source_path="/a",
            destination_path="/b",
        )
        rule_long = V1PathMappingRule(
            source_path_format=V1PathFormat.POSIX,
            source_path="/longer/path",
            destination_path="/dest/path",
        )

        # Set up the mock session's _path_mapping_rules as an empty list.
        mock_session_instance._path_mapping_rules = []

        adapter.extend_path_mapping_rules([rule_short, rule_long])

        # The rules are converted to v0 and sorted by descending source path length.
        rules = mock_session_instance._path_mapping_rules
        assert len(rules) == 2
        # Longer path should be first after sort.
        assert rules[0].source_path == PurePosixPath("/longer/path")
        assert rules[1].source_path == PurePosixPath("/a")


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

    def test_action_status_when_v0_status_present_returns_v1(
        self, adapter: PythonSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        """The action_status property converts the v0 session's status to _v1."""
        from openjd.sessions import ActionState, ActionStatus
        from openjd.sessions._v1 import ActionState as V1ActionState, ActionStatus as V1ActionStatus

        mock_session_instance.action_status = ActionStatus(
            state=ActionState.RUNNING,
            progress=42,
            status_message="working",
        )

        result = adapter.action_status
        assert isinstance(result, V1ActionStatus)
        assert result.state == V1ActionState.RUNNING
        assert result.progress == 42
        assert result.status_message == "working"

    def test_action_status_when_none_returns_none(
        self, adapter: PythonSessionRuntime, mock_session_instance: MagicMock
    ) -> None:
        mock_session_instance.action_status = None

        assert adapter.action_status is None

    def test_callback_when_invoked_converts_v0_status_to_v1(
        self, mock_openjd_session: MagicMock
    ) -> None:
        """The wrapped callback converts v0 ActionStatus to _v1 ActionStatus."""
        from openjd.sessions import ActionState, ActionStatus
        from openjd.sessions._v1 import ActionState as V1ActionState, ActionStatus as V1ActionStatus

        original_callback = MagicMock()
        config = SessionRuntimeConfig(
            session_id="session-cb",
            job_parameter_values={},
            path_mapping_rules=None,
            retain_working_dir=False,
            user=None,
            action_callback=original_callback,
            os_env_vars=None,
            session_root_directory=Path("/tmp/sessions/session-cb"),
        )

        PythonSessionRuntime(config)

        # Grab the wrapped callback that was passed to the v0 session.
        wrapped_callback = mock_openjd_session.call_args.kwargs["callback"]

        # Simulate a v0 ActionStatus
        v0_status = ActionStatus(
            state=ActionState.SUCCESS,
            progress=100,
            status_message="done",
            exit_code=0,
        )
        wrapped_callback("session-cb", v0_status)

        original_callback.assert_called_once()
        call_args = original_callback.call_args
        assert call_args[0][0] == "session-cb"
        v1_status = call_args[0][1]
        assert isinstance(v1_status, V1ActionStatus)
        assert v1_status.state == V1ActionState.SUCCESS
        assert v1_status.progress == 100
        assert v1_status.status_message == "done"
        assert v1_status.exit_code == 0


class TestV0CompatConversions:
    """Tests for the v1→v0 conversion helpers in python.py."""

    def test_to_v0_parameter_values_when_valid_type_converts(self) -> None:
        from openjd.model import ParameterValue, ParameterValueType

        values: dict[str, dict[str, Any]] = {
            "StringParam": {"type": "STRING", "value": "hello"},
            "IntParam": {"type": "INT", "value": "42"},
            "FloatParam": {"type": "FLOAT", "value": "3.14"},
            "PathParam": {"type": "PATH", "value": "/tmp/out"},
        }

        result = _to_v0_parameter_values(values)

        assert result == {
            "StringParam": ParameterValue(type=ParameterValueType.STRING, value="hello"),
            "IntParam": ParameterValue(type=ParameterValueType.INT, value="42"),
            "FloatParam": ParameterValue(type=ParameterValueType.FLOAT, value="3.14"),
            "PathParam": ParameterValue(type=ParameterValueType.PATH, value="/tmp/out"),
        }

    def test_to_v0_parameter_values_when_unknown_type_raises(self) -> None:
        """A parameter type the v0 enum does not define fails loud."""
        values: dict[str, dict[str, Any]] = {
            "P": {"type": "HOLOGRAM", "value": "x"},
        }
        with pytest.raises(ValueError, match="HOLOGRAM.*ParameterValueType enum does not define"):
            _to_v0_parameter_values(values)

    def test_to_v0_parameter_values_when_empty_returns_empty(self) -> None:
        assert _to_v0_parameter_values({}) == {}

    def test_to_v0_path_mapping_rule_when_posix_converts(self) -> None:
        from openjd.expr import PathFormat as V1PathFormat, PathMappingRule as V1PathMappingRule
        from openjd.sessions import PathFormat, PathMappingRule as V0PathMappingRule

        v1_rule = V1PathMappingRule(
            source_path_format=V1PathFormat.POSIX,
            source_path="/source/path",
            destination_path="/dest/path",
        )

        result = _to_v0_path_mapping_rule(v1_rule)

        assert isinstance(result, V0PathMappingRule)
        assert result.source_path_format == PathFormat.POSIX
        assert result.source_path == PurePosixPath("/source/path")
        assert result.destination_path == Path("/dest/path")

    def test_to_v0_path_mapping_rule_when_windows_uses_pure_windows_path(self) -> None:
        from openjd.expr import PathFormat as V1PathFormat, PathMappingRule as V1PathMappingRule
        from openjd.sessions import PathFormat

        v1_rule = V1PathMappingRule(
            source_path_format=V1PathFormat.WINDOWS,
            source_path="C:\\Users\\source",
            destination_path="/dest",
        )

        result = _to_v0_path_mapping_rule(v1_rule)

        assert result.source_path_format == PathFormat.WINDOWS
        assert isinstance(result.source_path, PureWindowsPath)
        assert str(result.source_path) == "C:\\Users\\source"

    def test_to_v0_path_mapping_rules_when_none_returns_none(self) -> None:
        assert _to_v0_path_mapping_rules(None) is None

    def test_to_v0_path_mapping_rules_when_list_converts_all(self) -> None:
        from openjd.expr import PathFormat as V1PathFormat, PathMappingRule as V1PathMappingRule

        rules = [
            V1PathMappingRule(
                source_path_format=V1PathFormat.POSIX,
                source_path="/a",
                destination_path="/b",
            ),
            V1PathMappingRule(
                source_path_format=V1PathFormat.POSIX,
                source_path="/c",
                destination_path="/d",
            ),
        ]

        result = _to_v0_path_mapping_rules(rules)

        assert result is not None
        assert len(result) == 2
        assert result[0].source_path == PurePosixPath("/a")
        assert result[1].source_path == PurePosixPath("/c")

    def test_to_v1_action_status_when_all_states_map(self) -> None:
        """Every member of the REAL v0 ActionState maps to a _v1 member.

        The member-count assertion is the drift tripwire: a state added on
        the v0 side must be added to _V1_ACTION_STATES deliberately.
        """
        from openjd.sessions import ActionState, ActionStatus
        from openjd.sessions._v1 import ActionStatus as V1ActionStatus

        # v0 ActionState is a standard Python enum — iterable.
        members = list(ActionState)
        assert len(members) == 5
        for member in members:
            v0_status = ActionStatus(state=member)
            v1_status = _to_v1_action_status(v0_status)
            assert isinstance(v1_status, V1ActionStatus)

    def test_to_v1_action_status_when_unrecognized_state_raises(self) -> None:
        """A v0 state missing from _V1_ACTION_STATES fails loud."""

        class _FakeState:
            pass

        fake_status = SimpleNamespace(state=_FakeState())

        with pytest.raises(ValueError, match="Unrecognized v0 ActionState"):
            _to_v1_action_status(fake_status)  # type: ignore[arg-type]

    def test_to_v1_action_status_when_all_fields_carried(self) -> None:
        """All ActionStatus fields are carried across the conversion."""
        from openjd.sessions import ActionState, ActionStatus
        from openjd.sessions._v1 import ActionState as V1ActionState

        v0_status = ActionStatus(
            state=ActionState.FAILED,
            progress=50,
            status_message="half done",
            fail_message="oops",
            exit_code=1,
        )

        result = _to_v1_action_status(v0_status)

        assert result.state == V1ActionState.FAILED
        assert result.progress == 50
        assert result.status_message == "half done"
        assert result.fail_message == "oops"
        assert result.exit_code == 1


class TestExitCodeToI32:
    """Tests for _exit_code_to_i32 — the 32-bit signed reinterpretation helper."""

    @pytest.mark.parametrize(
        "exitcode, expected_result",
        [
            pytest.param(0x80000000, -2147483648, id="minint_hex"),
            pytest.param(0xFFFD0000, -196608, id="out-of-range-32bit"),
            pytest.param(0xFFFFFFFD0000, -196608, id="out-of-range-big"),
            pytest.param(0xC0000005, -1073741819, id="windows-access-violation"),
        ],
    )
    def test_exit_code_to_i32_when_out_of_range_truncates(
        self, exitcode: int, expected_result: int
    ) -> None:
        assert _exit_code_to_i32(exitcode) == expected_result

    @pytest.mark.parametrize(
        "exitcode, expected_result",
        [
            pytest.param(0, 0, id="zero"),
            pytest.param(1, 1, id="one"),
            pytest.param(-1, -1, id="minus-one"),
            pytest.param(0x7FFFFFFF, 0x7FFFFFFF, id="maxint"),
            pytest.param(-2147483648, -2147483648, id="minint"),
        ],
    )
    def test_exit_code_to_i32_when_in_range_passes_through(
        self, exitcode: int, expected_result: int
    ) -> None:
        assert _exit_code_to_i32(exitcode) == expected_result


class TestV0BridgeExitCodeOverflow:
    """Integration test: the v0→v1 bridge does not raise OverflowError for
    large unsigned exit codes (e.g. Windows 0xC0000005)."""

    def test_to_v1_action_status_when_exit_code_overflows_i32_reinterprets(self) -> None:
        from openjd.sessions import ActionState, ActionStatus
        from openjd.sessions._v1 import ActionState as V1ActionState

        # 0xC0000005 (3221225477) exceeds i32 max — would crash without the fix.
        v0_status = ActionStatus(
            state=ActionState.FAILED,
            exit_code=3221225477,
        )

        result = _to_v1_action_status(v0_status)

        assert result.state == V1ActionState.FAILED
        # Reinterpreted as signed 32-bit: 0xC0000005 → -1073741819
        assert result.exit_code == -1073741819
