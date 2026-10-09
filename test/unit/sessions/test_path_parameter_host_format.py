# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""A PATH parameter reaches the task in the host's path format.

openjd-sessions 0.12.1 (OpenJobDescription/openjd-sessions-for-python#364)
renders ``Param.<PATH>``, ``Task.Param.<PATH>`` and each ``LIST[PATH]`` element
with the host's separators whether or not a path mapping rule matched. Before
it, only a matched rule's output took the host format, so on a Windows host a
POSIX-spelled parameter with no matching rule reached the task as written.

These tests drive the worker's own chain -- the API parameter shape through
``parameters_from_api_response`` and ``path_mapping_api_model_to_openjd``,
``StepDetails.from_boto``, ``RunStepTaskAction``, ``PythonSessionRuntime`` and a
real subprocess -- and assert on the text the task printed. The input/expected
pairs are copied from upstream's
``test/openjd/sessions_v0/test_path_parameter_host_format.py``.

The host is simulated by patching ``openjd.sessions._path_mapping.os_name``, the
one seam upstream patches: both ``PathMappingRule.apply`` and the new
``to_host_path_separators`` read it, so patching it alone yields a consistent
host. A POSIX host renders both readings identically, so without the patch these
assertions would pass whatever the session did.
"""

from __future__ import annotations

import logging
import time
import uuid
from pathlib import Path
from typing import Any, Optional
from unittest.mock import Mock, patch

import openjd.sessions._path_mapping as openjd_path_mapping
import pytest
from openjd.sessions import ActionState

from deadline_worker_agent.api_models import StepDetailsData
from deadline_worker_agent.sessions.actions.run_step_task import RunStepTaskAction
from deadline_worker_agent.sessions.job_entities.job_details import (
    parameters_from_api_response,
    path_mapping_api_model_to_openjd,
)
from deadline_worker_agent.sessions.job_entities.step_details import StepDetails
from deadline_worker_agent.sessions.runtime import SessionRuntimeConfig
from deadline_worker_agent.sessions.runtime.python import PythonSessionRuntime

_ACTION_TIMEOUT = 30.0

_POSIX_TEXT = "/path/a.exr"
_WINDOWS_TEXT = r"\path\a.exr"

# The task prints `OUT=[<value>]`; the brackets make an empty value visible.
_PREFIX = "OUT=["


class _RunTaskOnlySession:
    """Stands in for ``deadline_worker_agent.sessions.Session``, whose
    ``run_task`` is a pass-through to the runtime. Same stand-in as
    test_step_scope_let_end_to_end.py."""

    def __init__(self, runtime: PythonSessionRuntime) -> None:
        self._runtime = runtime

    def run_task(self, **kwargs: Any) -> None:
        self._runtime.run_task(**kwargs)


def _render(
    caplog: pytest.LogCaptureFixture,
    session_root: Path,
    *,
    os_name: str,
    arg: str,
    job_params: Optional[dict[str, Any]] = None,
    task_params: Optional[dict[str, Any]] = None,
    rules: Optional[list[dict[str, str]]] = None,
    expr: bool = False,
) -> str:
    """Run one ``echo OUT=[<arg>]`` task on a simulated host and return what
    it printed between the brackets.

    ``job_params``, ``task_params`` and ``rules`` are in the API shape the
    service serves, so the worker's own conversion runs.
    """
    extensions = ["EXPR"] if expr else []
    payload: StepDetailsData = {
        "jobId": "job-123",
        "stepId": "step-123",
        "schemaVersion": "jobtemplate-2023-09",
        "dependencies": [],
        "extensions": extensions,
        "template": {
            "name": "MyStep",
            "script": {"actions": {"onRun": {"command": "echo", "args": [f"{_PREFIX}{arg}]"]}}},
        },
    }
    details = StepDetails.from_boto(payload)

    runtime = PythonSessionRuntime(
        SessionRuntimeConfig(
            session_id=f"session-{uuid.uuid4().hex}",
            job_parameter_values=parameters_from_api_response(job_params or {}),
            path_mapping_rules=path_mapping_api_model_to_openjd(rules) if rules else None,  # type: ignore[arg-type]
            retain_working_dir=False,
            user=None,
            action_callback=lambda session_id, status: None,
            os_env_vars=None,
            session_root_directory=session_root,
            supported_extensions=tuple(extensions),
        )
    )
    caplog.set_level(logging.INFO)
    try:
        action = RunStepTaskAction(
            id="sessionaction-123",
            details=details,
            task_id="task-456",
            task_parameter_values=parameters_from_api_response(task_params or {}),
        )
        with patch.object(openjd_path_mapping, "os_name", os_name):
            action.start(session=_RunTaskOnlySession(runtime), executor=Mock())  # type: ignore[arg-type]
            deadline = time.monotonic() + _ACTION_TIMEOUT
            while True:
                status = runtime.action_status
                if status is not None and status.state != ActionState.RUNNING:
                    break
                if time.monotonic() > deadline:
                    pytest.fail(f"task did not finish within {_ACTION_TIMEOUT}s ({status})")
                time.sleep(0.05)
        assert status.state == ActionState.SUCCESS, status
    finally:
        runtime.cleanup()

    # Match the stdout line exactly. On a real Windows host the session also
    # logs the resolved command line at INFO, which contains the same text.
    outputs = [m for m in caplog.messages if m.startswith(_PREFIX) and m.endswith("]")]
    assert len(outputs) == 1, caplog.messages
    return outputs[0][len(_PREFIX) : -1]


def _path(value: str) -> dict[str, Any]:
    return {"InputFile": {"path": value}}


class TestPathParameterTakesTheHostFormat:
    @pytest.mark.parametrize(
        "arg,param_kind",
        [
            pytest.param("{{Param.InputFile}}", "job", id="Param"),
            pytest.param("{{Task.Param.InputFile}}", "task", id="Task.Param"),
        ],
    )
    @pytest.mark.parametrize(
        "os_name,expected",
        [
            pytest.param("nt", _WINDOWS_TEXT, id="windows host renders backslashes"),
            pytest.param("posix", _POSIX_TEXT, id="posix host leaves the value alone"),
        ],
    )
    def test_path_parameter(
        self,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
        arg: str,
        param_kind: str,
        os_name: str,
        expected: str,
    ) -> None:
        params = _path(_POSIX_TEXT)
        rendered = _render(
            caplog,
            tmp_path,
            os_name=os_name,
            arg=arg,
            job_params=params if param_kind == "job" else None,
            task_params=params if param_kind == "task" else None,
        )
        assert rendered == expected

    @pytest.mark.parametrize(
        "arg,param_kind",
        [
            pytest.param("{{RawParam.InputFile}}", "job", id="RawParam"),
            pytest.param("{{Task.RawParam.InputFile}}", "task", id="Task.RawParam"),
        ],
    )
    def test_the_raw_form_is_not_reformatted(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture, arg: str, param_kind: str
    ) -> None:
        params = _path(_POSIX_TEXT)
        rendered = _render(
            caplog,
            tmp_path,
            os_name="nt",
            arg=arg,
            job_params=params if param_kind == "job" else None,
            task_params=params if param_kind == "task" else None,
        )
        assert rendered == _POSIX_TEXT


class TestPathParameterFormatAndPathMappingAgree:
    """Rules in the API shape, converted by ``path_mapping_api_model_to_openjd``.
    That conversion emits only POSIX- or WINDOWS-format rules, so upstream's
    URI-format rule case cannot be built through the worker."""

    def test_a_non_matching_rule_still_leaves_a_host_format_value(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        rules = [{"sourcePathFormat": "POSIX", "sourcePath": "/nowhere", "destinationPath": "/x"}]
        rendered = _render(
            caplog,
            tmp_path,
            os_name="nt",
            arg="{{Task.Param.InputFile}}",
            task_params=_path(_POSIX_TEXT),
            rules=rules,
        )
        assert rendered == _WINDOWS_TEXT

    def test_a_matching_rule_is_unchanged(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        rules = [
            {"sourcePathFormat": "POSIX", "sourcePath": "/path", "destinationPath": r"C:\dest"}
        ]
        rendered = _render(
            caplog,
            tmp_path,
            os_name="nt",
            arg="{{Task.Param.InputFile}}",
            task_params=_path(_POSIX_TEXT),
            rules=rules,
        )
        assert rendered == r"C:\dest\a.exr"


class TestPathParameterFormatIsSeparatorsOnly:
    @pytest.mark.parametrize(
        "given,expected",
        [
            pytest.param("/a//b", r"\a\\b", id="duplicate separators survive"),
            pytest.param("/a/", "\\a\\", id="trailing separator survives"),
            pytest.param("relative/path", r"relative\path", id="relative path"),
            pytest.param("", "", id="empty value stays empty"),
            pytest.param(r"C:\already\windows", r"C:\already\windows", id="already windows"),
            pytest.param("C://Users/foo", "C://Users/foo", id="one-char scheme is a URI"),
            pytest.param("C:/Users/foo", r"C:\Users\foo", id="drive letter is not a URI"),
            pytest.param("x://y/z", "x://y/z", id="one-char scheme, non-drive letter"),
            pytest.param("s3://bucket/key/with/slashes", "s3://bucket/key/with/slashes", id="uri"),
        ],
    )
    def test_windows_host_replaces_separators_only(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture, given: str, expected: str
    ) -> None:
        rendered = _render(
            caplog,
            tmp_path,
            os_name="nt",
            arg="{{Task.Param.InputFile}}",
            task_params=_path(given),
        )
        assert rendered == expected


class TestListPathParameterTakesTheHostFormat:
    """LIST[PATH] is EXPR-only, so these run with the EXPR extension."""

    @pytest.mark.parametrize(
        "os_name,expected",
        [
            pytest.param("nt", r"\path\a.exr|\other\b.exr", id="windows host"),
            pytest.param("posix", "/path/a.exr|/other/b.exr", id="posix host"),
        ],
    )
    def test_every_element(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture, os_name: str, expected: str
    ) -> None:
        rendered = _render(
            caplog,
            tmp_path,
            os_name=os_name,
            arg="{{ Param.Inputs[0] }}|{{ Param.Inputs[1] }}",
            job_params={"Inputs": {"pathList": ["/path/a.exr", "/other/b.exr"]}},
            expr=True,
        )
        assert rendered == expected


class TestOtherParameterTypesAreUntouched:
    @pytest.mark.parametrize("arg", ["{{Param.Text}}", "{{Task.Param.Text}}"])
    def test_a_string_parameter_keeps_its_slashes_on_a_windows_host(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture, arg: str
    ) -> None:
        params = {"Text": {"string": _POSIX_TEXT}}
        rendered = _render(
            caplog,
            tmp_path,
            os_name="nt",
            arg=arg,
            job_params=params if arg.startswith("{{Param") else None,
            task_params=params if arg.startswith("{{Task") else None,
        )
        assert rendered == _POSIX_TEXT
