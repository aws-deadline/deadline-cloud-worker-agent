# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from typing import Any, cast
import pytest

from deadline_worker_agent.api_models import EnvironmentDetailsData
from deadline_worker_agent.sessions.job_entities.environment_details import EnvironmentDetails


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(
            {
                "jobId": "job-0000",
                "template": {},
                "environmentId": "env-0000",
                "schemaVersion": "jobtemplate-0000-00",
            },
            id="only required fields",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "template": {},
                "environmentId": "env-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "extensions": ["WRAP_ACTIONS", "EXPR"],
            },
            id="with extensions list",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "template": {},
                "environmentId": "env-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "extensions": [],
            },
            id="with empty extensions list",
        ),
    ],
)
def test_input_validation_success(data: dict[str, Any]) -> None:
    """Test that validate_entity_data() can successfully handle valid input data."""
    EnvironmentDetails.validate_entity_data(entity_data=data)


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(
            {
                "template": {},
                "environmentId": "env-0000",
                "schemaVersion": "jobtemplate-0000-00",
            },
            id="missing jobId",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "environmentId": "env-0000",
                "schemaVersion": "jobtemplate-0000-00",
            },
            id="missing template",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "template": {},
                "schemaVersion": "jobtemplate-0000-00",
            },
            id="missing environmentId",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "template": {},
                "environmentId": "env-0000",
            },
            id="missing schemaVersion",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "template": "",
                "environmentId": "env-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "unknown": "unknown",
            },
            id="nonvalid template - not dict",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "template": {},
                "environmentId": "env-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "unknown": "unknown",
            },
            id="unknown field",
        ),
    ],
)
def test_input_validation_failure(data: dict[str, Any]) -> None:
    """Test that validate_entity_data() raises a ValueError when nonvalid input data is provided."""
    with pytest.raises(ValueError):
        EnvironmentDetails.validate_entity_data(entity_data=data)


class TestFromBotoWrapActions:
    """Tests that from_boto successfully parses environment templates using WRAP_ACTIONS."""

    def test_from_boto_parses_wrap_actions_environment(self) -> None:
        """from_boto should parse an environment template that declares wrap actions
        (onWrapEnvEnter, onWrapTaskRun, onWrapEnvExit) without raising."""
        environment_details_data = {
            "jobId": "job-0000",
            "environmentId": "env-0000",
            "schemaVersion": "jobtemplate-2023-09",
            "template": {
                "name": "WrapEnv",
                "script": {
                    "actions": {
                        "onWrapEnvEnter": {"command": "/bin/echo", "args": ["enter"]},
                        "onWrapTaskRun": {"command": "/bin/echo", "args": ["run"]},
                        "onWrapEnvExit": {"command": "/bin/echo", "args": ["exit"]},
                    }
                },
            },
            "extensions": ["EXPR", "WRAP_ACTIONS"],
        }

        result = EnvironmentDetails.from_boto(
            cast(EnvironmentDetailsData, environment_details_data)
        )

        assert result.environment.name == "WrapEnv"
        assert result.environment.script is not None
        actions = result.environment.script.actions
        assert actions is not None
        assert actions.onWrapEnvEnter is not None
        assert actions.onWrapEnvEnter.command == "/bin/echo"
        assert actions.onWrapEnvEnter.args == ["enter"]
        assert actions.onWrapTaskRun is not None
        assert actions.onWrapTaskRun.command == "/bin/echo"
        assert actions.onWrapTaskRun.args == ["run"]
        assert actions.onWrapEnvExit is not None
        assert actions.onWrapEnvExit.command == "/bin/echo"
        assert actions.onWrapEnvExit.args == ["exit"]
