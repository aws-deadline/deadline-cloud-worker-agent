# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from typing import Any, cast
import pytest

from deadline_worker_agent.api_models import StepDetailsData
from deadline_worker_agent.sessions.job_entities.step_details import StepDetails


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(
            {
                "jobId": "job-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "template": {},
                "stepId": "step-0000",
            },
            id="only required fields",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "template": {},
                "stepId": "step-0000",
                "dependencies": [],
            },
            id="all fields with empty dependencies list",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "template": {},
                "stepId": "step-0000",
                "dependencies": ["step-1", "step-2", "step-3"],
            },
            id="all fields",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "template": {},
                "stepId": "step-0000",
                "extensions": ["WRAP_ACTIONS", "EXPR"],
            },
            id="with extensions list",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "template": {},
                "stepId": "step-0000",
                "extensions": [],
            },
            id="with empty extensions list",
        ),
    ],
)
def test_input_validation_success(data: dict[str, Any]) -> None:
    """Test that validate_entity_data() can successfully handle valid input data."""
    StepDetails.validate_entity_data(entity_data=data)


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(
            {
                "schemaVersion": "jobtemplate-0000-00",
                "template": {},
                "stepId": "step-0000",
            },
            id="missing jobId",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "template": {},
                "stepId": "step-0000",
            },
            id="missing schemaVersion",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "stepId": "step-0000",
            },
            id="missing template",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "template": {},
            },
            id="missing stepId",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "template": {},
                "stepId": "step-0000",
                "dependencies": "",
            },
            id="nonvalid dependencies - not list",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "template": {},
                "stepId": "step-0000",
                "unkown": "",
            },
            id="unknown field",
        ),
    ],
)
def test_input_validation_failure(data: dict[str, Any]) -> None:
    """Test that validate_entity_data() raises a ValueError when nonvalid input data is provided."""
    with pytest.raises(ValueError):
        StepDetails.validate_entity_data(entity_data=data)


class TestFromBotoWrapActions:
    """Tests that from_boto successfully parses step templates with wrap-action environments."""

    def test_from_boto_parses_step_with_wrap_step_environment(self) -> None:
        """from_boto should parse a step template (new API shape) whose stepEnvironments
        contains an environment declaring wrap actions."""
        step_details_data = {
            "jobId": "job-0000",
            "schemaVersion": "jobtemplate-2023-09",
            "stepId": "step-0000",
            "dependencies": [],
            "template": {
                "name": "TestStep",
                "script": {
                    "actions": {
                        "onRun": {"command": "/bin/echo", "args": ["run"]},
                    }
                },
                "stepEnvironments": [
                    {
                        "name": "WrapEnv",
                        "script": {
                            "actions": {
                                "onWrapEnvEnter": {"command": "/bin/echo", "args": ["enter"]},
                                "onWrapTaskRun": {"command": "/bin/echo", "args": ["wrap-run"]},
                                "onWrapEnvExit": {"command": "/bin/echo", "args": ["exit"]},
                            }
                        },
                    }
                ],
            },
            "extensions": ["EXPR", "WRAP_ACTIONS"],
        }

        result = StepDetails.from_boto(cast(StepDetailsData, step_details_data))

        assert result.step_template.name == "TestStep"
        assert result.step_id == "step-0000"
        step_envs = result.step_template.stepEnvironments
        assert step_envs is not None
        assert len(step_envs) == 1
        wrap_env = step_envs[0]
        assert wrap_env.name == "WrapEnv"
        assert wrap_env.script is not None
        actions = wrap_env.script.actions
        assert actions is not None
        assert actions.onWrapEnvEnter is not None
        assert actions.onWrapEnvEnter.command == "/bin/echo"
        assert actions.onWrapEnvEnter.args == ["enter"]
        assert actions.onWrapTaskRun is not None
        assert actions.onWrapTaskRun.command == "/bin/echo"
        assert actions.onWrapTaskRun.args == ["wrap-run"]
        assert actions.onWrapEnvExit is not None
        assert actions.onWrapEnvExit.command == "/bin/echo"
        assert actions.onWrapEnvExit.args == ["exit"]


class TestFromBotoExtensions:
    """Tests that from_boto correctly resolves extensions from the entity data."""

    def test_absent_extensions_falls_back(self) -> None:
        """When 'extensions' is absent, from_boto defaults to empty extensions
        and still parses a basic template successfully."""
        step_details_data = {
            "jobId": "job-0000",
            "schemaVersion": "jobtemplate-2023-09",
            "stepId": "step-0000",
            "dependencies": [],
            "template": {
                "name": "TestStep",
                "script": {
                    "actions": {
                        "onRun": {"command": "/bin/echo", "args": ["hello"]},
                    }
                },
            },
        }

        result = StepDetails.from_boto(cast(StepDetailsData, step_details_data))

        assert result.step_template.name == "TestStep"


class TestResolvedSymbolTable:
    """Tests for the resolvedSymbolTable field on StepDetails."""

    def test_from_boto_extracts_resolved_symbol_table_when_present(self) -> None:
        """from_boto sets resolved_symbol_table_json when the field is present."""
        symtab_json = '[{"name":"Job.Name","type":"string","value":"MyJob"}]'
        step_details_data = {
            "jobId": "job-0000",
            "schemaVersion": "jobtemplate-2023-09",
            "stepId": "step-0000",
            "dependencies": [],
            "template": {
                "name": "TestStep",
                "script": {
                    "actions": {
                        "onRun": {"command": "/bin/echo", "args": ["hello"]},
                    }
                },
            },
            "resolvedSymbolTable": symtab_json,
        }

        result = StepDetails.from_boto(cast(StepDetailsData, step_details_data))

        assert result.resolved_symbol_table_json == symtab_json
