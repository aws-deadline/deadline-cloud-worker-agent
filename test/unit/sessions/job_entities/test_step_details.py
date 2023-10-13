# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from typing import Any
import pytest

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
            id="invalid dependencies - not list",
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
    """Test that validate_entity_data() raises a ValueError when invalid input data is provided."""
    with pytest.raises(ValueError):
        StepDetails.validate_entity_data(entity_data=data)
