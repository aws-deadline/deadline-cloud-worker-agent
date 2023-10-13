# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from typing import Any
import pytest

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
            id="invalid template - not dict",
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
    """Test that validate_entity_data() raises a ValueError when invalid input data is provided."""
    with pytest.raises(ValueError):
        EnvironmentDetails.validate_entity_data(entity_data=data)
