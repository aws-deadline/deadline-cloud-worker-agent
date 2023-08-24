# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, cast

from openjobio.model import parse_model, SchemaVersion, UnsupportedSchema
from openjobio.model.v2022_09_01 import StepScript as StepScript_2022_09_01
from openjobio.sessions import StepScriptModel

from ...api_models import StepDetailsData
from .job_entity_type import JobEntityType
from .validation import Field, validate_object


@dataclass
class StepDetails:
    """Details required to perform work for a step"""

    ENTITY_TYPE = JobEntityType.STEP_DETAILS.value
    """The JobEntityType handled by this class"""

    script: StepScriptModel
    """The step's OpenJobIO script"""

    dependencies: list[str] = field(default_factory=list)
    """The dependencies (a list of IDs) that the step depends on"""

    @classmethod
    def from_boto(cls, step_details_data: StepDetailsData) -> StepDetails:
        """Converts an stepDetails entity received from BatchGetJobEntity API response into a
        StepDetails instance

        Parameters
        ----------
        step_details_data : StepDetailsData
            Step details JSON object as received from BatchGetJobEntity.

        Returns
        -------
        StepDetails:
            A converted StepDetails instance.

        Raises
        ------
        RuntimeError:
            If the environment's OpenJobIO schema version not unsupported
        """

        schema_version = SchemaVersion(step_details_data["schemaVersion"])

        if schema_version == SchemaVersion.v2022_09_01:
            step_script = parse_model(
                model=StepScript_2022_09_01, obj=step_details_data["template"]
            )
        else:
            raise UnsupportedSchema(schema_version.value)

        return StepDetails(
            script=step_script,
            dependencies=step_details_data["dependencies"],
        )

    @classmethod
    def validate_entity_data(cls, entity_data: dict[str, Any]) -> StepDetailsData:
        """Performs input validation on a response element recceived from boto3's call to
        the BatchGetJobEntity Amazon Deadline Cloud API.

        Parameters
        ----------
        entity_data : dict[str, Any]
            The environmentDetails entity data received from BatchGetJobEntity

        Returns
        -------
        deadline_worker_agent.api_models.StepDetailsData:
            The input cast as a JobDetailsData after input validation

        Raises
        ------
        ValueError:
            Validation failure
        """
        if not isinstance(entity_data, dict):
            raise ValueError(f"Expected a JSON object but got {type(entity_data)}")
        validate_object(
            data=entity_data,
            fields=(
                Field(key="jobId", expected_type=str, required=True),
                Field(key="schemaVersion", expected_type=str, required=True),
                Field(key="template", expected_type=dict, required=True),
                Field(key="stepId", expected_type=str, required=True),
                Field(key="dependencies", expected_type=list, required=False),
            ),
        )
        if dependencies := entity_data.get("dependencies"):
            for dependency in dependencies:
                if not isinstance(dependency, str):
                    raise ValueError(
                        f"Expected dependencies to be strings but got {type(dependency)}"
                    )

        return cast(StepDetailsData, entity_data)
