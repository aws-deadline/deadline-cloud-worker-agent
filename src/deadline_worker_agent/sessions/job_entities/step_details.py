# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

from openjd.model import parse_model, TemplateSpecificationVersion, UnsupportedSchema
from openjd.model.v2023_09 import StepTemplate as StepTemplate_2023_09

from ...api_models import StepDetailsData
from .job_entity_type import JobEntityType
from .validation import Field, validate_object

if TYPE_CHECKING:
    # Replace with `StepTemplate` from openjd-model once that lib adds one.
    StepTemplate = StepTemplate_2023_09
else:
    StepTemplate = Any


@dataclass
class StepDetails:
    """Details required to perform work for a step"""

    ENTITY_TYPE = JobEntityType.STEP_DETAILS.value
    """The JobEntityType handled by this class"""

    step_template: StepTemplate
    """The step's Open Job Description step template.
    """

    step_id: str
    """The AWS Deadline Cloud resource ID for the Step.
    """

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
            If the environment's Open Job Description schema version not unsupported
        """

        schema_version = TemplateSpecificationVersion(step_details_data["schemaVersion"])

        if schema_version == TemplateSpecificationVersion.JOBTEMPLATE_v2023_09:
            # Jan 23, 2024: Forwards compatibility. The 'template' field is changing from a StepScript to
            # a StepTemplate. Remove the StepScript case after the transition is complete.
            details_data = step_details_data["template"]
            if "name" in details_data:
                # New API shape -- 'template' contains a StepTemplate
                step_template = parse_model(model=StepTemplate_2023_09, obj=details_data)
            else:
                # Old API shape -- 'template' contains a StepScript.
                # If we're GA and you're reading this, then delete this code path.
                step_template = parse_model(
                    model=StepTemplate_2023_09, obj={"name": "Placeholder", "script": details_data}
                )
        else:
            raise UnsupportedSchema(schema_version.value)

        return StepDetails(
            step_template=step_template,
            step_id=step_details_data["stepId"],
            dependencies=step_details_data["dependencies"],
        )

    @classmethod
    def validate_entity_data(cls, entity_data: dict[str, Any]) -> StepDetailsData:
        """Performs input validation on a response element recceived from boto3's call to
        the BatchGetJobEntity AWS Deadline Cloud API.

        Parameters
        ----------
        entity_data : dict[str, Any]
            The stepDetails entity data received from BatchGetJobEntity

        Returns
        -------
        deadline_worker_agent.api_models.StepDetailsData:
            The input cast as a StepDetailsData after input validation

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
