# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from dataclasses import dataclass
from typing import Any, cast

from openjd.model import parse_model, TemplateSpecificationVersion, UnsupportedSchema
from openjd.model.v2023_09 import Environment as Environment_2023_09
from openjd.sessions import EnvironmentModel

from ...api_models import EnvironmentDetailsData
from .job_entity_type import JobEntityType
from .validation import Field, validate_object


@dataclass
class EnvironmentDetails:
    """Details required to activate and deactivate environments"""

    ENTITY_TYPE = JobEntityType.ENVIRONMENT_DETAILS.value
    """The JobEntityType handled by this class"""

    environment: EnvironmentModel
    """The environment"""

    @classmethod
    def from_boto(cls, environment_details_data: EnvironmentDetailsData) -> EnvironmentDetails:
        """Converts an environmentDetails entity received from BatchGetJobEntity API response into
        an EnvironmentDetails instance

        Parameters
        ----------
        environment_details_data : EnvironmentDetailsData
            Environment details JSON object as received from BatchGetJobEntity.

        Returns
        -------
        EnvironmentDetails:
            A converted EnvironmentDetails instance.

        Raises
        ------
        RuntimeError:
            If the environment's Open Job Description schema version not unsupported
        """

        schema_version = TemplateSpecificationVersion(environment_details_data["schemaVersion"])

        if schema_version in (
            TemplateSpecificationVersion.JOBTEMPLATE_v2023_09,
            TemplateSpecificationVersion.ENVIRONMENT_v2023_09,
        ):
            environment = parse_model(
                model=Environment_2023_09, obj=environment_details_data["template"]
            )
        else:
            raise UnsupportedSchema(schema_version.value)

        return EnvironmentDetails(environment=environment)

    @classmethod
    def validate_entity_data(cls, entity_data: dict[str, Any]) -> EnvironmentDetailsData:
        """
        Performs input validation on an entity response JSON object received from boto3's call to
        the BatchGetJobEntity AWS Deadline Cloud API.

        Parameters
        ----------
        entity_data : dict[str, Any]
            The environmentDetails entity data received from BatchGetJobEntity

        Returns
        -------
        deadline_worker_agent.api_models.EnvironmentDetailsData:
            A validated EnvironmentDetailsData instance

        Raises
        ------
        ValueError:
            Validation failure
        """

        validate_object(
            data=entity_data,
            fields=(
                Field(key="template", expected_type=dict, required=True),
                Field(key="environmentId", expected_type=str, required=True),
                Field(key="jobId", expected_type=str, required=True),
                Field(key="schemaVersion", expected_type=str, required=True),
            ),
        )

        return cast(EnvironmentDetailsData, entity_data)
