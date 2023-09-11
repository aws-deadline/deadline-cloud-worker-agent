# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from dataclasses import dataclass
from typing import Any, cast

from openjd.sessions import Parameter, ParameterType
from deadline.job_attachments.models import JobAttachmentsFileSystem

from ...api_models import (
    FloatParameter,
    IntParameter,
    JobAttachmentDetailsData,
    PathParameter,
    StringParameter,
)
from .job_entity_type import JobEntityType
from .validation import Field, validate_object


def parameters_data_to_list(
    params: dict[str, StringParameter | PathParameter | IntParameter | FloatParameter | str]
) -> list[Parameter]:
    result = list[Parameter]()
    for name, value in params.items():
        # TODO: Change to the correct type once typing information is available
        # in the task_run action details.
        if isinstance(value, str):
            # old style for the API - TODO remove this once the assign API is updated
            result.append(Parameter(ParameterType.STRING, name, value))
        elif "string" in value:
            value = cast(StringParameter, value)
            result.append(Parameter(ParameterType.STRING, name, value["string"]))
        elif "int" in value:
            value = cast(IntParameter, value)
            result.append(Parameter(ParameterType.INT, name, value["int"]))
        elif "float" in value:
            value = cast(FloatParameter, value)
            result.append(Parameter(ParameterType.FLOAT, name, value["float"]))
        elif "path" in value:
            value = cast(PathParameter, value)
            result.append(Parameter(ParameterType.PATH, name, value["path"]))
        else:
            # TODO - PATH parameter types
            raise ValueError(f"Parameter {name} -- unknown form in API response: {str(value)}")
    return result


@dataclass(frozen=True)
class JobAttachmentManifestProperties:
    """Information used to facilitate the transfer of input/output job attachments and mapping of
    their paths"""

    root_path: str
    """The input root path to be mapped"""

    root_path_format: str
    """The operating system family (posix/windows) associated with the asset's root_path"""

    file_system_location_name: str | None = None
    """The name of the file system location"""

    input_manifest_path: str | None = None
    """A (partial) key path of an S3 object that points to a file manifest.
    It is relative to the location configured in the Queue."""

    input_manifest_hash: str | None = None
    """The hash of the manifest, for data provenance"""

    output_relative_directories: list[str] | None = None
    """Directories whose output must by synchronized after any job tasks are complete"""


@dataclass(frozen=True)
class JobAttachmentDetails:
    """Details required to transfer input/output job attachments and map their paths"""

    ENTITY_TYPE = JobEntityType.JOB_ATTACHMENT_DETAILS.value
    """The JobEntityType handled by this class"""

    manifests: list[JobAttachmentManifestProperties]
    """The manifests' configuration for the job.

    Each item in the list specifies its path, required input assets, and output assets.
    """

    job_attachments_file_system: JobAttachmentsFileSystem = JobAttachmentsFileSystem.COPIED
    """Method to use when loading assets required for a job"""

    @classmethod
    def from_boto(
        cls, job_attachments_details_data: JobAttachmentDetailsData
    ) -> JobAttachmentDetails:
        """Converts an jobAttachmentDetails entity received from BatchGetJobEntity API response
        into an JobAttachmentDetails instance

        Parameters
        ----------
        job_attachments_details_data : JobAttachmentDetailsData
            Job attachment details JSON object as received from BatchGetJobEntity.

        Returns
        -------
        JobAttachmentDetails:
            A converted JobAttachmentDetails instance.

        Raises
        ------
        RuntimeError:
            If the environment's Open Job Description schema version not unsupported
        """

        return JobAttachmentDetails(
            manifests=[
                JobAttachmentManifestProperties(
                    output_relative_directories=manifest_properties.get(
                        "outputRelativeDirectories", []
                    ),
                    file_system_location_name=manifest_properties.get(
                        "fileSystemLocationName", None
                    ),
                    input_manifest_path=manifest_properties.get("inputManifestPath", ""),
                    input_manifest_hash=manifest_properties.get("inputManifestHash", ""),
                    root_path=manifest_properties["rootPath"],
                    root_path_format=manifest_properties["rootPathFormat"],
                )
                for manifest_properties in job_attachments_details_data["attachments"]["manifests"]
            ],
            job_attachments_file_system=JobAttachmentsFileSystem(
                job_attachments_details_data["attachments"].get(
                    "fileSystem", JobAttachmentsFileSystem.COPIED
                )
            ),
        )

    @classmethod
    def validate_entity_data(cls, entity_data: dict[str, Any]) -> JobAttachmentDetailsData:
        """Performs input validation on a response element recceived from boto3's call to
        the BatchGetJobEntity Amazon Deadline Cloud API.

        Parameters
        ----------
        entity_data : dict[str, Any]
            The element "data" field to validate and cast into a JobDetailsData instance

        Returns
        -------
        deadline_worker_agent.api_models.JobDetailsData:
            The input cast as a JobDetailsData after input validation

        Raises
        ------
        ValueError:
            Validation failure
        """

        validate_object(
            data=entity_data,
            fields=(
                Field(key="jobId", expected_type=str, required=True),
                Field(key="stepId", expected_type=str, required=False),
                Field(
                    key="attachments",
                    expected_type=dict,
                    required=True,
                    fields=(
                        Field(
                            key="manifests",
                            expected_type=list,
                            required=True,
                            fields=(
                                Field(
                                    key="fileSystemLocationName",
                                    expected_type=str,
                                    required=False,
                                ),
                                Field(key="rootPath", expected_type=str, required=True),
                                Field(key="rootPathFormat", expected_type=str, required=True),
                                Field(
                                    key="outputRelativeDirectories",
                                    expected_type=list,
                                    required=False,
                                ),
                                Field(key="inputManifestPath", expected_type=str, required=False),
                                Field(key="inputManifestHash", expected_type=str, required=False),
                            ),
                        ),
                        Field(key="fileSystem", expected_type=str, required=False),
                    ),
                ),
            ),
        )

        return cast(JobAttachmentDetailsData, entity_data)
