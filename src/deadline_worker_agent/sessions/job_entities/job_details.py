# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import PurePath, PurePosixPath, PureWindowsPath
from typing import Any, cast
import os

from openjobio.model import SchemaVersion
from openjobio.sessions import (
    Parameter,
    ParameterType,
    PathMappingOS,
    PosixSessionUser,
)
from openjobio.sessions import PathMappingRule as OJIOPathMappingRule

from ...api_models import (
    FloatParameter,
    IntParameter,
    JobDetailsData,
    JobAttachmentQueueSettings as JobAttachmentSettingsBoto,
    JobsRunAs as JobsRunAsModel,
    PathMappingRule,
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


def path_mapping_api_model_to_ojio(
    path_mapping_rules: list[PathMappingRule],
) -> list[OJIOPathMappingRule]:
    """Converts path_mapping_rules from a BatchGetJobEntity response
    to the format expected by OJIO. effectively camelCase to snake_case"""
    rules: list[OJIOPathMappingRule] = []
    for api_rule in path_mapping_rules:
        api_source_path_format = (
            # delete sourceOs once removed from api response
            api_rule["sourcePathFormat"]
            if "sourcePathFormat" in api_rule
            else api_rule["sourceOs"]
        )
        source_path_format: PathMappingOS = (
            PathMappingOS.WINDOWS
            if api_source_path_format.lower() == "windows"
            else PathMappingOS.POSIX
        )
        source_path: PurePath = (
            PureWindowsPath(api_rule["sourcePath"])
            if source_path_format == PathMappingOS.WINDOWS
            else PurePosixPath(api_rule["sourcePath"])
        )
        destination_path: PurePath = PurePath(api_rule["destinationPath"])
        rules.append(
            OJIOPathMappingRule(
                source_os=source_path_format,
                source_path=source_path,
                destination_path=destination_path,
            )
        )
    return rules


def jobs_runs_as_api_model_to_worker_agent(
    jobs_run_as_data: JobsRunAsModel | None,
) -> JobsRunAs | None:
    """Converts the 'JobsRunAs' api model to the 'JobsRunAs' dataclass
    expected by the Worker Agent.
    """
    jobs_run_as: JobsRunAs | None = None
    if not jobs_run_as_data:
        return None

    if os.name == "posix":
        jobs_run_as_posix = jobs_run_as_data.get("posix", {})
        user = jobs_run_as_posix.get("user", "")
        group = jobs_run_as_posix.get("group", "")
        if not (user and group):
            return None

        jobs_run_as = JobsRunAs(
            posix=PosixSessionUser(user=user, group=group),
        )
    else:
        # TODO: windows support
        raise NotImplementedError(f"{os.name} is not supported")

    return jobs_run_as


@dataclass(frozen=True)
class JobAttachmentSettings:
    """Job attachment settings for a queue"""

    s3_bucket_name: str
    """The name of the S3 bucket where job attachments are transferred to/from"""

    root_prefix: str
    """The top-level prefix that all other prefixes are relative to"""

    @classmethod
    def from_boto(cls, data: JobAttachmentSettingsBoto) -> JobAttachmentSettings:
        return JobAttachmentSettings(
            s3_bucket_name=data["s3BucketName"],
            root_prefix=data["rootPrefix"],
        )


@dataclass(frozen=True)
class JobsRunAs:
    posix: PosixSessionUser
    # TODO: windows support


@dataclass(frozen=True)
class JobDetails:
    """A job's details required by the Worker"""

    ENTITY_TYPE = JobEntityType.JOB_DETAILS.value
    """The JobEntityType handled by this class"""

    log_group_name: str
    """The name of the log group for the session"""

    schema_version: SchemaVersion
    """The OpenJobIO schema version"""

    job_attachment_settings: JobAttachmentSettings | None = None
    """The job attachment settings of the job's queue"""

    parameters: list[Parameter] = field(default_factory=list)
    """The job's parameters"""

    jobs_run_as: JobsRunAs | None = None
    """The user associated with the job's Amazon Deadline Cloud queue"""

    path_mapping_rules: list[OJIOPathMappingRule] = field(default_factory=list)
    """The path mapping rules for the job"""

    queue_role_arn: str | None = None
    """The ARN of the Job's Queue Role, if it has one."""

    @classmethod
    def from_boto(cls, job_details_data: JobDetailsData) -> JobDetails:
        """Parses the data returned in the BatchGetJobEntity response

        Parameters
        ----------
        job_details_data : JobDetailsData
            The entity data returned in the BatchGetJobEntity response

        Returns
        -------
        deadline_worker_agent.api_models.JobDetails
            The parsed JobDetails instance
        """

        job_parameters_data: dict = job_details_data.get("parameters", {})
        job_parameters = parameters_data_to_list(job_parameters_data)
        path_mapping_rules: list[OJIOPathMappingRule] = []
        path_mapping_rules_data = job_details_data.get("pathMappingRules", None)
        if path_mapping_rules_data:
            path_mapping_rules = path_mapping_api_model_to_ojio(path_mapping_rules_data)

        job_attachment_settings: JobAttachmentSettings | None = None
        if job_attachment_settings_boto := job_details_data.get("jobAttachmentSettings", None):
            job_attachment_settings = JobAttachmentSettings.from_boto(job_attachment_settings_boto)

        jobs_run_as_data = job_details_data.get("jobsRunAs", None)
        jobs_run_as: JobsRunAs | None = jobs_runs_as_api_model_to_worker_agent(jobs_run_as_data)

        # Note: Record the empty string as a None as well.
        queue_role_arn: str | None = (
            job_details_data.get("queueSessionRoleArn", None)
            or job_details_data.get("queueRoleArn", None)
            or None
        )

        return JobDetails(
            parameters=job_parameters,
            schema_version=SchemaVersion(job_details_data["schemaVersion"]),
            log_group_name=job_details_data["logGroupName"],
            jobs_run_as=jobs_run_as,
            path_mapping_rules=path_mapping_rules,
            job_attachment_settings=job_attachment_settings,
            queue_role_arn=queue_role_arn,
        )

    @classmethod
    def validate_entity_data(cls, entity_data: dict[str, Any]) -> JobDetailsData:
        """Performs input validation on a response element recceived from boto3's call to
        the BatchGetJobEntity Amazon Deadline Cloud API.

        Parameters
        ----------
        entity_data : dict[str, Any]
            The jobDetails entity data received from BatchGetJobEntity

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
                Field(key="logGroupName", expected_type=str, required=True),
                Field(key="schemaVersion", expected_type=str, required=True),
                Field(key="osUser", expected_type=str, required=False),
                Field(
                    key="parameters",
                    expected_type=dict,
                    required=False,
                ),
                Field(
                    key="pathMappingRules",
                    expected_type=list,
                    required=False,
                ),
                Field(
                    key="jobsRunAs",
                    expected_type=dict,
                    required=False,
                    fields=(
                        Field(
                            key="posix",
                            expected_type=dict,
                            required=False,
                            fields=(
                                Field(key="user", expected_type=str, required=True),
                                Field(key="group", expected_type=str, required=True),
                            ),
                        ),
                    ),
                ),
                Field(
                    key="jobAttachmentSettings",
                    expected_type=dict,
                    required=False,
                    fields=(
                        Field(key="s3BucketName", expected_type=str, required=True),
                        Field(key="rootPrefix", expected_type=str, required=True),
                    ),
                ),
                # TODO - Remove queueSessionRoleArn
                Field(key="queueSessionRoleArn", expected_type=str, required=False),
                Field(key="queueRoleArn", expected_type=str, required=False),
            ),
        )

        # Validating job parameters reqiures special validation since keys are dynamic
        if job_parameters := entity_data.get("parameters", None):
            assert isinstance(job_parameters, dict)
            cls._validate_job_parameters(job_parameters)

        # Validating path mapping rules requires special validation
        if path_mapping_rules := entity_data.get("pathMappingRules", None):
            assert isinstance(path_mapping_rules, list)
            for i, path_mapping_rule in enumerate(path_mapping_rules):
                if not isinstance(path_mapping_rule, dict):
                    raise ValueError(
                        f'Expected elements of "pathMappingRules" to be a dict but got {type(path_mapping_rule)} at element {i}'
                    )
                validate_object(
                    data=path_mapping_rule,
                    fields=(
                        # TODO: remove sourceOs and make sourcePathFormat required
                        Field(key="sourceOs", expected_type=str, required=False),
                        Field(key="sourcePathFormat", expected_type=str, required=False),
                        Field(key="sourcePath", expected_type=str, required=True),
                        Field(key="destinationPath", expected_type=str, required=True),
                    ),
                )

        return cast(JobDetailsData, entity_data)

    @classmethod
    def _validate_job_parameters(cls, job_parameters: dict[str, Any]) -> None:
        for key, value in job_parameters.items():
            if not isinstance(value, dict):
                raise ValueError(f'Expected parameters["{key}"] to be a dict but got {type(value)}')
            value = cast(dict[str, Any], value)
            value_keys = list(value.keys())
            if len(value_keys) != 1:
                keys_str = ", ".join(f'"{key}"' for key in value_keys)
                raise ValueError(
                    f'Expected parameters["{key}"] to have a single key, but got {keys_str}'
                )
            type_key = list(value.keys())[0]
            if type_key not in ("string", "path", "int", "float"):
                raise ValueError(
                    f'Expected parameters["{key}"] to have a single key with one of "string", "path", "int", "float" but got "{type_key}"'
                )
            param_value = list(value.values())[0]
            if not isinstance(param_value, str):
                raise ValueError(
                    f'Expected parameters["{key}"] to have a single a single key whose value is a string but the value was {type(param_value)}'
                )
