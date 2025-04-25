# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import PurePath, PurePosixPath, PureWindowsPath
from typing import Any, cast
import os

from openjd.model import (
    JobParameterValues,
    ParameterValue,
    ParameterValueType,
    SpecificationRevision,
    TemplateSpecificationVersion,
    UnsupportedSchema,
)
from openjd.sessions import (
    PathFormat,
    PosixSessionUser,
    WindowsSessionUser,
)
from openjd.sessions import PathMappingRule as OPENJDPathMappingRule

from ...api_models import (
    FloatParameter,
    IntParameter,
    JobDetailsData,
    JobAttachmentQueueSettings as JobAttachmentSettingsBoto,
    JobRunAsUser as JobRunAsUserModel,
    PathMappingRule,
    PathParameter,
    StringParameter,
    ChunkIntParameter,
)
from ...config import JobsRunAsUserOverride
from .job_entity_type import JobEntityType
from .validation import Field, validate_object


def parameters_from_api_response(
    params: dict[
        str,
        StringParameter | PathParameter | IntParameter | FloatParameter | ChunkIntParameter | str,
    ],
) -> dict[str, ParameterValue]:
    result = dict[str, ParameterValue]()
    for name, value in params.items():
        if "string" in value:
            value = cast(StringParameter, value)
            param_value = ParameterValue(type=ParameterValueType.STRING, value=value["string"])
        elif "int" in value:
            value = cast(IntParameter, value)
            param_value = ParameterValue(type=ParameterValueType.INT, value=value["int"])
        elif "float" in value:
            value = cast(FloatParameter, value)
            param_value = ParameterValue(type=ParameterValueType.FLOAT, value=value["float"])
        elif "path" in value:
            value = cast(PathParameter, value)
            param_value = ParameterValue(type=ParameterValueType.PATH, value=value["path"])
        elif "chunkInt" in value:
            value = cast(ChunkIntParameter, value)
            param_value = ParameterValue(type=ParameterValueType.CHUNK_INT, value=value["chunkInt"])
        else:
            raise ValueError(f"Parameter {name} -- unknown form in API response: {str(value)}")
        result[name] = param_value
    return result


def path_mapping_api_model_to_openjd(
    path_mapping_rules: list[PathMappingRule],
) -> list[OPENJDPathMappingRule]:
    """Converts path_mapping_rules from a BatchGetJobEntity response
    to the format expected by Open Job Description. effectively camelCase to snake_case"""
    rules: list[OPENJDPathMappingRule] = []
    for api_rule in path_mapping_rules:
        api_source_path_format = api_rule["sourcePathFormat"]
        source_path_format: PathFormat = (
            PathFormat.WINDOWS if api_source_path_format.lower() == "windows" else PathFormat.POSIX
        )
        source_path: PurePath = (
            PureWindowsPath(api_rule["sourcePath"])
            if source_path_format == PathFormat.WINDOWS
            else PurePosixPath(api_rule["sourcePath"])
        )
        destination_path: PurePath = PurePath(api_rule["destinationPath"])
        rules.append(
            OPENJDPathMappingRule(
                source_path_format=source_path_format,
                source_path=source_path,
                destination_path=destination_path,
            )
        )
    return rules


def job_run_as_user_api_model_to_worker_agent(
    job_run_as_user_data: JobRunAsUserModel,
) -> JobRunAsUser | None:
    """Converts the 'JobRunAsUser' api model to the 'JobRunAsUser' dataclass
    expected by the Worker Agent.
    """
    # Only two options for "runAs": WORKER_AGENT_USER & QUEUE_CONFIGURED_USER
    if job_run_as_user_data["runAs"] == "WORKER_AGENT_USER":
        job_run_as_user = JobRunAsUser(is_worker_agent_user=True)
        return job_run_as_user

    # We have a QUEUE_CONFIGURED_USER, so extract the data for this platform.
    if os.name == "posix":
        job_run_as_user_posix = job_run_as_user_data.get("posix", None)
        if job_run_as_user_posix is None:
            # Note: This may happen in an SMF case as follows:
            #  Customer has a Windows-based CMF, and configures the QUEUE_CONFIGURED_USER
            # for Windows, but also connects the Queue to a posix-based SMF.
            #  So, this would mean that we're in a posix-based SMF. Return None; the agent
            # must have been started with a jobRunAsUser override.
            return None

        job_run_as_user = JobRunAsUser(
            posix=PosixSessionUser(
                user=job_run_as_user_posix["user"],
                group=job_run_as_user_posix["group"],
            ),
        )
    else:
        job_run_as_user_windows = job_run_as_user_data.get("windows", None)
        if job_run_as_user_windows is None:
            # Note: This may happen in an SMF case as follows:
            #  Customer has a posix-based CMF, and configures the QUEUE_CONFIGURED_USER
            # for posix, but also connects the Queue to a Windows-based SMF.
            #  So, this would mean that we're in a Windows-based SMF. Return None; the agent
            # must have been started with a jobRunAsUser override.
            return None
        user = job_run_as_user_windows["user"]
        passwordArn = job_run_as_user_windows["passwordArn"]
        job_run_as_user = JobRunAsUser(
            windows_settings=JobRunAsWindowsUser(user=user, passwordArn=passwordArn),
        )

    return job_run_as_user


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
class JobRunAsWindowsUser:
    passwordArn: str
    user: str


@dataclass
class JobRunAsUser:
    posix: PosixSessionUser | None = None
    windows: WindowsSessionUser | None = None
    windows_settings: JobRunAsWindowsUser | None = None
    is_worker_agent_user: bool = False

    def __eq__(self, other: Any) -> bool:
        if other is None:
            return False

        if self.posix and other.posix:
            posix_eq = self.posix.user == other.posix.user and self.posix.group == other.posix.group
        else:
            posix_eq = self.posix is None and other.posix is None

        if self.windows and other.windows:
            windows_eq = (
                self.windows.user == other.windows.user
                and self.windows.password == other.windows.password
            )
        else:
            windows_eq = self.windows is None and other.windows is None

        if self.windows_settings and other.windows_settings:
            windows_settings_eq = (
                self.windows_settings.user == other.windows_settings.user
                and self.windows_settings.passwordArn == other.windows_settings.passwordArn
            )
        else:
            windows_settings_eq = self.windows_settings is None and other.windows_settings is None

        return posix_eq and windows_eq and windows_settings_eq


@dataclass(frozen=True)
class JobDetails:
    """A job's details required by the Worker"""

    ENTITY_TYPE = JobEntityType.JOB_DETAILS.value
    """The JobEntityType handled by this class"""

    log_group_name: str
    """The name of the log group for the session"""

    schema_version: SpecificationRevision
    """The Open Job Description schema version"""

    job_attachment_settings: JobAttachmentSettings | None = None
    """The job attachment settings of the job's queue"""

    parameters: JobParameterValues = field(default_factory=dict)
    """The job's parameters"""

    job_run_as_user: JobRunAsUser | None = None
    """The user associated with the job's AWS Deadline Cloud queue"""

    path_mapping_rules: list[OPENJDPathMappingRule] = field(default_factory=list)
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
        job_parameters = parameters_from_api_response(job_parameters_data)
        path_mapping_rules: list[OPENJDPathMappingRule] = []
        path_mapping_rules_data = job_details_data.get("pathMappingRules", None)
        if path_mapping_rules_data:
            path_mapping_rules = path_mapping_api_model_to_openjd(path_mapping_rules_data)

        job_attachment_settings: JobAttachmentSettings | None = None
        if job_attachment_settings_boto := job_details_data.get("jobAttachmentSettings", None):
            job_attachment_settings = JobAttachmentSettings.from_boto(job_attachment_settings_boto)

        job_run_as_user_data = job_details_data.get("jobRunAsUser", None)
        job_run_as_user: JobRunAsUser | None = (
            job_run_as_user_api_model_to_worker_agent(job_run_as_user_data)
            if job_run_as_user_data is not None
            else None
        )

        # Note: Record the empty string as a None as well.
        queue_role_arn: str | None = job_details_data.get("queueRoleArn", None)

        given_schema_version = TemplateSpecificationVersion(job_details_data["schemaVersion"])

        if given_schema_version == TemplateSpecificationVersion.JOBTEMPLATE_v2023_09:
            schema_version = SpecificationRevision.v2023_09
        else:
            raise UnsupportedSchema(given_schema_version.value)

        return JobDetails(
            parameters=job_parameters,
            schema_version=schema_version,
            log_group_name=job_details_data["logGroupName"],
            job_run_as_user=job_run_as_user,
            path_mapping_rules=path_mapping_rules,
            job_attachment_settings=job_attachment_settings,
            queue_role_arn=queue_role_arn,
        )

    @classmethod
    def validate_entity_data(
        cls, entity_data: dict[str, Any], job_user_override: JobsRunAsUserOverride | None
    ) -> JobDetailsData:
        """Performs input validation on a response element received from boto3's call to
        the BatchGetJobEntity AWS Deadline Cloud API.

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
                    key="jobRunAsUser",
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
                        Field(
                            key="runAs",
                            expected_type=str,
                            required=True,
                        ),
                        Field(
                            key="windows",
                            expected_type=dict,
                            required=False,
                            fields=(
                                Field(key="user", expected_type=str, required=True),
                                Field(key="passwordArn", expected_type=str, required=True),
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
                Field(key="queueRoleArn", expected_type=str, required=False),
            ),
        )

        # Validating job parameters requires special validation since keys are dynamic
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
                        Field(key="sourcePathFormat", expected_type=str, required=True),
                        Field(key="sourcePath", expected_type=str, required=True),
                        Field(key="destinationPath", expected_type=str, required=True),
                    ),
                )

        if job_user_override is not None and (
            job_user_override.run_as_agent or job_user_override.job_user
        ):
            # If there is an override, we don't care about the job details jobRunAsUser
            entity_data.pop("jobRunAsUser", None)
        elif run_as_value := entity_data.get("jobRunAsUser", dict()).get("runAs", None):
            # Validate jobRunAsUser -> runAs is one of ("QUEUE_CONFIGURED_USER" / "WORKER_AGENT_USER")
            if run_as_value not in ("QUEUE_CONFIGURED_USER", "WORKER_AGENT_USER"):
                raise ValueError(
                    f'Expected "jobRunAs" -> "runAs" to be one of "QUEUE_CONFIGURED_USER", "WORKER_AGENT_USER" but got "{run_as_value}"'
                )
            elif run_as_value == "QUEUE_CONFIGURED_USER":
                run_as_posix = entity_data["jobRunAsUser"].get("posix", None)
                run_as_windows = entity_data["jobRunAsUser"].get("windows", None)
                if os.name == "nt" and not run_as_windows:
                    raise ValueError(
                        'Expected "jobRunAs" -> "windows" to exist when "jobRunAs" -> "runAs" is "QUEUE_CONFIGURED_USER" but it was not present'
                    )
                if os.name == "posix" and not run_as_posix:
                    raise ValueError(
                        'Expected "jobRunAs" -> "posix" to exist when "jobRunAs" -> "runAs" is "QUEUE_CONFIGURED_USER" but it was not present'
                    )
                if run_as_posix:
                    if run_as_posix["user"] == "":
                        raise ValueError(
                            'Got empty "jobRunAs" -> "posix" -> "user" but "jobRunAs" -> "runAs" is "QUEUE_CONFIGURED_USER"'
                        )
                    if run_as_posix["group"] == "":
                        raise ValueError(
                            'Got empty "jobRunAs" -> "posix" -> "group" but "jobRunAs" -> "runAs" is "QUEUE_CONFIGURED_USER"'
                        )
                if run_as_windows:
                    if run_as_windows["user"] == "":
                        raise ValueError(
                            'Got empty "jobRunAs" -> "windows" -> "user" but "jobRunAs" -> "runAs" is "QUEUE_CONFIGURED_USER"'
                        )
                    if run_as_windows["passwordArn"] == "":
                        raise ValueError(
                            'Got empty "jobRunAs" -> "windows" -> "passwordArn" but "jobRunAs" -> "runAs" is "QUEUE_CONFIGURED_USER"'
                        )
            elif run_as_value == "WORKER_AGENT_USER" and "posix" in entity_data["jobRunAsUser"]:
                raise ValueError(
                    f'Expected "jobRunAs" -> "posix" is not valid when "jobRunAs" -> "runAs" is "WORKER_AGENT_USER" but got {entity_data["jobRunAsUser"]["posix"]}'
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
