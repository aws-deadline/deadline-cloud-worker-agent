# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from typing import Any, cast
import pytest

from deadline_worker_agent.sessions.job_entities.job_details import (
    JobDetails,
    JobRunAsUser,
    JobRunAsWindowsUser,
)
from deadline_worker_agent.api_models import JobDetailsData
from openjd.model import SpecificationRevision
from openjd.sessions import PosixSessionUser, SessionUser, WindowsSessionUser
import os


@pytest.fixture
def os_user() -> SessionUser:
    if os.name == "posix":
        return PosixSessionUser(user="user1", group="group1")
    else:
        return WindowsSessionUser(user="user1", password="fakepassword")


@pytest.fixture
def job_details_with_user(os_user) -> JobDetails:
    if os.name == "posix":
        posix_user = cast(PosixSessionUser, os_user)
        return JobDetails(
            log_group_name="/aws/deadline/queue-0000",
            schema_version=SpecificationRevision.v2023_09,
            job_run_as_user=JobRunAsUser(posix=posix_user),
        )
    else:
        return JobDetails(
            log_group_name="/aws/deadline/queue-0000",
            schema_version=SpecificationRevision.v2023_09,
            job_run_as_user=JobRunAsUser(
                windows_settings=JobRunAsWindowsUser(user="user1", passwordArn="anarn")
            ),
        )


@pytest.fixture
def job_details_no_user() -> JobDetails:
    return JobDetails(
        log_group_name="/aws/deadline/queue-0000",
        schema_version=SpecificationRevision.v2023_09,
    )


@pytest.fixture
def job_details_only_run_as_worker_agent_user() -> JobDetails:
    return JobDetails(
        log_group_name="/aws/deadline/queue-0000",
        schema_version=SpecificationRevision.v2023_09,
        job_run_as_user=JobRunAsUser(is_worker_agent_user=True),
    )


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "jobRunAsUser": {
                    "posix": {
                        "user": "user1",
                        "group": "group1",
                    },
                },
            },
            id="only required fields - posix",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "jobRunAsUser": {
                    "windows": {
                        "user": "user1",
                        "passwordArn": "anarn",
                    },
                },
            },
            id="only required fields - windows",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "jobRunAsUser": {
                    "posix": {
                        "user": "",
                        "group": "",
                    },
                },
            },
            id="only required fields, empty user",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "jobRunAsUser": {
                    "posix": {
                        "user": "",
                        "group": "",
                    },
                },
            },
            id="only required fields, empty user",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "jobRunAsUser": {
                    "posix": {
                        "user": "",
                        "group": "",
                    },
                },
            },
            id="only required fields, empty user",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "jobRunAsUser": {
                    "posix": {
                        "user": "",
                        "group": "",
                    },
                },
            },
            id="only required fields, empty user",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "parameters": {
                    "param1": {
                        "string": "param1value",
                    },
                    "param2": {
                        "path": "param2value",
                    },
                },
                "jobRunAsUser": {
                    "posix": {
                        "user": "user1",
                        "group": "group1",
                    },
                },
            },
            id="valid parameters",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "pathMappingRules": [],
                "jobRunAsUser": {
                    "posix": {
                        "user": "user1",
                        "group": "group1",
                    },
                },
            },
            id="valid pathMappingRules - empty list",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "pathMappingRules": [
                    {
                        "sourcePathFormat": "posix",
                        "sourcePath": "/source/path",
                        "destinationPath": "/destination/path",
                    },
                ],
                "jobRunAsUser": {
                    "posix": {
                        "user": "user1",
                        "group": "group1",
                    },
                },
            },
            id="valid pathMappingRules",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "jobRunAsUser": {},
            },
            id="valid jobRunAsUser - empty dict",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "jobRunAsUser": {
                    "posix": {
                        "user": "user1",
                        "group": "group1",
                    },
                    # (no "runAs" here)
                },
            },
            id="valid old jobRunAsUser",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "jobRunAsUser": {
                    "posix": {
                        "user": "user1",
                        "group": "group1",
                    },
                    "windows": {
                        "user": "user1",
                        "passwordArn": "anarn",
                    },
                    "runAs": "QUEUE_CONFIGURED_USER",
                },
            },
            id="valid new jobRunAsUser",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "jobAttachmentSettings": {
                    "s3BucketName": "mybucket",
                    "rootPrefix": "myprefix",
                },
                "jobRunAsUser": {
                    "posix": {
                        "user": "user1",
                        "group": "group1",
                    },
                },
            },
            id="valid jobAttachmentSettings",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "osUser": "",
                "parameters": {
                    "param1": {
                        "string": "param1value",
                    },
                    "param2": {
                        "path": "param2value",
                    },
                },
                "pathMappingRules": [
                    {
                        "sourcePathFormat": "posix",
                        "sourcePath": "/source/path",
                        "destinationPath": "/destination/path",
                    },
                ],
                "jobRunAsUser": {
                    "posix": {
                        "user": "user1",
                        "group": "group1",
                    },
                },
                "jobAttachmentSettings": {
                    "s3BucketName": "mybucket",
                    "rootPrefix": "myprefix",
                },
                "queueRoleArn": "0000:role/myqueuerole",
            },
            id="all fields",
        ),
    ],
)
def test_input_validation_success(data: dict[str, Any]) -> None:
    """Test that validate_entity_data() can successfully handle valid input data."""
    JobDetails.validate_entity_data(entity_data=data)


@pytest.mark.parametrize(
    "data, expected",
    [
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-2023-09",
                "jobRunAsUser": {
                    "posix": {
                        "user": "user1",
                        "group": "group1",
                    },
                    "windows": {
                        "user": "user1",
                        "passwordArn": "anarn",
                    },
                },
            },
            "job_details_with_user",
            id="only required fields",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-2023-09",
                "jobRunAsUser": {
                    "posix": {
                        "user": "user1",
                        "group": "group1",
                    },
                    "windows": {
                        "user": "user1",
                        "passwordArn": "anarn",
                    },
                    "runAs": "QUEUE_CONFIGURED_USER",
                },
            },
            "job_details_with_user",
            id="required fields with runAs QUEUE_CONFIGURED_USER",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-2023-09",
                "jobRunAsUser": {
                    "posix": {
                        "user": "user1",
                        "group": "group1",
                    },
                    "runAs": "QUEUE_CONFIGURED_USER",
                },
            },
            "job_details_no_user",
            marks=pytest.mark.skipif(os.name != "nt", reason="Windows-only test."),
            id="only posix user given on windows",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-2023-09",
                "jobRunAsUser": {
                    "windows": {
                        "user": "user1",
                        "group": "group1",
                        "passwordArn": "anarn",
                    },
                    "runAs": "QUEUE_CONFIGURED_USER",
                },
            },
            "job_details_no_user",
            marks=pytest.mark.skipif(os.name != "posix", reason="Posix-only test."),
            id="only windows user given on posix",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-2023-09",
                "jobRunAsUser": {
                    "posix": {
                        "user": "",
                        "group": "",
                    },
                },
            },
            "job_details_no_user",
            id="required with empty posix user/group",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-2023-09",
                "jobRunAsUser": {
                    "windows": {
                        "user": "",
                        "passwordArn": "",
                    },
                },
            },
            "job_details_no_user",
            id="required with empty windows user",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-2023-09",
                "jobRunAsUser": {
                    "runAs": "WORKER_AGENT_USER",
                },
            },
            "job_details_only_run_as_worker_agent_user",
            id="required with runAs WORKER_AGENT_USER",
        ),
    ],
)
def test_convert_job_user_from_boto(data: JobDetailsData, expected: JobDetails, request) -> None:
    # WHEN
    job_details = JobDetails.from_boto(data)
    expected_job_details: JobDetails = request.getfixturevalue(expected)
    # THEN
    assert job_details == expected_job_details


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "jobRunAsUser": {
                    "posix": {
                        "user": "abc",
                        "group": "abc",
                    },
                    "runAs": "QUEUE_CONFIGURED_USER",
                },
            },
            id="missing schemaVersion",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "jobRunAsUser": {
                    "posix": {
                        "user": "abc",
                        "group": "abc",
                    },
                    "runAs": "QUEUE_CONFIGURED_USER",
                },
            },
            id="missing logGroupName",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "parameters": {
                    "param1": {"string": "param1value", "anotherKey": "value"},
                },
                "jobRunAsUser": {
                    "posix": {
                        "user": "abc",
                        "group": "abc",
                    },
                    "runAs": "QUEUE_CONFIGURED_USER",
                },
            },
            id="nonvalid parameters - a parameter has two keys.",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "parameters": {
                    "param1": {
                        "unknownType": "param1value",
                    },
                },
                "jobRunAsUser": {
                    "posix": {
                        "user": "abc",
                        "group": "abc",
                    },
                    "runAs": "QUEUE_CONFIGURED_USER",
                },
            },
            id="nonvalid parameters - a type key is unknown type.",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "pathMappingRules": [
                    {
                        "sourcePath": "/source/path",
                        "destinationPath": "/destination/path",
                    },
                ],
                "jobRunAsUser": {
                    "posix": {
                        "user": "abc",
                        "group": "abc",
                    },
                    "runAs": "QUEUE_CONFIGURED_USER",
                },
            },
            id="nonvalid pathMappingRules - missing sourcePathFormat",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "pathMappingRules": {},
                "jobRunAsUser": {
                    "posix": {
                        "user": "abc",
                        "group": "abc",
                    },
                    "runAs": "QUEUE_CONFIGURED_USER",
                },
            },
            id="nonvalid pathMappingRules - not list",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "jobRunAsUser": {
                    "posix": {
                        "group": "group1",
                    },
                    "runAs": "QUEUE_CONFIGURED_USER",
                },
            },
            id="nonvalid jobRunAsUser - missing posix user",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "jobRunAsUser": {
                    "windows": {"passwordArn": "anarn"},
                    "runAs": "QUEUE_CONFIGURED_USER",
                },
            },
            id="nonvalid jobRunAsUser - missing windows user",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "jobRunAsUser": {
                    "posix": {
                        "user": "user1",
                    },
                    "runAs": "QUEUE_CONFIGURED_USER",
                },
            },
            id="nonvalid jobRunAsUser - missing posix group",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "jobRunAsUser": {
                    "windows": {"user": "user1", "group": "group1"},
                    "runAs": "QUEUE_CONFIGURED_USER",
                },
            },
            id="nonvalid jobRunAsUser - missing windows passwordArn",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "jobRunAsUser": {
                    "posix": {
                        # Empty value
                        "user": "",
                        "group": "abc",
                    },
                    "runAs": "QUEUE_CONFIGURED_USER",
                },
            },
            id="nonvalid new-style jobRunAsUser - empty posix user",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "jobRunAsUser": {
                    "windows": {
                        # Empty value
                        "user": "",
                        "passwordArn": "anarn",
                    },
                    "runAs": "QUEUE_CONFIGURED_USER",
                },
            },
            id="nonvalid new-style jobRunAsUser - empty windows user",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "jobRunAsUser": {
                    "posix": {
                        "user": "abc",
                        # Empty value
                        "group": "",
                    },
                    "runAs": "QUEUE_CONFIGURED_USER",
                },
            },
            id="nonvalid new-style jobRunAsUser - empty posix group",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "jobRunAsUser": {
                    "windows": {
                        "user": "abc",
                        # Empty value
                        "passwordArn": "",
                    },
                    "runAs": "QUEUE_CONFIGURED_USER",
                },
            },
            id="nonvalid new-style jobRunAsUser - empty windows passwordArn",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "jobRunAsUser": [
                    {
                        "posix": {
                            "user": "user1",
                        },
                        "runAs": "QUEUE_CONFIGURED_USER",
                    }
                ],
            },
            id="nonvalid jobRunAsUser - not dict",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
            },
            id="missing jobRunAsUser",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "jobRunAsUser": {
                    "posix": {
                        "user": "abc",
                        "group": "abc",
                    },
                    "runAs": "BAD_VALUE",
                },
            },
            id="nonvalid jobRunAsUser runAs",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "jobAttachmentSettings": {
                    "s3BucketName": "mybucket",
                },
                "jobRunAsUser": {
                    "posix": {
                        "user": "abc",
                        "group": "abc",
                    },
                    "runAs": "QUEUE_CONFIGURED_USER",
                },
            },
            id="nonvalid jobAttachmentSettings - missing rootPrefix",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "jobRunAsUser": {
                    "posix": {
                        "user": "abc",
                        "group": "abc",
                    },
                    "runAs": "QUEUE_CONFIGURED_USER",
                },
                "unknown": "field",
            },
            id="unknown field",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "jobRunAsUser": {
                    "posix": {
                        "user": "abc",
                        "group": "abc",
                    },
                    "runAs": "QUEUE_CONFIGURED_USER",
                },
                "pathMappingRules": [
                    {
                        "sourcePath": "/source/path",
                        "destinationPath": "/destination/path",
                        "unknown": "unknown",
                    },
                ],
            },
            id="unknown field in pathMappingRules",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "jobRunAsUser": {
                    "posix": {
                        "user": "user1",
                        "group": "group1",
                    },
                    "runAs": "QUEUE_CONFIGURED_USER",
                },
            },
            marks=pytest.mark.skipif(os.name != "nt", reason="Windows-only test."),
            id="nonvalid jobRunAsUser - missing windows on Windows OS",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "jobRunAsUser": {
                    "windows": {"user": "user1", "passwordArn": "anarn"},
                    "runAs": "QUEUE_CONFIGURED_USER",
                },
            },
            marks=pytest.mark.skipif(os.name != "posix", reason="Posix-only test."),
            id="nonvalid jobRunAsUser - missing posix on POSIX OS",
        ),
    ],
)
def test_input_validation_failure(data: dict[str, Any]) -> None:
    """Test that validate_entity_data() raises a ValueError when nonvalid input data is provided."""
    with pytest.raises(ValueError):
        JobDetails.validate_entity_data(entity_data=data)
