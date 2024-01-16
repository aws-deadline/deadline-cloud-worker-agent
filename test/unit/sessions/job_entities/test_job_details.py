# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from typing import Any
import pytest

from deadline_worker_agent.sessions.job_entities.job_details import JobDetails, JobRunAsUser
from deadline_worker_agent.api_models import JobDetailsData
from openjd.model import SchemaVersion
from openjd.sessions import PosixSessionUser


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
            id="only required fields",
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
                },
            },
            JobDetails(
                log_group_name="/aws/deadline/queue-0000",
                schema_version=SchemaVersion.v2023_09,
                job_run_as_user=JobRunAsUser(posix=PosixSessionUser(user="user1", group="group1")),
            ),
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
                    "runAs": "QUEUE_CONFIGURED_USER",
                },
            },
            JobDetails(
                log_group_name="/aws/deadline/queue-0000",
                schema_version=SchemaVersion.v2023_09,
                job_run_as_user=JobRunAsUser(posix=PosixSessionUser(user="user1", group="group1")),
            ),
            id="required fields with runAs QUEUE_CONFIGURED_USER",
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
            JobDetails(
                log_group_name="/aws/deadline/queue-0000",
                schema_version=SchemaVersion.v2023_09,
            ),
            id="required with empty user/group",
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
            JobDetails(
                log_group_name="/aws/deadline/queue-0000",
                schema_version=SchemaVersion.v2023_09,
            ),
            id="required with runAs WORKER_AGENT_USER",
        ),
    ],
)
def test_convert_job_user_from_boto(data: JobDetailsData, expected: JobDetails) -> None:
    # WHEN
    job_details = JobDetails.from_boto(data)
    # THEN
    assert job_details == expected


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
            id="nonvalid jobRunAsUser - missing user",
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
            id="nonvalid jobRunAsUser - missing group",
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
            id="nonvalid new-style jobRunAsUser - empty user",
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
            id="nonvalid new-style jobRunAsUser - empty group",
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
    ],
)
def test_input_validation_failure(data: dict[str, Any]) -> None:
    """Test that validate_entity_data() raises a ValueError when nonvalid input data is provided."""
    with pytest.raises(ValueError):
        JobDetails.validate_entity_data(entity_data=data)
