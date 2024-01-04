# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from typing import Any
import pytest

from deadline_worker_agent.sessions.job_entities.job_details import JobDetails


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
                },
            },
            id="valid jobRunAsUser",
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
    "data",
    [
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
            },
            id="missing schemaVersion",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "schemaVersion": "jobtemplate-0000-00",
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
            },
            id="invalid parameters - a parameter has two keys.",
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
            },
            id="invalid parameters - a type key is unknown type.",
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
            },
            id="invalid pathMappingRules - missing sourcePathFormat",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "pathMappingRules": {},
            },
            id="invalid pathMappingRules - not list",
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
                },
            },
            id="invalid jobRunAsUser - missing group",
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
                    }
                ],
            },
            id="invalid jobRunAsUser - not dict",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "jobAttachmentSettings": {
                    "s3BucketName": "mybucket",
                },
            },
            id="invalid jobAttachmentSettings - missing rootPrefix",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "logGroupName": "/aws/deadline/queue-0000",
                "schemaVersion": "jobtemplate-0000-00",
                "unknown": "field",
            },
            id="unknown field",
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
                        "unknown": "unknown",
                    },
                ],
            },
            id="unknown field in pathMappingRules",
        ),
    ],
)
def test_input_validation_failure(data: dict[str, Any]) -> None:
    """Test that validate_entity_data() raises a ValueError when invalid input data is provided."""
    with pytest.raises(ValueError):
        JobDetails.validate_entity_data(entity_data=data)
