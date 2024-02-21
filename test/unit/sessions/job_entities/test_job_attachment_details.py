# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from typing import Any
import pytest

from deadline.job_attachments.models import JobAttachmentsFileSystem
from deadline_worker_agent.sessions.job_entities.job_attachment_details import JobAttachmentDetails


@pytest.mark.parametrize("loading_method", [e.value for e in JobAttachmentsFileSystem])
def test_asset_loading_method(loading_method):
    """Test that the loading method is read from the boto data into JobAttachmentDetails"""
    entity_obj = JobAttachmentDetails.from_boto(
        job_attachments_details_data={
            "jobId": "job-0000",
            "attachments": {
                "manifests": [],
                "fileSystem": loading_method,
            },
        },
    )

    assert entity_obj is not None
    assert entity_obj.job_attachments_file_system == loading_method


def test_asset_loading_method_default():
    """Test that the loading method is set to default when not included in boto data"""
    entity_obj = JobAttachmentDetails.from_boto(
        job_attachments_details_data={
            "jobId": "job-0000",
            "attachments": {
                "manifests": [],
            },
        },
    )

    assert entity_obj is not None
    assert entity_obj.job_attachments_file_system == JobAttachmentsFileSystem.COPIED


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(
            {
                "jobId": "job-0000",
                "attachments": {
                    "manifests": [],
                },
            },
            id="only required fields with empty manifests list",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "attachments": {
                    "manifests": [
                        {
                            "rootPath": "/myroot",
                            "rootPathFormat": "posix",
                        }
                    ],
                },
            },
            id="only required fields",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "stepId": "step-0000",
                "attachments": {
                    "manifests": [
                        {
                            "fileSystemLocationName": "mylocation1",
                            "rootPath": "/myroot",
                            "rootPathFormat": "posix",
                            "outputRelativeDirectories": [],
                            "inputManifestPath": "farm-0000/queue-0000/Inputs/0000/input1.xxh128",
                            "inputManifestHash": "hash1",
                        },
                        {
                            "fileSystemLocationName": "mylocation2",
                            "rootPath": "/myroot",
                            "rootPathFormat": "posix",
                            "outputRelativeDirectories": ["./output1", "./output2"],
                            "inputManifestPath": "farm-0000/queue-0000/Inputs/0000/input2.xxh128",
                            "inputManifestHash": "hash2",
                        },
                    ],
                    "fileSystem": "copied",
                },
            },
            id="all fields",
        ),
    ],
)
def test_input_validation_success(data: dict[str, Any]) -> None:
    """Test that validate_entity_data() can successfully handle valid input data."""
    JobAttachmentDetails.validate_entity_data(entity_data=data)


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(
            {
                "attachments": {
                    "manifests": [],
                    "fileSystem": "copied",
                },
            },
            id="missing jobId",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
            },
            id="missing attachments",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "attachments": {
                    "fileSystem": "copied",
                },
            },
            id="nonvalid attachments - missing manifests",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "attachments": {
                    "manifests": [
                        {
                            "fileSystemLocationName": "mylocation",
                            "rootPath": "myroot",
                        },
                    ],
                },
            },
            id="nonvalid manifests - missing rootPathFormat",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "attachments": {
                    "manifests": {},
                },
            },
            id="nonvalid manifests - not list",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "stepId": "step-0000",
                "attachments": {
                    "manifests": [
                        {
                            "fileSystemLocationName": "mylocation1",
                            "rootPath": "/myroot",
                            "rootPathFormat": "posix",
                            "outputRelativeDirectories": "./output",
                        },
                    ],
                    "fileSystem": "copied",
                },
            },
            id="nonvalid outputRelativeDirectories - not list",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "attachments": {
                    "manifests": [],
                },
                "unknown": "unknown",
            },
            id="unknown field",
        ),
        pytest.param(
            {
                "jobId": "job-0000",
                "attachments": {
                    "manifests": [],
                    "unknown": "unknown",
                },
            },
            id="unknown field in attachments",
        ),
    ],
)
def test_input_validation_failure(data: dict[str, Any]) -> None:
    """Test that validate_entity_data() raises a ValueError when nonvalid input data is provided."""
    with pytest.raises(ValueError):
        JobAttachmentDetails.validate_entity_data(entity_data=data)
