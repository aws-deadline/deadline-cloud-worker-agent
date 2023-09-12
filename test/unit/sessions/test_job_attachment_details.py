# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest

from deadline.job_attachments._utils import AssetLoadingMethod
from deadline_worker_agent.sessions.job_entities.job_attachment_details import JobAttachmentDetails


@pytest.mark.parametrize("loading_method", [e.value for e in AssetLoadingMethod])
def test_asset_loading_method(loading_method):
    """Test that the loading method is read from the boto data into JobAttachmentDetails"""
    entity_obj = JobAttachmentDetails.from_boto(
        job_attachments_details_data={
            "jobId": "myjob",
            "attachments": {
                "manifests": [],
                "assetLoadingMethod": loading_method,
            },
        },
    )

    assert entity_obj is not None
    assert entity_obj.asset_loading_method == loading_method


def test_asset_loading_method_default():
    """Test that the loading method is set to default when not included in boto data"""
    entity_obj = JobAttachmentDetails.from_boto(
        job_attachments_details_data={
            "jobId": "myjob",
            "attachments": {
                "manifests": [],
            },
        },
    )

    assert entity_obj is not None
    assert entity_obj.asset_loading_method == AssetLoadingMethod.PRELOAD
