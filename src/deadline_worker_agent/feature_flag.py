# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os

# This feature is still a work-in-progress and untested on Windows

ASSET_SYNC_JOB_USER_FEATURE = (
    os.environ.get("ASSET_SYNC_JOB_USER_FEATURE", "false").lower() == "true"
)
