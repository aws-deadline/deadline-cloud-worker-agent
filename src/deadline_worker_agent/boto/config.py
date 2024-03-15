# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from botocore.config import Config

from .._version import __version__ as worker_agent_version


DEADLINE_BOTOCORE_CONFIG = Config(
    retries={
        "max_attempts": 1,
    },
    user_agent_extra=f"deadline_worker_agent/{worker_agent_version}",
)
"""
Botocore client configuration for AWS Deadline Cloud. This overrides to:
    - botocore retries - the worker agent has its own retry logic for AWS Deadline Cloud
      API requests. See the `aws/deadline` sub-package for that retry logic.
    - add deadline-worker-agent version to user User-Agent request header
"""

OTHER_BOTOCORE_CONFIG = Config(user_agent_extra=f"deadline_worker_agent/{worker_agent_version}")
"""
Botocore client configuration for other AWS services. This overrides to:
    - add deadline-worker-agent version to user User-Agent request header
"""
