# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from botocore.config import Config

from .._version import __version__ as worker_agent_version


DEADLINE_BOTOCORE_CONFIG = Config(
    user_agent_extra=f"deadline_worker_agent/{worker_agent_version}",
)
"""
Botocore client configuration for Amazon Deadline Cloud. This overrides to:
    - add deadline-worker-agent version to user User-Agent request header
"""

OTHER_BOTOCORE_CONFIG = Config(user_agent_extra=f"deadline_worker_agent/{worker_agent_version}")
"""
Botocore client configuration for other AWS services. This overrides to:
    - add deadline-worker-agent version to user User-Agent request header
"""
