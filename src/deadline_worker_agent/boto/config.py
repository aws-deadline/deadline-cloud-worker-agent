# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from botocore.config import Config

from .._version import __version__ as worker_agent_version
from deadline.client import version as deadline_client_lib_version
from openjd.sessions import version as openjd_sessions_version


def construct_user_agent() -> str:
    """
    Compute the user agent string to send over boto requests.
        - Contains the versions of Deadline Worker Agent, Deadline Client Library, OpenJD Sessions.
    """
    return f"deadline_worker_agent/{worker_agent_version} deadline_cloud/{deadline_client_lib_version} openjd_sessions/{openjd_sessions_version}"


DEADLINE_BOTOCORE_CONFIG = Config(
    retries={
        "max_attempts": 1,
    },
    user_agent_extra=construct_user_agent(),
)

"""
Botocore client configuration for AWS Deadline Cloud. This overrides to:
    - botocore retries - the worker agent has its own retry logic for AWS Deadline Cloud
      API requests. See the `aws/deadline` sub-package for that retry logic.
    - add deadline, deadline worker agent and openjd package versions to user User-Agent request header
"""

OTHER_BOTOCORE_CONFIG = Config(user_agent_extra=construct_user_agent())
"""
Botocore client configuration for other AWS services. This overrides to:
    - add deadline, deadline worker agent and openjd package versions to user User-Agent request header
"""
