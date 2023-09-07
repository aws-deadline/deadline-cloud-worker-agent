# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from deadline_worker_agent.boto.logger import logger


def test_logger_name() -> None:
    """Asserts the boto logger name is 'deadline_worker_agent.boto'"""

    # THEN
    assert logger.name == "deadline_worker_agent.boto"
