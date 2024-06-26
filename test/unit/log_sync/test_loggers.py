# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from deadline_worker_agent.log_sync.loggers import ROOT_LOGGER, logger


def test_root_logger() -> None:
    """
    Assert that the module is configuring the expected logger. This test combines with downstream
    tests that assert the behavior of configuring this module-level logger variable.
    """

    # THEN
    assert ROOT_LOGGER.name == "root"


def test_logger_name() -> None:
    """
    Assert that the module is configuring the expected logger. This test combines with downstream
    tests that assert the behavior of configuring this module-level logger variable.
    """

    # THEN
    assert logger.name == "deadline_worker_agent.log_sync"
