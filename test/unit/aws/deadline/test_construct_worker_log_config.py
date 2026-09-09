# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.


import pytest

from deadline_worker_agent.aws.deadline import (
    construct_worker_log_config,
    WorkerLogConfig,
)

from deadline_worker_agent.api_models import (
    LogConfiguration,
)


from deadline_worker_agent.log_sync.cloudwatch import (
    LOG_CONFIG_OPTION_GROUP_NAME_KEY,
    LOG_CONFIG_OPTION_STREAM_NAME_KEY,
)

CLOUDWATCH_LOG_GROUP = "log-group"
CLOUDWATCH_LOG_STREAM = "log-stream"
AWSLOGS_LOG_CONFIGURATION = LogConfiguration(
    logDriver="awslogs",
    options={
        LOG_CONFIG_OPTION_GROUP_NAME_KEY: CLOUDWATCH_LOG_GROUP,
        LOG_CONFIG_OPTION_STREAM_NAME_KEY: CLOUDWATCH_LOG_STREAM,
    },
)
AWSLOGS_LOG_CONFIGURATION_MISSING_OPTIONS = LogConfiguration(
    logDriver="awslogs",
)
AWSLOGS_LOG_CONFIGURATION_WITH_ERROR = LogConfiguration(
    logDriver="awslogs",
    options={
        LOG_CONFIG_OPTION_GROUP_NAME_KEY: CLOUDWATCH_LOG_GROUP,
    },
    error="Could not create log stream",
)
AWSLOGS_LOG_CONFIGURATION_MISSING_GROUP = LogConfiguration(
    logDriver="awslogs",
    options={
        LOG_CONFIG_OPTION_STREAM_NAME_KEY: CLOUDWATCH_LOG_STREAM,
    },
    error="Missing log group",
)
AWSLOGS_LOG_CONFIGURATION_MISSING_STREAM = LogConfiguration(
    logDriver="awslogs",
    options={
        LOG_CONFIG_OPTION_GROUP_NAME_KEY: CLOUDWATCH_LOG_GROUP,
    },
    # This would have an error, but let's make sure that we don't blow up
    # if the error is missing. Otherwise, this is identical to
    # AWSLOGS_LOG_CONFIGURATION_WITH_ERROR
)
UNKNOWN_LOG_CONFIGURATION = LogConfiguration(
    logDriver="unknown",
)
EMPTY_LOG_CONFIGURATION: LogConfiguration = {}  # type: ignore


def test_success() -> None:
    # Tests the happy-path of the construct_worker_log_config() function

    # GIVEN
    expected_result = WorkerLogConfig(
        cloudwatch_log_group=CLOUDWATCH_LOG_GROUP, cloudwatch_log_stream=CLOUDWATCH_LOG_STREAM
    )

    # WHEN
    result = construct_worker_log_config(AWSLOGS_LOG_CONFIGURATION)

    # THEN
    assert result == expected_result


@pytest.mark.parametrize(
    "log_config",
    [
        AWSLOGS_LOG_CONFIGURATION_MISSING_OPTIONS,
        AWSLOGS_LOG_CONFIGURATION_WITH_ERROR,
        AWSLOGS_LOG_CONFIGURATION_MISSING_GROUP,
        AWSLOGS_LOG_CONFIGURATION_MISSING_STREAM,
        UNKNOWN_LOG_CONFIGURATION,
        EMPTY_LOG_CONFIGURATION,
    ],
)
def test_returns_none(log_config: LogConfiguration) -> None:
    # Test that any unknown or malformed response will return a None
    # Note: Cutting regression testing log messages to save some time.

    # WHEN
    result = construct_worker_log_config(log_config)

    # THEN
    assert result is None
