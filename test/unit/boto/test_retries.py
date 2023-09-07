"""Tests for deadline_worker_agent.boto.retries.NoOverflowExponentialBackoff"""

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from botocore.retries.standard import RetryContext
from pytest import FixtureRequest, fixture
from unittest.mock import Mock
import math

import deadline_worker_agent.boto.retries as boto_retries_mod

ATTEMPT_NUMBER_PARAMETERS = (1, 3, 4, 8)
MAX_BACKOFF_PARAMETERS = (2, 4, 6)
RANDOM_VALUES = (0.0, 0.5, 1.0)


@fixture
def base() -> float:
    return 2


@fixture(
    params=MAX_BACKOFF_PARAMETERS, ids=[f"max_backoff({val})" for val in MAX_BACKOFF_PARAMETERS]
)
def max_backoff(request: FixtureRequest) -> float:
    return request.param


@fixture(
    params=ATTEMPT_NUMBER_PARAMETERS,
    ids=[f"attempt_number({val})" for val in ATTEMPT_NUMBER_PARAMETERS],
)
def attempt_number(request: FixtureRequest) -> int:
    return request.param


@fixture(params=RANDOM_VALUES, ids=[f"random_value({val})" for val in RANDOM_VALUES])
def random_value(request: FixtureRequest) -> float:
    return request.param


class TestNoOverflowExponentialBackoff:
    @fixture
    def is_exponential(
        self,
        base: float,
        max_backoff: float,
        attempt_number: int,
    ) -> bool:
        return (attempt_number - 1) <= (math.log(max_backoff, base) * 2)

    def test_delay(
        self,
        base: float,
        max_backoff: float,
        attempt_number: int,
        random_value: float,
        is_exponential: bool,
    ) -> None:
        """Asserts backoff behavior works as expected"""
        # GIVEN
        context = RetryContext(attempt_number=attempt_number)
        random_between = Mock(return_value=random_value)
        backoff = boto_retries_mod.NoOverflowExponentialBackoff(
            max_backoff=max_backoff,
            random_between=random_between,
        )
        if is_exponential:
            expected = min(max_backoff, random_value * base ** (attempt_number - 1))
        else:
            expected = random_value

        # WHEN
        result = backoff.delay_amount(context=context)

        # THEN
        assert result == expected
        print(f"is_exponential: {is_exponential}")
        if is_exponential:
            random_between.assert_called_once_with(0, 1)
        else:
            random_between.assert_called_once_with(0.8 * max_backoff, max_backoff)
