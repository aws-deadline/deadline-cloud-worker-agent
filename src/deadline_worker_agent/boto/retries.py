# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
import math
import random
from typing import Callable

from botocore.retries.standard import ExponentialBackoff


class NoOverflowExponentialBackoff(ExponentialBackoff):
    """Truncated binary exponential backoff with jitter, rand(0, 1) * min(2 ** (attempt - 1),
    max_backoff), that avoids integer overflow for high attempt numbers.

    This is the algorithm botocore.retries.standard.ExponentialBackoff used before botocore 1.44,
    computed here rather than inherited. botocore 1.44 scales it by 50 ms for any error it cannot
    classify as throttling, which would make every agent retry, throttled ones included, start
    at 50 ms instead of 1 s.

    We use this in the worker agent because the agent is intended to be long-running and we don't
    want worker admins to need to restart the worker agents over many worker hosts when there
    is a service issue.
    """

    _BASE = 2

    _log_val: float
    _random_between: Callable[[float, float], float]

    def __init__(
        self,
        *,
        max_backoff: float = 20,
        random_between: Callable[[float, float], float] = random.uniform,
    ) -> None:
        super(NoOverflowExponentialBackoff, self).__init__(
            max_backoff=max_backoff,
            random=self._unit_random,
        )
        self._max_backoff = max_backoff
        self._log_val = math.log(max_backoff, self._BASE)
        self._random_between = random_between

    def delay_amount(self, context):
        exponent = context.attempt_number - 1
        if exponent <= (self._log_val * 2):
            return self._unit_random() * min(self._BASE**exponent, self._max_backoff)

        return self._random_between(
            self._max_backoff * 0.8,
            self._max_backoff,
        )

    def _unit_random(self) -> float:
        """Returns a random number between 0 and 1 using the random_between function provided
        when constructing the instance
        """
        return self._random_between(0, 1)
