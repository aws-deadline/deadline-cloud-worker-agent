# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
import math
import random
from typing import Callable

from botocore.retries.standard import ExponentialBackoff


class NoOverflowExponentialBackoff(ExponentialBackoff):
    """This is the same exponential backoff algorithm as its parent class
    (botocore.retries.standard.ExponentialBackoff) except that it avoids integer overflow
    for high attempt numbers.

    We use this in the worker agent because the agent is intended to be long-running and we don't
    want worker admins to need to restart the worker agents over many worker hosts when there
    is a service issue.
    """

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
        self._log_val = math.log(max_backoff, self._base)
        self._random_between = random_between

    def delay_amount(self, context):
        if (context.attempt_number - 1) <= (self._log_val * 2):
            return super().delay_amount(context)

        return self._random_between(
            self._max_backoff * 0.8,
            self._max_backoff,
        )

    def _unit_random(self) -> float:
        """Returns a random number between 0 and 1 using the random_between function provided
        when constructing the instance
        """
        return self._random_between(0, 1)
