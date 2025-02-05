# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Module for progress handling"""

from logging import getLogger

logger = getLogger(__name__)


def normalize_progress(start: float, end: float, segment_progress: float) -> float:
    """
    Normalize the given segment_progress into a progress between start and end.

    For example, if segment_progress is 50 (ie, 50% done a render).
    And the start is 50.0 and end is 60.0 (ie, a render represents being done 50-60% of a Task.),
    the return value would be 65.0 as it's 50% of the way between 50.0 and 60.0.

    start must be lower than and not equal to end. All inputs must be positive numbers.

    Any error in input value will result in the returning of the absolute value of start
    """
    if start >= end:
        logger.error(f"start must be lower than end. start was {start} and end was {end}.")
        return abs(start)

    if start < 0 or end < 0 or segment_progress < 0:
        logger.error(
            f"start, end, and segment_progress must all be positive floating point numbers. "
            f"start was {start}, end was {end}, and segment_progress was {segment_progress}."
        )
        return abs(start)

    chunk_percentage = (segment_progress / 100) * (end - start)
    return chunk_percentage + start
