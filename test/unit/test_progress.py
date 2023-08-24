# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for the progress module"""

import pytest

from deadline_worker_agent.progress import normalize_progress


@pytest.mark.parametrize(
    ("start", "end", "segment_progress", "expected_progress"),
    [
        (90.0, 100.0, 10.0, 91.0),
        (80.0, 100.0, 20.0, 84.0),
        (70.0, 100.0, 30.0, 79.0),
        (60.0, 100.0, 40.0, 76.0),
        (50.0, 100.0, 50.0, 75.0),
        (45.0, 100.0, 60.0, 78.0),
        (40.0, 100.0, 100.0, 100.0),
        (20.0, 90.0, 10.0, 27.0),
        (30.0, 80.0, 20.0, 40.0),
        (40.0, 70.0, 30.0, 49.0),
        (50.0, 65.0, 40.0, 56.0),
        (50.0, 57.0, 100.0, 57.0),
        (0.0, 90.0, 10.0, 9.0),
        (0.0, 80.0, 20.0, 16.0),
        (0.0, 70.0, 30.0, 21.0),
        (0.0, 60.0, 40.0, 24.0),
        (0.0, 50.0, 50.0, 25.0),
        (0.0, 40.0, 60.0, 24.0),
        (0.0, 30.0, 100.0, 30.0),
        (0.0, 100.0, 60.0, 60.0),
        (0.0, 100.0, 100.0, 100.0),
    ],
)
def test_nomalize_progress(
    start: float, end: float, segment_progress: float, expected_progress: float
):
    """
    Asserts that the correct amount of progress is made given a segment.
    """
    assert normalize_progress(start, end, segment_progress) == expected_progress


@pytest.mark.parametrize(
    ("start", "end", "segment_progress", "expected_return"),
    [
        (34.0, 32.0, 9.0, 34.0),
        (-1.0, -21.0, 10.0, 1.0),
    ],
)
def test_nomalize_progress_start_greater_than_end(
    start: float, end: float, segment_progress: float, expected_return: float
):
    """
    Asserts if the normalize_progress function is given a start
    greater than the end, that the absoulte value of start is returned.
    """
    assert normalize_progress(start, end, segment_progress) == expected_return


@pytest.mark.parametrize(
    ("start", "end", "segment_progress", "expected_return"),
    [
        (-34.0, -34.0, 9, 34.0),
        (-34.0, -34.0, 9, 34.0),
    ],
)
def test_nomalize_progress_start_equal_to_end(
    start: float, end: float, segment_progress: float, expected_return: float
):
    """
    Asserts that if start and end are equal, that the absolute value of start is returned.
    """
    assert normalize_progress(start, end, segment_progress) == expected_return


@pytest.mark.parametrize(
    ("start", "end", "segment_progress", "expected_return"),
    [
        (-1.0, 1.0, 10.0, 1.0),
        (-21.0, -11.0, 10.0, 21.0),
        (-21.0, -11.0, -10.0, 21.0),
        (21.0, -11.0, 10.0, 21.0),
        (21.0, -11.0, -10.0, 21.0),
        (-21.0, -11.0, 10.0, 21.0),
        (1.0, 11.0, -10.0, 1.0),
    ],
)
def test_nomalize_progress_negative_input(
    start: float, end: float, segment_progress: float, expected_return: float
):
    """
    Asserts that if any of the inputs to normalize_progress are zero, that the absolute
    value of the start value is returned.
    """
    assert normalize_progress(start, end, segment_progress) == expected_return
