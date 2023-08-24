# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for __main__"""

from __future__ import annotations

from unittest.mock import Mock, patch

from deadline_worker_agent import __main__


@patch.object(__main__, "entrypoint")
def test_main(entrypoint_mock: Mock):
    # GIVEN
    entrypoint_mock.assert_not_called()

    # Simulate the deadline_worker_agent package being the
    # Python entrypoint
    with patch.object(__main__, "__name__", new="__main__"):
        __main__.init()

    entrypoint_mock.assert_called_once()
