# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os

import pytest


@pytest.fixture(
    params=(
        pytest.param("/foo", marks=pytest.mark.skipif(os.name == "nt", reason="POSIX-only test")),
        pytest.param("/bar", marks=pytest.mark.skipif(os.name == "nt", reason="POSIX-only test")),
        pytest.param(
            "C:\\SessionDir1",
            marks=pytest.mark.skipif(os.name != "nt", reason="Windows-only test"),
        ),
        pytest.param(
            "C:\\SessionDir2",
            marks=pytest.mark.skipif(os.name != "nt", reason="Windows-only test"),
        ),
    ),
)
def session_root_dir(request: pytest.FixtureRequest) -> str:
    return request.param
