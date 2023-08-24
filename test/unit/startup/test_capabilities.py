# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for the deadline_worker_agent.startup.capabilities module"""

import pytest

from deadline_worker_agent.startup.capabilities import Capabilities


def test_for_update_worker() -> None:
    """Ensures that Capabilities.for_update_worker() returns a dictionary representation of
    the capabilities in the format expected in the UpdateWorkerState API request, for example:

    {
        "amounts": [
            {
                "name": "amount.cap1",
                "value": 1
            },
            // ...
        ],
        "attributes": [
            {
                "name": "attr.cap2",
                "values": [
                    "a",
                    // ...
                ]
            },
            // ...
        ]
    }
    """
    # GIVEN
    capabilities = Capabilities(
        amounts={
            "amount.first": 12,
            "vendora:amount.second": 1,
        },
        attributes={
            "attr.first": ["a", "b"],
            "vendorb:attr.second": ["g"],
        },
    )

    # WHEN
    result = capabilities.for_update_worker()

    # THEN
    assert result == {
        "amounts": [
            {
                "name": "amount.first",
                "value": 12,
            },
            {
                "name": "vendora:amount.second",
                "value": 1,
            },
        ],
        "attributes": [
            {
                "name": "attr.first",
                "values": ["a", "b"],
            },
            {
                "name": "vendorb:attr.second",
                "values": ["g"],
            },
        ],
    }


@pytest.mark.parametrize(
    argnames=("lhs", "rhs", "expected_result"),
    argvalues=(
        pytest.param(
            Capabilities(amounts={"amount.a": 1}, attributes={"attr.b": ["a", "b"]}),
            Capabilities(amounts={"amount.b": 2}, attributes={"attr.a": ["c"]}),
            Capabilities(
                amounts={"amount.a": 1, "amount.b": 2},
                attributes={"attr.b": ["a", "b"], "attr.a": ["c"]},
            ),
            id="disjoint",
        ),
        pytest.param(
            Capabilities(amounts={"amount.a": 1}, attributes={"attr.b": ["a", "b"]}),
            Capabilities(amounts={"amount.a": 2}, attributes={"attr.b": ["c"]}),
            Capabilities(amounts={"amount.a": 2}, attributes={"attr.b": ["c"]}),
            id="overlapping",
        ),
        pytest.param(
            Capabilities(
                amounts={"amount.a": 1, "amount.b": 99},
                attributes={"attr.a": ["z"], "attr.b": ["a", "b"]},
            ),
            Capabilities(amounts={"amount.a": 2}, attributes={"attr.b": ["c"]}),
            Capabilities(
                amounts={"amount.a": 2, "amount.b": 99},
                attributes={"attr.a": ["z"], "attr.b": ["c"]},
            ),
            id="partially-overlapping",
        ),
    ),
)
def test_merge(
    lhs: Capabilities,
    rhs: Capabilities,
    expected_result: Capabilities,
) -> None:
    """Tests that Capabilities.merge(...) correctly merges two Capabilities instances. This should
    return a new Capabilities instance and values from the LHS should be replaced (if existing) with
    values from the RHS."""
    # WHEN
    result = lhs.merge(rhs)

    # THEN
    assert result == expected_result
