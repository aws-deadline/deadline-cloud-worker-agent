# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for the deadline_worker_agent.startup.capabilities module"""

from typing import Any
from pydantic import ValidationError
import pytest

from deadline_worker_agent.startup.capabilities import Capabilities


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(
            {"amounts": {}, "attributes": {}},
            id="empty fields",
        ),
        pytest.param(
            {
                "amounts": {
                    "amount.slots": 20,
                    "deadline:amount.pets": 99,
                },
                "attributes": {
                    "attr.groups": ["simulation"],
                    "acmewidgetsco:attr.admins": ["bob", "alice"],
                },
            },
            id="full fields",
        ),
    ],
)
def test_input_validation_success(data: dict[str, Any]) -> None:
    """Asserts that a valid input dictionary passes Capabilities model validation"""
    Capabilities.parse_obj(data)


@pytest.mark.parametrize(
    "data",
    [
        pytest.param({}, id="missing amounts and attributes"),
        pytest.param({"attributes": {}}, id="missing amounts"),
        pytest.param({"amounts": {}}, id="missing attributes"),
        pytest.param(
            {"amounts": {"amount": 20}, "attributes": {}},
            id="invalid amounts - a dictionary key is invalid (no segment)",
        ),
        pytest.param(
            {"amounts": {"amount.0seg": 20}, "attributes": {}},
            id="invalid amounts - a dictionary key is invalid (invalid segment)",
        ),
        pytest.param(
            {"amounts": {"not_amount.slots": 20}, "attributes": {}},
            id="invalid amounts - a dictionary key is invalid (invalid capability name)",
        ),
        pytest.param(
            {"amounts": {"amount.slots": -20}, "attributes": {}},
            id="invalid amounts - a dictionary value is not NonNegativeFloat",
        ),
        pytest.param(
            {"amounts": {}, "attributes": {"attr": ["a", "b"]}},
            id="invalid attributes  - a dictionary key is invalid (no segment)",
        ),
        pytest.param(
            {"amounts": {}, "attributes": {"attr.(seg)": ["a", "b"]}},
            id="invalid attributes  - a dictionary key is invalid (invalid segment)",
        ),
        pytest.param(
            {"amounts": {}, "attributes": {"not_attr.groups": ["a", "b"]}},
            id="invalid attributes  - a dictionary key is invalid (invalid capability name)",
        ),
        pytest.param(
            {"amounts": {}, "attributes": {"attr.groups": "a"}},
            id="invalid attributes  - a dictionary value is not list[str]",
        ),
    ],
)
def test_input_validation_failure(data: dict[str, Any]) -> None:
    """Tests that an invalid input dictionary fails Capabilities model validation"""
    # WHEN
    with pytest.raises(ValidationError) as excinfo:
        Capabilities.parse_obj(data)

    # THEN
    assert len(excinfo.value.errors()) > 0


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
