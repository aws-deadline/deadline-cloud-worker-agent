# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from typing import cast

import pytest

from openjd.model import ParameterValue, ParameterValueType

from deadline_worker_agent.sessions.job_entities.job_details import parameters_from_api_response


class TestParametersFromApiResponse:
    @pytest.mark.parametrize(
        "param_name, param_dict, expected_type, expected_value",
        [
            ("stringParam", {"string": "value"}, ParameterValueType.STRING, "value"),
            ("pathParam", {"path": "/path/to/file"}, ParameterValueType.PATH, "/path/to/file"),
            ("intParam", {"int": "42"}, ParameterValueType.INT, "42"),
            ("floatParam", {"float": "3.14"}, ParameterValueType.FLOAT, "3.14"),
            ("chunkIntParam", {"chunkInt": "1-5"}, ParameterValueType.CHUNK_INT, "1-5"),
            ("boolParam", {"bool": True}, ParameterValueType.BOOL, True),
            ("rangeExprParam", {"rangeExpr": "1-10:2"}, ParameterValueType.RANGE_EXPR, "1-10:2"),
            (
                "stringListParam",
                {"stringList": ["a", "b", "c"]},
                ParameterValueType.LIST_STRING,
                ["a", "b", "c"],
            ),
            (
                "pathListParam",
                {"pathList": ["/path/a", "/path/b"]},
                ParameterValueType.LIST_PATH,
                ["/path/a", "/path/b"],
            ),
            (
                "intListParam",
                {"intList": ["1", "2", "3"]},
                ParameterValueType.LIST_INT,
                ["1", "2", "3"],
            ),
            (
                "floatListParam",
                {"floatList": ["1.1", "2.2"]},
                ParameterValueType.LIST_FLOAT,
                ["1.1", "2.2"],
            ),
            (
                "boolListParam",
                {"boolList": [True, False, True]},
                ParameterValueType.LIST_BOOL,
                [True, False, True],
            ),
            (
                "intListListParam",
                {"intListList": [["1", "2"], ["3", "4"]]},
                ParameterValueType.LIST_LIST_INT,
                [["1", "2"], ["3", "4"]],
            ),
        ],
    )
    def test_parameters_from_api_response(
        self, param_name, param_dict, expected_type, expected_value
    ):
        # GIVEN
        params = {param_name: param_dict}

        # WHEN
        result = parameters_from_api_response(params)

        # THEN
        assert len(result) == 1
        assert result[param_name].type == expected_type
        assert result[param_name].value == expected_value


# The service now transmits scalar boolean job parameters as constrained
# strings from Open Job Description's case-insensitive boolean vocabulary, but
# jobs created before that change may still be served with native JSON
# booleans. Both wire forms must decode to a native Python bool at this
# boundary. Expected values below are literal, derived from the specification
# vocabulary rather than from the implementation.
@pytest.mark.parametrize(
    "wire_value, expected",
    [
        # Word tokens: lowercase plus one cased variant each.
        pytest.param("true", True, id="true-lower"),
        pytest.param("True", True, id="true-cased"),
        pytest.param("false", False, id="false-lower"),
        pytest.param("False", False, id="false-cased"),
        pytest.param("yes", True, id="yes-lower"),
        pytest.param("YES", True, id="yes-upper"),
        pytest.param("no", False, id="no-lower"),
        pytest.param("No", False, id="no-cased"),
        pytest.param("on", True, id="on-lower"),
        pytest.param("ON", True, id="on-upper"),
        pytest.param("off", False, id="off-lower"),
        pytest.param("Off", False, id="off-cased"),
        # Numeric tokens have no alphabetic case to vary.
        pytest.param("1", True, id="one"),
        pytest.param("0", False, id="zero"),
        pytest.param("1.0", True, id="one-float"),
        pytest.param("0.0", False, id="zero-float"),
        # Native booleans (pre-change jobs) pass through unchanged.
        pytest.param(True, True, id="native-true"),
        pytest.param(False, False, id="native-false"),
    ],
)
def test_bool_parameter_decoded_when_string_or_native(
    wire_value: str | bool, expected: bool
) -> None:
    # GIVEN
    params = {"boolParam": {"bool": wire_value}}

    # WHEN
    result = parameters_from_api_response(cast(dict, params))

    # THEN
    assert result["boolParam"] == ParameterValue(type=ParameterValueType.BOOL, value=expected)


@pytest.mark.parametrize(
    "wire_value",
    [
        pytest.param("maybe", id="unrecognized-string"),
        pytest.param("", id="empty-string"),
        pytest.param(" true", id="leading-whitespace"),
        # bool is a subclass of int; the native int 1 must be rejected -- only
        # the *string* "1" is accepted.
        pytest.param(1, id="native-int-one"),
        pytest.param(None, id="none"),
    ],
)
def test_bool_parameter_rejected_when_value_invalid(wire_value: object) -> None:
    # GIVEN
    params = {"boolParam": {"bool": wire_value}}

    # WHEN / THEN
    with pytest.raises(ValueError, match=r"boolParam"):
        parameters_from_api_response(cast(dict, params))


def test_bool_list_parameter_decoded_when_elements_mix_string_and_native() -> None:
    # GIVEN
    params = {"boolListParam": {"boolList": ["true", False, "NO", "1.0", "off"]}}

    # WHEN
    result = parameters_from_api_response(cast(dict, params))

    # THEN
    assert result["boolListParam"] == ParameterValue(
        type=ParameterValueType.LIST_BOOL,
        value=[True, False, False, True, False],
    )


def test_bool_list_parameter_rejected_when_any_element_invalid() -> None:
    # GIVEN
    params = {"boolListParam": {"boolList": ["true", "maybe", False]}}

    # WHEN / THEN
    with pytest.raises(ValueError, match=r"boolListParam"):
        parameters_from_api_response(cast(dict, params))


def test_bool_list_parameter_rejected_when_value_not_a_list() -> None:
    # GIVEN a boolList whose value is not a list; it must raise ValueError
    # naming the parameter rather than leaking a TypeError from iteration.
    params = {"boolListParam": {"boolList": None}}

    # WHEN / THEN
    with pytest.raises(ValueError, match=r"boolListParam"):
        parameters_from_api_response(cast(dict, params))
