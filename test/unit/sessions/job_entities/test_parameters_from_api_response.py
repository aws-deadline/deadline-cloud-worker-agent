# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest

from openjd.model import ParameterValueType

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

    @pytest.mark.parametrize(
        "param_dict, expected_type, expected_value",
        [
            # True-valued vocabulary, case-insensitive.
            pytest.param({"bool": "true"}, ParameterValueType.BOOL, True, id="bool-true"),
            pytest.param({"bool": "TRUE"}, ParameterValueType.BOOL, True, id="bool-TRUE"),
            pytest.param({"bool": "True"}, ParameterValueType.BOOL, True, id="bool-True"),
            pytest.param({"bool": "yes"}, ParameterValueType.BOOL, True, id="bool-yes"),
            pytest.param({"bool": "YES"}, ParameterValueType.BOOL, True, id="bool-YES"),
            pytest.param({"bool": "on"}, ParameterValueType.BOOL, True, id="bool-on"),
            pytest.param({"bool": "ON"}, ParameterValueType.BOOL, True, id="bool-ON"),
            pytest.param({"bool": "1"}, ParameterValueType.BOOL, True, id="bool-1"),
            pytest.param({"bool": "1.0"}, ParameterValueType.BOOL, True, id="bool-1.0"),
            # False-valued vocabulary, case-insensitive.
            pytest.param({"bool": "false"}, ParameterValueType.BOOL, False, id="bool-false"),
            pytest.param({"bool": "FALSE"}, ParameterValueType.BOOL, False, id="bool-FALSE"),
            pytest.param({"bool": "False"}, ParameterValueType.BOOL, False, id="bool-False"),
            pytest.param({"bool": "no"}, ParameterValueType.BOOL, False, id="bool-no"),
            pytest.param({"bool": "NO"}, ParameterValueType.BOOL, False, id="bool-NO"),
            pytest.param({"bool": "off"}, ParameterValueType.BOOL, False, id="bool-off"),
            pytest.param({"bool": "OFF"}, ParameterValueType.BOOL, False, id="bool-OFF"),
            pytest.param({"bool": "0"}, ParameterValueType.BOOL, False, id="bool-0"),
            pytest.param({"bool": "0.0"}, ParameterValueType.BOOL, False, id="bool-0.0"),
            # Native booleans pass through unchanged (pre-migration jobs).
            pytest.param({"bool": True}, ParameterValueType.BOOL, True, id="bool-native-true"),
            pytest.param({"bool": False}, ParameterValueType.BOOL, False, id="bool-native-false"),
            pytest.param(
                {"boolList": ["true", "false"]},
                ParameterValueType.LIST_BOOL,
                [True, False],
                id="boollist-string",
            ),
            pytest.param(
                {"boolList": []},
                ParameterValueType.LIST_BOOL,
                [],
                id="boollist-empty",
            ),
            pytest.param(
                {"boolList": [True, False]},
                ParameterValueType.LIST_BOOL,
                [True, False],
                id="boollist-native",
            ),
            pytest.param(
                {"boolList": ["yes", "off", True]},
                ParameterValueType.LIST_BOOL,
                [True, False, True],
                id="boollist-mixed-vocabulary",
            ),
        ],
    )
    def test_bool_parameters_coerced_to_native(
        self, param_dict, expected_type, expected_value
    ) -> None:
        # GIVEN
        params = {"boolParam": param_dict}

        # WHEN
        result = parameters_from_api_response(params)

        # THEN
        assert len(result) == 1
        assert result["boolParam"].type == expected_type
        assert result["boolParam"].value == expected_value
        # Guard against a truthy-but-non-bool value (e.g. the raw string "true"
        # or an int) sneaking through: the coerced result must be native bool.
        coerced = result["boolParam"].value
        if isinstance(coerced, list):
            assert all(type(element) is bool for element in coerced)
        else:
            assert type(coerced) is bool

    @pytest.mark.parametrize(
        "param_dict, match",
        [
            # Not in the vocabulary.
            pytest.param({"bool": "maybe"}, r"got 'maybe'", id="bool-maybe-rejected"),
            pytest.param({"bool": ""}, r"got ''", id="bool-empty-string-rejected"),
            pytest.param({"bool": "2"}, r"got '2'", id="bool-two-string-rejected"),
            # Zero-padded / extra-precision numeric forms outside the pattern.
            pytest.param({"bool": "01"}, r"got '01'", id="bool-01-rejected"),
            pytest.param({"bool": "00"}, r"got '00'", id="bool-00-rejected"),
            pytest.param({"bool": "1.00"}, r"got '1\.00'", id="bool-1.00-rejected"),
            pytest.param({"bool": "0.00"}, r"got '0\.00'", id="bool-0.00-rejected"),
            pytest.param({"bool": "truee"}, r"got 'truee'", id="bool-truee-rejected"),
            # Surrounding whitespace is not permitted (no strip()).
            pytest.param({"bool": " true"}, r"got ' true'", id="bool-leading-space-rejected"),
            pytest.param({"bool": "true "}, r"got 'true '", id="bool-trailing-space-rejected"),
            # Native ints must not be treated as booleans.
            pytest.param({"bool": 0}, r"got 0", id="bool-zero-int-rejected"),
            pytest.param({"bool": 1}, r"got 1", id="bool-one-int-rejected"),
            # Other non-str, non-bool types.
            pytest.param({"bool": None}, r"got None", id="bool-none-rejected"),
            pytest.param({"bool": 1.5}, r"got 1\.5", id="bool-float-rejected"),
            pytest.param({"bool": ["true"]}, r"got \['true'\]", id="bool-list-rejected"),
            pytest.param({"bool": {}}, r"got \{\}", id="bool-dict-rejected"),
            # Bad element inside an otherwise-valid list.
            pytest.param(
                {"boolList": ["true", "maybe"]}, r"got 'maybe'", id="boollist-bad-element-rejected"
            ),
            # A non-list boolList value must be rejected with a ValueError, not
            # a TypeError from attempting to iterate a non-iterable. The decode
            # path shape-checks before iterating so callers only ever have to
            # catch ValueError for a malformed boolean parameter.
            pytest.param(
                {"boolList": None}, r"to be a list but got None", id="boollist-none-rejected"
            ),
            pytest.param(
                {"boolList": "true"}, r"to be a list but got 'true'", id="boollist-string-rejected"
            ),
            pytest.param({"boolList": 5}, r"to be a list but got 5", id="boollist-int-rejected"),
        ],
    )
    def test_bool_parameters_reject_invalid_values(self, param_dict, match) -> None:
        # WHEN / THEN
        with pytest.raises(ValueError, match=match):
            parameters_from_api_response({"boolParam": param_dict})
