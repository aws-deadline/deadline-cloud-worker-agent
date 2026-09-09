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
