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
