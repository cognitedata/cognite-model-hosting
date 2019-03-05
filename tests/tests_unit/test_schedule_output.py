import json
from collections import namedtuple

import pandas as pd
import pytest

from cognite.model_hosting.schedules import ScheduleOutput, to_output
from cognite.model_hosting.schedules.exceptions import (
    DataframeMissingTimestampColumn,
    DuplicateAliasInScheduledOutput,
    InvalidScheduleOutputFormat,
)


class TestConvertToOutput:
    ValidTestCase = namedtuple("TestCase", ["input", "expected_output"])

    valid_test_cases = [
        ValidTestCase(
            input=pd.DataFrame({"timestamp": [1000, 2000, 3000], "x": [1, 2, 3]}),
            expected_output={"timeSeries": {"x": [[1000, 1], [2000, 2], [3000, 3]]}},
        ),
        ValidTestCase(
            input=pd.DataFrame({"timestamp": [1000, 2000, 3000], "x": [1, 2, 3], "y": [4, 5, 6]}),
            expected_output={
                "timeSeries": {"x": [[1000, 1], [2000, 2], [3000, 3]], "y": [[1000, 4], [2000, 5], [3000, 6]]}
            },
        ),
        ValidTestCase(
            input=[
                pd.DataFrame({"timestamp": [1000, 2000, 3000], "x": [1, 2, 3], "y": [4, 5, 6]}),
                pd.DataFrame({"timestamp": [4000, 5000, 6000], "z": [7, 8, 9]}),
            ],
            expected_output={
                "timeSeries": {
                    "x": [[1000, 1], [2000, 2], [3000, 3]],
                    "y": [[1000, 4], [2000, 5], [3000, 6]],
                    "z": [[4000, 7], [5000, 8], [6000, 9]],
                }
            },
        ),
    ]

    @pytest.mark.parametrize("input, expected_output", valid_test_cases)
    def test_convert_to_output_format_ok(self, input, expected_output):
        assert expected_output == to_output(input)

    def test_convert_to_output_no_timestamp(self):
        with pytest.raises(DataframeMissingTimestampColumn, match="missing"):
            to_output(pd.DataFrame({"x": [1, 2, 3], "y": [2, 3, 4]}))

    def test_convert_to_output_duplicate_alias(self):
        with pytest.raises(DuplicateAliasInScheduledOutput, match="multiple"):
            to_output([pd.DataFrame({"x": [1], "timestamp": [1]}), pd.DataFrame({"x": [1], "timestamp": [1]})])


class TestValidateOutputFormat:
    InvalidTestCase = namedtuple("InvalidTestCase", ["input", "error_msg"])
    invalid_test_cases = [
        InvalidTestCase(input={"time_series": {}}, error_msg={"time_series": ["Unknown field."]}),
        InvalidTestCase(
            input={"timeSeries": {"x": [1]}}, error_msg={"timeSeries": {"x": {"value": {"0": ["Not a valid list."]}}}}
        ),
        InvalidTestCase(
            input={"timeSeries": {"x": [[1]]}}, error_msg={"timeSeries": {"x": {"value": {"0": ["Length must be 2."]}}}}
        ),
        InvalidTestCase(
            input={"timeSeries": [[1, 1], [1, 2]]}, error_msg={"timeSeries": ["Not a valid mapping type."]}
        ),
    ]

    @pytest.mark.parametrize("input, error_msg", invalid_test_cases)
    def test_schedule_output_invalid_format(self, input, error_msg):
        with pytest.raises(InvalidScheduleOutputFormat) as e:
            s = ScheduleOutput(input)
        assert json.dumps(e.value.errors) == json.dumps(error_msg)


class TestConvertToDatapoints:
    ValidTestCase = namedtuple("TestCase", ["input", "alias", "expected_output"])
    InvalidTestCase = namedtuple("InvalidTestCase", ["input", "alias", "match"])

    valid_test_cases = [
        ValidTestCase(
            input={"timeSeries": {"x": [[1000, 1], [2000, 2], [3000, 3]]}},
            alias="x",
            expected_output=pd.DataFrame({"timestamp": [1000, 2000, 3000], "x": [1, 2, 3]}),
        ),
        ValidTestCase(
            input={"timeSeries": {"x": [[1000, 1], [2000, 2], [3000, 3]], "y": [[1000, 4], [2000, 5], [3000, 6]]}},
            alias=["x", "y"],
            expected_output={
                "x": pd.DataFrame({"timestamp": [1000, 2000, 3000], "x": [1, 2, 3]}),
                "y": pd.DataFrame({"timestamp": [1000, 2000, 3000], "y": [4, 5, 6]}),
            },
        ),
    ]

    invalid_test_cases = [
        InvalidTestCase(
            input={"timeSeries": {"x": [[1000, 1], [2000, 2], [3000, 3]]}}, alias="z", match="z is not a valid alias"
        )
    ]

    @pytest.mark.parametrize("input, alias, expected_output", valid_test_cases)
    def test_get_datapoints_ok(self, input, alias, expected_output):
        so = ScheduleOutput(input)
        actual_output = so.get_datapoints(alias)
        if isinstance(expected_output, dict):
            for key in expected_output:
                pd.testing.assert_frame_equal(expected_output[key], actual_output[key], check_dtype=False)
        elif isinstance(expected_output, pd.DataFrame):
            pd.testing.assert_frame_equal(expected_output, actual_output, check_dtype=False)
        else:
            raise AssertionError("Expected output must be dict or DataFrame")

    @pytest.mark.parametrize("input, alias, match", invalid_test_cases)
    def test_get_datapoints_invalid(self, input, alias, match):
        with pytest.raises(AssertionError, match=match):
            so = ScheduleOutput(input)
            so.get_datapoints(alias)


class TestConvertToDataFrame:
    ValidTestCase = namedtuple("TestCase", ["input", "alias", "expected_output"])
    InvalidTestCase = namedtuple("InvalidTestCase", ["input", "alias", "match"])

    valid_test_cases = [
        ValidTestCase(
            input={"timeSeries": {"x": [[1000, 1], [2000, 2], [3000, 3]]}},
            alias="x",
            expected_output=pd.DataFrame({"timestamp": [1000, 2000, 3000], "x": [1, 2, 3]}),
        ),
        ValidTestCase(
            input={"timeSeries": {"x": [[1000, 1], [2000, 2], [3000, 3]], "y": [[1000, 4], [2000, 5], [3000, 6]]}},
            alias=["x", "y"],
            expected_output=pd.DataFrame({"timestamp": [1000, 2000, 3000], "x": [1, 2, 3], "y": [4, 5, 6]}),
        ),
    ]

    invalid_test_cases = [
        InvalidTestCase(
            input={"timeSeries": {"x": [[1000, 1], [2000, 2], [3000, 3]]}}, alias="z", match="z is not a valid alias"
        ),
        InvalidTestCase(
            input={"timeSeries": {"x": [[1000, 1], [2000, 2], [3000, 3]], "y": [[2000, 4], [3000, 5], [4000, 6]]}},
            alias=["x", "y"],
            match="Timestamps for aliases \['x', 'y'\] are not aligned",
        ),
    ]

    @pytest.mark.parametrize("input, alias, expected_output", valid_test_cases)
    def test_get_dataframe_ok(self, input, alias, expected_output):
        so = ScheduleOutput(input)
        actual_output = so.get_dataframe(alias)
        pd.testing.assert_frame_equal(expected_output, actual_output, check_dtype=False)

    @pytest.mark.parametrize("input, alias, match", invalid_test_cases)
    def test_get_dataframe_invalid(self, input, alias, match):
        with pytest.raises(AssertionError, match=match):
            so = ScheduleOutput(input)
            so.get_dataframe(alias)


class TestScheduleOutputImmutable:
    def test_output_immutable(self):
        output = {"timeSeries": {"x": [[0, 1]]}}
        so = ScheduleOutput(output)
        output["timeSeries"]["x"] = None
        assert so._output != output
