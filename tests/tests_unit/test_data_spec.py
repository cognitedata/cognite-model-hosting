import json
import time
from collections import namedtuple
from datetime import datetime, timedelta
from unittest import mock

import pytest

from cognite.model_hosting.data_spec import (
    DataSpec,
    FileSpec,
    ScheduleDataSpec,
    ScheduleInputSpec,
    ScheduleInputTimeSeriesSpec,
    ScheduleOutputSpec,
    ScheduleOutputTimeSeriesSpec,
    TimeSeriesSpec,
)
from cognite.model_hosting.data_spec.exceptions import SpecValidationError


class TestSpecValidation:
    TestCase = namedtuple("TestCase", ["name", "obj", "primitive"])
    InvalidTestCase = namedtuple("TestCase", ["name", "type", "constructor", "primitive", "errors"])

    valid_test_cases = [
        TestCase("minimal_file_spec", FileSpec(id=6), {"id": 6}),
        TestCase(
            "minimal_time_series_spec", TimeSeriesSpec(id=6, start=123, end=234), {"id": 6, "start": 123, "end": 234}
        ),
        TestCase(
            "time_series_include_outside_points",
            TimeSeriesSpec(id=6, start=123, end=234, include_outside_points=True),
            {"id": 6, "start": 123, "end": 234, "includeOutsidePoints": True},
        ),
        TestCase(
            "time_series_aggregate",
            TimeSeriesSpec(id=6, start=123, end=234, aggregate="average", granularity="1m"),
            {"id": 6, "start": 123, "end": 234, "aggregate": "average", "granularity": "1m"},
        ),
        TestCase(
            "schedule_input_time_series",
            ScheduleInputTimeSeriesSpec(id=6, aggregate="average", granularity="1m"),
            {"id": 6, "aggregate": "average", "granularity": "1m"},
        ),
        TestCase("empty_data_spec", DataSpec(), {}),
        TestCase(
            "full_data_spec",
            DataSpec(
                time_series={
                    "ts1": TimeSeriesSpec(id=6, start=123, end=234),
                    "ts2": TimeSeriesSpec(id=7, start=1234, end=2345),
                },
                files={"f1": FileSpec(id=3), "f2": FileSpec(id=4)},
            ),
            {
                "timeSeries": {
                    "ts1": {"id": 6, "start": 123, "end": 234},
                    "ts2": {"id": 7, "start": 1234, "end": 2345},
                },
                "files": {"f1": {"id": 3}, "f2": {"id": 4}},
            },
        ),
        TestCase("data_spec_with_metadata", DataSpec(metadata={"bla": "bla"}), {"metadata": {"bla": "bla"}}),
        TestCase(
            "full_data_spec_with_metadata",
            DataSpec(
                time_series={
                    "ts1": TimeSeriesSpec(id=6, start=123, end=234),
                    "ts2": TimeSeriesSpec(id=7, start=1234, end=2345),
                },
                files={"f1": FileSpec(id=3), "f2": FileSpec(id=4)},
                metadata={"key1": "value", "key2": 1},
            ),
            {
                "timeSeries": {
                    "ts1": {"id": 6, "start": 123, "end": 234},
                    "ts2": {"id": 7, "start": 1234, "end": 2345},
                },
                "files": {"f1": {"id": 3}, "f2": {"id": 4}},
                "metadata": {"key1": "value", "key2": 1},
            },
        ),
        TestCase("minimal_schedule_input_spec", ScheduleInputSpec(), {}),
        TestCase(
            "full_schedule_input_spec",
            ScheduleInputSpec(
                time_series={"ts1": ScheduleInputTimeSeriesSpec(id=6), "ts2": ScheduleInputTimeSeriesSpec(id=7)}
            ),
            {"timeSeries": {"ts1": {"id": 6}, "ts2": {"id": 7}}},
        ),
        TestCase(
            "schedule_output_time_series_spec",
            ScheduleOutputTimeSeriesSpec(id=123, offset=-5),
            {"id": 123, "offset": -5},
        ),
        TestCase("minimal_schedule_output_spec", ScheduleOutputSpec(), {}),
        TestCase(
            "full_schedule_output_spec",
            ScheduleOutputSpec(
                time_series={
                    "ts1": ScheduleOutputTimeSeriesSpec(id=123, offset=5),
                    "ts2": ScheduleOutputTimeSeriesSpec(id=234, offset=0),
                }
            ),
            {"timeSeries": {"ts1": {"id": 123, "offset": 5}, "ts2": {"id": 234, "offset": 0}}},
        ),
        TestCase(
            "minimal_schedule_data_spec",
            ScheduleDataSpec(input=ScheduleInputSpec(), output=ScheduleOutputSpec(), stride=1, window_size=2, start=3),
            {"input": {}, "output": {}, "stride": 1, "windowSize": 2, "start": 3, "slack": 0},
        ),
        TestCase(
            "full_schedule_data_spec",
            ScheduleDataSpec(
                input=ScheduleInputSpec(time_series={"ts1": ScheduleInputTimeSeriesSpec(id=5)}),
                output=ScheduleOutputSpec(time_series={"ts1": ScheduleOutputTimeSeriesSpec(id=123, offset=100)}),
                stride=1,
                window_size=2,
                start=3,
                slack=4,
            ),
            {
                "input": {"timeSeries": {"ts1": {"id": 5}}},
                "output": {"timeSeries": {"ts1": {"id": 123, "offset": 100}}},
                "stride": 1,
                "windowSize": 2,
                "start": 3,
                "slack": 4,
            },
        ),
        TestCase(
            "schedule_data_spec_with_aggregates",
            ScheduleDataSpec(
                input=ScheduleInputSpec(
                    time_series={
                        "ts1": ScheduleInputTimeSeriesSpec(id=5, aggregate="average", granularity="3h"),
                        "ts2": ScheduleInputTimeSeriesSpec(id=6, aggregate="max", granularity="10m"),
                    }
                ),
                output=ScheduleOutputSpec(),
                stride=60 * 60 * 1000,
                window_size=3 * 60 * 60 * 1000,
                start=123 * 60 * 60 * 1000,
            ),
            {
                "input": {
                    "timeSeries": {
                        "ts1": {"id": 5, "aggregate": "average", "granularity": "3h"},
                        "ts2": {"id": 6, "aggregate": "max", "granularity": "10m"},
                    }
                },
                "output": {},
                "stride": 60 * 60 * 1000,
                "windowSize": 3 * 60 * 60 * 1000,
                "start": 123 * 60 * 60 * 1000,
                "slack": 0,
            },
        ),
    ]

    invalid_test_cases = [
        InvalidTestCase(
            name="file_missing_id",
            type=FileSpec,
            constructor=lambda: FileSpec(id=None),
            primitive={},
            errors={"id": ["Missing data for required field."]},
        ),
        InvalidTestCase(
            name="time_series_missing_id",
            type=TimeSeriesSpec,
            constructor=lambda: TimeSeriesSpec(id=None, start=1, end=2),
            primitive={"start": 1, "end": 2},
            errors={"id": ["Missing data for required field."]},
        ),
        InvalidTestCase(
            name="time_series_missing_fields",
            type=TimeSeriesSpec,
            constructor=None,
            primitive={},
            errors={
                "id": ["Missing data for required field."],
                "start": ["Missing data for required field."],
                "end": ["Missing data for required field."],
            },
        ),
        InvalidTestCase(
            name="time_series_aggregates_but_not_granularity",
            type=TimeSeriesSpec,
            constructor=lambda: TimeSeriesSpec(id=6, start=123, end=234, aggregate="average"),
            primitive={"id": 6, "start": 123, "end": 234, "aggregate": "average"},
            errors={"granularity": ["granularity must be specified for aggregates."]},
        ),
        InvalidTestCase(
            name="time_series_aggregates_but_include_outside_points",
            type=TimeSeriesSpec,
            constructor=lambda: TimeSeriesSpec(
                id=6, start=123, end=234, aggregate="average", granularity="1m", include_outside_points=True
            ),
            primitive={
                "id": 6,
                "start": 123,
                "end": 234,
                "aggregate": "average",
                "granularity": "1m",
                "includeOutsidePoints": True,
            },
            errors={"includeOutsidePoints": ["Can't include outside points for aggregates."]},
        ),
        InvalidTestCase(
            name="time_series_invalid_aggregate_function",
            type=TimeSeriesSpec,
            constructor=lambda: TimeSeriesSpec(id=6, start=123, end=234, aggregate="avg", granularity="1m"),
            primitive={"id": 6, "start": 123, "end": 234, "aggregate": "avg", "granularity": "1m"},
            errors={
                "aggregate": ["Not a valid aggregate function. Cannot use shorthand name."],
                "granularity": ["granularity can only be specified for aggregates."],
            },
        ),
        InvalidTestCase(
            name="time_series_not_aggregate_but_granularity",
            type=TimeSeriesSpec,
            constructor=lambda: TimeSeriesSpec(id=6, start=123, end=234, granularity="1m"),
            primitive={"id": 6, "start": 123, "end": 234, "granularity": "1m"},
            errors={"granularity": ["granularity can only be specified for aggregates."]},
        ),
        InvalidTestCase(
            name="time_series_invalid_granularity",
            type=TimeSeriesSpec,
            constructor=lambda: TimeSeriesSpec(id=6, start=123, end=234, granularity="bla"),
            primitive={"id": 6, "start": 123, "end": 234, "granularity": "bla"},
            errors={
                "granularity": [
                    "Invalid granularity format: `bla`. Must be on format <integer>(s|m|h|d). E.g. '5m', '3h' or '1d'."
                ]
            },
        ),
        InvalidTestCase(
            name="data_spec_nested_errors",
            type=DataSpec,
            constructor=lambda: DataSpec(files={"f1": FileSpec(id=None)}),
            primitive={"files": {"f1": {}}},
            errors={"files": {"f1": {"value": {"id": ["Missing data for required field."]}}}},
        ),
        InvalidTestCase(
            name="data_spec_with_metadata_invalid_type",
            type=DataSpec,
            constructor=lambda: DataSpec(metadata={"keybla": ["valuebla"]}),
            primitive={"metadata": {"keybla": ["valuebla"]}},
            errors={
                "metadata": {
                    "keybla": {
                        "value": [
                            "Invalid metadata value type '<class 'list'>'. Must be one of (<class 'str'>, <class 'numbers.Number'>)."
                        ]
                    }
                }
            },
        ),
        InvalidTestCase(
            name="data_spec_invalid_alias",
            type=DataSpec,
            constructor=lambda: DataSpec(
                files={"1f": FileSpec(id=1)}, time_series={"F1": TimeSeriesSpec(id=1, start=0, end=10)}
            ),
            primitive={"files": {"1f": {"id": 1}}, "timeSeries": {"F1": {"id": 1, "start": 0, "end": 10}}},
            errors={
                "files": {
                    "1f": {
                        "key": [
                            "Invalid alias. Must be 1 to 50 lowercase alphanumeric characters or `_`. Must start with a letter and cannot end with `_` ."
                        ]
                    }
                },
                "timeSeries": {
                    "F1": {
                        "key": [
                            "Invalid alias. Must be 1 to 50 lowercase alphanumeric characters or `_`. Must start with a letter and cannot end with `_` ."
                        ]
                    }
                },
            },
        ),
        # TODO add when Marshmallow fixes inconsistencies (https://github.com/marshmallow-code/marshmallow/issues/1132)
        # InvalidTestCase(
        #     name="schedule_data_spec_nested_errors",
        #     type=ScheduleDataSpec,
        #     constructor=lambda: ScheduleDataSpec(
        #         stride="1m", window_size="5m", time_series={"ts1": ScheduleTimeSeriesSpec(id="abc")}
        #     ),
        #     primitive={"stride": "1m", "windowSize": "5m", "timeSeries": {"ts1": {"id": "abc"}}},
        #     errors={"timeSeries": {"ts1": {"value": {"id": ["Not a valid integer."]}}}},
        # ),
        InvalidTestCase(
            name="schedule_input_time_series_with_start_end",
            type=ScheduleInputTimeSeriesSpec,
            constructor=None,
            primitive={"id": 6, "start": 123, "end": 234},
            errors={"start": ["Unknown field."], "end": ["Unknown field."]},
        ),
        InvalidTestCase(
            name="schedule_output_time_series_spec_missing_fields",
            type=ScheduleOutputTimeSeriesSpec,
            constructor=lambda: ScheduleOutputTimeSeriesSpec(id=None, offset=None),
            primitive={},
            errors={"id": ["Missing data for required field."], "offset": ["Missing data for required field."]},
        ),
        InvalidTestCase(
            name="schedule_data_spec_missing_fields",
            type=ScheduleDataSpec,
            constructor=None,
            primitive={"start": 123},
            errors={
                "input": ["Missing data for required field."],
                "output": ["Missing data for required field."],
                "windowSize": ["Missing data for required field."],
                "stride": ["Missing data for required field."],
            },
        ),
        InvalidTestCase(
            name="schedule_data_spec_invalid_stride_window_size",
            type=ScheduleDataSpec,
            constructor=None,
            primitive={"input": {}, "output": {}, "windowSize": 0, "stride": -1, "start": 123},
            errors={"stride": ["Must be at least 1."], "windowSize": ["Must be at least 1."]},
        ),
        InvalidTestCase(
            name="schedule_data_spec_invalid_stride_window_size_start_multiple_of_largest_granularity_unit",
            type=ScheduleDataSpec,
            constructor=None,
            primitive={
                "input": {
                    "timeSeries": {
                        "ts1": {"id": 5, "aggregate": "average", "granularity": "3h"},
                        "ts2": {"id": 6, "aggregate": "max", "granularity": "10m"},
                    }
                },
                "output": {},
                "windowSize": 123123,
                "stride": 123123,
                "start": 123123,
            },
            errors={
                "stride": ["Must be a multiple of the largest granularity unit in the input time series."],
                "windowSize": [
                    "Must be greater than or equal to the largest granularity of any of aggregated input time series."
                ],
                "start": ["Must be a multiple of the largest granularity unit in the input time series."],
            },
        ),
    ]

    @pytest.mark.parametrize("name, obj, primitive", valid_test_cases)
    def test_valid_dump(self, name, obj, primitive):
        dumped = obj.dump()
        assert dumped == primitive

    @pytest.mark.parametrize("name, obj, primitive", valid_test_cases)
    def test_valid_load(self, name, obj, primitive):
        loaded = obj.__class__.load(primitive)
        assert loaded == obj

    @pytest.mark.parametrize("name, obj, primitive", valid_test_cases)
    def test_valid_json_serializable(self, name, obj, primitive):
        json_data = obj.to_json()
        from_json = obj.__class__.from_json(json_data)
        assert from_json == obj, "\nFrom JSON: ({})\n{}\nObj:({})\n{}\n".format(
            type(from_json), from_json, type(obj), obj
        )

    def remove_defaultdict_in_errors(self, d):
        return json.loads(json.dumps(d))

    @pytest.mark.parametrize("name, type, constructor, primitive, errors", invalid_test_cases)
    def test_invalid(self, name, type, constructor, primitive, errors):
        should_fail = {"load": lambda: type.load(primitive), "from_json": lambda: type.from_json(json.dumps(primitive))}
        if constructor is not None:
            if type in (DataSpec, ScheduleDataSpec):
                should_fail["constructor"] = constructor
            else:
                should_fail["dump"] = constructor().dump
                should_fail["to_json"] = constructor().to_json

        for method_name, method in should_fail.items():
            with pytest.raises(SpecValidationError) as excinfo:
                method()

            actual_errors = self.remove_defaultdict_in_errors(excinfo.value.errors)
            if actual_errors != errors:
                pytest.fail("\nMethod: {}\nErrors:\n{}\nExpected:\n{}\n".format(name, actual_errors, errors))

    @pytest.mark.parametrize("name, obj, primitive", valid_test_cases)
    def test_valid_str_repr(self, name, obj, primitive):
        assert str(obj) == obj.to_json()


def test_validation_exception_str_repr():
    e = SpecValidationError({"key": "value"})
    assert str(e) == '{\n    "key": "value"\n}'


def test_copy():
    data_spec = DataSpec(files={"f1": FileSpec(id=123)})
    data_spec_copied = data_spec.copy()

    assert data_spec == data_spec_copied

    data_spec.files["f1"].id = 234

    assert data_spec != data_spec_copied


class TestSpecConstructor:
    @pytest.fixture(autouse=True, scope="class")
    def mock_time(self):
        with mock.patch("cognite.model_hosting._cognite_model_hosting_common.utils.time.time") as m:
            m.return_value = 10 ** 6
            yield

    TestCase = namedtuple("TestCase", ["name", "constructor", "primitive"])
    InvalidTestCase = namedtuple("InvalidTestCase", ["name", "constructor", "exception", "error_match"])

    test_cases = [
        TestCase(
            name="time_series_time_ago",
            constructor=lambda: TimeSeriesSpec(id=123, start="2m-ago", end="now"),
            primitive={"id": 123, "start": 10 ** 9 - 2 * 60 * 1000, "end": 10 ** 9},
        ),
        TestCase(
            name="time_series_datetime",
            constructor=lambda: TimeSeriesSpec(id=123, start=datetime(2018, 1, 1), end=datetime(2018, 1, 2)),
            primitive={"id": 123, "start": 1514764800000, "end": 1514851200000},
        ),
        TestCase(
            name="schedule_data_spec_default_start_now",
            constructor=lambda: ScheduleDataSpec(
                input=ScheduleInputSpec(), output=ScheduleOutputSpec(), stride=123, window_size=234, slack=345
            ),
            primitive={"input": {}, "output": {}, "stride": 123, "windowSize": 234, "start": 10 ** 9, "slack": 345},
        ),
        TestCase(
            name="schedule_data_spec_default_slack_zero",
            constructor=lambda: ScheduleDataSpec(
                input=ScheduleInputSpec(), output=ScheduleOutputSpec(), stride=123, window_size=234
            ),
            primitive={"input": {}, "output": {}, "stride": 123, "windowSize": 234, "start": 10 ** 9, "slack": 0},
        ),
        TestCase(
            name="schedule_data_spec_string_formats",
            constructor=lambda: ScheduleDataSpec(
                input=ScheduleInputSpec(),
                output=ScheduleOutputSpec(),
                stride="1m",
                window_size="2m",
                start="2m-ago",
                slack="3m",
            ),
            primitive={
                "input": {},
                "output": {},
                "stride": 60000,
                "windowSize": 120000,
                "start": 10 ** 9 - 2 * 60 * 1000,
                "slack": 180000,
            },
        ),
        TestCase(
            name="schedule_data_spec_datetime",
            constructor=lambda: ScheduleDataSpec(
                input=ScheduleInputSpec(),
                output=ScheduleOutputSpec(),
                stride=timedelta(minutes=1),
                window_size=timedelta(minutes=2),
                start=datetime(2018, 1, 1),
                slack=timedelta(minutes=3),
            ),
            primitive={
                "input": {},
                "output": {},
                "stride": 60000,
                "windowSize": 120000,
                "start": 1514764800000,
                "slack": 180000,
            },
        ),
    ]

    invalid_test_cases = [
        InvalidTestCase(
            name="time_series_start_none",
            constructor=lambda: TimeSeriesSpec(id=123, start=None, end=2),
            exception=TypeError,
            error_match="type",
        ),
        InvalidTestCase(
            name="time_series_start_none",
            constructor=lambda: TimeSeriesSpec(id=123, start=2, end=None),
            exception=TypeError,
            error_match="type",
        ),
    ]

    @pytest.mark.parametrize("name, constructor, primitive", test_cases)
    def test_valid(self, name, constructor, primitive):
        spec = constructor()
        assert spec.dump() == primitive

    @pytest.mark.parametrize("name, constructor, exception, error_match", invalid_test_cases)
    def test_invalid(self, name, constructor, exception, error_match):
        with pytest.raises(exception, match=error_match):
            constructor()


class TestSpecWithTimeAgoFormat:
    def test_create_multiple_ts_specs_aligned_start_end(self):
        specs_before = []
        for i in range(30):
            specs_before.append(TimeSeriesSpec(id=i, start="1d-ago", end="now"))

        time.sleep(0.2)

        specs_after = []
        for i in range(30):
            specs_after.append(TimeSeriesSpec(id=i, start="1d-ago", end="now"))

        assert 1 == len(set([(spec.start, spec.end) for spec in specs_before]))
        assert 1 == len(set([(spec.start, spec.end) for spec in specs_after]))
        assert 2 == len(set([(spec.start, spec.end) for spec in specs_before + specs_after]))


def test_schedule_data_spec_schema_slack_missing_default_to_zero():
    schedule_data_spec = ScheduleDataSpec.load({"input": {}, "output": {}, "stride": 1, "windowSize": 1, "start": 1})
    assert 0 == schedule_data_spec.slack


def test_empty_specs_default_fields_to_empty_dict():
    ds = ScheduleDataSpec(input=ScheduleInputSpec(), output=ScheduleOutputSpec(), stride=1, window_size=1, start=1)
    assert {} == ds.input.time_series
    assert {} == ds.output.time_series
