import json
from collections import namedtuple
from datetime import datetime
from unittest import mock

import pytest

from cognite.data_fetcher.data_spec import DataSpec, FileSpec, ScheduleDataSpec, ScheduleTimeSeriesSpec, TimeSeriesSpec
from cognite.data_fetcher.exceptions import SpecValidationError


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
            TimeSeriesSpec(id=6, start=123, end=234, aggregate="avg", granularity="1m"),
            {"id": 6, "start": 123, "end": 234, "aggregate": "avg", "granularity": "1m"},
        ),
        TestCase(
            "schedule_time_series",
            ScheduleTimeSeriesSpec(id=6, aggregate="avg", granularity="1m"),
            {"id": 6, "aggregate": "avg", "granularity": "1m"},
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
        TestCase(
            "minimal_schedule_data_spec",
            ScheduleDataSpec(stride="1m", window_size="5m", start=123),
            {"stride": "1m", "windowSize": "5m", "start": 123},
        ),
        TestCase(
            "full_schedule_data_spec",
            ScheduleDataSpec(
                stride="1m",
                window_size="5m",
                start=123,
                time_series={"ts1": ScheduleTimeSeriesSpec(id=6), "ts2": ScheduleTimeSeriesSpec(id=7)},
            ),
            {"stride": "1m", "windowSize": "5m", "start": 123, "timeSeries": {"ts1": {"id": 6}, "ts2": {"id": 7}}},
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
            constructor=lambda: TimeSeriesSpec(id=6, start=123, end=234, aggregate="avg"),
            primitive={"id": 6, "start": 123, "end": 234, "aggregate": "avg"},
            errors={"granularity": ["granularity must be specified for aggregates."]},
        ),
        InvalidTestCase(
            name="time_series_aggregates_but_include_outside_points",
            type=TimeSeriesSpec,
            constructor=lambda: TimeSeriesSpec(
                id=6, start=123, end=234, aggregate="avg", granularity="1m", include_outside_points=True
            ),
            primitive={
                "id": 6,
                "start": 123,
                "end": 234,
                "aggregate": "avg",
                "granularity": "1m",
                "includeOutsidePoints": True,
            },
            errors={"includeOutsidePoints": ["Can't include outside points for aggregates."]},
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
            errors={"granularity": ["Invalid granularity format. Must be e.g. '1d', '2hour', '60second'"]},
        ),
        InvalidTestCase(
            name="schedule_time_series_with_start_end",
            type=ScheduleTimeSeriesSpec,
            constructor=None,
            primitive={"id": 6, "start": 123, "end": 234},
            errors={"start": ["Unknown field."], "end": ["Unknown field."]},
        ),
        InvalidTestCase(
            name="schedule_data_spec_missing_fields",
            type=ScheduleDataSpec,
            constructor=lambda: ScheduleDataSpec(window_size=None, stride=None, start=123),
            primitive={"start": 123},
            errors={"windowSize": ["Missing data for required field."], "stride": ["Missing data for required field."]},
        ),
        InvalidTestCase(
            name="schedule_data_spec_invalid_stride_window_size",
            type=ScheduleDataSpec,
            constructor=lambda: ScheduleDataSpec(window_size="blabla", stride="blabla", start=123),
            primitive={"windowSize": "blabla", "stride": "blabla", "start": 123},
            errors={
                "stride": ["Invalid stride format. Must be e.g. '1d', '2hour', '60second'"],
                "windowSize": ["Invalid windowSize format. Must be e.g. '1d', '2hour', '60second'"],
            },
        ),
        InvalidTestCase(
            name="data_spec_nested_errors",
            type=DataSpec,
            constructor=lambda: DataSpec(files={"f1": FileSpec(id=None)}),
            primitive={"files": {"f1": {}}},
            errors={"files": {"f1": {"value": {"id": ["Missing data for required field."]}}}},
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
    ]

    @pytest.mark.parametrize("name, obj, primitive", valid_test_cases)
    def test_valid_dump(self, name, obj, primitive):
        dumped = obj.dump()
        assert dumped == primitive, "\nDumped:\n{}\nPrimitive:\n{}\n".format(dumped, primitive)

    @pytest.mark.parametrize("name, obj, primitive", valid_test_cases)
    def test_valid_load(self, name, obj, primitive):
        loaded = obj.__class__.load(primitive)
        assert loaded == obj, "\nLoaded: ({})\n{}\nObj: ({})\n{}\n".format(type(loaded), loaded, type(obj), obj)

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
        with mock.patch("cognite.data_fetcher._utils.time.time") as m:
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
            name="schedule_data_spec_start_time_ago",
            constructor=lambda: ScheduleDataSpec(stride="1h", window_size="5h", start="2m-ago"),
            primitive={"stride": "1h", "windowSize": "5h", "start": 10 ** 9 - 2 * 60 * 1000},
        ),
        TestCase(
            name="schedule_data_spec_start_datetime",
            constructor=lambda: ScheduleDataSpec(stride="1h", window_size="5h", start=datetime(2018, 1, 1)),
            primitive={"stride": "1h", "windowSize": "5h", "start": 1514764800000},
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
