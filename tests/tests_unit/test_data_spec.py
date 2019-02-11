import json
from collections import namedtuple

import pytest

from cognite.data_fetcher.data_spec import (
    DataSpec,
    FileSpec,
    ScheduleDataSpec,
    ScheduleTimeSeriesSpec,
    TimeSeriesSpec,
    ValidationException,
)

TestCase = namedtuple("TestCase", ["name", "obj", "primitive"])
InvalidTestCase = namedtuple("TestCase", ["name", "obj", "primitive", "errors"])

valid_test_cases = [
    TestCase("minimal_file_spec", FileSpec(id=6), {"id": 6}),
    TestCase("minimal_time_series_spec", TimeSeriesSpec(id=6, start=123, end=234), {"id": 6, "start": 123, "end": 234}),
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
            "timeSeries": {"ts1": {"id": 6, "start": 123, "end": 234}, "ts2": {"id": 7, "start": 1234, "end": 2345}},
            "files": {"f1": {"id": 3}, "f2": {"id": 4}},
        },
    ),
    TestCase(
        "minimal_schedule_data_spec",
        ScheduleDataSpec(stride="1m", window_size="5m"),
        {"stride": "1m", "windowSize": "5m"},
    ),
    TestCase(
        "full_schedule_data_spec",
        ScheduleDataSpec(
            stride="1m",
            window_size="5m",
            time_series={"ts1": ScheduleTimeSeriesSpec(id=6), "ts2": ScheduleTimeSeriesSpec(id=7)},
        ),
        {"stride": "1m", "windowSize": "5m", "timeSeries": {"ts1": {"id": 6}, "ts2": {"id": 7}}},
    ),
]

invalid_test_cases = [
    InvalidTestCase("file_missing_id", FileSpec(id=None), {}, {"id": ["Missing data for required field."]}),
    InvalidTestCase(
        "time_series_missing_fields",
        TimeSeriesSpec(id=None, start=None, end=None),
        {},
        {
            "id": ["Missing data for required field."],
            "start": ["Missing data for required field."],
            "end": ["Missing data for required field."],
        },
    ),
    InvalidTestCase(
        "time_series_aggregates_but_not_granularity",
        TimeSeriesSpec(id=6, start=123, end=234, aggregate="avg"),
        {"id": 6, "start": 123, "end": 234, "aggregate": "avg"},
        {"granularity": ["granularity must be specified for aggregates."]},
    ),
    InvalidTestCase(
        "time_series_aggregates_but_include_outside_points",
        TimeSeriesSpec(id=6, start=123, end=234, aggregate="avg", granularity="1m", include_outside_points=True),
        {"id": 6, "start": 123, "end": 234, "aggregate": "avg", "granularity": "1m", "includeOutsidePoints": True},
        {"includeOutsidePoints": ["Can't include outside points for aggregates."]},
    ),
    InvalidTestCase(
        "schedule_time_series_with_start_end",
        ScheduleTimeSeriesSpec,
        {"id": 6, "start": 123, "end": 234},
        {"start": ["Unknown field."], "end": ["Unknown field."]},
    ),
    InvalidTestCase(
        "schedule_data_spec_missing_fields",
        ScheduleDataSpec(window_size=None, stride=None),
        {},
        {"windowSize": ["Missing data for required field."], "stride": ["Missing data for required field."]},
    ),
]


@pytest.mark.parametrize("name, obj, primitive", valid_test_cases)
def test_valid_dump(name, obj, primitive):
    dumped = obj.dump()
    assert dumped == primitive, "\nDumped:\n{}\nPrimitive:\n{}\n".format(dumped, primitive)


@pytest.mark.parametrize("name, obj, primitive", valid_test_cases)
def test_valid_load(name, obj, primitive):
    loaded = obj.__class__.load(primitive)
    assert loaded == obj, "\nLoaded: ({})\n{}\nObj: ({})\n{}\n".format(type(loaded), loaded, type(obj), obj)


@pytest.mark.parametrize("name, obj, primitive", valid_test_cases)
def test_valid_json_serializable(name, obj, primitive):
    json_data = obj.to_json()
    from_json = obj.__class__.from_json(json_data)
    assert from_json == obj, "\nFrom JSON: ({})\n{}\nObj:({})\n{}\n".format(type(from_json), from_json, type(obj), obj)


@pytest.mark.parametrize("name, obj, primitive, errors", invalid_test_cases)
def test_invalid(name, obj, primitive, errors):
    if type(obj) == type:
        should_fail = {"load": lambda: obj.load(primitive), "from_json": lambda: obj.from_json(json.dumps(primitive))}
    else:
        should_fail = {
            "dump": obj.dump,
            "validate": obj.validate,
            "load": lambda: obj.__class__.load(primitive),
            "to_json": obj.to_json,
            "from_json": lambda: obj.__class__.from_json(json.dumps(primitive)),
        }

    for name, method in should_fail.items():
        with pytest.raises(ValidationException) as excinfo:
            method()

        if excinfo.value.errors != errors:
            pytest.fail("\nMethod: {}\nErrors:\n{}\nExpected:\n{}\n".format(name, excinfo.value.errors, errors))


@pytest.mark.parametrize("name, obj, primitive", valid_test_cases)
def test_valid_str_repr(name, obj, primitive):
    assert str(obj) == str(obj.__dict__)


@pytest.mark.parametrize("name, obj, primitive, errors", invalid_test_cases)
def test_invalid_str_repr(name, obj, primitive, errors):
    if type(obj) != type:
        assert str(obj) == str(obj.__dict__)


def test_validation_exception_str_repr():
    e = ValidationException({"key": "value"})
    assert str(e) == '{\n    "key": "value"\n}'
