from collections import namedtuple

import pytest

from cognite.model_hosting._cognite_model_hosting_common.utils import calculate_windows
from cognite.model_hosting.data_spec import (
    DataSpec,
    ScheduleDataSpec,
    ScheduleInputSpec,
    ScheduleInputTimeSeriesSpec,
    ScheduleOutputSpec,
    TimeSeriesSpec,
)


class TestCalculateWindowIntervals:
    TestCase = namedtuple("TestCase", ["start", "end", "stride", "window_size", "first", "expected_windows"])

    test_cases = [
        TestCase(start=1, end=2, stride=1, window_size=1, first=1, expected_windows=[(0, 1)]),
        TestCase(start=2, end=5, stride=1, window_size=1, first=1, expected_windows=[(1, 2), (2, 3), (3, 4)]),
        TestCase(start=1, end=5, stride=2, window_size=1, first=1, expected_windows=[(0, 1), (2, 3)]),
        TestCase(start=5, end=6, stride=2, window_size=1, first=1, expected_windows=[(4, 5)]),
        TestCase(start=5, end=5, stride=2, window_size=1, first=1, expected_windows=[]),
        TestCase(start=2, end=6, stride=1, window_size=2, first=2, expected_windows=[(0, 2), (1, 3), (2, 4), (3, 5)]),
        TestCase(start=3, end=6, stride=1, window_size=3, first=3, expected_windows=[(0, 3), (1, 4), (2, 5)]),
        TestCase(start=5, end=11, stride=3, window_size=4, first=5, expected_windows=[(1, 5), (4, 8)]),
        TestCase(start=6, end=11, stride=3, window_size=4, first=5, expected_windows=[(4, 8)]),
        TestCase(start=11, end=17, stride=3, window_size=4, first=5, expected_windows=[(7, 11), (10, 14)]),
        TestCase(start=5, end=12, stride=3, window_size=4, first=5, expected_windows=[(1, 5), (4, 8), (7, 11)]),
        TestCase(start=5, end=11, stride=3, window_size=4, first=8, expected_windows=[(4, 8)]),
        TestCase(start=5, end=9, stride=3, window_size=4, first=5, expected_windows=[(1, 5), (4, 8)]),
        TestCase(start=11, end=12, stride=3, window_size=4, first=5, expected_windows=[(7, 11)]),
    ]

    @pytest.mark.parametrize("start, end, stride, window_size, first, expected_windows", test_cases)
    def test_calculate_window_intervals(self, start, end, stride, window_size, first, expected_windows):
        assert expected_windows == calculate_windows(start, end, stride, window_size, first)


def test_get_instances():
    schedule_ts_spec = {"ts1": ScheduleInputTimeSeriesSpec(id=1), "ts2": ScheduleInputTimeSeriesSpec(id=2)}
    schedule_data_spec = ScheduleDataSpec(
        input=ScheduleInputSpec(time_series=schedule_ts_spec),
        output=ScheduleOutputSpec(),
        stride="1m",
        window_size="1m",
        start=60000,
    )
    data_specs = schedule_data_spec.get_instances(start=60000, end=6 * 60000)

    expected_data_specs = []
    for i in range(0, 5 * 60000, 60000):
        time_series_spec1 = TimeSeriesSpec(id=1, start=i, end=i + 60000)
        time_series_spec2 = TimeSeriesSpec(id=2, start=i, end=i + 60000)
        data_spec = DataSpec(time_series={"ts1": time_series_spec1, "ts2": time_series_spec2})
        expected_data_specs.append(data_spec)

    assert len(expected_data_specs) == len(data_specs)
    for expected, actual in zip(expected_data_specs, data_specs):
        assert expected == actual


class TestGetScheduleTimestamps:
    TestCase = namedtuple("TestCase", ["schedule_stride", "schedule_start", "start", "end", "expected_timestamps"])
    test_cases = [
        TestCase(schedule_stride=3, schedule_start=2, start=5, end=12, expected_timestamps=[5, 8, 11]),
        TestCase(schedule_stride=3, schedule_start=2, start=5, end=11, expected_timestamps=[5, 8]),
        TestCase(schedule_stride=3, schedule_start=2, start=6, end=12, expected_timestamps=[8, 11]),
        TestCase(schedule_stride=1, schedule_start=2, start=5, end=10, expected_timestamps=[5, 6, 7, 8, 9]),
        TestCase(schedule_stride=3, schedule_start=12, start=5, end=12, expected_timestamps=[]),
    ]

    @pytest.mark.parametrize("window_size", [1, 2, 3])  # window size shouldn't matter
    @pytest.mark.parametrize("schedule_stride, schedule_start, start, end, expected_timestamp", test_cases)
    def test_get_execution_timestamps(
        self, window_size, schedule_stride, schedule_start, start, end, expected_timestamp
    ):
        schedule_data_spec = ScheduleDataSpec(
            input=ScheduleInputSpec(),
            output=ScheduleOutputSpec(),
            stride=schedule_stride,
            window_size=window_size,
            start=schedule_start,
        )
        assert expected_timestamp == schedule_data_spec.get_execution_timestamps(start, end)
