from collections import namedtuple

import pytest

from cognite.data_fetcher._utils import calculate_window_intervals
from cognite.data_fetcher.data_spec import DataSpec, ScheduleDataSpec, ScheduleTimeSeriesSpec, TimeSeriesSpec

WindowIntervalTestCase = namedtuple("TestCase", ["start", "end", "stride", "window_size", "expected_windows"])

window_interval_test_cases = [
    WindowIntervalTestCase(start=1, end=5, stride=1, window_size=1, expected_windows=[(1, 2), (2, 3), (3, 4), (4, 5)]),
    WindowIntervalTestCase(start=1, end=5, stride=2, window_size=1, expected_windows=[(2, 3), (4, 5)]),
    WindowIntervalTestCase(start=1, end=5, stride=1, window_size=2, expected_windows=[(0, 2), (1, 3), (2, 4), (3, 5)]),
    WindowIntervalTestCase(start=1, end=5, stride=1, window_size=3, expected_windows=[(-1, 2), (0, 3), (1, 4), (2, 5)]),
    WindowIntervalTestCase(start=1, end=5, stride=3, window_size=4, expected_windows=[(0, 4)]),
]


@pytest.mark.parametrize("start, end, stride, window_size, expected_windows", window_interval_test_cases)
def test_calculate_window_intervals(start, end, stride, window_size, expected_windows):
    assert expected_windows == calculate_window_intervals(start, end, stride, window_size)


def test_get_windowed_data_specs():
    schedule_ts_spec = {"ts1": ScheduleTimeSeriesSpec(id=1), "ts2": ScheduleTimeSeriesSpec(id=2)}
    schedule_data_spec = ScheduleDataSpec(stride="1m", window_size="1m", time_series=schedule_ts_spec)
    data_specs = schedule_data_spec.get_data_specs(start=0, end=5 * 60000)

    expected_data_specs = []
    for i in range(0, 5 * 60000, 60000):
        time_series_spec1 = TimeSeriesSpec(id=1, start=i, end=i + 60000)
        time_series_spec2 = TimeSeriesSpec(id=2, start=i, end=i + 60000)
        data_spec = DataSpec(time_series={"ts1": time_series_spec1, "ts2": time_series_spec2})
        expected_data_specs.append(data_spec)

    assert len(expected_data_specs) == len(data_specs)
    for expected, actual in zip(expected_data_specs, data_specs):
        assert expected == actual
