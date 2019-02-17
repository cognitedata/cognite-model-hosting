from collections import namedtuple

import pytest

from cognite.data_fetcher._utils import calculate_windows
from cognite.data_fetcher.data_spec import DataSpec, ScheduleDataSpec, ScheduleTimeSeriesSpec, TimeSeriesSpec

WindowIntervalTestCase = namedtuple("TestCase", ["start", "end", "stride", "window_size", "first", "expected_windows"])

window_interval_test_cases = [
    WindowIntervalTestCase(start=1, end=2, stride=1, window_size=1, first=1, expected_windows=[(0, 1)]),
    WindowIntervalTestCase(start=2, end=5, stride=1, window_size=1, first=1, expected_windows=[(1, 2), (2, 3), (3, 4)]),
    WindowIntervalTestCase(start=1, end=5, stride=2, window_size=1, first=1, expected_windows=[(0, 1), (2, 3)]),
    WindowIntervalTestCase(start=5, end=6, stride=2, window_size=1, first=1, expected_windows=[(4, 5)]),
    WindowIntervalTestCase(start=5, end=5, stride=2, window_size=1, first=1, expected_windows=[]),
    WindowIntervalTestCase(
        start=2, end=6, stride=1, window_size=2, first=2, expected_windows=[(0, 2), (1, 3), (2, 4), (3, 5)]
    ),
    WindowIntervalTestCase(start=3, end=6, stride=1, window_size=3, first=3, expected_windows=[(0, 3), (1, 4), (2, 5)]),
    WindowIntervalTestCase(start=5, end=11, stride=3, window_size=4, first=5, expected_windows=[(1, 5), (4, 8)]),
    WindowIntervalTestCase(start=11, end=17, stride=3, window_size=4, first=5, expected_windows=[(7, 11), (10, 14)]),
    WindowIntervalTestCase(
        start=5, end=12, stride=3, window_size=4, first=5, expected_windows=[(1, 5), (4, 8), (7, 11)]
    ),
    WindowIntervalTestCase(start=5, end=11, stride=3, window_size=4, first=8, expected_windows=[(4, 8)]),
    WindowIntervalTestCase(start=5, end=9, stride=3, window_size=4, first=5, expected_windows=[(1, 5), (4, 8)]),
    WindowIntervalTestCase(start=11, end=12, stride=3, window_size=4, first=5, expected_windows=[(7, 11)]),
]


@pytest.mark.parametrize("start, end, stride, window_size, first, expected_windows", window_interval_test_cases)
def test_calculate_window_intervals(start, end, stride, window_size, first, expected_windows):
    assert expected_windows == calculate_windows(start, end, stride, window_size, first)


def test_get_windowed_data_specs():
    schedule_ts_spec = {"ts1": ScheduleTimeSeriesSpec(id=1), "ts2": ScheduleTimeSeriesSpec(id=2)}
    schedule_data_spec = ScheduleDataSpec(stride="1m", window_size="1m", start=60000, time_series=schedule_ts_spec)
    data_specs = schedule_data_spec.get_data_specs(start=60000, end=6 * 60000)

    expected_data_specs = []
    for i in range(0, 5 * 60000, 60000):
        time_series_spec1 = TimeSeriesSpec(id=1, start=i, end=i + 60000)
        time_series_spec2 = TimeSeriesSpec(id=2, start=i, end=i + 60000)
        data_spec = DataSpec(time_series={"ts1": time_series_spec1, "ts2": time_series_spec2})
        expected_data_specs.append(data_spec)

    assert len(expected_data_specs) == len(data_specs)
    for expected, actual in zip(expected_data_specs, data_specs):
        assert expected == actual
