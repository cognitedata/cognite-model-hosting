from datetime import datetime

from cognite.data_fetcher._utils import interval_to_ms
from tests.utils import get_time_w_offset, round_to_nearest


def test_interval_to_ms_with_ms():
    assert (1514764800000, 1546300800000) == interval_to_ms(1514764800000, 1546300800000)


def test_interval_to_ms_with_datetime():
    assert (1514764800000, 1546300800000) == interval_to_ms(datetime(2018, 1, 1), datetime(2019, 1, 1))


def test_interval_to_ms_with_time_ago():
    time_now = round_to_nearest(get_time_w_offset(), 1000)
    one_day_ago = round_to_nearest(get_time_w_offset(days=1), 1000)

    one_day_ago_ms, time_now_ms = interval_to_ms("1d-ago", "now")
    one_day_ago_ms = round_to_nearest(one_day_ago_ms, 1000)
    time_now_ms = round_to_nearest(time_now_ms, 1000)

    assert time_now == time_now_ms
    assert one_day_ago == one_day_ago_ms


def test_interval_to_ms_with_end_none():
    time_now = round_to_nearest(get_time_w_offset(), 1000)
    _, time_now_ms = interval_to_ms(0, None)
    time_now_ms = round_to_nearest(time_now_ms, 1000)
    assert time_now == time_now_ms
