from datetime import datetime
from time import sleep
from unittest import mock

import pytest

from cognite.data_fetcher._utils import to_ms


class TestToMs:
    @pytest.mark.parametrize("t", [None, 1.23, [], {}])
    def test_invalid_type(self, t):
        with pytest.raises(TypeError, match="must be"):
            to_ms(t)

    def test_ms(self):
        assert 1514760000000 == to_ms(1514760000000)
        assert 1514764800000 == to_ms(1514764800000)

    def test_datetime(self):
        assert 1514764800000 == to_ms(datetime(2018, 1, 1))
        assert 1546300800000 == to_ms(datetime(2019, 1, 1))

    @mock.patch("cognite.data_fetcher._utils.time.time")
    @pytest.mark.parametrize(
        "time_ago_string, expected_timestamp",
        [
            ("now", 10 ** 9),
            ("1s-ago", 10 ** 9 - 1 * 1000),
            ("13s-ago", 10 ** 9 - 13 * 1000),
            ("1m-ago", 10 ** 9 - 1 * 60 * 1000),
            ("13m-ago", 10 ** 9 - 13 * 60 * 1000),
            ("1h-ago", 10 ** 9 - 1 * 60 * 60 * 1000),
            ("13h-ago", 10 ** 9 - 13 * 60 * 60 * 1000),
            ("1d-ago", 10 ** 9 - 1 * 24 * 60 * 60 * 1000),
            ("13d-ago", 10 ** 9 - 13 * 24 * 60 * 60 * 1000),
            ("1w-ago", 10 ** 9 - 1 * 7 * 24 * 60 * 60 * 1000),
            ("13w-ago", 10 ** 9 - 13 * 7 * 24 * 60 * 60 * 1000),
        ],
    )
    def test_time_ago(self, time_mock, time_ago_string, expected_timestamp):
        time_mock.return_value = 1000000

        assert to_ms(time_ago_string) == expected_timestamp

    def test_time_ago_real_time(self):
        expected_time_now = datetime.now().timestamp() * 1000
        time_now = to_ms("now")
        print(expected_time_now, time_now)
        assert abs(expected_time_now - time_now) < 10

        sleep(0.1)

        time_now = to_ms("now")
        assert abs(expected_time_now - time_now) > 90
