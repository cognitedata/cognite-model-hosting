import re
import time
from datetime import datetime, timezone
from typing import List, Tuple, Union


def calculate_window_intervals(start: int, end: int, stride: int, window_size: int) -> List[Tuple[int, int]]:
    next_end = start + stride
    if end < next_end:
        return []
    intervals = []
    while next_end <= end:
        intervals.append((next_end - window_size, next_end))
        next_end += stride
    return intervals


def _time_ago_to_ms(time_ago_string: str) -> int:
    """Returns millisecond representation of time-ago string"""
    if time_ago_string == "now":
        return 0
    pattern = r"(\d+)([a-z])-ago"
    res = re.match(pattern, str(time_ago_string))
    if res:
        magnitude = int(res.group(1))
        unit = res.group(2)
        unit_in_ms = {"s": 1000, "m": 60000, "h": 3600000, "d": 86400000, "w": 604800000}
        return magnitude * unit_in_ms[unit]
    raise ValueError("Invalid time-ago format. Must be e.g. '3d-ago' or '1w-ago'.")


def _datetime_to_ms(dt):
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


def to_ms(t: Union[int, str, datetime]):
    """Returns the ms representation of some timestamp given by milliseconds, time-ago format or datetime object"""
    time_now = int(round(time.time() * 1000))
    if isinstance(t, int):
        return t
    elif isinstance(t, str):
        return time_now - _time_ago_to_ms(t)
    elif isinstance(t, datetime):
        return _datetime_to_ms(t)
    else:
        raise TypeError("Timestamp {} was of type {}, but must be str, int or datetime,".format(t, type(t)))


def granularity_to_ms(granularity):
    """Returns millisecond representation of granularity time string"""
    unit_in_ms = {
        "s": 1000,
        "second": 1000,
        "m": 60000,
        "minute": 60000,
        "h": 3600000,
        "hour": 3600000,
        "d": 86400000,
        "day": 86400000,
    }
    pattern = r"(\d+)({})".format("|".join(unit_in_ms))
    res = re.match(pattern, granularity)
    if res:
        magnitude = res.group(1)
        unit = res.group(2)
        return int(magnitude) * unit_in_ms[unit]
    raise ValueError("Invalid granularity format. Must be e.g. '3d', '1hour', or '30s'")
