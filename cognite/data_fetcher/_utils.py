import re
import time
from datetime import timezone, datetime
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
    raise ValueError("Invalid time-ago representation")


def datetime_to_ms(dt):
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


def interval_to_ms(start: Union[int, str, datetime], end: Union[int, str, datetime, None]):
    """Returns the ms representation of start-end-interval whether it is time-ago, datetime or None."""
    time_now = int(round(time.time() * 1000))
    if isinstance(start, datetime):
        start = datetime_to_ms(start)
    elif isinstance(start, str):
        start = time_now - _time_ago_to_ms(start)
    elif isinstance(start, int):
        pass
    else:
        raise TypeError("'start' must be str, int, or datetime")

    if isinstance(end, datetime):
        end = datetime_to_ms(end)
    elif isinstance(end, str):
        end = time_now - _time_ago_to_ms(end)
    elif end is None:
        end = time_now
    elif isinstance(end, int):
        pass
    else:
        raise TypeError("'end' must be str, int or None")

    return start, end


def granularity_to_ms(time_string):
    """Returns millisecond representation of granularity time string"""
    magnitude = int("".join([c for c in time_string if c.isdigit()]))
    unit = "".join([c for c in time_string if c.isalpha()])
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
    return magnitude * unit_in_ms[unit]