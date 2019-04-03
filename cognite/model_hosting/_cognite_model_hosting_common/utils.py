import re
import time
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Union


def calculate_windows(start: int, end: int, stride: int, window_size: int, first: int) -> List[Tuple[int, int]]:
    next = max(start, first)
    if (next - first) % stride != 0:
        next += stride - ((start - first) % stride)
    windows = []
    while next < end:
        windows.append((next - window_size, next))
        next += stride
    return windows


_unit_in_ms_without_week = {"s": 1000, "m": 60000, "h": 3600000, "d": 86400000}
_unit_in_ms = {**_unit_in_ms_without_week, "w": 604800000}


def _time_string_to_ms(pattern, string, unit_in_ms):
    pattern = pattern.format("|".join(unit_in_ms))
    res = re.fullmatch(pattern, string)
    if res:
        magnitude = int(res.group(1))
        unit = res.group(2)
        return magnitude * unit_in_ms[unit]
    return None


def granularity_to_ms(granularity: str) -> int:
    ms = _time_string_to_ms(r"(\d+)({})", granularity, _unit_in_ms_without_week)
    if ms is None:
        raise ValueError(
            "Invalid granularity format: `{}`. Must be on format <integer>(s|m|h|d). E.g. '5m', '3h' or '1d'.".format(
                granularity
            )
        )
    return ms


def granularity_unit_to_ms(granularity: str) -> int:
    granularity = re.sub(r"^\d+", "1", granularity)
    return granularity_to_ms(granularity)


def _time_ago_to_ms(time_ago_string: str) -> int:
    """Returns millisecond representation of time-ago string"""
    if time_ago_string == "now":
        return 0
    ms = _time_string_to_ms(r"(\d+)({})-ago", time_ago_string, _unit_in_ms)
    if ms is None:
        raise ValueError(
            "Invalid time-ago format: `{}`. Must be on format <integer>(s|m|h|d|w)-ago or 'now'. E.g. '3d-ago' or '1w-ago'.".format(
                time_ago_string
            )
        )
    return ms


def _datetime_to_ms(dt):
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


class NowCache:
    _cached_now = 0

    @classmethod
    def get_time_now(cls):
        now = int(round(time.time() * 1000))
        if now - cls._cached_now > 100:
            cls._cached_now = now
        return cls._cached_now


def timestamp_to_ms(t: Union[int, str, datetime]):
    """Returns the ms representation of some timestamp given by milliseconds, time-ago format or datetime object"""
    time_now = NowCache.get_time_now()
    if isinstance(t, int):
        ms = t
    elif isinstance(t, str):
        ms = time_now - _time_ago_to_ms(t)
    elif isinstance(t, datetime):
        ms = _datetime_to_ms(t)
    else:
        raise TypeError("Timestamp `{}` was of type {}, but must be int, str or datetime,".format(t, type(t)))

    if ms < 0:
        raise ValueError(
            "Timestamps can't be negative - they must represent a time after 1.1.1970, but {} was provided".format(ms)
        )

    return ms


def _time_interval_str_to_ms(interval_str: str):
    ms = _time_string_to_ms(r"(\d+)({})", interval_str, _unit_in_ms)
    if ms is None:
        raise ValueError(
            "Invalid time interval format: `{}`. Must be on format <integer>(s|m|h|d|w). E.g. '5m', '3h' or '1d'.".format(
                interval_str
            )
        )
    return ms


def time_interval_to_ms(t: Union[int, str, timedelta], allow_zero=False, allow_inf=False):
    """Returns millisecond representation of time interval"""
    if isinstance(t, int):
        ms = t
    elif isinstance(t, str):
        ms = _time_interval_str_to_ms(t)
    elif isinstance(t, timedelta):
        ms = int(round(t.total_seconds() * 1000))
    else:
        raise TypeError("Time interval `{}` was of type {}, but must be int, str or timedelta,".format(t, type(t)))

    if (ms == 0 and not allow_zero) or (ms == -1 and not allow_inf) or ms < -1:
        if allow_zero and allow_inf:
            msg = "Time interval has to be -1, 0 or positive"
        elif not allow_zero and allow_inf:
            msg = "Time interval has to be -1 or positive"
        elif allow_zero and not allow_inf:
            msg = "Time interval has to be 0 or positive"
        else:
            msg = "Time interval has to be positive"

        raise ValueError("{}, but got {} ms".format(msg, ms))

    return ms


def _time_offset_str_to_ms(offset_str: str):
    ms = _time_string_to_ms(r"(-?\d+)({})", offset_str, _unit_in_ms)
    if ms is None:
        raise ValueError(
            "Invalid time offset format: `{}`. Must be on format [-]<integer>(s|m|h|d|w). E.g. '-5m', '-3h' or '1d'.".format(
                offset_str
            )
        )
    return ms


def time_offset_to_ms(t: Union[int, str, timedelta]):
    if isinstance(t, int):
        return t
    elif isinstance(t, str):
        return _time_offset_str_to_ms(t)
    elif isinstance(t, timedelta):
        return int(round(t.total_seconds() * 1000))
    else:
        raise TypeError("Time offset `{}` was of type {}, but must be int, str or timedelta,".format(t, type(t)))
