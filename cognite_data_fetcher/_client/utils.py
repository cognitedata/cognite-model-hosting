import asyncio
import json
import re
import time
from typing import Dict, List, Tuple, Union

from cognite_data_fetcher.exceptions import DataFetcherHttpError


def format_params(d: Dict) -> Dict:
    formatted = {}
    for k, v in d.items():
        if v is None:
            continue
        elif isinstance(v, bool):
            formatted[k] = str(v).lower()
        elif isinstance(v, list):
            formatted[k] = str(v)
        else:
            formatted[k] = v
    return formatted


def choose_num_of_retries(param: int, env: str, default: int) -> int:
    if param is not None:
        return param
    elif env is not None:
        return int(env)
    else:
        return default


def _status_is_valid(status_code: int) -> bool:
    return status_code < 400


def _raise_HTTP_error(code, x_request_id, response_body):
    extra = {}
    try:
        if isinstance(response_body, str):
            response_body = json.loads(response_body)
        error = response_body["error"]
        msg = error["message"]
        extra = error.get("extra")
    except:
        msg = response_body
    raise DataFetcherHttpError(msg, code, x_request_id, extra=extra)


async def _sleep_with_exponentital_backoff(number_of_attempts: int, backoff_factor: int = 0.5, max_backoff: int = 30):
    backoff = backoff_factor * ((2 ** number_of_attempts) - 1)
    sleep_time = min(backoff, max_backoff)
    await asyncio.sleep(sleep_time)


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


def _interval_to_ms(start: Union[str, int], end: Union[str, int, None]) -> Tuple[int, int]:
    """Returns the ms representation of start-end-interval whether it is time-ago, datetime or None."""
    time_now = int(round(time.time() * 1000))
    if isinstance(start, str):
        start = time_now - _time_ago_to_ms(start)
    elif isinstance(start, int):
        pass
    else:
        raise ValueError("'start' must be str or int")

    if isinstance(end, str):
        end = time_now - _time_ago_to_ms(end)
    elif end is None:
        end = time_now
    elif isinstance(end, int):
        pass
    else:
        raise ValueError("'end' must be str, int or None")

    return start, end
