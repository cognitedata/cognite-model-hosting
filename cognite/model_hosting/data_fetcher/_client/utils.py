import asyncio
import json
from typing import Dict

from cognite.model_hosting.data_fetcher.exceptions import DataFetcherHttpError


def format_params(d: Dict) -> Dict:
    formatted = {}
    for k, v in d.items():
        if v is None:
            continue
        elif isinstance(v, bool):
            formatted[k] = str(v).lower()
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
