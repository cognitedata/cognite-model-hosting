import json

from cognite.model_hosting.data_fetcher.exceptions import DataFetcherHttpError


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
