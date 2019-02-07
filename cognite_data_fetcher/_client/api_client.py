import asyncio
import json
import os
from typing import Any, Dict, Union

import aiohttp

import cognite_data_fetcher
from cognite_data_fetcher._client.utils import choose_num_of_retries, format_params

DEFAULT_BASE_URL = "https://api.cognitedata.com"
DEFAULT_NUM_OF_RETRIES = 3
DEFAULT_HEADERS = {"content-type": "application/json", "accept": "application/json"}

HTTP_STATUS_CODES_TO_RETRY = [429, 500, 502, 503]
HTTP_METHODS_TO_RETRY = ["GET", "DELETE"]

timeout = aiohttp.ClientTimeout(total=60)
client_session = aiohttp.ClientSession(timeout=timeout, headers=DEFAULT_HEADERS)


class DataFetcherHttpError(Exception):
    def __init__(self, message, code=None, x_request_id=None, extra=None):
        self.message = message
        self.code = code
        self.x_request_id = x_request_id
        self.extra = extra

    def __str__(self):
        if self.extra:
            pretty_extra = json.dumps(self.extra, indent=4, sort_keys=True)
            return "{} | code: {} | X-Request-ID: {}\n{}".format(
                self.message, self.code, self.x_request_id, pretty_extra
            )
        return "{} | code: {} | X-Request-ID: {}".format(self.message, self.code, self.x_request_id)


class ApiKeyError(Exception):
    pass


def _status_is_valid(status_code: int):
    return status_code < 400


def _raise_HTTP_error(code, x_request_id, response_body):
    extra = {}
    try:
        error = response_body["error"]
        msg = error["message"]
        extra = error.get("extra")
    except:
        msg = response_body
    raise DataFetcherHttpError(msg, code, x_request_id, extra=extra)


async def _sleep_with_exponentital_backoff(number_of_attempts: int):
    sleep_time = 2 ** number_of_attempts
    await asyncio.sleep(sleep_time)


class ApiClient:
    def __init__(self, api_key: str = None, project: str = None, base_url: str = None, num_of_retries: int = None):
        environment_api_key = os.getenv("COGNITE_API_KEY")
        environment_project = os.getenv("COGNITE_PROJECT")
        environment_base_url = os.getenv("COGNITE_BASE_URL")
        environment_num_of_retries = os.getenv("COGNITE_NUM_RETRIES")

        self._api_key = api_key or environment_api_key
        self._base_url = base_url or environment_base_url or DEFAULT_BASE_URL
<<<<<<< Updated upstream
        if num_of_retries is not None:
            self._num_of_retries = num_of_retries
        elif environment_num_of_retries is not None:
            self._num_of_retries = int(environment_num_of_retries)
        else:
            self._num_of_retries = DEFAULT_NUM_OF_RETRIES
        self._headers = {"api-key": self._api_key}
        self._project = self._get_project(self._api_key)
=======
        self._num_of_retries = choose_num_of_retries(num_of_retries, environment_num_of_retries, DEFAULT_NUM_OF_RETRIES)
        self._headers = {
            "api-key": self._api_key,
            "content-type": "application/json",
            "accept": "application/json",
            "User-agent": "cognite-data-fetcher/{}".format(cognite_data_fetcher.__version__),
        }
        self._project = project or environment_project or self._get_project(self._api_key)
>>>>>>> Stashed changes
        self._base_url_v0_5 = self._base_url + "/api/0.5/projects/{}".format(self._project)
        self._base_url_v0_6 = self._base_url + "/api/0.6/projects/{}".format(self._project)

    async def get(self, url: str, params: Dict[str, Union[str, int]] = None, api_version: Union[str, None] = "0.5"):
        return await self._do_request_with_retry(
            "GET", url, params=format_params(params or {}), api_version=api_version
        )

    async def post(self, url: str, body: Dict[str, Any], api_version: Union[str, None] = "0.5"):
        return await self._do_request_with_retry("POST", url, json=body, api_version=api_version)

    async def delete(self, url, params: Dict[str, Union[str, int]] = None, api_version: Union[str, None] = "0.5"):
        return await self._do_request_with_retry(
            "DELETE", url, params=format_params(params or {}), api_version=api_version
        )

    async def _do_request_with_retry(self, method, url, api_version: Union[str, None] = "0.5", **kwargs):
        number_of_attempts = 0
        while True:
            response_body, status, request_id = await self._do_request(method, url, api_version=api_version, **kwargs)
            if _status_is_valid(status):
                return response_body

            if (
                number_of_attempts == self._num_of_retries
                or status not in HTTP_STATUS_CODES_TO_RETRY
                or method not in HTTP_METHODS_TO_RETRY
            ):
                _raise_HTTP_error(status, request_id, response_body)
            await _sleep_with_exponentital_backoff(number_of_attempts)
            number_of_attempts += 1

    async def _do_request(self, method, url, api_version: Union[str, None] = "0.5", **kwargs):
        if api_version == "0.5":
            full_url = self._base_url_v0_5 + url
        elif api_version == "0.6":
            full_url = self._base_url_v0_6 + url
        else:
            full_url = self._base_url + url
        async with client_session.request(method, full_url, headers=self._headers, **kwargs) as response:
            response_body = await response.json()
            status = response.status
            request_id = response.headers.get("X-Request-ID")
            return response_body, status, request_id

    def _get_project(self, api_key: str) -> str:
        if api_key is None:
            raise ApiKeyError("No API key was specified")
        response = asyncio.get_event_loop().run_until_complete(self.get("/login/status", api_version=None))
        if not response["data"]["loggedIn"]:
            raise ApiKeyError("Invalid API Key")
        return response["data"]["project"]
