import asyncio
import os
import sys
from typing import Any, Dict, Union

import aiohttp

import cognite.model_hosting
from cognite.model_hosting.data_fetcher._client import utils
from cognite.model_hosting.data_fetcher.exceptions import ApiKeyError

DEFAULT_BASE_URL = "https://api.cognitedata.com"
DEFAULT_NUM_OF_RETRIES = 3

HTTP_STATUS_CODES_TO_RETRY = [429, 500, 502, 503]
HTTP_METHODS_TO_RETRY = ["GET", "DELETE"]

CLIENT_SESSION = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))


class ApiClient:
    def __init__(self, api_key: str = None, project: str = None, base_url: str = None, num_of_retries: int = None):
        thread_local_api_key = None
        thread_local_project = None
        if "cognite._thread_local" in sys.modules:
            from cognite._thread_local import credentials

            thread_local_api_key = getattr(credentials, "api_key", None)
            thread_local_project = getattr(credentials, "project", None)

        environment_api_key = os.getenv("COGNITE_API_KEY")
        environment_base_url = os.getenv("COGNITE_BASE_URL")
        environment_num_of_retries = os.getenv("COGNITE_NUM_RETRIES")

        self._client_session = CLIENT_SESSION
        self._api_key = api_key or thread_local_api_key or environment_api_key
        self._base_url = base_url or environment_base_url or DEFAULT_BASE_URL
        self._num_of_retries = utils.choose_num_of_retries(
            num_of_retries, environment_num_of_retries, DEFAULT_NUM_OF_RETRIES
        )
        self._headers = {
            "api-key": self._api_key,
            "content-type": "application/json",
            "accept": "application/json",
            "User-agent": "cognite-data-fetcher/{}".format(cognite.model_hosting.__version__),
        }
        self._project = project or thread_local_project or self._get_project(self._api_key)
        self._base_url_v0_5 = self._base_url + "/api/0.5/projects/{}".format(self._project)
        self._base_url_v0_6 = self._base_url + "/api/0.6/projects/{}".format(self._project)

    async def get(self, url: str, params: Dict[str, Any] = None, api_version: Union[str, None] = "0.5"):
        return await self._do_request_with_retry(
            "GET", url, params=utils.format_params(params or {}), api_version=api_version
        )

    async def post(
        self, url: str, body: Dict[str, Any], headers: Dict[str, str] = None, api_version: Union[str, None] = "0.5"
    ):
        return await self._do_request_with_retry("POST", url, json=body, api_version=api_version, headers=headers)

    async def delete(self, url, params: Dict[str, Any] = None, api_version: Union[str, None] = "0.5"):
        return await self._do_request_with_retry(
            "DELETE", url, params=utils.format_params(params or {}), api_version=api_version
        )

    async def _do_request_with_retry(
        self, method, url, api_version: Union[str, None], headers: Dict[str, str] = None, **kwargs
    ):
        number_of_attempts = 0
        while True:
            response_body, status, request_id = await self._do_request(
                method, url, api_version=api_version, headers=headers, **kwargs
            )
            if utils._status_is_valid(status):
                return response_body

            if (
                number_of_attempts == self._num_of_retries
                or status not in HTTP_STATUS_CODES_TO_RETRY
                or method not in HTTP_METHODS_TO_RETRY
            ):
                utils._raise_HTTP_error(status, request_id, response_body)
            await utils._sleep_with_exponentital_backoff(number_of_attempts)
            number_of_attempts += 1

    async def _do_request(self, method, url, api_version: Union[str, None], headers: Dict[str, str] = None, **kwargs):
        full_url = self._get_full_url(url, api_version)
        headers = {**self._headers, **(headers or {})}
        async with self._client_session.request(method, full_url, headers=headers, **kwargs) as response:
            if headers["accept"] == "application/json":
                response_body = await response.json()
            else:
                response_body = await response.text()
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

    def _get_full_url(self, url, api_version: str = None):
        if api_version == "0.5":
            return self._base_url_v0_5 + url
        elif api_version == "0.6":
            return self._base_url_v0_6 + url
        return self._base_url + url
