import gzip
import json
import logging
import os
import sys
from typing import Any, Dict, Optional, Union

import requests as _requests
from requests import Response
from requests.adapters import HTTPAdapter
from urllib3 import Retry

import cognite.model_hosting._cognite_model_hosting_common.version
from cognite.model_hosting.data_fetcher._client.utils import _status_is_valid
from cognite.model_hosting.data_fetcher.exceptions import ApiKeyError, DataFetcherHttpError

DEFAULT_BASE_URL = "https://api.cognitedata.com"
NUM_OF_RETRIES = int(os.getenv("COGNITE_DATA_FETCHER_RETRIES", 3))
TIMEOUT = int(os.getenv("COGNITE_DATA_FETCHER_TIMEOUT", 10))
BACKOFF_MAX = 30

HTTP_STATUS_CODES_TO_RETRY = [429, 500, 502, 503]

MAX_CONNECTION_POOL_SIZE = 16

log = logging.getLogger("data-fetcher")


class RetryWithMaxBackoff(Retry):
    def get_backoff_time(self):
        return min(BACKOFF_MAX, super().get_backoff_time())


def _init_requests_session():
    session = _requests.Session()
    retry = RetryWithMaxBackoff(
        total=NUM_OF_RETRIES,
        backoff_factor=0.3,
        status_forcelist=HTTP_STATUS_CODES_TO_RETRY,
        raise_on_status=False,
        method_whitelist=False,  # Will retry on all methods since we only fetch data
    )
    adapter = HTTPAdapter(max_retries=retry, pool_maxsize=MAX_CONNECTION_POOL_SIZE)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


_REQUESTS_SESSION = _init_requests_session()


def _raise_API_error(res: Response):
    x_request_id = res.headers.get("X-Request-Id")
    code = res.status_code
    extra = {}
    try:
        error = res.json()["error"]
        if isinstance(error, str):
            msg = error
        else:
            msg = error["message"]
            extra = error.get("extra")
    except:
        msg = res.content

    log.error("HTTP Error %s: %s", code, msg, extra={"X-Request-ID": x_request_id, "extra": extra})
    raise DataFetcherHttpError(msg, code, x_request_id, extra=extra)


class ApiClient:
    def __init__(self, api_key: str = None, project: str = None, base_url: str = None):
        thread_local_api_key, thread_local_project = self._get_thread_local_credentials()
        environment_api_key = os.getenv("COGNITE_API_KEY")
        environment_base_url = os.getenv("COGNITE_BASE_URL")

        self._requests_session = _REQUESTS_SESSION
        self._api_key = api_key or thread_local_api_key or environment_api_key
        self._base_url = base_url or environment_base_url or DEFAULT_BASE_URL
        self._headers = {
            "api-key": self._api_key,
            "content-type": "application/json",
            "accept": "application/json",
            "User-Agent": "CogniteDataFetcher/{}".format(
                cognite.model_hosting._cognite_model_hosting_common.version.__version__
            ),
        }
        self._project = project or thread_local_project or self._get_project(self._api_key)
        self._base_url_v0_5 = self._base_url + "/api/0.5/projects/{}".format(self._project)
        self._base_url_v0_6 = self._base_url + "/api/0.6/projects/{}".format(self._project)

    def get(
        self,
        url: str,
        api_version: Optional[str] = "0.5",
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
    ):
        return self._do_request("GET", url, api_version=api_version, headers=headers, params=params)

    def post(
        self,
        url: str,
        api_version: Optional[str] = "0.5",
        json: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
    ):
        return self._do_request("POST", url, api_version=api_version, headers=headers, params=params, data=json)

    def delete(
        self,
        url: str,
        api_version: Optional[str] = "0.5",
        json: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
    ):
        return self._do_request("DELETE", url, api_version=api_version, headers=headers, params=params, data=json)

    def _do_request(
        self,
        method: str,
        url: str,
        api_version: Optional[str] = "0.5",
        data: Optional[Union[str, bytes]] = None,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
    ):
        full_url = self._get_full_url(url, api_version)
        headers = {**self._headers, **(headers or {})}
        if data:
            headers["Content-Encoding"] = "gzip"
            data = gzip.compress(json.dumps(data).encode())
        response = self._requests_session.request(
            method, full_url, headers=headers, params=params, data=data, timeout=TIMEOUT
        )
        if not _status_is_valid(response.status_code):
            _raise_API_error(response)
        return response

    def _get_project(self, api_key: str) -> str:
        if api_key is None:
            raise ApiKeyError("No API key was specified")
        response = self.get("/login/status", api_version=None).json()
        if not response["data"]["loggedIn"]:
            raise ApiKeyError("Invalid API Key")
        return response["data"]["project"]

    def _get_full_url(self, url, api_version: str = None):
        if api_version == "0.5":
            return self._base_url_v0_5 + url
        elif api_version == "0.6":
            return self._base_url_v0_6 + url
        return self._base_url + url

    def _get_thread_local_credentials(self):
        thread_local_api_key = None
        thread_local_project = None
        if "cognite._thread_local" in sys.modules:
            from cognite._thread_local import credentials

            thread_local_api_key = getattr(credentials, "api_key", None)
            thread_local_project = getattr(credentials, "project", None)
        return thread_local_api_key, thread_local_project
