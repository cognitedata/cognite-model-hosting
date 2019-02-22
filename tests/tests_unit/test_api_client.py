import os
import random
import sys
import threading
import types
from multiprocessing.pool import ThreadPool
from time import sleep

import pytest
import responses

from cognite.model_hosting.data_fetcher._client.api_client import DEFAULT_BASE_URL, DEFAULT_NUM_OF_RETRIES, ApiClient
from cognite.model_hosting.data_fetcher.exceptions import ApiKeyError, DataFetcherHttpError
from tests.utils import BASE_URL, BASE_URL_V0_5


@pytest.fixture
def unset_env_api_key():
    tmp_key = os.environ.pop("COGNITE_API_KEY")
    yield
    os.environ["COGNITE_API_KEY"] = tmp_key


def test_create_client_no_api_key(unset_env_api_key):
    with pytest.raises(ApiKeyError) as e:
        ApiClient()

    assert "No API key was specified" == e.value.args[0]


@responses.activate
def test_create_client_invalid_api_key():
    responses.add(responses.GET, BASE_URL + "/login/status", status=200, json={"data": {"loggedIn": False}})
    with pytest.raises(ApiKeyError) as e:
        ApiClient()

    assert "Invalid API Key" == e.value.args[0]


def test_create_default_client(rsps):
    client = ApiClient()

    assert DEFAULT_BASE_URL == client._base_url
    assert "test" == client._project


@pytest.fixture
def set_environment_config():
    os.environ["COGNITE_BASE_URL"] = "http://test"
    yield
    del os.environ["COGNITE_BASE_URL"]


@responses.activate
def test_create_client_environment_config(set_environment_config):
    responses.add(
        responses.GET, "http://test/login/status", status=200, json={"data": {"project": "test", "loggedIn": True}}
    )
    client = ApiClient()

    assert "http://test" == client._base_url
    assert "test" == client._project


@responses.activate
def test_create_client_param_config():
    responses.add(
        responses.GET, "http://test/login/status", status=200, json={"data": {"project": "test", "loggedIn": True}}
    )
    client = ApiClient(api_key="test", project="test", base_url="http://test")

    assert "test" == client._api_key
    assert "http://test" == client._base_url
    assert "test" == client._project


@pytest.fixture
def thread_local_credentials_module():
    credentials_module = types.ModuleType("cognite._thread_local")
    credentials_module.credentials = threading.local()
    sys.modules["cognite._thread_local"] = credentials_module
    yield
    del sys.modules["cognite._thread_local"]


def create_client_and_check_config(i):
    from cognite._thread_local import credentials

    api_key = "thread-local-api-key{}".format(i)
    project = "thread-local-project{}".format(i)

    credentials.api_key = api_key
    credentials.project = project

    sleep(random.random())
    client = ApiClient()

    assert api_key == client._api_key
    assert project == client._project


def test_create_client_thread_local_config(thread_local_credentials_module):
    with ThreadPool() as pool:
        pool.map(create_client_and_check_config, list(range(16)))


@pytest.fixture
def mock_response_all_http_methods_200(rsps):
    mock_response_body = {"data": "something"}
    rsps.assert_all_requests_are_fired = False
    rsps.add(rsps.GET, BASE_URL_V0_5 + "/any", status=200, json=mock_response_body)
    rsps.add(rsps.POST, BASE_URL_V0_5 + "/any", status=200, json=mock_response_body)
    rsps.add(rsps.DELETE, BASE_URL_V0_5 + "/any", status=200, json=mock_response_body)
    yield mock_response_body


def test_get(mock_response_all_http_methods_200):
    client = ApiClient()

    res = client.get(url="/any").json()
    assert mock_response_all_http_methods_200 == res


def test_post(mock_response_all_http_methods_200):
    client = ApiClient()

    res = client.post(url="/any", json={"blabla": "blabla"}).json()
    assert mock_response_all_http_methods_200 == res


def test_delete(mock_response_all_http_methods_200):
    client = ApiClient()

    res = client.delete(url="/any").json()
    assert mock_response_all_http_methods_200 == res


@pytest.fixture
def mock_response_all_http_methods_400(rsps):
    mock_response_body = {"error": {"message": "bla"}}
    rsps.assert_all_requests_are_fired = False
    rsps.add(rsps.GET, BASE_URL_V0_5 + "/any", status=400, json=mock_response_body)
    rsps.add(rsps.POST, BASE_URL_V0_5 + "/any", status=400, json=mock_response_body)
    rsps.add(rsps.DELETE, BASE_URL_V0_5 + "/any", status=400, json=mock_response_body)
    yield mock_response_body


def test_get_error(mock_response_all_http_methods_400):
    client = ApiClient()

    with pytest.raises(DataFetcherHttpError, match="bla.*400"):
        client.get(url="/any")


def test_post_error(mock_response_all_http_methods_400):
    client = ApiClient()

    with pytest.raises(DataFetcherHttpError, match="bla.*400"):
        client.post(url="/any", json={"blabla": "blabla"})


def test_delete_error(mock_response_all_http_methods_400):
    client = ApiClient()

    with pytest.raises(DataFetcherHttpError) as e:
        client.delete(url="/any")

    assert "bla" == e.value.message
    assert 400 == e.value.code
