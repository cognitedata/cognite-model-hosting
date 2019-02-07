import os

import pytest
from aioresponses import aioresponses

from tests.utils import BASE_URL


@pytest.fixture(scope="session", autouse=True)
def unset_env_project():
    tmp_project = None
    if "COGNITE_PROJECT" in os.environ:
        tmp_project = os.environ.pop("COGNITE_PROJECT")
    yield
    if tmp_project:
        os.environ["COGNITE_PROJECT"] = tmp_project


@pytest.fixture
def mock_async_response():
    with aioresponses() as mocked:
        mocked.get(BASE_URL + "/login/status", status=200, payload={"data": {"project": "test", "loggedIn": True}})
        yield mocked
