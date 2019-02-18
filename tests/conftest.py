import pytest
from aioresponses import aioresponses

from tests.utils import BASE_URL


@pytest.fixture
def http_mock():
    with aioresponses() as mocked:
        mocked.get(BASE_URL + "/login/status", status=200, payload={"data": {"project": "test", "loggedIn": True}})
        yield mocked
