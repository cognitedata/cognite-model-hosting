import typing as _typing  # isort:skip

# Hack to let aiohttp and aioresponses support Python 3.5.0
_typing.TYPE_CHECKING = False  # isort:skip

import pytest
from aioresponses import aioresponses

from tests.utils import BASE_URL


@pytest.fixture
def http_mock():
    with aioresponses() as mocked:
        mocked.get(BASE_URL + "/login/status", status=200, payload={"data": {"project": "test", "loggedIn": True}})
        yield mocked
