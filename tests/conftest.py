import pytest
import responses

from tests.utils import BASE_URL


@pytest.fixture
def rsps():
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET, BASE_URL + "/login/status", status=200, json={"data": {"project": "test", "loggedIn": True}}
        )
        yield rsps


@pytest.fixture(autouse=True)
def reset_now_cache():
    from cognite.model_hosting._utils import NowCache

    NowCache._cached_now = 0
