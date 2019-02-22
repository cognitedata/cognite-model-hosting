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
