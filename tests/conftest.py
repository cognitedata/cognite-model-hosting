import pytest
import responses

from tests.utils import BASE_URL


@pytest.fixture
def rsps():
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            BASE_URL + "/login/status",
            status=200,
            json={"data": {"user": "test", "project": "test", "loggedIn": True, "projectId": 123}},
        )
        yield rsps


@pytest.fixture(autouse=True)
def reset_now_cache():
    from cognite.model_hosting._cognite_model_hosting_common.utils import NowCache

    NowCache._cached_now = 0
