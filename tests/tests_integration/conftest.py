import re
from datetime import datetime

import pytest

from cognite.model_hosting.data_fetcher._client.api_client import ApiClient


@pytest.fixture
def now():
    return int(datetime.now().timestamp() * 1000)


@pytest.fixture(scope="session")
def ts_ids():
    client = ApiClient()
    ts_ids = {}
    res = client.get("/timeseries", params={"q": "test__constant", "limit": 100})
    assert 200 == res.status_code
    for ts in res.json()["data"]["items"]:
        short_name = re.fullmatch(r"test__(constant_[0-9]+)_with_noise", ts["name"]).group(1)
        ts_ids[short_name] = ts["id"]

    return ts_ids


@pytest.fixture(scope="session")
def file_ids():
    client = ApiClient()
    file_ids = {}

    res = client.get("/files", params={"dir": "test/subdir"})
    for file in res.json()["data"]["items"]:
        file_ids[file["fileName"]] = file["id"]

    res = client.get("/files", params={"dir": "test"})
    for file in res.json()["data"]["items"]:
        file_ids[file["fileName"]] = file["id"]

    return file_ids
