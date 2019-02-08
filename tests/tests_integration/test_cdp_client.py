import os
import sys
from pathlib import Path

import pandas as pd
import pytest

from cognite_data_fetcher._client.cdp_client import CdpClient
from tests.utils import run_until_complete

CLIENT = None


@pytest.fixture(scope="session", autouse=True)
def cdp_client():
    global CLIENT
    CLIENT = CdpClient()


@pytest.fixture(scope="session")
def time_series_in_tenant():
    ts_list = run_until_complete(
        CLIENT.get("/timeseries".format(CLIENT._project), params={"limit": 3, "description": "A"})
    )
    return ts_list["data"]["items"]


def test_get_datapoints(time_series_in_tenant):
    ts_id = time_series_in_tenant[0]["id"]
    ts_name = time_series_in_tenant[0]["name"]
    dps = run_until_complete(CLIENT.get_datapoints(ts_id, start="900d-ago", limit=10))
    assert ts_name == dps["data"]["items"][0]["name"]
    assert len(dps["data"]["items"][0]["datapoints"]) > 0


def test_get_datapoints_frame(time_series_in_tenant):
    time_series = [{"id": ts["id"], "aggregate": "min"} for ts in time_series_in_tenant]
    time_series.append({"id": time_series_in_tenant[0]["id"], "aggregate": "max"})
    res = run_until_complete(CLIENT.get_datapoints_frame(time_series, granularity="1h", start="900d-ago", limit=10))
    assert isinstance(res, pd.DataFrame)
    assert (10, 5) == res.shape


def test_get_time_series_by_id(time_series_in_tenant):
    ts_ids = [ts["id"] for ts in time_series_in_tenant]
    res = run_until_complete(CLIENT.get_time_series_by_id(ts_ids))

    fetched_ids = [ts["id"] for ts in res["data"]["items"]]

    for ts_id in ts_ids:
        assert ts_id in fetched_ids


@pytest.fixture()
def file_in_tenant():
    file_list = run_until_complete(CLIENT.get("/files".format(CLIENT._project), params={"limit": 1}))
    return file_list["data"]["items"][0]


def test_download_file(file_in_tenant):
    target_path = os.path.dirname(os.path.abspath(__file__)) + "/file"

    run_until_complete(CLIENT.download_file(file_in_tenant["id"], target_path))
    assert Path(target_path).is_file()
    os.remove(target_path)


def test_download_file_in_memory(file_in_tenant):
    file_bytes = run_until_complete(CLIENT.download_file_in_memory(file_in_tenant["id"]))
    assert isinstance(file_bytes, bytes)
