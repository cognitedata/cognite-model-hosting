import os
from pathlib import Path

import pandas as pd
import pytest

from cognite.model_hosting.data_fetcher._client.cdp_client import CdpClient
from tests.utils import random_string

CLIENT = None


@pytest.fixture(scope="session", autouse=True)
def cdp_client():
    global CLIENT
    CLIENT = CdpClient()


@pytest.fixture(scope="session")
def time_series_in_tenant():
    ts_list = CLIENT.get("/timeseries".format(CLIENT._project), params={"limit": 3, "description": "A"}).json()
    return ts_list["data"]["items"]


def test_get_datapoints_frame_single(time_series_in_tenant):
    ts_id = time_series_in_tenant[0]["id"]
    df = CLIENT.get_datapoints_frame_single(ts_id, start=1498044290000, end=1498044700000)
    assert df.shape[1] == 2
    assert df.shape[0] > 0


def test_get_datapoints_frame(time_series_in_tenant):
    time_series = [{"id": ts["id"], "aggregate": "min"} for ts in time_series_in_tenant]
    time_series.append({"id": time_series_in_tenant[0]["id"], "aggregate": "max"})
    res = CLIENT.get_datapoints_frame(time_series, granularity="1m", start=1498044290000, end=1498044700000)
    assert isinstance(res, pd.DataFrame)
    assert 5 == res.shape[1]
    assert res.shape[0] > 0


def test_get_time_series_by_id(time_series_in_tenant):
    ts_ids = [ts["id"] for ts in time_series_in_tenant]
    res = CLIENT.get_time_series_by_id(ts_ids)

    fetched_ids = [ts["id"] for ts in res]

    for ts_id in ts_ids:
        assert ts_id in fetched_ids


@pytest.fixture()
def file_in_tenant():
    file_list = CLIENT.get("/files".format(CLIENT._project), params={"limit": 1})
    return file_list.json()["data"]["items"][0]


def test_download_file(file_in_tenant):
    target_path = os.path.dirname(os.path.abspath(__file__)) + "/{}".format(random_string())

    CLIENT.download_file(file_in_tenant["id"], target_path)
    assert Path(target_path).is_file()
    os.remove(target_path)


def test_download_file_to_memory(file_in_tenant):
    file_bytes = CLIENT.download_file_to_memory(file_in_tenant["id"])
    assert isinstance(file_bytes, bytes)
