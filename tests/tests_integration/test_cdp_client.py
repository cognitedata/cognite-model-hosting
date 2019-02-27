import os
from pathlib import Path

import pandas as pd
import pytest

from cognite.model_hosting.data_fetcher._client.cdp_client import CdpClient
from tests.utils import get_time_series_and_create_if_missing, random_string

CLIENT = CdpClient()

prefix = "DO_NOT_DELETE_MODEL_HOSTING_TEST_TS"
TEST_TS_1 = prefix + "_1"
TEST_TS_2 = prefix + "_2"

DPS_START = 1544569200000
DPS_END = DPS_START + 10000


@pytest.fixture(scope="session")
def time_series_in_tenant():
    return get_time_series_and_create_if_missing([TEST_TS_1, TEST_TS_2], prefix, DPS_START, DPS_END)


def test_get_datapoints_frame_single(time_series_in_tenant):
    ts_id = time_series_in_tenant[0]["id"]
    df = CLIENT.get_datapoints_frame_single(ts_id, start=DPS_START, end=DPS_END)
    assert 2 == df.shape[1]
    assert df.shape[0] > 0


def test_get_datapoints_frame(time_series_in_tenant):
    time_series = [
        {"id": time_series_in_tenant[0]["id"], "aggregate": "min"},
        {"id": time_series_in_tenant[1]["id"], "aggregate": "max"},
    ]
    res = CLIENT.get_datapoints_frame(time_series, granularity="1s", start=DPS_START, end=DPS_END)
    assert isinstance(res, pd.DataFrame)
    assert 3 == res.shape[1]
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
