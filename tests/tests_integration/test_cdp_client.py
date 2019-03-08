import os
from pathlib import Path

import pandas as pd
import pytest

from cognite.model_hosting.data_fetcher._client.cdp_client import CdpClient
from tests.utils import random_string

CLIENT = CdpClient()

# Time series generated by live data generator deployment
TS_ID_1 = 2568463408456579
TS_ID_2 = 6107429507410783


def test_get_datapoints_frame_single():
    df = CLIENT.get_datapoints_frame_single(TS_ID_1, start=1543363200000, end=1543449600000)
    assert df.shape[1] == 2
    assert df.shape[0] > 0


def test_get_datapoints_frame():
    time_series = [{"id": TS_ID_1, "aggregate": "min"}, {"id": TS_ID_2, "aggregate": "max"}]
    res = CLIENT.get_datapoints_frame(time_series, granularity="1h", start=1543363200000, end=1543449600000)
    assert isinstance(res, pd.DataFrame)
    assert 3 == res.shape[1]
    assert res.shape[0] > 0


def test_get_time_series_by_id():
    res = CLIENT.get_time_series_by_id([TS_ID_1, TS_ID_2])

    fetched_ids = [ts["id"] for ts in res]

    for ts_id in [TS_ID_1, TS_ID_2]:
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
