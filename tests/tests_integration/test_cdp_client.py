import os
from pathlib import Path

import pandas as pd
import pytest

from cognite.model_hosting.data_fetcher._client.cdp_client import CdpClient
from tests.utils import random_string

CLIENT = CdpClient()


def test_get_datapoints_frame_single(ts_ids, now):
    df = CLIENT.get_datapoints_frame_single(ts_ids["constant_3"], start=now - 3600 * 1000, end=now)
    assert all(["timestamp", "value"] == df.columns)
    assert 3 == round(df["value"].mean())


def test_get_datapoints_frame(ts_ids, now):
    time_series = [{"id": ts_ids["constant_3"], "aggregate": "min"}, {"id": ts_ids["constant_4"], "aggregate": "max"}]
    df = CLIENT.get_datapoints_frame(time_series, granularity="1h", start=now - 6 * 3600 * 1000, end=now)
    assert isinstance(df, pd.DataFrame)
    assert all(["timestamp", "test__constant_3_with_noise|min", "test__constant_4_with_noise|max"] == df.columns)
    assert 3 == round(df["test__constant_3_with_noise|min"].mean())
    assert 4 == round(df["test__constant_4_with_noise|max"].mean())


def test_get_time_series_by_id(ts_ids):
    ids_to_fetch = [ts_ids["constant_3"], ts_ids["constant_4"]]
    res = CLIENT.get_time_series_by_id(ids_to_fetch)

    fetched_ids = [ts["id"] for ts in res]

    assert set(ids_to_fetch) == set(fetched_ids)


@pytest.fixture()
def file_in_tenant():
    file_list = CLIENT.get("/files".format(CLIENT._project), params={"limit": 1})
    return file_list.json()["data"]["items"][0]


def test_download_file(file_ids):
    target_path = os.path.dirname(os.path.abspath(__file__)) + "/{}".format(random_string())

    print(file_ids["a.txt"])
    CLIENT.download_file(file_ids["a.txt"], target_path)
    assert Path(target_path).is_file()
    with open(target_path, "r") as f:
        assert "a" == f.read()
    os.remove(target_path)


def test_download_file_to_memory(file_ids):
    file_bytes = CLIENT.download_file_to_memory(file_ids["a.txt"])
    assert isinstance(file_bytes, bytes)
    assert "a" == file_bytes.decode()
