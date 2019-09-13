import os
from pathlib import Path

import pandas as pd

from cognite.model_hosting.data_fetcher._cdp_client import CdpClient
from tests.utils import random_string

CLIENT = CdpClient()


def test_get_datapoints_frame_single(ts_ids, now):
    df = CLIENT.get_datapoints_frame_single(ts_ids["constant_3"], start=now - 3600 * 1000, end=now)
    assert all(["value"] == df.columns)
    assert 3 == round(df["value"].mean())


def test_get_datapoints_frame(ts_ids, now):
    time_series = [
        {"id": ts_ids["constant_3"], "aggregates": ["min"]},
        {"id": ts_ids["constant_4"], "aggregates": ["max"]},
    ]
    df = CLIENT.get_datapoints_frame(time_series, granularity="1h", start=now - 6 * 3600 * 1000, end=now)
    assert isinstance(df, pd.DataFrame)
    print(df.columns)
    assert all(["{}|min".format(ts_ids["constant_3"]), "{}|max".format(ts_ids["constant_4"])] == df.columns)
    assert 3 == round(df["{}|min".format(ts_ids["constant_3"])].mean())
    assert 4 == round(df["{}|max".format(ts_ids["constant_4"])].mean())


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
