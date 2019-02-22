import asyncio
import re

import pandas as pd
import pytest

from cognite.model_hosting.data_fetcher._client.cdp_client import CdpClient
from tests.utils import BASE_URL_V0_5, BASE_URL_V0_6


@pytest.fixture
def mock_get_datapoints_successive(rsps):
    dps_100_000 = {
        "data": {"items": [{"name": "ts", "datapoints": [{"timestamp": i, "value": i} for i in range(100000)]}]}
    }
    dps_50_000 = {
        "data": {"items": [{"name": "ts", "datapoints": [{"timestamp": i, "value": i} for i in range(100000, 150000)]}]}
    }

    rsps.add(
        rsps.POST, BASE_URL_V0_6 + "/timeseries/byids", status=200, json={"data": {"items": [{"name": "ts", "id": 1}]}}
    )
    rsps.add(rsps.GET, re.compile(BASE_URL_V0_5 + "/timeseries/data/ts?(.*start=0.*)"), status=200, json=dps_100_000)
    rsps.add(
        rsps.GET, re.compile(BASE_URL_V0_5 + "/timeseries/data/ts?(.*start=100000.*)"), status=200, json=dps_50_000
    )


def test_get_datapoints_paging(mock_get_datapoints_successive):
    client = CdpClient()
    res = client.get_datapoints_frame_single(id=1, start=0, end=150000)
    assert (150000, 2) == res.shape


@pytest.fixture
def mock_get_datapoints_frame_successive(rsps):
    dps_100_000 = [{"timestamp": i, "ts": i} for i in range(0, 100000)]
    dps_50_000 = [{"timestamp": i, "ts": i} for i in range(100000, 150000)]

    dps_100_000_csv = pd.DataFrame(dps_100_000).to_csv(index=False)
    dps_50_000_csv = pd.DataFrame(dps_50_000).to_csv(index=False)

    rsps.add(
        rsps.POST, BASE_URL_V0_6 + "/timeseries/byids", status=200, json={"data": {"items": [{"name": "ts", "id": 1}]}}
    )
    rsps.add(rsps.POST, BASE_URL_V0_5 + "/timeseries/dataframe", status=200, body=dps_100_000_csv)
    rsps.add(rsps.POST, BASE_URL_V0_5 + "/timeseries/dataframe", status=200, body=dps_50_000_csv)


def test_get_datapoints_frame_paging(mock_get_datapoints_frame_successive):
    client = CdpClient()
    res = client.get_datapoints_frame(
        time_series=[{"id": 1, "aggregate": "avg"}], granularity="1s", start=0, end=150000
    )
    assert (150000, 2) == res.shape
