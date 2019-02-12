import asyncio

import pandas as pd
import pytest
from cognite.data_fetcher._client.cdp_client import CdpClient

from tests.utils import BASE_URL_V0_5, BASE_URL_V0_6


@pytest.fixture
def mock_get_datapoints_successive(http_mock):
    dps_100_000 = {
        "data": {"items": [{"name": "ts", "datapoints": [{"timestamp": i, "value": i} for i in range(100000)]}]}
    }
    dps_50_000 = {
        "data": {"items": [{"name": "ts", "datapoints": [{"timestamp": i, "value": i} for i in range(100000, 150000)]}]}
    }

    http_mock.post(
        BASE_URL_V0_6 + "/timeseries/byids", status=200, payload={"data": {"items": [{"name": "ts", "id": 1}]}}
    )
    http_mock.get(
        BASE_URL_V0_5 + "/timeseries/data/ts?end=150000&includeOutsidePoints=false&limit=100000&start=0",
        status=200,
        payload=dps_100_000,
    )
    http_mock.get(
        BASE_URL_V0_5 + "/timeseries/data/ts?end=150000&includeOutsidePoints=false&limit=100000&start=100000",
        status=200,
        payload=dps_50_000,
    )


def test_get_datapoints_paging(mock_get_datapoints_successive):
    client = CdpClient()
    res = asyncio.get_event_loop().run_until_complete(client.get_datapoints_frame_single(id=1, start=0, end=150000))
    assert (150000, 2) == res.shape


@pytest.fixture
def mock_get_datapoints_frame_successive(http_mock):
    dps_100_000 = [{"timestamp": i, "ts": i} for i in range(0, 100000)]
    dps_50_000 = [{"timestamp": i, "ts": i} for i in range(100000, 150000)]

    dps_100_000_csv = pd.DataFrame(dps_100_000).to_csv(index=False)
    dps_50_000_csv = pd.DataFrame(dps_50_000).to_csv(index=False)

    http_mock.post(
        BASE_URL_V0_6 + "/timeseries/byids", status=200, payload={"data": {"items": [{"name": "ts", "id": 1}]}}
    )
    http_mock.post(BASE_URL_V0_5 + "/timeseries/dataframe", status=200, body=dps_100_000_csv)
    http_mock.post(BASE_URL_V0_5 + "/timeseries/dataframe", status=200, body=dps_50_000_csv)


def test_get_datapoints_frame_paging(mock_get_datapoints_frame_successive):
    cl = CdpClient()
    res = asyncio.get_event_loop().run_until_complete(
        cl.get_datapoints_frame(time_series=[{"id": 1, "aggregate": "avg"}], granularity="1s", start=0, end=150000)
    )
    assert (150000, 2) == res.shape
