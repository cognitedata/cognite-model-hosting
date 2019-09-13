import pandas as pd
import pytest

from cognite.client.data_classes import Datapoints, DatapointsList
from cognite.client.testing import mock_cognite_client
from cognite.model_hosting.data_fetcher._client.cdp_client import CdpClient, DatapointsFrameQuery


@pytest.fixture
def mock_cogcli_datapoints_retrieve_single():
    with mock_cognite_client() as cogmock:
        cogmock.datapoints.retrieve.return_value = Datapoints(
            id=1, external_id="1", value=[1, 2, 3], timestamp=[1000, 2000, 3000]
        )
        yield


def test_get_datapoints_frame_single(mock_cogcli_datapoints_retrieve_single):
    client = CdpClient()
    res = client.get_datapoints_frame_single(id=1, start=0, end=4000)
    assert (3, 1) == res.shape
    assert res.columns == ["value"]


@pytest.fixture
def mock_cogcli_datapoints_query():
    with mock_cognite_client() as cogmock:
        cogmock.datapoints.query.return_value = [
            DatapointsList([Datapoints(id=1, external_id="1", value=[1, 2, 3], timestamp=[1000, 2000, 3000])])
        ]
        yield


def test_get_datapoints_frame_multiple(mock_cogcli_datapoints_query):
    client = CdpClient()
    res = client.get_datapoints_frame_multiple(
        [DatapointsFrameQuery(id=1, start=0, end=4000, aggregate=None, granularity=None, include_outside_points=False)]
    )
    assert (3, 1) == res[0].shape
    assert res[0].columns == ["value"]


@pytest.fixture
def mock_cogcli_retrieve_dataframe():
    with mock_cognite_client() as cogmock:
        cogmock.datapoints.retrieve_dataframe.return_value = pd.DataFrame(
            [[1], [2], [3]], columns=["1"], index=[3000, 4000, 5000]
        )
        yield


def test_get_datapoints_frame(mock_cogcli_retrieve_dataframe):
    client = CdpClient()
    res = client.get_datapoints_frame(
        time_series=[{"id": 1, "aggregate": "avg"}], granularity="1s", start=0, end=150000
    )
    assert (3, 1) == res.shape
    assert res.columns == ["1"]
