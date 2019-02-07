from cognite_data_fetcher._client.cdp_client import CdpClient
from tests.utils import run_until_complete

CLIENT = None


@pytest.fixture(scope="session", autouse=True)
def cdp_client():
    global CLIENT
    CLIENT = CdpClient()


def test_get_datapoints():
    ts_list = run_until_complete(CLIENT.get("/timeseries".format(CLIENT._project), params={"limit": 1}))
    ts_name = ts_list["data"]["items"][0]["name"]
    dps = run_until_complete(CLIENT.get_datapoints(ts_name, start="1m-ago"))
    assert dps
