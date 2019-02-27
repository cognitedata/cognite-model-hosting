import random
import string
from time import sleep
from typing import List

from cognite.model_hosting.data_fetcher._client.cdp_client import CdpClient

BASE_URL = "https://api.cognitedata.com"
BASE_URL_V0_5 = BASE_URL + "/api/0.5/projects/test"
BASE_URL_V0_6 = BASE_URL + "/api/0.6/projects/test"


def random_string(length: int = 5):
    return "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(length))


def get_time_series_and_create_if_missing(names: List[str], prefix, start, end):
    client = CdpClient()
    time_series = client.get("/timeseries", params={"limit": 10, "q": prefix}).json()["data"]["items"]

    if not len(time_series) == len(names):
        missing_time_series = list(set(names) - set([ts["name"] for ts in time_series]))
        for ts_name in missing_time_series:
            client.post("/timeseries", json={"items": [{"name": ts_name}]}).json()

        for ts_name in missing_time_series:
            res = client.get("/timeseries", params={"q": ts_name}).json()["data"]["items"]
            while len(res) == 0:
                res = client.get("/timeseries", params={"q": ts_name}).json()["data"]["items"]
                sleep(0.5)
            ts = res[0]
            dps = [{"timestamp": timestamp, "value": val} for val, timestamp in enumerate(range(start, end, 100))]
            client.post("/timeseries/{}/data".format(ts["id"]), json={"items": dps})
        time_series = client.get("/timeseries", params={"limit": 10, "q": prefix}).json()["data"]["items"]
    return time_series
