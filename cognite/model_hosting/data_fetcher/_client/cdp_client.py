import io
from collections import OrderedDict
from typing import Dict, List, Union

import pandas as pd

import cognite.model_hosting._cognite_model_hosting_common.utils as utils
from cognite.model_hosting.data_fetcher._client.api_client import ApiClient

DATAPOINTS_LIMIT = 100000
DATAPOINTS_LIMIT_AGGREGATES = 10000


class CdpClient(ApiClient):
    def get_datapoints_frame_single(
        self,
        id: int,
        start: int,
        end: int,
        aggregate: str = None,
        granularity: str = None,
        include_outside_points: bool = False,
    ) -> pd.DataFrame:
        limit = DATAPOINTS_LIMIT_AGGREGATES if aggregate else DATAPOINTS_LIMIT
        params = {
            "aggregates": aggregate,
            "granularity": granularity,
            "limit": limit,
            "start": start,
            "end": end,
            "includeOutsidePoints": include_outside_points,
        }
        url = "/timeseries/{}/data".format(id)
        datapoints = []
        while (not datapoints or len(datapoints[-1]) == limit) and params["end"] > params["start"]:
            res = self.get(url, params=params)
            res = res.json()["data"]["items"][0]["datapoints"]
            if not res:
                break
            datapoints.append(res)
            latest_timestamp = int(datapoints[-1][-1]["timestamp"])
            params["start"] = latest_timestamp + (utils.granularity_to_ms(granularity) if granularity else 1)
        dps = []
        [dps.extend(el) for el in datapoints]
        timestamps = [dp["timestamp"] for dp in dps]
        value_name = aggregate or "value"
        values = [dp[value_name] for dp in dps]
        df = pd.DataFrame(OrderedDict([("timestamp", timestamps), (value_name, values)]))
        if include_outside_points:
            df.drop_duplicates(inplace=True)
        return df

    def get_datapoints_frame(
        self, time_series: List[Dict[str, Union[int, str]]], granularity: str, start: int, end: int
    ) -> pd.DataFrame:
        limit = DATAPOINTS_LIMIT // len(time_series)

        ts_ids = [ts["id"] for ts in time_series]
        ts_names = {ts["id"]: ts["name"] for ts in (self.get_time_series_by_id(ts_ids))}

        time_series_by_name = [{"name": ts_names[ts["id"]], "aggregates": [ts["aggregate"]]} for ts in time_series]
        body = {"items": time_series_by_name, "granularity": granularity, "start": start, "end": end, "limit": limit}
        url = "/timeseries/dataframe"

        dataframes = []
        while (not dataframes or dataframes[-1].shape[0] == limit) and body["end"] > body["start"]:
            res = self.post(url=url, json=body, headers={"accept": "text/csv"})
            dataframes.append(pd.read_csv(io.StringIO(res.text)))
            if dataframes[-1].empty:
                break
            latest_timestamp = int(dataframes[-1].iloc[-1, 0])
            body["start"] = latest_timestamp + utils.granularity_to_ms(granularity)
        return pd.concat(dataframes).reset_index(drop=True)

    def get_time_series_by_id(self, ids: List[int]) -> List:
        url = "/timeseries/byids"
        body = {"items": list(set(ids))}
        return (self.post(url, json=body)).json()["data"]["items"]

    def download_file(self, id: int, target_path: str, chunk_size: int = 2 ** 21) -> None:
        url = "/files/{}/downloadlink".format(id)
        download_url = self.get(url=url).json()["data"]

        with self._requests_session.get(download_url, stream=True) as r:
            with open(target_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)

    def download_file_to_memory(self, id) -> bytes:
        url = "/files/{}/downloadlink".format(id)
        download_url = (self.get(url=url)).json()["data"]

        with self._requests_session.get(download_url) as response:
            return response.content
