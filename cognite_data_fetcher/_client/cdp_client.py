import io
from typing import Dict, List, Union
from urllib.parse import quote

import pandas as pd

from cognite_data_fetcher._client.api_client import ApiClient

DATAPOINTS_LIMIT = 100000
DATAPOINTS_LIMIT_AGGREGATES = 10000


class CdpClient(ApiClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def get_datapoints(
        self,
        id: int,
        start: Union[str, int],
        end: Union[str, int] = None,
        aggregates: List[str] = None,
        granularity: str = None,
        limit: int = None,
        include_outside_points: bool = False,
    ):
        params = {
            "aggregates": aggregates,
            "granularity": granularity,
            "limit": limit or (DATAPOINTS_LIMIT_AGGREGATES if aggregates else DATAPOINTS_LIMIT),
            "start": start,
            "end": end,
            "includeOutsidePoints": include_outside_points,
        }
        ts = await self.get_time_series_by_id([id])
        url = "/timeseries/data/{}".format(quote(ts["data"]["items"][0]["name"], safe=""))
        return await self.get(url, params=params)

    async def get_datapoints_frame(
        self, time_series: List[Dict[str, Union[int, List[str]]]], granularity: str, start, end=None, limit=None
    ):
        ts_ids = [ts["id"] for ts in time_series]
        ts_names = {ts["id"]: ts["name"] for ts in (await self.get_time_series_by_id(ts_ids))["data"]["items"]}

        time_series_by_name = [{"name": ts_names[ts["id"]], "aggregates": [ts["aggregate"]]} for ts in time_series]
        body = {"items": time_series_by_name, "granularity": granularity, "start": start, "end": end, "limit": limit}
        url = "/timeseries/dataframe"
        res = await self.post(url, body=body, headers={"accept": "text/csv"})
        return pd.read_csv(io.StringIO(res))

    async def get_time_series_by_id(self, ids: List[int]):
        url = "/timeseries/byids"
        body = {"items": list(set(ids))}
        return await self.post(url, body=body, api_version="0.6")

    async def download_file(self, id: int, target_path: str, chunk_size: int = 100):
        url = "/files/{}/downloadlink".format(id)
        download_url = (await self.get(url=url))["data"]

        async with self._client_session.get(download_url) as response:
            with open(target_path, "wb") as fd:
                while True:
                    chunk = await response.content.read(chunk_size)
                    if not chunk:
                        break
                    fd.write(chunk)

    async def download_file_in_memory(self, id):
        url = "/files/{}/downloadlink".format(id)
        download_url = (await self.get(url=url))["data"]

        async with self._client_session.get(download_url) as response:
            return await response.read()
