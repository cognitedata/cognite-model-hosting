from typing import List, Union
from urllib.parse import quote

from cognite_data_fetcher._client.api_client import ApiClient

DATAPOINTS_LIMIT = 100000
DATAPOINTS_LIMIT_AGGREGATES = 10000


class CdpClient(ApiClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def get_datapoints(
        self,
        name: str,
        start: Union[str, int],
        end: Union[str, int] = None,
        aggregates: List[str] = None,
        granularity: str = None,
        include_outside_points: bool = False,
    ):
        params = {
            "aggregates": aggregates,
            "granularity": granularity,
            "limit": DATAPOINTS_LIMIT_AGGREGATES if aggregates else DATAPOINTS_LIMIT,
            "start": start,
            "end": end,
            "includeOutsidePoints": include_outside_points,
        }
        url = "/timeseries/data/{}".format(quote(name, safe=""))
        return await self.get(url, params=params)
