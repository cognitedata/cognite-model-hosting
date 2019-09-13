import random
import string
from typing import Dict, List, Union

import pandas as pd

from cognite.client import CogniteClient
from cognite.client.data_classes import DatapointsQuery


class DatapointsFrameQuery:
    def __init__(self, id, external_id, start, end, aggregate, granularity, include_outside_points):
        self.id = id
        self.external_id = external_id
        self.start = start
        self.end = end
        self.aggregate = aggregate
        self.granularity = granularity
        self.include_outside_points = include_outside_points


class CdpClient:
    def __init__(self, api_key: str = None, project: str = None, base_url: str = None, client_name: str = None):
        self.cognite_client = CogniteClient(
            api_key=api_key, project=project, base_url=base_url, client_name=client_name
        )
        self.max_workers = self.cognite_client.config.max_workers

    def get_datapoints_frame_single(
        self,
        id: int,
        external_id: str,
        start: int,
        end: int,
        aggregate: str = None,
        granularity: str = None,
        include_outside_points: bool = False,
    ) -> pd.DataFrame:
        df = self.cognite_client.datapoints.retrieve(
            id=id,
            external_id=external_id,
            start=start,
            end=end,
            aggregates=[aggregate] if aggregate else None,
            granularity=granularity,
            include_outside_points=include_outside_points,
        ).to_pandas()
        df.columns = [aggregate if aggregate else "value"]
        return df

    def get_datapoints_frame_multiple(self, queries: List[DatapointsFrameQuery]) -> List[pd.DataFrame]:
        datapoints_queries = [
            DatapointsQuery(
                id=q.id,
                start=q.start,
                end=q.end,
                aggregates=[q.aggregate] if q.aggregate else None,
                granularity=q.granularity,
                include_outside_points=q.include_outside_points,
            )
            for q in queries
        ]
        res = self.cognite_client.datapoints.query(datapoints_queries)
        dfs = [dpslist[0].to_pandas() for dpslist in res]
        for df, q in zip(dfs, queries):
            df.columns = [q.aggregate if q.aggregate else "value"]
        return dfs

    def get_datapoints_frame(
        self, time_series: List[Dict[str, Union[int, str]]], granularity: str, start: int, end: int
    ) -> pd.DataFrame:
        return self.cognite_client.datapoints.retrieve_dataframe(
            id=time_series, granularity=granularity, start=start, end=end, aggregates=[]
        )

    def download_file(self, id: int, external_id: str, target_path: str) -> None:
        self.cognite_client.files.download_to_path(id=id, external_id=external_id, path=target_path)

    def download_file_to_memory(self, id: int, external_id: str) -> bytes:
        return self.cognite_client.files.download_bytes(id=id, external_id=external_id)
