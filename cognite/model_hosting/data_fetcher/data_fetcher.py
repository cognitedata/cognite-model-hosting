import asyncio
import os
from typing import Dict, List, Union

import pandas as pd

from cognite.model_hosting._utils import get_aggregate_func_return_name
from cognite.model_hosting.data_fetcher._client.cdp_client import CdpClient
from cognite.model_hosting.data_fetcher.exceptions import DirectoryDoesNotExist, InvalidAlias, InvalidFetchRequest
from cognite.model_hosting.data_spec import DataSpec, FileSpec, TimeSeriesSpec
from cognite.model_hosting.data_spec.exceptions import SpecValidationError


class FileFetcher:
    def __init__(self, file_specs: Dict[str, FileSpec], cdp_client: CdpClient):
        self._file_specs = file_specs
        self._cdp_client = cdp_client

    @property
    def aliases(self) -> List[str]:
        return list(self._file_specs.keys())

    def get_spec(self, alias: str) -> FileSpec:
        if alias not in self.aliases:
            raise InvalidAlias(alias)
        return self._file_specs[alias].copy()

    def fetch(self, alias: Union[str, List[str]], directory: str = None) -> None:
        directory = directory or os.getcwd()
        if not os.path.isdir(directory):
            raise DirectoryDoesNotExist(directory)

        if isinstance(alias, str):
            futures = [self._download_single_file(alias, directory)]
        elif isinstance(alias, list):
            futures = [self._download_single_file(a, directory) for a in alias]
        else:
            raise TypeError("alias must be of type str or list, was {}".format(type(alias)))

        asyncio.get_event_loop().run_until_complete(asyncio.gather(*futures))

    def fetch_to_memory(self, alias: Union[str, List[str]]) -> Union[bytes, Dict[str, bytes]]:
        if isinstance(alias, str):
            return asyncio.get_event_loop().run_until_complete(self._download_single_file_to_memory(alias))[alias]
        elif isinstance(alias, list):
            files = {}
            futures = [self._download_single_file_to_memory(a) for a in alias]
            res = asyncio.get_event_loop().run_until_complete(asyncio.gather(*futures))
            for file in res:
                files.update(file)
            return files
        raise TypeError("alias must be of type str or list, was {}".format(type(alias)))

    async def _download_single_file(self, alias: str, directory: str):
        file_id = self.get_spec(alias).id
        file_path = os.path.join(directory, alias)
        await self._cdp_client.download_file(file_id, file_path)

    async def _download_single_file_to_memory(self, alias):
        file_id = self.get_spec(alias).id
        return {alias: await self._cdp_client.download_file_to_memory(file_id)}


class TimeSeriesFetcher:
    def __init__(self, time_series_specs: Dict[str, TimeSeriesSpec], cdp_client: CdpClient):
        self._specs = time_series_specs
        self._cdp_client = cdp_client

    @property
    def aliases(self) -> List:
        return list(self._specs.keys())

    def get_spec(self, alias: str) -> TimeSeriesSpec:
        self._check_valid_alias(alias)
        return self._specs[alias].copy()

    def _check_valid_alias(self, alias: str):
        if type(alias) != str:
            raise TypeError("Alias `{}` ({}) was not a string".format(alias, type(alias)))
        if alias not in self._specs:
            raise InvalidAlias("No alias `{}` in the spec".format(alias))

    def _check_valid_aliases(self, aliases: List[str]):
        for alias in aliases:
            self._check_valid_alias(alias)

    def _check_only_aggregates(self, aliases: List[str]):
        for alias in aliases:
            if self._specs[alias].aggregate is None:
                raise InvalidFetchRequest("All time series of a data frame needs to be aggregates")

    def _get_common_start_end_granularity(self, aliases):
        starts = set()
        ends = set()
        granularities = set()
        for alias in aliases:
            starts.add(self._specs[alias].start)
            ends.add(self._specs[alias].end)
            granularities.add(self._specs[alias].granularity)

        if len(starts) != 1 or len(ends) != 1:
            raise InvalidFetchRequest(
                "The time series are not aligned. They need to have same start and end to be part of the same data frame"
            )

        if len(granularities) != 1:
            raise InvalidFetchRequest(
                "Granularity mismatch. All time series must have same granularity to be part of the same data frame"
            )

        return starts.pop(), ends.pop(), granularities.pop()

    def __convert_ts_names_to_aliases(self, df: pd.DataFrame) -> pd.DataFrame:
        name_to_label = {}
        ts_ids = [ts.id for ts in self._specs.values()]
        time_series = asyncio.get_event_loop().run_until_complete(self._cdp_client.get_time_series_by_id(ts_ids))
        ts_names = {ts["id"]: ts["name"] for ts in time_series}
        for alias, ts_spec in self._specs.items():
            if ts_spec.aggregate:
                agg_return_name = get_aggregate_func_return_name(ts_spec.aggregate)
                name_to_label[ts_names[ts_spec.id] + "|" + agg_return_name] = alias
            else:
                name_to_label[ts_names[ts_spec.id]] = alias
        return df.rename(columns=name_to_label)

    def fetch_dataframe(self, aliases: List[str]) -> pd.DataFrame:
        if type(aliases) != list:
            raise TypeError("Invalid argument type. Aliases should be a list of string")
        self._check_valid_aliases(aliases)
        self._check_only_aggregates(aliases)
        time_series = []
        for alias in aliases:
            spec = self._specs[alias]
            time_series.append({"id": spec.id, "aggregate": spec.aggregate})
        start, end, granularity = self._get_common_start_end_granularity(aliases)
        df = asyncio.get_event_loop().run_until_complete(
            self._cdp_client.get_datapoints_frame(time_series, granularity, start, end)
        )
        df_with_alias_columns = self.__convert_ts_names_to_aliases(df)
        return df_with_alias_columns

    def _fetch_datapoints_single(self, alias):
        self._check_valid_alias(alias)

        spec = self._specs[alias]
        return asyncio.get_event_loop().run_until_complete(
            self._cdp_client.get_datapoints_frame_single(
                spec.id, spec.start, spec.end, spec.aggregate, spec.granularity, spec.include_outside_points
            )
        )

    def _fetch_datapoints_multiple(self, aliases):
        self._check_valid_aliases(aliases)

        futures = []
        for alias in aliases:
            spec = self._specs[alias]
            futures.append(
                self._cdp_client.get_datapoints_frame_single(
                    spec.id, spec.start, spec.end, spec.aggregate, spec.granularity, spec.include_outside_points
                )
            )
        res = asyncio.get_event_loop().run_until_complete(asyncio.gather(*futures))
        data_frames = {alias: df for alias, df in zip(aliases, res)}
        return data_frames

    def fetch_datapoints(self, alias: Union[str, List[str]]) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
        if type(alias) == str:
            return self._fetch_datapoints_single(alias)
        elif type(alias) == list:
            return self._fetch_datapoints_multiple(alias)
        else:
            raise TypeError(
                "Invalid argument type. Specify either a single alias (string) or a list of aliases (list of strings)"
            )


class DataFetcher:
    def __init__(
        self, data_spec: Union[DataSpec, Dict, str], api_key: str = None, project: str = None, base_url: str = None
    ):
        self._data_spec = self._load_data_spec(data_spec)
        self._data_spec.validate()
        self._cdp_client = CdpClient(api_key=api_key, project=project, base_url=base_url)

        self._files_fetcher = FileFetcher(self._data_spec.files, self._cdp_client)
        self._time_series_fetcher = TimeSeriesFetcher(self._data_spec.time_series, self._cdp_client)

    def _load_data_spec(self, data_spec):
        if type(data_spec) == DataSpec:
            return data_spec.copy()
        elif type(data_spec) == dict:
            return DataSpec.load(data_spec)
        elif type(data_spec) == str:
            return DataSpec.from_json(data_spec)
        else:
            raise SpecValidationError("data_spec has to be of type DataSpec, dict or str (json).")

    def get_data_spec(self):
        return self._data_spec.copy()

    @property
    def files(self) -> FileFetcher:
        return self._files_fetcher

    @property
    def time_series(self) -> TimeSeriesFetcher:
        return self._time_series_fetcher
