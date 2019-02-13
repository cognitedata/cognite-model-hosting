import asyncio
import os
from typing import Dict, List, Union

from cognite.data_fetcher._client.cdp_client import CdpClient
from cognite.data_fetcher.data_spec import DataSpec, FileSpec, TimeSeriesSpec
from cognite.data_fetcher.exceptions import DirectoryDoesNotExist, InvalidAlias, SpecValidationError


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

    def fetch(self, alias: Union[str, List[str]], directory: str = None):
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
        self._time_series_specs = time_series_specs
        self._cdp_client = cdp_client

    @property
    def aliases(self) -> List:
        return list(self._time_series_specs.keys())


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
