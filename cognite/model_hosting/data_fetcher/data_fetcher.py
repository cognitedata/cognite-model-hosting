import os
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Dict, List, Union

import pandas as pd

from cognite.model_hosting.data_fetcher._client.api_client import MAX_CONNECTION_POOL_SIZE
from cognite.model_hosting.data_fetcher._client.cdp_client import CdpClient
from cognite.model_hosting.data_fetcher.exceptions import DirectoryDoesNotExist, InvalidAlias, InvalidFetchRequest
from cognite.model_hosting.data_spec import DataSpec, FileSpec, TimeSeriesSpec
from cognite.model_hosting.data_spec.exceptions import SpecValidationError

_NUMBER_OF_THREADS = MAX_CONNECTION_POOL_SIZE


def _execute_tasks_concurrently(func, tasks):
    with ThreadPoolExecutor(_NUMBER_OF_THREADS) as p:
        futures = [p.submit(func, *task) for task in tasks]
        return [future.result() for future in futures]


class FileFetcher:
    """An object used for fetching files from CDP.

    .. attention:: This class should never be instantiated directly, but rather accessed through the DataFetcher class.

    Examples:
        Using the FileFetcher::

            from cognite.model_hosting.data_fetcher import DataFetcher

            data_fetcher = DataFetcher(data_spec=...)

            my_file = data_fetcher.files.fetch_to_memory(alias="my_file")
    """

    def __init__(self, file_specs: Dict[str, FileSpec], cdp_client: CdpClient):
        self._file_specs = file_specs
        self._cdp_client = cdp_client

    @property
    def aliases(self) -> List[str]:
        """Returns the file aliases defined in the data spec passed to the data fetcher.

        Returns:
            List[str]: The file aliases defined in the data spec passed to the data fetcher.
        """
        return list(self._file_specs.keys())

    def get_spec(self, alias: str) -> FileSpec:
        """Returns the FileSpec given by the alias

        Args:
            alias (str): The alias of the file.

        Returns:
            FileSpec: The file spec given by the alias.
        """
        if alias not in self.aliases:
            raise InvalidAlias(alias)
        return self._file_specs[alias].copy()

    def fetch(self, alias: Union[str, List[str]], directory: str = None) -> None:
        """Fetches the file(s) given by the provided alias(es) to a given directory.

        If provided, the directory must exist. If not provided, it will default to the current working directory.

        If a single alias is passed, a pandas DataFrame will be returned. If a list of aliases is passed, a dictionary
        which maps aliases to DataFrames is returned.

        Args:
            alias (Union[List[str], str]): The alias(es) to download file(s) for.
            directory(str, optional): The directory to download the file(s) to.

        Returns:
            None
        """
        directory = directory or os.getcwd()
        if not os.path.isdir(directory):
            raise DirectoryDoesNotExist(directory)

        if isinstance(alias, str):
            tasks = [(alias, directory)]
        elif isinstance(alias, list):
            tasks = [(a, directory) for a in alias]
        else:
            raise TypeError("alias must be of type str or list, was {}".format(type(alias)))

        _execute_tasks_concurrently(self._download_single_file, tasks)

    def fetch_to_memory(self, alias: Union[str, List[str]]) -> Union[bytes, Dict[str, bytes]]:
        """Fetches the file(s) given by the provided alias(es) to memory.

        If a list of aliases is passed, this method will return a dictionary mapping aliases to their respective file
        bytes.

        Args:
            alias (Union[List[str], str]): The alias(es) to download file(s) for.

        Returns:
            Union[bytes, Dict[str, bytes]]: The file(s).
        """
        if isinstance(alias, str):
            return self._download_single_file_to_memory(alias)[alias]
        elif isinstance(alias, list):
            files = {}
            tasks = [(a,) for a in alias]
            res = _execute_tasks_concurrently(self._download_single_file_to_memory, tasks)
            for file in res:
                files.update(file)
            return files
        raise TypeError("alias must be of type str or list, was {}".format(type(alias)))

    def _download_single_file(self, alias: str, directory: str):
        file_id = self.get_spec(alias).id
        file_path = os.path.join(directory, alias)
        self._cdp_client.download_file(file_id, file_path)

    def _download_single_file_to_memory(self, alias):
        file_id = self.get_spec(alias).id
        return {alias: self._cdp_client.download_file_to_memory(file_id)}


class TimeSeriesFetcher:
    """An object used for fetching time series data from CDP.

    .. attention:: This class should never be instantiated directly, but rather accessed through the DataFetcher class.

    Examples:
        Using the TimeSeriesFetcher::

            from cognite.model_hosting.data_fetcher import DataFetcher

            data_fetcher = DataFetcher(data_spec=...)

            my_datapoints = data_fetcher.time_series.fetch_datapoints(alias="my_ts_1")
    """

    def __init__(self, time_series_specs: Dict[str, TimeSeriesSpec], cdp_client: CdpClient):
        self._specs = time_series_specs
        self._cdp_client = cdp_client

    @property
    def aliases(self) -> List:
        """Returns the time series aliases defined in the data spec passed to the data fetcher.

        Returns:
            List[str]: The time series aliases defined in the data spec passed to the data fetcher.
        """
        return list(self._specs.keys())

    def get_spec(self, alias: str) -> TimeSeriesSpec:
        """Returns the TimeSeriesSpec given by the alias.

        Args:
            alias (str): The alias of the time series.

        Returns:
            TimeSeriesSpec: The time series spec given by the alias.
        """
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

    def fetch_dataframe(self, aliases: List[str]) -> pd.DataFrame:
        """Fetches a time-aligned dataframe of the time series specified by the provided aliases.

        This method requires that all specified aliases must refer to time series aggregates with the same granularity,
        start, and end.

        Args:
            aliases (List[str]): The list of aliases to retrieve a dataframe for.

        Returns:
            pandas.DataFrame: A pandas dataframe with the requested data.
        """
        if type(aliases) != list:
            raise TypeError("Invalid argument type. Aliases should be a list of string")
        self._check_valid_aliases(aliases)
        self._check_only_aggregates(aliases)
        time_series = []
        for alias in aliases:
            spec = self._specs[alias]
            time_series.append({"id": spec.id, "aggregate": spec.aggregate})
        start, end, granularity = self._get_common_start_end_granularity(aliases)
        df = self._cdp_client.get_datapoints_frame(time_series, granularity, start, end)
        df.columns = ["timestamp"] + aliases
        return df

    def _fetch_datapoints_single(self, alias):
        self._check_valid_alias(alias)

        spec = self._specs[alias]
        return self._cdp_client.get_datapoints_frame_single(
            spec.id, spec.start, spec.end, spec.aggregate, spec.granularity, spec.include_outside_points
        )

    def _fetch_datapoints_multiple(self, aliases):
        self._check_valid_aliases(aliases)

        tasks = []
        for alias in aliases:
            spec = self._specs[alias]
            tasks.append((spec.id, spec.start, spec.end, spec.aggregate, spec.granularity, spec.include_outside_points))
        res = _execute_tasks_concurrently(self._cdp_client.get_datapoints_frame_single, tasks)
        data_frames = {alias: df for alias, df in zip(aliases, res)}
        return data_frames

    def fetch_datapoints(self, alias: Union[str, List[str]]) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
        """Fetches dataframes for the time series specified by the aliases.

        If a single alias is passed, a pandas DataFrame will be returned. If a list of aliases is passed, a dictionary
        which maps aliases to DataFrames is returned.

        Args:
            alias (Union[List[str], str]): The alias(es) to retrieve data for.

        Returns:
            Union[pd.DataFrame, Dict[str, pd.DataFrame]: The requested dataframe(s).
        """
        if type(alias) == str:
            return self._fetch_datapoints_single(alias)
        elif type(alias) == list:
            return self._fetch_datapoints_multiple(alias)
        else:
            raise TypeError(
                "Invalid argument type. Specify either a single alias (string) or a list of aliases (list of strings)"
            )


class DataFetcher:
    """Creates an instance of DataFetcher.

    Args:
        data_spec (DataSpec): The data spec which describes the desired data.
        api_key (str, optional): API key for authenticating against CDP. Defaults to the value of the environment
                                variable "COGNITE_API_KEY".
        project (str, optional): Project. Defaults to project of given API key.
        base_url (str, optional): Base url to send requests to. Defaults to "https://api.cognitedata.com".
    """

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
        """Returns a copy of the DataSpec passed to the DataFetcher.

        Returns:
            DataSpec: A copy of the DataSpec passed to the DataFetcher.
        """
        return self._data_spec.copy()

    @property
    def files(self) -> FileFetcher:
        """Returns an instance of FileFetcher.

        Returns:
            FileFetcher: An instance of FileFetcher.
        """
        return self._files_fetcher

    @property
    def time_series(self) -> TimeSeriesFetcher:
        """Returns an instance of TimeSeriesFetcher.

        Returns:
            TimeSeriesFetcher: An instance of TimeSeriesFetcher.
        """
        return self._time_series_fetcher
