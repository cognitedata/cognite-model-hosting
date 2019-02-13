from typing import Dict, List, Union

from cognite.data_fetcher.data_spec import DataSpec, FileSpec, TimeSeriesSpec
from cognite.data_fetcher.exceptions import SpecValidationError


class FilesFetcher:
    def __init__(self, file_specs: Dict[str, FileSpec]):
        self._file_specs = file_specs

    @property
    def aliases(self) -> List:
        return list(self._file_specs.keys())


class TimeSeriesFetcher:
    def __init__(self, time_series_specs: Dict[str, TimeSeriesSpec]):
        self._time_series_specs = time_series_specs

    @property
    def aliases(self) -> List:
        return list(self._time_series_specs.keys())


class DataFetcher:
    def __init__(self, data_spec: Union[DataSpec, Dict, str]):
        self._data_spec = self._load_data_spec(data_spec)
        self._data_spec.validate()

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
    def files(self) -> FilesFetcher:
        return FilesFetcher(self._data_spec.files)

    @property
    def time_series(self) -> TimeSeriesFetcher:
        return TimeSeriesFetcher(self._data_spec.time_series)
