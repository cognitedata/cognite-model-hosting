import os
from typing import List
from unittest.mock import patch

import pandas as pd
import pytest

from cognite.model_hosting.data_fetcher import DataFetcher
from cognite.model_hosting.data_fetcher._cdp_client import DatapointsFrameQuery
from cognite.model_hosting.data_fetcher.exceptions import DirectoryDoesNotExist, InvalidAlias, InvalidFetchRequest
from cognite.model_hosting.data_spec import DataSpec, FileSpec, TimeSeriesSpec
from cognite.model_hosting.data_spec.exceptions import SpecValidationError
from tests.utils import BASE_URL_V1


@pytest.mark.parametrize("data_spec", [DataSpec(), {}, "{}"])
def test_empty_data_spec(rsps, data_spec):
    data_fetcher = DataFetcher(data_spec)
    assert data_fetcher.get_data_spec() == DataSpec()


def test_invalid_spec_type():
    with pytest.raises(SpecValidationError, match="has to be of type"):
        DataFetcher(123)


def test_get_data_spec():
    data_spec = DataSpec(files={"f1": FileSpec(id=123)})
    getted_data_spec = DataFetcher(data_spec).get_data_spec()
    getted_data_spec.files["f1"].id = 234
    assert data_spec.files["f1"].id == 123


class TestFileFetcher:
    @pytest.fixture(scope="class")
    def file_specs(self):
        return {"f1": FileSpec(id=123), "f2": FileSpec(id=234)}

    @pytest.fixture
    def data_fetcher(self, file_specs, rsps):
        data_spec = DataSpec(files=file_specs)
        data_fetcher = DataFetcher(data_spec)
        return data_fetcher

    def test_get_aliases(self, data_fetcher):
        aliases = data_fetcher.files.aliases
        assert 2 == len(aliases)
        assert {"f1", "f2"} == set(aliases)

    def test_get_spec(self, data_fetcher, file_specs):
        spec = data_fetcher.files.get_spec("f1")
        assert spec.id == 123
        assert spec == file_specs["f1"]

    def test_get_spec_does_not_exist(self, data_fetcher):
        with pytest.raises(InvalidAlias):
            data_fetcher.files.get_spec("does-not-exist")

    def test_get_spec_does_not_mutate_fetcher_state(self, data_fetcher):
        mutated_spec = data_fetcher.files.get_spec("f1")
        mutated_spec.id = 1000
        spec = data_fetcher.files.get_spec("f1")
        assert spec.id != 1000

    @pytest.fixture
    def mock_file_download(self, rsps):
        mock_download_url = "http://download.url"
        rsps.assert_all_requests_are_fired = False
        rsps.add(
            rsps.POST,
            BASE_URL_V1 + "/files/downloadlink",
            status=200,
            json={"items": [{"id": 123, "downloadUrl": mock_download_url}]},
        )
        rsps.add(rsps.GET, mock_download_url, status=200, body=b"blablabla")
        rsps.add(
            rsps.POST,
            BASE_URL_V1 + "/files/downloadlink",
            status=200,
            json={"items": [{"id": 123, "downloadUrl": mock_download_url}]},
        )
        rsps.add(rsps.GET, mock_download_url, status=200, body=b"blablabla")

    @staticmethod
    def assert_file_exists_and_has_content(file_path, content):
        assert os.path.isfile(file_path)
        with open(file_path, "rb") as f:
            assert content == f.read()
        os.remove(file_path)

    @pytest.mark.parametrize("directory", [None, "/tmp"])
    def test_fetch_single_file(self, mock_file_download, data_fetcher, directory):
        data_fetcher.files.fetch(alias="f1", directory=directory)
        file_path = os.path.join(directory or os.getcwd(), "f1")

        self.assert_file_exists_and_has_content(file_path, b"blablabla")

    @pytest.mark.parametrize("directory", [None, "/tmp"])
    def test_fetch_multiple_files(self, mock_file_download, data_fetcher, file_specs, directory):
        data_fetcher.files.fetch(alias=list(file_specs), directory=directory)
        file_paths = [os.path.join(directory or os.getcwd(), file_name) for file_name in list(file_specs)]

        for fp in file_paths:
            self.assert_file_exists_and_has_content(fp, b"blablabla")

    def test_fetch_file_invalid_alias(self, data_fetcher):
        with pytest.raises(InvalidAlias):
            data_fetcher.files.fetch("does-not-exist")

    def test_fetch_file_invalid_alias_type(self, data_fetcher):
        with pytest.raises(TypeError):
            data_fetcher.files.fetch(alias=123)

    def test_fetch_file_invalid_directory(self, data_fetcher, mock_file_download):
        with pytest.raises(DirectoryDoesNotExist):
            data_fetcher.files.fetch(alias="f1", directory="/does/not/exist")

    def test_fetch_single_file_to_memory(self, data_fetcher, mock_file_download):
        content = data_fetcher.files.fetch_to_memory("f1")
        assert b"blablabla" == content

    def test_fetch_multiple_files_to_memory(self, data_fetcher, mock_file_download):
        content = data_fetcher.files.fetch_to_memory(["f1", "f2"])
        assert {"f1": b"blablabla", "f2": b"blablabla"} == content

    def test_fetch_file_to_memory_invalid_alias_type(self, data_fetcher):
        with pytest.raises(TypeError):
            data_fetcher.files.fetch_to_memory(alias=123)

    def test_fetch_file_to_memory_invalid_alias(self, data_fetcher):
        with pytest.raises(InvalidAlias):
            data_fetcher.files.fetch_to_memory("does-not-exist")


class TestTimeSeries:
    @pytest.fixture(scope="class")
    def data_fetcher(self):
        return DataFetcher(
            DataSpec(
                time_series={
                    "ts1": TimeSeriesSpec(id=1234, start=3000, end=5000, aggregate="average", granularity="1s"),
                    "ts2": TimeSeriesSpec(id=2345, start=3000, end=5000, aggregate="max", granularity="1s"),
                    "ts3": TimeSeriesSpec(id=3456, start=4000, end=9000, aggregate="min", granularity="1s"),
                    "ts4": TimeSeriesSpec(id=4567, start=3000, end=5000, aggregate="average", granularity="1m"),
                    "ts5": TimeSeriesSpec(id=5678, start=6000, end=8000),
                }
            )
        )

    @pytest.fixture
    def cdp_client_mock(self, data_fetcher):
        with patch.object(data_fetcher.time_series, "_cdp_client") as mock:
            yield mock

    def test_get_aliases(self, data_fetcher):
        aliases = data_fetcher.time_series.aliases
        assert len(aliases) == 5
        assert set(aliases) == {"ts1", "ts2", "ts3", "ts4", "ts5"}

    def test_get_spec(self, data_fetcher):
        spec = data_fetcher.time_series.get_spec("ts2")
        assert spec == TimeSeriesSpec(id=2345, start=3000, end=5000, aggregate="max", granularity="1s")

        spec.end += 1000
        assert spec != data_fetcher.time_series.get_spec("ts2")  # Ensure immutability

    def test_get_spec_invalid_alias(self, data_fetcher):
        with pytest.raises(InvalidAlias, match="non-existing-alias"):
            data_fetcher.time_series.get_spec("non-existing-alias")
        with pytest.raises(TypeError, match="string"):
            data_fetcher.time_series.get_spec(123)

    def test_fetch_dataframe(self, data_fetcher, cdp_client_mock):
        def get_datapoints_frame(time_series, granularity, start, end):
            assert time_series == [{"id": 1234, "aggregates": ["average"]}, {"id": 2345, "aggregates": ["max"]}]
            assert granularity == "1s"
            assert start == 3000
            assert end == 5000
            return pd.DataFrame([[1, 10], [2, 20], [3, 30]], columns=["ts1", "ts2"], index=[1000, 2000, 3000])

        cdp_client_mock.get_datapoints_frame.side_effect = get_datapoints_frame
        df = data_fetcher.time_series.fetch_dataframe(["ts1", "ts2"])
        assert (df.columns == ["ts1", "ts2"]).all()
        assert df.shape == (3, 2)

    def test_fetch_dataframe_invalid_alias(self, data_fetcher, cdp_client_mock):
        with pytest.raises(InvalidAlias, match="non-existing-alias"):
            data_fetcher.time_series.fetch_dataframe(["ts1", "non-existing-alias"])
        with pytest.raises(TypeError, match="string"):
            data_fetcher.time_series.fetch_dataframe(["ts1", 123])

    def test_fetch_dataframe_invalid_type(self, data_fetcher, cdp_client_mock):
        with pytest.raises(TypeError, match="type"):
            data_fetcher.time_series.fetch_dataframe("ts1")

    def test_fetch_dataframe_not_aligned(self, data_fetcher, cdp_client_mock):
        with pytest.raises(InvalidFetchRequest, match="aligned"):
            data_fetcher.time_series.fetch_dataframe(["ts1", "ts3"])

    def test_fetch_dataframe_granularity_mismatch(self, data_fetcher, cdp_client_mock):
        with pytest.raises(InvalidFetchRequest, match="granularity"):
            data_fetcher.time_series.fetch_dataframe(["ts1", "ts4"])

    def test_fetch_dataframe_not_aggregate(self, data_fetcher, cdp_client_mock):
        with pytest.raises(InvalidFetchRequest, match="aggregate"):
            data_fetcher.time_series.fetch_dataframe(["ts1", "ts5"])

    def test_fetch_dataframe_column_names_are_aliases(self, data_fetcher, cdp_client_mock):
        def get_datapoints_frame(*args, **kwargs):
            return pd.DataFrame(
                [[1, 10], [2, 20], [3, 30]], columns=["myts1|average", "myts2|max"], index=[1000, 2000, 3000]
            )

        cdp_client_mock.get_datapoints_frame.side_effect = get_datapoints_frame

        df = data_fetcher.time_series.fetch_dataframe(["ts1", "ts2"])
        assert (df.columns == ["ts1", "ts2"]).all()

    def test_fetch_datapoints_invalid_type(self, data_fetcher, cdp_client_mock):
        with pytest.raises(TypeError, match="type"):
            data_fetcher.time_series.fetch_datapoints(123)

    def test_fetch_datapoints_single(self, data_fetcher, cdp_client_mock):
        def get_datapoints_frame_single(id, start, end, aggregate, granularity, include_outside_points):
            assert id == 1234
            assert start == 3000
            assert end == 5000
            assert aggregate == "average"
            assert granularity == "1s"
            assert include_outside_points is None
            return pd.DataFrame([[3000, 1], [4000, 2], [5000, 3]], columns=["timestamp", "value"])

        cdp_client_mock.get_datapoints_frame_single.side_effect = get_datapoints_frame_single
        df = data_fetcher.time_series.fetch_datapoints("ts1")
        assert (df.columns == ["timestamp", "value"]).all()
        assert df.shape == (3, 2)

    def test_fetch_datapoints_single_invalid_alias(self, data_fetcher, cdp_client_mock):
        with pytest.raises(InvalidAlias, match="non-existing-alias"):
            data_fetcher.time_series.fetch_datapoints("non-existing-alias")

    def test_fetch_datapoints_multiple(self, data_fetcher, cdp_client_mock):
        def get_datapoints_frame_multiple(queries: List[DatapointsFrameQuery]):
            results = []
            for q in queries:
                if q.id == 1234:
                    assert q.start == 3000
                    assert q.end == 5000
                    assert q.aggregate == "average"
                    assert q.granularity == "1s"
                    assert q.include_outside_points is None
                    results.append(pd.DataFrame([[1], [2], [3]], columns=["value"], index=[3000, 4000, 5000]))
                elif q.id == 5678:
                    assert q.start == 6000
                    assert q.end == 8000
                    assert q.aggregate is None
                    assert q.granularity is None
                    assert q.include_outside_points is None
                    results.append(pd.DataFrame([[10], [20], [30]], columns=["value"], index=[6400, 7300, 7900]))
                else:
                    raise AssertionError
            return results

        cdp_client_mock.get_datapoints_frame_multiple.side_effect = get_datapoints_frame_multiple

        dfs = data_fetcher.time_series.fetch_datapoints(["ts1", "ts5"])

        for df in dfs.values():
            assert (df.columns == ["value"]).all()
            assert df.shape == (3, 1)

        assert (dfs["ts1"].index == [3000, 4000, 5000]).all()
        assert (dfs["ts1"]["value"] == [1, 2, 3]).all()

        assert (dfs["ts5"].index == [6400, 7300, 7900]).all()
        assert (dfs["ts5"]["value"] == [10, 20, 30]).all()

    def test_fetch_datapoints_multiple_invalid_alias(self, data_fetcher, cdp_client_mock):
        with pytest.raises(InvalidAlias, match="non-existing-alias"):
            data_fetcher.time_series.fetch_datapoints(["ts1", "non-existing-alias"])
