import os

import pytest

from cognite.data_fetcher.data_fetcher import DataFetcher
from cognite.data_fetcher.data_spec import DataSpec, FileSpec, TimeSeriesSpec
from cognite.data_fetcher.exceptions import DirectoryDoesNotExist, InvalidAlias, SpecValidationError
from tests.utils import BASE_URL_V0_5


@pytest.mark.parametrize("data_spec", [DataSpec(), {}, "{}"])
def test_empty_data_spec(data_spec):
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
    def data_fetcher(self, file_specs, http_mock):
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

    @pytest.fixture
    def mock_file_download(self, http_mock):
        mock_download_url = "http://download.url"
        http_mock.get(BASE_URL_V0_5 + "/files/123/downloadlink", status=200, payload={"data": mock_download_url})
        http_mock.get(mock_download_url, status=200, body=b"blablabla")
        http_mock.get(BASE_URL_V0_5 + "/files/234/downloadlink", status=200, payload={"data": mock_download_url})
        http_mock.get(mock_download_url, status=200, body=b"blablabla")

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
    def test_get_aliases(self):
        data_spec = DataSpec(
            time_series={"ts1": TimeSeriesSpec(id=123, start=4, end=5), "ts2": TimeSeriesSpec(id=234, start=4, end=5)}
        )
        data_fetcher = DataFetcher(data_spec)
        aliases = data_fetcher.time_series.aliases
        assert len(aliases) == 2
        assert set(aliases) == {"ts1", "ts2"}
