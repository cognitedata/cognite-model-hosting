import pytest

from cognite.data_fetcher.data_fetcher import DataFetcher
from cognite.data_fetcher.data_spec import DataSpec, FileSpec, TimeSeriesSpec
from cognite.data_fetcher.exceptions import SpecValidationError


@pytest.mark.parametrize("data_spec", [DataSpec(), {}, "{}"])
def test_empty_data_spec(http_mock, data_spec):
    data_fetcher = DataFetcher(data_spec)
    assert data_fetcher.get_data_spec() == DataSpec()


def test_invalid_spec_type():
    with pytest.raises(SpecValidationError, match="has to be of type"):
        DataFetcher(123)


def test_get_data_spec(http_mock):
    data_spec = DataSpec(files={"f1": FileSpec(id=123)})
    getted_data_spec = DataFetcher(data_spec).get_data_spec()
    getted_data_spec.files["f1"].id = 234
    assert data_spec.files["f1"].id == 123


class TestFiles:
    def test_get_aliases(self, http_mock):
        data_spec = DataSpec(files={"f1": FileSpec(id=123), "f2": FileSpec(id=234)})
        data_fetcher = DataFetcher(data_spec)
        aliases = data_fetcher.files.aliases
        assert len(aliases) == 2
        assert set(aliases) == {"f1", "f2"}


class TestTimeSeries:
    def test_get_aliases(self, http_mock):
        data_spec = DataSpec(
            time_series={"ts1": TimeSeriesSpec(id=123, start=4, end=5), "ts2": TimeSeriesSpec(id=234, start=4, end=5)}
        )
        data_fetcher = DataFetcher(data_spec)
        aliases = data_fetcher.time_series.aliases
        assert len(aliases) == 2
        assert set(aliases) == {"ts1", "ts2"}
