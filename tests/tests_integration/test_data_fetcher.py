import os
from shutil import rmtree
from tempfile import TemporaryDirectory

import pytest

from cognite.model_hosting.data_fetcher import DataFetcher
from cognite.model_hosting.data_fetcher.exceptions import InvalidFetchRequest
from cognite.model_hosting.data_spec import DataSpec, FileSpec, TimeSeriesSpec


class TestTimeSeries:
    @pytest.fixture
    def data_fetcher(self, ts_ids, now) -> DataFetcher:
        one_hour_ago = now - 3600 * 1000
        return DataFetcher(
            DataSpec(
                time_series={
                    "constant3": TimeSeriesSpec(id=ts_ids["constant_3"], start=one_hour_ago, end=now),
                    "constant3_duplicate": TimeSeriesSpec(id=ts_ids["constant_3"], start=one_hour_ago, end=now),
                    "constant3_avg_1s": TimeSeriesSpec(
                        id=ts_ids["constant_3"], start=one_hour_ago, end=now, aggregate="average", granularity="1s"
                    ),
                    "constant3_avg_1s_duplicate": TimeSeriesSpec(
                        id=ts_ids["constant_3"], start=one_hour_ago, end=now, aggregate="average", granularity="1s"
                    ),
                    "constant3_avg_1m": TimeSeriesSpec(
                        id=ts_ids["constant_3"], start=one_hour_ago, end=now, aggregate="average", granularity="1m"
                    ),
                    "constant3_max_1m": TimeSeriesSpec(
                        id=ts_ids["constant_3"], start=one_hour_ago, end=now, aggregate="max", granularity="1m"
                    ),
                    "constant4": TimeSeriesSpec(id=ts_ids["constant_4"], start=one_hour_ago, end=now),
                    "constant4_avg_1s": TimeSeriesSpec(
                        id=ts_ids["constant_4"], start=one_hour_ago, end=now, aggregate="average", granularity="1s"
                    ),
                    "constant5_min_1s": TimeSeriesSpec(
                        id=ts_ids["constant_5"], start=one_hour_ago, end=now, aggregate="min", granularity="1s"
                    ),
                    "constant6_max_1s": TimeSeriesSpec(
                        id=ts_ids["constant_6"], start=one_hour_ago, end=now, aggregate="max", granularity="1s"
                    ),
                }
            )
        )

    def assert_data_frame(self, df, columns, column_means):
        assert all(columns == df.columns)
        for column, mean in column_means.items():
            assert mean == round(df[column].mean())

    def test_fetch_datapoints_single(self, data_fetcher):
        df = data_fetcher.time_series.fetch_datapoints("constant3")
        self.assert_data_frame(df, ["value"], {"value": 3})

    def test_fetch_datapoints_single_many_datapoints(self, ts_ids, now):
        data_fetcher = DataFetcher(
            DataSpec(
                time_series={
                    "constant3": TimeSeriesSpec(id=ts_ids["constant_3"], start=now - 48 * 3600 * 1000, end=now)
                }
            )
        )
        df = data_fetcher.time_series.fetch_datapoints("constant3")
        self.assert_data_frame(df, ["value"], {"value": 3})

    def test_fetch_datapoints_multiple(self, data_fetcher):
        dfs = data_fetcher.time_series.fetch_datapoints(["constant3", "constant4"])
        assert 2 == len(dfs)

        self.assert_data_frame(dfs["constant3"], ["value"], {"value": 3})
        self.assert_data_frame(dfs["constant4"], ["value"], {"value": 4})

    def test_fetch_datapoints_multiple_many_datapoints(self, ts_ids, now):
        data_fetcher = DataFetcher(
            DataSpec(
                time_series={
                    "constant3": TimeSeriesSpec(id=ts_ids["constant_3"], start=now - 48 * 3600 * 1000, end=now),
                    "constant4": TimeSeriesSpec(id=ts_ids["constant_4"], start=now - 48 * 3600 * 1000, end=now),
                }
            )
        )
        dfs = data_fetcher.time_series.fetch_datapoints(["constant3", "constant4"])
        self.assert_data_frame(dfs["constant3"], ["value"], {"value": 3})
        self.assert_data_frame(dfs["constant4"], ["value"], {"value": 4})

    def test_fetch_datapoints_many_time_series(self, ts_ids, now):
        data_fetcher = DataFetcher(
            DataSpec(
                time_series={
                    "constant{}".format(i): TimeSeriesSpec(
                        id=ts_ids["constant_{}".format(i)], start=now - 3600 * 1000, end=now
                    )
                    for i in range(100)
                }
            )
        )
        dfs = data_fetcher.time_series.fetch_datapoints(["constant{}".format(i) for i in range(100)])
        for i in range(100):
            self.assert_data_frame(dfs["constant{}".format(i)], ["value"], {"value": i})

    def test_fetch_datapoints_raw_and_aggregate_of_same_time_series(self, data_fetcher):
        dfs = data_fetcher.time_series.fetch_datapoints(["constant3", "constant3_avg_1m"])
        assert 2 == len(dfs)

        self.assert_data_frame(dfs["constant3"], ["value"], {"value": 3})
        self.assert_data_frame(dfs["constant3_avg_1m"], ["average"], {"average": 3})
        assert len(dfs["constant3"]) > len(dfs["constant3_avg_1m"])

    def test_fetch_datapoints_duplicate_raw(self, data_fetcher):
        dfs = data_fetcher.time_series.fetch_datapoints(["constant3", "constant3_duplicate"])
        assert 2 == len(dfs)

        self.assert_data_frame(dfs["constant3"], ["value"], {"value": 3})
        self.assert_data_frame(dfs["constant3_duplicate"], ["value"], {"value": 3})
        assert (dfs["constant3"] == dfs["constant3_duplicate"]).all().all()

    def test_fetch_datapoints_duplicate_aggregates(self, data_fetcher):
        dfs = data_fetcher.time_series.fetch_datapoints(["constant3_avg_1s", "constant3_avg_1s_duplicate"])
        assert 2 == len(dfs)

        self.assert_data_frame(dfs["constant3_avg_1s"], ["average"], {"average": 3})
        self.assert_data_frame(dfs["constant3_avg_1s_duplicate"], ["average"], {"average": 3})
        assert (dfs["constant3_avg_1s"][:100] == dfs["constant3_avg_1s_duplicate"][:100]).all().all()

    def test_fetch_dataframe(self, data_fetcher):
        df = data_fetcher.time_series.fetch_dataframe(
            ["constant5_min_1s", "constant4_avg_1s", "constant3_avg_1s", "constant6_max_1s"]
        )
        self.assert_data_frame(
            df,
            ["constant5_min_1s", "constant4_avg_1s", "constant3_avg_1s", "constant6_max_1s"],
            {"constant3_avg_1s": 3, "constant4_avg_1s": 4, "constant5_min_1s": 5, "constant6_max_1s": 6},
        )

    def test_fetch_dataframe_duplicate(self, data_fetcher):
        with pytest.raises(InvalidFetchRequest, match="reference the same time series"):
            data_fetcher.time_series.fetch_dataframe(["constant3_avg_1s", "constant3_avg_1s_duplicate"])


class TestFiles:
    @pytest.fixture
    def data_fetcher(self, file_ids) -> DataFetcher:
        return DataFetcher(
            DataSpec(
                files={
                    "a": FileSpec(id=file_ids["a.txt"]),
                    "a_duplicate": FileSpec(id=file_ids["a.txt"]),
                    "b": FileSpec(id=file_ids["b.txt"]),
                    "big": FileSpec(id=file_ids["big.txt"]),
                }
            )
        )

    def test_fetch_single(self, data_fetcher):
        with TemporaryDirectory() as dir:
            data_fetcher.files.fetch("a", directory=dir)
            assert 1 == len(os.listdir(dir))
            with open(os.path.join(dir, "a")) as f:
                assert "a" == f.read()

    def test_fetch_multiple(self, data_fetcher):
        with TemporaryDirectory() as dir:
            data_fetcher.files.fetch(["a", "b"], directory=dir)
            assert 2 == len(os.listdir(dir))
            with open(os.path.join(dir, "a")) as f:
                assert "a" == f.read()
            with open(os.path.join(dir, "b")) as f:
                assert "b" == f.read()

    def test_fetch_single_to_memory(self, data_fetcher):
        content = data_fetcher.files.fetch_to_memory("a")
        assert "a" == content.decode()

    def test_fetch_multiple_to_memory(self, data_fetcher):
        content = data_fetcher.files.fetch_to_memory(["a", "b"])
        assert 2 == len(content)
        assert "a" == content["a"].decode()
        assert "b" == content["b"].decode()

    def test_fetch_to_memory_duplicate(self, data_fetcher):
        content = data_fetcher.files.fetch_to_memory(["a", "a_duplicate", "b"])
        assert 3 == len(content)
        assert "a" == content["a"].decode()
        assert "a" == content["a_duplicate"].decode()
        assert "b" == content["b"].decode()
