import json
from collections import defaultdict
from typing import Dict, List, Union

import pandas as pd
from marshmallow import RAISE, Schema, ValidationError, fields, validate

from cognite.model_hosting.schedules.exceptions import (
    DataframeMissingTimestampColumn,
    DuplicateAliasInScheduledOutput,
    InvalidScheduleOutputFormat,
)


def to_output(dataframe: Union[pd.DataFrame, List[pd.DataFrame]]):
    """Converts your data to a json serializable output format complying with the schedules feature.

    Args:
        dataframe (Union[List[pd.DataFrame, pd.DataFrame]]: A dataframe or list of dataframes.
    Returns:
        Dict: The data on a json serializable and schedules compliant output format.

    Examples:

        The correct output format looks like this::

            {
                "timeSeries":
                    {
                        "my-alias-1": [(t0, p0), (t1, p1), ...],
                        "my-alias-2": [(t0, p0), (t1, p1), ...],
                    }
            }
    """
    output = defaultdict(lambda: {})
    if isinstance(dataframe, pd.DataFrame):
        output["timeSeries"] = _convert_df_to_output_format(dataframe)
    elif isinstance(dataframe, List):
        for df in dataframe:
            if set(df.columns) - set(output["timeSeries"].keys()) != set(df.columns):
                raise DuplicateAliasInScheduledOutput("An alias has been provided multiple times")
            output["timeSeries"].update(_convert_df_to_output_format(df))
    else:
        raise TypeError("dataframe should be a pandas DataFrame or list of pandas DataFrames")
    return output


def _convert_df_to_output_format(df: pd.DataFrame):
    if not "timestamp" in df.columns:
        raise DataframeMissingTimestampColumn("DataFrame missing timestamp column")
    column_names = [col for col in df.columns if col != "timestamp"]
    output = {name: df[["timestamp", name]].values.tolist() for name in column_names}
    return output


class _ScheduleOutputSchema(Schema):
    class Meta:
        unknown = RAISE

    timeSeries = fields.Dict(
        keys=fields.Str(), values=fields.List(fields.List(fields.Float(), validate=validate.Length(equal=2)))
    )


_schedule_output_schema = _ScheduleOutputSchema(unknown=RAISE)


class ScheduleOutput:
    """Helper class for parsing and converting output from scheduled predictions.

    Args:
        output(Dict): The output returned from the scheduled prediction.
    """

    def __init__(self, output: Dict):
        self._output = self._load(output)

    def __str__(self):
        return json.dumps(self._output, indent=4, sort_keys=True)

    @staticmethod
    def _load(output):
        try:
            return _schedule_output_schema.load(output)
        except ValidationError as e:
            raise InvalidScheduleOutputFormat(e.messages) from e

    def _validate_alias(self, type: str, alias: str):
        assert self._output.get(type, {}).get(alias) is not None, "{} is not a valid alias".format(alias)

    def _validate_aligned(self, aliases: List[str]):
        timestamps = set()
        for alias in aliases:
            self._validate_alias("timeSeries", alias)
            timestamps.add(tuple(point[0] for point in self._output["timeSeries"][alias]))
        assert 1 == len(timestamps), "Timestamps for aliases {} are not aligned".format(aliases)

    def _get_dataframe_single_alias(self, alias) -> pd.DataFrame:
        self._validate_alias("timeSeries", alias)
        data = self._output["timeSeries"][alias]
        timestamps = [point[0] for point in data]
        datapoints = [point[1] for point in data]
        return pd.DataFrame({"timestamp": timestamps, alias: datapoints})

    def _get_dataframe_multiple_aliases(self, aliases: List[str]) -> pd.DataFrame:
        self._validate_aligned(aliases)
        data = {}
        timestamps = [p[0] for p in self._output["timeSeries"][aliases[0]]]
        data["timestamp"] = timestamps
        for a in aliases:
            data[a] = [p[1] for p in self._output["timeSeries"][a]]
        return pd.DataFrame(data)

    def get_dataframe(self, alias: Union[str, List[str]]) -> pd.DataFrame:
        """Returns a time-aligned dataframe of the specified alias(es).

        Assumes that all aliases specify output time series with matching timestamps.

        Args:
             alias(Union[str, List[str]]): alias or list of aliases

        Returns:
            pd.DataFrame: The dataframe containing the time series for the specified alias(es).
        """
        if isinstance(alias, str):
            return self._get_dataframe_single_alias(alias)
        elif isinstance(alias, List):
            return self._get_dataframe_multiple_aliases(alias)
        raise TypeError("alias must be a string or list of strings")

    def get_datapoints(self, alias: Union[str, List[str]]) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
        """Returns the dataframes for the specified alias(es).

        Args:
            alias (Union[str, List[str]]): alias or list of aliases.

        Returns:
            Union[pd.DataFrame, Dict[str, pd.DataFrame]: A single dataframe if a single alias has been specified. Or a
                dictionary mapping alias to dataframe if a list of aliases has been provided.
        """
        if isinstance(alias, str):
            return self._get_dataframe_single_alias(alias)
        elif isinstance(alias, List):
            dataframes = {}
            for a in alias:
                dataframes[a] = self._get_dataframe_single_alias(a)
            return dataframes
        raise TypeError("alias must be a string or list of strings")
