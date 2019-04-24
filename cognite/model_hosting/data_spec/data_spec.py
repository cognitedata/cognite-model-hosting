import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Union

from marshmallow import (
    RAISE,
    Schema,
    ValidationError,
    fields,
    post_dump,
    post_load,
    validate,
    validates,
    validates_schema,
)

from cognite.model_hosting._cognite_model_hosting_common.utils import (
    calculate_windows,
    granularity_to_ms,
    granularity_unit_to_ms,
    time_interval_to_ms,
    timestamp_to_ms,
)
from cognite.model_hosting.data_spec.exceptions import SpecValidationError

INVALID_AGGREGATE_FUNCTIONS = ["avg", "cv", "dv", "int", "step", "tv"]


class _BaseSpec:
    _schema = None  # Set by subclass

    def dump(self):
        """Dumps the data spec into a Python data structure.

        Raises:
            SpecValidationError: If the spec is not valid.

        Returns:
            Dict: The data spec as a Python data structure.
        """
        try:
            dumped = self._schema.dump(self)
        except ValidationError as e:
            raise SpecValidationError(e.messages) from e
        errors = self._schema.validate(dumped)
        if errors:
            raise SpecValidationError(errors)

        return dumped

    def validate(self):
        """Checks whether or not the data spec is valid.

        Raises:
            SpecValidationError: If the spec is not valid.
        """
        self.dump()

    @classmethod
    def load(cls, data):
        """Loads the data from a Python data structure.

        Raises:
            SpecValidationError: If the spec is not valid.

        Returns:
            The data spec object.
        """
        try:
            return cls._schema.load(data)
        except ValidationError as e:
            raise SpecValidationError(e.messages) from e

    def to_json(self):
        """Returns a json representation of the data spec.

        Raises:
            SpecValidationError: If the spec is not valid.

        Returns:
            str: The json representation of the data spec.
        """
        return json.dumps(self.dump(), indent=4, sort_keys=True)

    @classmethod
    def from_json(cls, s: str):
        """Loads the data spec from a json representation.

        Raises:
            SpecValidationError: If the spec is not valid.

        Returns:
            The data spec object.
        """
        return cls.load(json.loads(s))

    def copy(self):
        """Returns a copy of the data spec.

        Raises:
            SpecValidationError: If the spec is not valid.
        """
        return self.from_json(self.to_json())

    def __str__(self):
        return self.to_json()

    def __eq__(self, other):
        if type(self) == type(other):
            return self.__dict__ == other.__dict__
        else:
            return False


class TimeSeriesSpec(_BaseSpec):
    """Creates a time series spec.

    If the granularity and aggregate parameters are omitted, the TimeSeriesSpec specifies raw data.

    Args:
        id (int): The id of the time series.
        start (Union[str, int, datetime]): The (inclusive) start of the time series. Can be either milliseconds since epoch,
        time-ago format (e.g. "1d-ago"), or a datetime object.
        end (Union[str, int, datetime]): The (exclusive) end of the time series. Same format as start. Can also be set to "now".
        aggregate (str, optional): The aggregate function to apply to the time series.
        granularity (str, optional): Granularity of the datapoints. e.g. "1m", "2h", or "3d".
        include_outside_points (bool): Whether or not to include the first point before and after start and end. Can
                                        only be used with raw data.
    """

    def __init__(
        self,
        id: int,
        start: Union[int, str, datetime],
        end: Union[int, str, datetime],
        aggregate: str = None,
        granularity: str = None,
        include_outside_points: bool = None,
    ):
        self.id = id
        self.start = timestamp_to_ms(start)
        self.end = timestamp_to_ms(end)
        self.aggregate = aggregate
        self.granularity = granularity
        self.include_outside_points = include_outside_points


class FileSpec(_BaseSpec):
    """Creates a file spec.

    Args:
        id (int): The id of the file.
    """

    def __init__(self, id: int):
        self.id = id


class DataSpec(_BaseSpec):
    """Creates a DataSpec.

    This object collects all data specs specific for a given resource type into a single object which can be passed
    to the DataFetcher. It includes aliases for all specs so that they may be referenced by a user-defined
    shorthand and abstracted away from specific resources.

    Args:
        time_series (Dict[str, TimeSeriesSpec]): A dictionary mapping aliases to TimeSeriesSpecs.
        files (Dict[str, FileSpec]): A dicionary mapping aliases to FileSpecs.
    """

    def __init__(self, time_series: Dict[str, TimeSeriesSpec] = None, files: Dict[str, FileSpec] = None):
        self.time_series = time_series or {}
        self.files = files or {}
        self.validate()


class ScheduleInputTimeSeriesSpec(_BaseSpec):
    """Creates a ScheduleOutputTimeSeriesSpec.

    This object defines the time series a schedule should read from.

    If the granularity and aggregate parameters are omitted, the spec specifies raw data.

    Args:
        id (int): The id of the output time series.
        aggregate (str, optional): The aggregate function to apply to the time series.
        granularity (str, optional): Granularity of the datapoints. e.g. "1m", "2h", or "3d".
        include_outside_points (bool, optional): Whether or not to include the first point before and after start and
            end. Can only be used with raw data.
    """

    def __init__(self, id: int, aggregate: str = None, granularity: str = None, include_outside_points: bool = None):
        self.id = id
        self.aggregate = aggregate
        self.granularity = granularity
        self.include_outside_points = include_outside_points


class ScheduleInputSpec(_BaseSpec):
    """Creates a ScheduleInputSpec.

    The provided aliases must be the same as the input fields defined on the model.

    Args:
        time_series (Dict[str, ScheduleInputTimeSeriesSpec]): A dictionary mapping aliases to
            ScheduleInputTimeSeriesSpec objects.
    """

    def __init__(self, time_series: Dict[str, ScheduleInputTimeSeriesSpec] = None):
        self.time_series = time_series or {}

    def _is_aggregates_used(self):
        for time_series_spec in self.time_series.values():
            if time_series_spec.aggregate:
                return True
        return False

    def _largest_granularity(self):
        largest = None
        for time_series_spec in self.time_series.values():
            if time_series_spec.aggregate:
                ms = granularity_to_ms(time_series_spec.granularity)
                if largest is None or ms > largest:
                    largest = ms
        return largest

    def _largest_granularity_unit(self):
        largest = None
        for time_series_spec in self.time_series.values():
            if time_series_spec.aggregate:
                ms = granularity_unit_to_ms(time_series_spec.granularity)
                if largest is None or ms > largest:
                    largest = ms
        return largest


class ScheduleOutputTimeSeriesSpec(_BaseSpec):
    """Creates a ScheduleOutputTimeSeriesSpec.

    This object defines the time series a schedule should write to. You need to specify an offset which
    defines where in time your schedule can write data to for a given window. Offset defaults to 0, meaning that your
    schedule can write to the same time window which it was feeded data from.

    Args:
        id (int): The id of the output time series.
        offset (Union[int, str, timedelta], optional): The offset of the window to which your schedule is allowed to
            write data.
    """

    def __init__(self, id: int, offset: Union[int, str, timedelta] = 0):
        self.id = id
        self.offset = offset


class ScheduleOutputSpec(_BaseSpec):
    """Creates a ScheduleOutputSpec.

    The provided aliases must be the same as the output fields defined on the model.

    Args:
        time_series (Dict[str, ScheduleInputTimeSeriesSpec]): A dictionary mapping aliases to
            ScheduleOutputTimeSeriesSpec objects.
    """

    def __init__(self, time_series: Dict[str, ScheduleOutputTimeSeriesSpec] = None):
        self.time_series = time_series or {}


class ScheduleDataSpec(_BaseSpec):
    """Creates a ScheduleDataSpec.

    This spec defines the input and output data for a given schedule, as well as how the hosting environment should
    feed the specified data to your model. This is done by specifying window size, a stride, and start time
    for the schedule.

    Args:
        input (ScheduleInputSpec): A schedule input spec describing input for a model.
        output (ScheduleOutputSpec): A schedule output spec describing output for a model.
        stride (Union[int, str, timedelta]): The interval at which predictions will be made. Can be either
            milliseconds, a timedelta object, or a time-string (e.g. "1h", "10d", "120s").
        window_size (Union[int, str, timedelta]): The size of each prediction window, i.e. how long back in time a
            prediction will look. Same format as stride.
        start (Union[int, str, datetime]): When the first prediction will be made.
        slack (Union[int, str, timedelta]): How long back in time input changes will trigger new predictions
    """

    def __init__(
        self,
        input: ScheduleInputSpec,
        output: ScheduleOutputSpec,
        stride: Union[int, str, timedelta],
        window_size: Union[int, str, timedelta],
        start: Union[int, str, datetime] = "now",
        slack: Union[int, str, timedelta] = 0,
    ):
        self.input = input
        self.output = output
        self.stride = time_interval_to_ms(stride)
        self.window_size = time_interval_to_ms(window_size)
        self.start = self._get_start(start, input)
        self.slack = time_interval_to_ms(slack, allow_zero=True, allow_inf=True)

        self.validate()

    @staticmethod
    def _get_start(start, input):
        if not isinstance(input, ScheduleInputSpec):
            raise TypeError("`input` argument must be of type ScheduleInputSpec")

        if start == "now" and input._is_aggregates_used():
            largest_granularity_unit = input._largest_granularity_unit()
            start = timestamp_to_ms(start)
            start -= start % largest_granularity_unit
            start += largest_granularity_unit
        else:
            start = timestamp_to_ms(start)

        return start

    def get_instances(self, start: Union[int, str, datetime], end: Union[int, str, datetime]) -> List[DataSpec]:
        """Returns the DataSpec objects describing the prediction windows executed between start and end.

        Args:
            start (Union[str, int, datetime]): The start of the time period. Can be either milliseconds since epoch,
                time-ago format (e.g. "1d-ago"), or a datetime object.
            end (Union[str, int, datetime]): The end of the time period. Same format as start. Can also be set to "now".

        Returns:
            List[DataSpec]: List of DataSpec objects, one for each prediction window.
        """
        start, end = timestamp_to_ms(start), timestamp_to_ms(end)

        windows = calculate_windows(
            start=start, end=end, stride=self.stride, window_size=self.window_size, first=self.start
        )

        data_specs = []
        for start, end in windows:
            time_series_specs = {
                alias: TimeSeriesSpec(
                    id=spec.id,
                    start=start,
                    end=end,
                    aggregate=spec.aggregate,
                    granularity=spec.granularity,
                    include_outside_points=spec.include_outside_points,
                )
                for alias, spec in self.input.time_series.items()
            }
            data_specs.append(DataSpec(time_series=time_series_specs))
        return data_specs

    def get_execution_timestamps(self, start: Union[int, str, datetime], end: Union[int, str, datetime]) -> List[int]:
        """Returns a list of timestamps indicating when each prediction will be executed.

        This corresponds to the end of each DataSpec returned from get_instances().

        Args:
            start (Union[str, int, datetime]): The start of the time period. Can be either milliseconds since epoch,
                    time-ago format (e.g. "1d-ago"), or a datetime object.
            end (Union[str, int, datetime]): The end of the time period. Same format as start. Can also be set to "now".

        Returns:
            List[int]: A list of timestamps.
        """
        start, end = timestamp_to_ms(start), timestamp_to_ms(end)

        windows = calculate_windows(
            start=start, end=end, stride=self.stride, window_size=self.window_size, first=self.start
        )

        return [w[1] for w in windows]


class _BaseSchema(Schema):
    _ignore_values = [None, {}]
    _default_spec = None

    def __init__(self, *args, **kwargs):
        self._spec = kwargs.get("spec", None) or self._default_spec
        assert self._spec is not None
        if "spec" in kwargs:
            kwargs.pop("spec")
        super().__init__(*args, **kwargs)

    class Meta:
        unknown = RAISE

    @post_dump
    def remove_none(self, data):
        return {k: v for k, v in data.items() if v not in self._ignore_values}

    @post_load
    def to_spec(self, data):
        return self._spec(**data)


class AliasField(fields.String):
    def __init__(self):
        super().__init__(required=True, validate=self.__validate)

    def __validate(self, field_name):
        pattern = "[a-z]([a-z0-9_]{0,48}[a-z0-9])?"
        if not re.fullmatch(pattern, field_name):
            raise ValidationError(
                "Invalid alias. Must be 1 to 50 lowercase alphanumeric characters or `_`. Must start with a letter "
                "and cannot end with `_` ."
            )


class _TimeSeriesSpecSchema(_BaseSchema):
    _default_spec = TimeSeriesSpec

    id = fields.Int(required=True)
    start = fields.Int(required=True)
    end = fields.Int(required=True)
    aggregate = fields.Str(
        validate=validate.NoneOf(
            INVALID_AGGREGATE_FUNCTIONS, error="Not a valid aggregate function. Cannot use shorthand name."
        )
    )
    granularity = fields.Str()
    includeOutsidePoints = fields.Bool(attribute="include_outside_points")

    @validates_schema(skip_on_field_errors=False)
    def validate_aggregate(self, data):
        errors = {}

        if "aggregate" in data:
            if "granularity" not in data:
                errors["granularity"] = ["granularity must be specified for aggregates."]
            if "include_outside_points" in data and data["include_outside_points"]:
                errors["includeOutsidePoints"] = ["Can't include outside points for aggregates."]
        else:
            if "granularity" in data:
                errors["granularity"] = ["granularity can only be specified for aggregates."]

        if errors:
            raise ValidationError(errors)

    @validates("granularity")
    def validate_granularity(self, granularity):
        try:
            granularity_to_ms(granularity)
        except ValueError as e:
            raise ValidationError(str(e)) from e


class _FileSpecSchema(_BaseSchema):
    _default_spec = FileSpec

    id = fields.Int(required=True)


class _DataSpecSchema(_BaseSchema):
    _default_spec = DataSpec

    timeSeries = fields.Dict(keys=AliasField(), values=fields.Nested(_TimeSeriesSpecSchema), attribute="time_series")
    files = fields.Dict(keys=AliasField(), values=fields.Nested(_FileSpecSchema))


class _ScheduleInputDataSpecSchema(_BaseSchema):
    _default_spec = ScheduleInputSpec

    timeSeries = fields.Dict(
        keys=AliasField(),
        values=fields.Nested(_TimeSeriesSpecSchema(spec=ScheduleInputTimeSeriesSpec, exclude=("start", "end"))),
        attribute="time_series",
    )


class _ScheduleOutputTimeSeriesSpecSchema(_BaseSchema):
    _default_spec = ScheduleOutputTimeSeriesSpec

    id = fields.Int(required=True)
    offset = fields.Int(required=True)


class _ScheduleOutputDataSpecSchema(_BaseSchema):
    _default_spec = ScheduleOutputSpec

    timeSeries = fields.Dict(
        keys=AliasField(), values=fields.Nested(_ScheduleOutputTimeSeriesSpecSchema), attribute="time_series"
    )


class _ScheduleDataSpecSchema(_BaseSchema):
    _ignore_values = []
    _default_spec = ScheduleDataSpec

    input = fields.Nested(_ScheduleInputDataSpecSchema, required=True)
    output = fields.Nested(_ScheduleOutputDataSpecSchema, required=True)
    stride = fields.Int(required=True, validate=validate.Range(min=1))
    windowSize = fields.Int(required=True, attribute="window_size", validate=validate.Range(min=1))
    start = fields.Int(required=True, validate=validate.Range(min=0))
    slack = fields.Int(missing=0, validate=validate.Range(min=-1))

    @staticmethod
    def _validate_aggregate_constraints(data):
        errors = {}

        largest_granularity_unit = data["input"]._largest_granularity_unit()
        for field in ["stride", "window_size", "start"]:
            if data[field] % largest_granularity_unit != 0:
                # To correct for surprising behaviour in Marshmallow
                field_camel_case = re.sub(r"_(.)", lambda m: m.group(1).upper(), field)
                errors[field_camel_case] = [
                    "Must be a multiple of the largest granularity unit in the input time series."
                ]

        largest_granularity = data["input"]._largest_granularity()
        if data["window_size"] < largest_granularity:
            errors["windowSize"] = [
                "Must be greater than or equal to the largest granularity of any of aggregated input time series."
            ]

        if errors:
            raise ValidationError(errors)

    @validates_schema(skip_on_field_errors=True)
    def _validate(self, data):
        if data["input"].time_series and data["input"]._is_aggregates_used():
            self._validate_aggregate_constraints(data)


TimeSeriesSpec._schema = _TimeSeriesSpecSchema()
ScheduleInputTimeSeriesSpec._schema = _TimeSeriesSpecSchema(spec=ScheduleInputTimeSeriesSpec, exclude=("start", "end"))
FileSpec._schema = _FileSpecSchema()
DataSpec._schema = _DataSpecSchema()
ScheduleInputSpec._schema = _ScheduleInputDataSpecSchema()
ScheduleOutputTimeSeriesSpec._schema = _ScheduleOutputTimeSeriesSpecSchema()
ScheduleOutputSpec._schema = _ScheduleOutputDataSpecSchema()
ScheduleDataSpec._schema = _ScheduleDataSpecSchema()
