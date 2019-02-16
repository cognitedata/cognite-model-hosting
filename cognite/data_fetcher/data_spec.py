import json
from datetime import datetime
from typing import Dict, Union

from marshmallow import RAISE, Schema, ValidationError, fields, post_dump, post_load, validates, validates_schema

from cognite.data_fetcher._utils import calculate_window_intervals, granularity_to_ms, to_ms
from cognite.data_fetcher.exceptions import SpecValidationError


class _BaseSpec:
    _schema = None  # Set by subclass

    def dump(self):
        try:
            dumped = self._schema.dump(self)
        except ValidationError as e:
            raise SpecValidationError(e.messages) from e
        errors = self._schema.validate(dumped)
        if errors:
            raise SpecValidationError(errors)

        return dumped

    def validate(self):
        self.dump()

    @classmethod
    def load(cls, data):
        try:
            return cls._schema.load(data)
        except ValidationError as e:
            raise SpecValidationError(e.messages) from e

    def to_json(self):
        return json.dumps(self.dump(), indent=4, sort_keys=True)

    @classmethod
    def from_json(cls, s: str):
        return cls.load(json.loads(s))

    def copy(self):
        return self.from_json(self.to_json())

    def __str__(self):
        return self.to_json()

    def __eq__(self, other):
        if type(self) == type(other):
            return self.__dict__ == other.__dict__
        else:
            return False


class TimeSeriesSpec(_BaseSpec):
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
        self.start = to_ms(start)
        self.end = to_ms(end)
        self.aggregate = aggregate
        self.granularity = granularity
        self.include_outside_points = include_outside_points


class ScheduleTimeSeriesSpec(_BaseSpec):
    def __init__(self, id: int, aggregate: str = None, granularity: str = None, include_outside_points: bool = None):
        self.id = id
        self.aggregate = aggregate
        self.granularity = granularity
        self.include_outside_points = include_outside_points


class FileSpec(_BaseSpec):
    def __init__(self, id: int):
        self.id = id


class DataSpec(_BaseSpec):
    def __init__(self, time_series: Dict[str, TimeSeriesSpec] = None, files: Dict[str, FileSpec] = None):
        self.time_series = time_series or {}
        self.files = files or {}

        self.validate()


class ScheduleDataSpec(_BaseSpec):
    def __init__(self, stride: str, window_size: str, time_series: Dict[str, ScheduleTimeSeriesSpec] = None):
        self.stride = stride
        self.window_size = window_size
        self.time_series = time_series or {}

        self.validate()

    def get_data_specs(self, start: Union[int, str, datetime], end: Union[int, str, datetime, None]):
        start, end = to_ms(start), to_ms(end)

        intervals = calculate_window_intervals(
            start=start, end=end, stride=granularity_to_ms(self.stride), window_size=granularity_to_ms(self.window_size)
        )

        data_specs = []
        for start, end in intervals:
            time_series_specs = {
                alias: TimeSeriesSpec(
                    id=spec.id,
                    start=start,
                    end=end,
                    aggregate=spec.aggregate,
                    granularity=spec.granularity,
                    include_outside_points=spec.include_outside_points,
                )
                for alias, spec in self.time_series.items()
            }
            data_specs.append(DataSpec(time_series=time_series_specs))
        return data_specs


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


class _TimeSeriesSpecSchema(_BaseSchema):
    _default_spec = TimeSeriesSpec

    id = fields.Int(required=True)
    start = fields.Int(required=True)
    end = fields.Int(required=True)
    aggregate = fields.Str()
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
            raise ValidationError("Invalid granularity format. Must be e.g. '1d', '2hour', '60second'") from e


class _FileSpecSchema(_BaseSchema):
    _default_spec = FileSpec

    id = fields.Int(required=True)


class _DataSpecSchema(_BaseSchema):
    _default_spec = DataSpec

    timeSeries = fields.Dict(keys=fields.Str(), values=fields.Nested(_TimeSeriesSpecSchema), attribute="time_series")
    files = fields.Dict(keys=fields.Str(), values=fields.Nested(_FileSpecSchema))


class _ScheduleDataSpecSchema(_BaseSchema):
    _default_spec = ScheduleDataSpec

    stride = fields.Str(required=True)
    windowSize = fields.Str(required=True, attribute="window_size")
    timeSeries = fields.Dict(
        keys=fields.Str(),
        values=fields.Nested(_TimeSeriesSpecSchema(spec=ScheduleTimeSeriesSpec, exclude=("start", "end"))),
        attribute="time_series",
    )

    @validates("stride")
    def validate_stride(self, stride):
        try:
            granularity_to_ms(stride)
        except ValueError as e:
            raise ValidationError("Invalid stride format. Must be e.g. '1d', '2hour', '60second'") from e

    @validates("windowSize")
    def validate_window_size(self, window_size):
        try:
            granularity_to_ms(window_size)
        except ValueError as e:
            raise ValidationError("Invalid windowSize format. Must be e.g. '1d', '2hour', '60second'") from e


TimeSeriesSpec._schema = _TimeSeriesSpecSchema()
ScheduleTimeSeriesSpec._schema = _TimeSeriesSpecSchema(spec=ScheduleTimeSeriesSpec, exclude=("start", "end"))
FileSpec._schema = _FileSpecSchema()
DataSpec._schema = _DataSpecSchema()
ScheduleDataSpec._schema = _ScheduleDataSpecSchema()
