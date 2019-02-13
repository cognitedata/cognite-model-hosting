import json
from typing import Dict

from marshmallow import RAISE, Schema, ValidationError, fields, post_dump, post_load, validates_schema

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

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        if type(self) == type(other):
            return self.__dict__ == other.__dict__
        else:
            return False


class TimeSeriesSpec(_BaseSpec):
    def __init__(
        self,
        id: int,
        start: int,
        end: int,
        aggregate: str = None,
        granularity: str = None,
        include_outside_points: bool = None,
    ):
        self.id = id
        self.start = start
        self.end = end
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


TimeSeriesSpec._schema = _TimeSeriesSpecSchema()
ScheduleTimeSeriesSpec._schema = _TimeSeriesSpecSchema(spec=ScheduleTimeSeriesSpec, exclude=("start", "end"))
FileSpec._schema = _FileSpecSchema()
DataSpec._schema = _DataSpecSchema()
ScheduleDataSpec._schema = _ScheduleDataSpecSchema()
