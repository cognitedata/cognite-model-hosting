import json


class InvalidScheduleOutputFormat(Exception):
    """Raised if the scheduled output is on an invalid format."""

    def __init__(self, errors):
        self.errors = errors

    def __str__(self):
        return json.dumps(self.errors, indent=4, sort_keys=True)


class DataframeMissingTimestampColumn(Exception):
    """Raised if trying to convert a dataframe without a timestamp column to scheduled output format."""

    pass


class DuplicateAliasInScheduledOutput(Exception):
    """Raised when an alias is passed more than once when converting to scheduled output format."""

    pass
