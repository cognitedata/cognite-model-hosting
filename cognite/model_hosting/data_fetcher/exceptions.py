import json


class DataFetcherHttpError(Exception):
    """Raised if an HTTP Error occurred while processing your request.

    Args:
        message (str):  The error message produced by the API.
        code (int):     The error code produced by the failure.
        x_request_id (str): The request-id generated for the failed request.
        extra (Dict):   A dict of any additional information.
    """

    def __init__(self, message, code=None, x_request_id=None, extra=None):
        self.message = message
        self.code = code
        self.x_request_id = x_request_id
        self.extra = extra

    def __str__(self):
        if self.extra:
            pretty_extra = json.dumps(self.extra, indent=4, sort_keys=True)
            return "{} | code: {} | X-Request-ID: {}\n{}".format(
                self.message, self.code, self.x_request_id, pretty_extra
            )
        return "{} | code: {} | X-Request-ID: {}".format(self.message, self.code, self.x_request_id)


class ApiKeyError(Exception):
    """Raised if the provided API key is missing or invalid."""

    pass


class DirectoryDoesNotExist(Exception):
    """Raised if the specified directory does not exist."""

    def __init__(self, directory):
        self.directory = directory

    def __str__(self):
        return "'{}' is not a directory".format(self.directory)


class InvalidFetchRequest(Exception):
    """Raised if an invalid fetch request is issued.

    For example if a request is issued for a time-aligned dataframe where the specified starts/ends or granularities
    of the time series are not the same.
    """

    pass


class InvalidAlias(Exception):
    """Raised if an invalid alias is specified."""

    def __init__(self, alias):
        self.alias = alias

    def __str__(self):
        return "Alias '{}' does not exist".format(self.alias)
