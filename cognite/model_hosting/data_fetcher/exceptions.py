import json


class DataFetcherHttpError(Exception):
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
    pass


class DirectoryDoesNotExist(Exception):
    def __init__(self, directory):
        self.directory = directory

    def __str__(self):
        return "'{}' is not a directory".format(self.directory)


class InvalidFetchRequest(Exception):
    pass


class InvalidAlias(Exception):
    def __init__(self, alias):
        self.alias = alias

    def __str__(self):
        return "Alias '{}' does not exist".format(self.alias)
