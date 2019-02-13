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


class SpecValidationError(Exception):
    def __init__(self, errors):
        self.errors = errors

    def __str__(self):
        return json.dumps(self.errors, indent=4, sort_keys=True)
