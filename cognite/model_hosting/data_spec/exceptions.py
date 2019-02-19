import json


class SpecValidationError(Exception):
    def __init__(self, errors):
        self.errors = errors

    def __str__(self):
        return json.dumps(self.errors, indent=4, sort_keys=True)
