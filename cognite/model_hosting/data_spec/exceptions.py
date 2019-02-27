import json


class SpecValidationError(Exception):
    """Raised if a data spec is invalid.

    Args:
        errors (Dict): A dictionary describing which fields are invalid and why.
    """

    def __init__(self, errors):
        self.errors = errors

    def __str__(self):
        return json.dumps(self.errors, indent=4, sort_keys=True)
