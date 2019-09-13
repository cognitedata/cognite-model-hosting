import random
import string

BASE_URL = "https://api.cognitedata.com"
BASE_URL_V1 = BASE_URL + "/api/v1/projects/test"


def random_string(length: int = 5):
    return "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(length))
