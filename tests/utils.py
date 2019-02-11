import asyncio
import random
import string

BASE_URL = "https://api.cognitedata.com"
BASE_URL_V0_5 = BASE_URL + "/api/0.5/projects/test"
BASE_URL_V0_6 = BASE_URL + "/api/0.6/projects/test"


def run_until_complete(*futures):
    res = asyncio.get_event_loop().run_until_complete(asyncio.gather(*futures))
    if len(futures) == 1:
        return res[0]
    return res


def random_string(length: int = 5):
    return "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(length))
