import asyncio
import random
import string
from datetime import datetime, timedelta

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


def round_to_nearest(x, base):
    return int(base * round(float(x) / base))


def get_time_w_offset(**kwargs):
    curr_time = datetime.now()
    offset_time = curr_time - timedelta(**kwargs)
    return int(round(offset_time.timestamp() * 1000))
