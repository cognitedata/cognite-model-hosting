import re
from datetime import datetime

import pytest

from cognite.client import CogniteClient


@pytest.fixture
def now():
    return int(datetime.now().timestamp() * 1000)


@pytest.fixture(scope="session")
def ts_ids():
    ts_ids = {}
    for ts in CogniteClient().time_series.search(limit=100, name="test__constant"):
        short_name = re.fullmatch(r"test__(constant_[0-9]+)_with_noise", ts.name).group(1)
        ts_ids[short_name] = ts.id
    return ts_ids


@pytest.fixture(scope="session")
def file_ids():
    file_ids = {}
    for file in CogniteClient().files.retrieve_multiple(
        external_ids=["test/subdir/a.txt", "test/subdir/b.txt", "test/subdir/c.txt", "test/big.txt"]
    ):
        file_ids[file.name] = file.id
    return file_ids
