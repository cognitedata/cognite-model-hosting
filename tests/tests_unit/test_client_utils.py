from cognite_data_fetcher._client import utils


def test_format_params():
    d = {"k1": None, "k2": True, "k3": 1, "k4": "bla", "k5": ["1"]}
    formatted = utils.format_params(d)
    assert formatted == {"k2": "true", "k3": 1, "k4": "bla", "k5": "['1']"}
