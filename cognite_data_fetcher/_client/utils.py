from typing import Dict


def format_params(d: Dict):
    formatted = {}
    for k, v in d.items():
        if v is None:
            continue
        elif isinstance(v, bool):
            formatted[k] = str(v).lower()
        else:
            formatted[k] = v
    return formatted


def choose_num_of_retries(param: int, env: str, default: int):
    if param is not None:
        return param
    elif env is not None:
        return int(env)
    else:
        return default
