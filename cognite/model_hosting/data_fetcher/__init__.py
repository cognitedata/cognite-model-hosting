import typing as _typing

# Hack to let aiohttp support Python 3.5.0
_typing.TYPE_CHECKING = False

from cognite.model_hosting.data_fetcher.data_fetcher import DataFetcher
