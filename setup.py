import re

from setuptools import find_packages, setup

packages = find_packages(exclude=["tests*"])

version = re.search('^__version__\s*=\s*"(.*)"', open("cognite_data_fetcher/__init__.py").read(), re.M).group(1)

setup(
    name="cognite-data-fetcher",
    version=version,
    description="Cognite Data Fetcher for Python",
    url="",  # TODO
    author="Nils Barlaug",
    author_email="nils.barlaug@cognite.com",
    packages=packages,
    install_requires=["aiohttp", "pandas"],
    python_requires=">=3.5",
)
