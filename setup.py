import re

from setuptools import find_packages, setup

version = re.search(
    '^__version__\s*=\s*"(.*)"', open("cognite/model_hosting/_cognite_model_hosting_common/version.py").read(), re.M
).group(1)

setup(
    name="cognite-model-hosting",
    version=version,
    description="Utilities Cognite's model hosting environment",
    url="",  # TODO
    author="Nils Barlaug",
    author_email="nils.barlaug@cognite.com",
    packages=["cognite.model_hosting." + p for p in find_packages(where="cognite/model_hosting")],
    install_requires=["cognite-sdk==1.*", "marshmallow==3.0.0rc4"],
    python_requires=">=3.5",
)
