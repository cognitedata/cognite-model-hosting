.. cognite-sdk documentation master file, created by
   sphinx-quickstart on Thu Jan 11 15:57:44 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Cognite Model Hosting Documentation
===================================

Installation
^^^^^^^^^^^^
To install this package run the following command

.. code-block:: bash

   pip install cognite-model-hosting

Overview
^^^^^^^^
cognite-model-hosting is an open-source library containing utilities for working with data from the Cognite
Data Platform (CDP) in the Model Hosting environment. Working with the data is split into two parts;

1) Specifying data, such as time series and files, using Data Specs
2) Fetching the described data using the Data Fetcher.

Data Specs are a collection of classes used to specify data in CDP. These specs are organized in a hierarchical way, so
that they can be collected in a single object, sent easily around and passed to an instance of a DataFetcher. The Data
Fetcher is then used to retrieve the specified data from the platform.

Examples
^^^^^^^^
.. code-block:: python

   from cognite.model_hosting.data_fetcher import DataFetcher
   from cognite.model_hosting.data_spec import *

   id1 = ...
   id2 = ...
   ts1 = TimeSeriesSpec(id=id1, start="3d-ago", end="now", granularity="1d", aggregate="average")
   ts2 = TimeSeriesSpec(id=id2, start="3d-ago", end="now", granularity="1d", aggregate="max")
   ds = DataSpec({"myts1": ts1, "myts2": ts2})
   print(ds)

   data_fetcher = DataFetcher(ds)
   df = data_fetcher.time_series.fetch_dataframe(["myts1", "myts2"])
   print(df)

.. toctree::
   :maxdepth: 3
   :caption: Contents

   cognite
