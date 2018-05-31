===============
timescale_bench
===============


.. image:: https://img.shields.io/pypi/v/timescale_bench.svg
        :target: https://pypi.python.org/pypi/timescale_bench

.. image:: https://img.shields.io/travis/davidblewett/timescale_bench.svg
        :target: https://travis-ci.org/davidblewett/timescale_bench

.. image:: https://readthedocs.org/projects/timescale-bench/badge/?version=latest
        :target: https://timescale-bench.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status




CLI for benchmarking SELECT query performance against a TimescaleDB instance.
Example::

    pip install -e git+https://github.com/davidblewett/timescale_bench.git#egg=timescale_bench
    export DSN=postgresql://postgres@localhost/homework
    timescale_bench -f /path/to/query_params.csv


* Free software: Apache Software License 2.0


Features
--------

* Input is a CSV file (either stdin or path on disk) with the following structure:

  * `hostname,start_time,end_time`

* Database connection details passed via connection string:

  * https://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-CONNSTRING

* Optionally output host per-minute min/max CPU usage
* Output Summary

  * # of queries processed
  * total processing time across all queries
  * the minimum query time (for a single query)
  * the median query time
  * the average query time
  * and the maximum query time.


Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
