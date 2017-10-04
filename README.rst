ModuleUltra
=========

.. image:: https://img.shields.io/pypi/v/ModuleUltra.svg
    :target: https://pypi.python.org/pypi/ModuleUltra
    :alt: Latest PyPI version

Easy to use pipelines for large biological datasets.

Goals
-----

Bioinformatics pipelines often involve a large number of files with complex organization and metadata, complex paths, and lots of dependencies. ModuleUltra is intended to make it easier run and distribute complex pipelines. It is built on top of DataSuper and SnakeMake. ModuleUltra defines both an API and CLI. 

ModuleUltra is probably overkill for small projects, it has been designed in particular for the MetaSUB project which has thousands of samples and complex analysis pipelines. ModuleUltra makes it easier to do consistent analysis on thousands of samples across many sites.

MetaSUB is also developing a program called DataSuper which tracks complex data and metadata.

In summary:
 - ModuleUltra makes it easy to install complex pipelines
 - ModuleUltra makes it easy to run pipelines only on slected subsets of the data
 - ModuleUltra makes it easy to run parts of large complex pipelines
 - ModuleUltra automatically tracks output of pipelines in DataSuper
 - ModuleUltra allows you to write most of your pipelines logic in SnakeMake, a popular pipeline system
 - ModuleUltra reduces the amount of boilerplate code necessary to make a pipeline

ModuleUltra is intended to improve reproducibility, features are being added to improve file provenance.

Installation
------------

Be aware that ModuleUltra is still an Alpha. There are still bugs and some unimplemented features.

ModuleUltra is currently being used on Ubuntu and RHEL systems. It should work on any *nix system.

To install:


.. code-block:: bash
   
    git clone <url>   

    python setup.py develop


Licence
-------
MIT License

Authors
-------

`ModuleUltra` was written by `David C. Danko <dcdanko@gmail.com>`_.
