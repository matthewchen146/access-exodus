Access Exodus
=============

.. image:: https://img.shields.io/github/actions/workflow/status/matthewchen146/access-exodus/tests.yml?label=tests
   :target: https://github.com/matthewchen146/access-exodus/actions/workflows/tests.yml
   :alt: Tests Status
.. image:: https://img.shields.io/github/actions/workflow/status/matthewchen146/access-exodus/docs.yml?label=docs
   :target: https://github.com/matthewchen146/access-exodus/actions/workflows/docs.yml
   :alt: Docs Status

This program aims to transfer data from Microsoft Access databases to a different SQL database.
Special config files with database and table information are read to determine what/where/how tables and columns are transferred.
The program relies on ODBC drivers and `aioodbc <https://github.com/aio-libs/aioodbc>`_ and `pyodc <https://github.com/mkleehammer/pyodbc>`_ to connect to both source and target databases
For now, ODBC drivers will have to be installed manually.
See below for useful links on intalling and using ODBC drivers.

    **⚠ Warning: This project is in very early stages and should not be used in production**

Current Features
----------------

- Transfer tables from multiple Access databases to one target database
- Define transfer specification in one config file

Not Supported
-------------

- GUI is work in progress
- Table schema not fully implemented

Usage
-----

    **⚠ Warning: Currently drops any existing tables in the target database and creates new tables**

- With the executable: `accex <path-to-config-file>`
    - If a config file is not specified, the program will find one in the current working directory with the `.accex` extension.
- From source: `python -m accex <path-to-config-file>`

`Documentation <https://matthewchen146.github.io/access-exodus/>`_

Currently Supported Database Targets
------------------------------------

================ =========
Database         Version  
---------------- ---------
PostgreSQL       14^
================ =========

Useful Links
------------

- https://download.microsoft.com/download/2/4/3/24375141-E08D-4803-AB0E-10F2E3A07AAA/AccessDatabaseEngine_X64.exe
- https://www.connectionstrings.com/microsoft-access-accdb-odbc-driver/
- https://www.microsoft.com/en-us/download/details.aspx?id=13255
- https://www.connectionstrings.com/postgresql-odbc-driver-psqlodbc/
- https://www.postgresql.org/ftp/odbc/versions/

Devlopment Setup
----------------

Using `poetry <https://python-poetry.org/>`_ to manage modules is recommended.

1. Install a supported version of python, such as with `pyenv <https://github.com/pyenv/pyenv>`_
    1. ``pyenv install 3.11.6``
    2. ``pyenv local 3.11.6`` to set the local python version
2. Install ``poetry`` and configure
    1. ``poetry config virtualenvs.in-project true``
3. Create a virtual environment
    1. ``poetry shell`` will create one if it does not exist and activates it
    2. ``poetry env info`` (should display the correct python version, with path to local .venv)
4. Install dependencies
    1. ``poetry install``
        - To install with optional dependencies, use ``poetry install --with group_1,group_2,...``
        - Groups can be found in ``pyproject.toml``

Tests
-----

Tests use `pytest <https://github.com/pytest-dev/pytest/>`_, `pytest-cov <https://github.com/pytest-dev/pytest-cov>`_ and `pytest-asyncio <https://github.com/pytest-dev/pytest-asyncio>`_

- ``poetry install --with test`` to install test dependencies
- ``poetry run pytest``

Building
--------

WIP
