# Access Exodus

This is a simple project that aims to transfer data from Microsoft Access databases to a different SQL database.
This can be done by writing a config file with database and table information, and reading that config with the program.
This uses `pyodbc` under the hood to connect to both source and target databases, so ODBC drivers will have to be installed manually.
See below for useful links on intalling and using ODBC drivers.

*Note: This project is in very early stages and currently only works on Windows due to the nature of Microsoft Access*

## Current Features

- Transfer tables from multiple Access databases to one target database in one config file

## Not Supported

- GUI is work in progress
- Schemas not yet implemented

## Usage

- With the executable: `accex <path-to-config-file>`
    - If a config file is not specified, the program will find one in the current working directory with the `.accex` extension.
- From source: `python src/accex_process <path-to-config-file>`

## Useful Links

https://download.microsoft.com/download/2/4/3/24375141-E08D-4803-AB0E-10F2E3A07AAA/AccessDatabaseEngine_X64.exe

https://www.connectionstrings.com/microsoft-access-accdb-odbc-driver/

https://www.microsoft.com/en-us/download/details.aspx?id=13255

https://www.connectionstrings.com/postgresql-odbc-driver-psqlodbc/

https://www.postgresql.org/ftp/odbc/versions/

## Instructions to Run from Source

1. `pyenv install 3.11.6`
2. `pyenv local 3.11.6`
3. `pyenv global 3.11.6` (for installing poetry)
4. `poetry config virtualenvs.in-project true`
5. `poetry shell`
6. `poetry env info` (should be 3.11.6, with path to local .venv)
7. `poetry install`
    - If no gui, do `poetry install --without gui`
    - If dev (such as for building), add `--with dev`

## Build Instructions

For CLI only (no gui)
- `pyinstaller --onefile --hidden-import=uuid .\src\accex_process.py`