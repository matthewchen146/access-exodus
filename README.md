# access-exodus

https://www.connectionstrings.com/microsoft-access-accdb-odbc-driver/

https://www.microsoft.com/en-us/download/details.aspx?id=13255

https://www.connectionstrings.com/postgresql-odbc-driver-psqlodbc/

https://www.postgresql.org/ftp/odbc/versions/

# Python

1. `pyenv install 3.11.6`
2. `pyenv local 3.11.6`
3. `pyenv global 3.11.6` (for installing poetry)
4. `poetry config virtualenvs.in-project true`
5. `poetry shell`
6. `poetry env info` (should be 3.11.6, with path to local .venv)
7. `poetry install`
    - If no gui, do `poetry install --without gui`