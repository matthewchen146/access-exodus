import pytest
import accex_config
import accex_process
import asyncio
import pyodbc

def test_data_transfer():
    config_path = "./tests/configs/config.accex"
    config = None
    with open(config_path, "r") as config_file:
        config = accex_config.parse_config(config_file.read())
    asyncio.run(accex_process.transfer(config))
    # verify that data matches
    
