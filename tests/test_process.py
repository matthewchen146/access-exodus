import pytest
import accex.config
import accex.process
import asyncio
import pyodbc

def test_data_transfer():
    return
    config_path = "./tests/configs/config.accex"
    config = None
    with open(config_path, "r") as config_file:
        config = accex.config.parse_config(config_file.read())
    asyncio.run(accex.process.transfer(config))
    # verify that data matches
    
