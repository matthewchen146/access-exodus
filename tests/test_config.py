import pytest
import os
import json
import sys
import logging
from collections import Counter
from unittest.mock import patch
import accex_config

def read_file(path: str) -> str:
    s = ""
    with open(path, "r") as file:
        s = file.read()
    return s

def test_find_config_path():
    example_config_file_name = "example_config.accex"
    cwd = os.getcwd()
    assert accex_config.find_config_path() == os.path.abspath(example_config_file_name)
    tmp_path = os.path.abspath("test_tmp")
    if not os.path.exists(tmp_path):
        os.mkdir(tmp_path)
    os.chdir(tmp_path)
    config_path = accex_config.find_config_path()
    os.chdir(cwd)
    os.rmdir(tmp_path)
    assert config_path == None

def test_resolve_config_path():
    example_config_file_name = "example_config.accex"
    cwd = os.getcwd()
    example_config_path = os.path.abspath(example_config_file_name)
    assert accex_config.resolve_config_path() == example_config_path
    tmp_path = os.path.abspath("test_tmp")
    if not os.path.exists(tmp_path):
        os.mkdir(tmp_path)
    os.chdir(tmp_path)
    config_path = accex_config.resolve_config_path()
    os.chdir(cwd)
    os.rmdir(tmp_path)
    assert config_path == None
    with patch.object(sys, "argv", [ __file__, example_config_file_name ]):
        assert accex_config.resolve_config_path() == example_config_path
    with patch.object(sys, "argv", [ __file__, example_config_path ]):
        assert accex_config.resolve_config_path() == example_config_path
    with patch.object(sys, "argv", [ __file__, "invalid_config.accex" ]):
        with pytest.raises(ValueError):
            accex_config.resolve_config_path()

def test_parse_config():
    config_text = read_file("./tests/configs/config.accex")
    config = accex_config.parse_config(config_text)
    answer_json = read_file("./tests/configs/config_answer.json")
    answer = json.loads(answer_json)
    assert Counter(answer) == Counter(config)

def test_parse_config_file():
    config = accex_config.parse_config_file("./tests/configs/config.accex")
    assert config

def test_write_config():
    config_text = read_file("./tests/configs/config.accex")
    config = accex_config.parse_config(config_text)
    out_config_text = accex_config.write_config(config)
    out_config = accex_config.parse_config(out_config_text)
    assert Counter(config) == Counter(out_config)

def test_write_config_file():
    config_text = read_file("./tests/configs/config.accex")
    config = accex_config.parse_config(config_text)
    out_config_path = "out_config.accex"
    if os.path.exists(out_config_path):
        os.remove(out_config_path)
    accex_config.write_config_file(config, out_config_path)
    assert os.path.exists(out_config_path)
    out_config_text = read_file(out_config_path)
    out_config = accex_config.parse_config(out_config_text)
    assert Counter(config) == Counter(out_config)
    os.remove(out_config_path)