import pytest
import os
import json
import sys
import random
import string
import logging
from unittest.mock import patch
from util import CWDContext
import accex.config.core as ac

def read_file(path: str) -> str:
    s = ""
    with open(path, "r") as file:
        s = file.read()
    return s

def generate_random_string(length: int, possible_chars: str = string.printable):
    return ''.join([random.choice(possible_chars) for _ in range(length)])

def generate_config_text():
    answer_dict = dict()

    # generate target
    answer_dict["TARGET_DSN_PARAMS"] = {
        "Driver": "PostgreSQL Unicode",
        "Server": "localhost",
        "Port": "5432",
        "Database": "postgres",
        "Uid": "postgres",
        "Pwd": "root"
    }

    target_types = ["serial primary key", "text", "numeric", "date", "int"]

    answer_dict["TARGETS"] = dict()

    num_target_tables = random.randint(1,3)

    for _ in range(num_target_tables):
        # target table
        table_name = generate_random_string(10, string.ascii_letters + "_")
        answer_dict["TARGETS"][table_name] = {
            "COLUMNS": dict([(generate_random_string(10, string.ascii_letters + "_"), random.choice(target_types)) for _ in range(random.randint(4,8))])
        }

    # generate source
    answer_dict["SOURCE_DSN_PARAMS"] = {
        "Driver": "Microsoft Access Driver (*.mdb, *.accdb)"
    }

    answer_dict["SOURCES"] = dict()

    for target_name, target_table in answer_dict["TARGETS"].items():
        # source table
        table_name = generate_random_string(10, string.ascii_letters + "_")
        answer_dict["SOURCES"][table_name] = {
            "DSN_PARAMS": {
                "DBQ": "./tests/databases/database.accdb"
            },
            "TARGET_TABLE": target_name,
            "COLUMNS": dict([(generate_random_string(10, string.ascii_letters + "_"), target_col) for target_col in target_table["COLUMNS"].keys()])
        }
    
    def str_block(block: dict, indents: int = 0, brackets: bool = True):
        prev_space = ''.join([' ' for _ in range(max(0, indents - 4))])
        space = ''.join([' ' for _ in range(indents)])
        s = "{\n" if brackets else ""
        for k, v in block.items():
            if isinstance(v, dict):
                s += f"{space}{k} {str_block(v, indents=indents + 4)}"
            else:
                s += f"{space}{k}: {v}"
            s += "\n"
        s += prev_space + "}\n" if brackets else "\n"
        return s

    config_text = str_block(answer_dict, brackets=False)

    return config_text, answer_dict

def test_config_class():
    assert True

def test_find_config_path():
    with CWDContext("test_tmp", True):
        assert ac.find_config_path() == None
        test_config_path = os.path.abspath("test_config.accex")
        with open(test_config_path, "w"):
            pass
        assert ac.find_config_path() == test_config_path

def test_resolve_config_path():
    with CWDContext("test_tmp", True):
        assert ac.resolve_config_path() == None
        test_config_name = "test_config.accex"
        test_config_path = os.path.abspath("test_config.accex")
        with open(test_config_path, "w"):
            pass
        assert ac.resolve_config_path() == test_config_path

        with patch.object(sys, "argv", [ __file__, test_config_name ]):
            assert ac.resolve_config_path(test_config_name) == test_config_path
        with patch.object(sys, "argv", [ __file__, test_config_path ]):
            assert ac.resolve_config_path(test_config_path) == test_config_path
        with patch.object(sys, "argv", [ __file__, "invalid_config_path.accex" ]):
            with pytest.raises(ValueError):
                ac.resolve_config_path("invalid_config_path.accex")

def test_remove_comments():
    s = "Cool\n\ncool"
    assert ac._remove_comments(s) == s
    s = "# comment\nnot comment"
    assert ac._remove_comments(s) == "\nnot comment"
    s = "title\n look at this ##comment    \nbut \\#not this\n\n#\\this yes"
    assert ac._remove_comments(s) == "title\n look at this \nbut #not this\n\n"
    s = "the remainder \\#\\#text\\##\\#\\#\\#comment"
    assert ac._remove_comments(s) == "the remainder ##text#"

def test_replace_env_vars():
    test_env_vars = { "PASS": "123", "DB_PORT": "9000", "user_id": "candy", "host": "762.43.2.355" }
    test_text = f"""
TARGET_DSN_PARAMS {{
    Driver: PostgreSQL Unicode
    Server: $host
    Port: ${{DB_PORT}}
    Database: postgres
    Uid: $user_id
    Pwd: $PASS
}}
"""
    answer_text = f"""
TARGET_DSN_PARAMS {{
    Driver: PostgreSQL Unicode
    Server: {test_env_vars['host']}
    Port: {test_env_vars['DB_PORT']}
    Database: postgres
    Uid: {test_env_vars['user_id']}
    Pwd: {test_env_vars['PASS']}
}}
"""
    with patch.dict("os.environ", test_env_vars):
        replaced_text = ac._replace_env_vars(test_text)
        assert answer_text == replaced_text

        # test empty env var
        with pytest.raises(ValueError):
            text = "\n$\n$DB_PORT"
            ac._replace_env_vars(text)
        
        # test empty env var with braces
        with pytest.raises(ValueError):
            text = "\n${}"
            ac._replace_env_vars(text)
        
        # test missing env var
        with pytest.raises(ValueError):
            text = "\n$MISSING_VAR"
            ac._replace_env_vars(text)


def test_parse_config():
    config_text, answer_dict = generate_config_text()
    config = ac.parse_config(config_text)
    assert config

def test_parse_config_file():
    config = ac.parse_config_file("./tests/configs/config.accex")
    assert config

def test_write_config():
    config_text = read_file("./tests/configs/config.accex")
    config = ac.parse_config(config_text)
    out_config_text = ac.write_config(config)
    out_config = ac.parse_config(out_config_text)
    assert config == out_config

def test_write_config_file():
    config_text = read_file("./tests/configs/config.accex")
    config = ac.parse_config(config_text)
    out_config_path = "out_config.accex"
    if os.path.exists(out_config_path):
        os.remove(out_config_path)
    ac.write_config_file(config, out_config_path)
    assert os.path.exists(out_config_path)
    out_config_text = read_file(out_config_path)
    out_config = ac.parse_config(out_config_text)
    assert config == out_config
    os.remove(out_config_path)

def test_main(capsys):
    # cover main with init func
    with patch.object(ac, "__name__", "__main__"):
        ac._main()
        capsys.readouterr()

    config_path = "./tests/configs/config.accex"
    config = ac.parse_config_file(config_path)
    config_text = ac.write_config(config)
    with patch.object(sys, "argv", [ __file__, config_path ]):
        ac._main()
        captured = capsys.readouterr()
        assert captured.out.strip() == config_text

    with patch.object(sys, "argv", [ __file__, "--json", config_path]):
        ac._main()
        captured = capsys.readouterr()
        assert captured.out.strip() == json.dumps(config)
    
    with patch.object(sys, "argv", [ __file__, "--json", "--json-format", config_path]):
        ac._main()
        captured = capsys.readouterr()
        assert captured.out.strip() == json.dumps(config, indent=4)
    
    out_config_path = "tmp_config.json"
    with patch.object(sys, "argv", [ __file__, "--json", "--out-file", out_config_path, config_path]):
        ac._main()
        assert os.path.exists(out_config_path)
        out_config_json = read_file(out_config_path)
        assert out_config_json == json.dumps(config)
    if os.path.exists(out_config_path):
        os.remove(out_config_path)
    
    # test no outfile defined exit
    # 2 is the code of argparse error
    argparse_error_code = 2
    with pytest.raises(SystemExit) as e:
        with patch.object(sys, "argv", [ __file__, "--out-file"]):
            ac._main()
    assert e.value.code == argparse_error_code

    # test no config found
    tmp_path = "test_tmp"
    with pytest.raises(SystemExit) as e:
        with CWDContext(tmp_path, True):
            ac._main()
    assert e.value.code == 1