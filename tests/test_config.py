import pytest
import accex_config
import json
from collections import Counter

def test_parse_config():
    config_path = "./tests/configs/config.accex"
    with open(config_path, "r") as config_file:
        parsed_config = accex_config.parse_config(config_file.read())
        answer_path = "./tests/configs/config_answer.json"
        with open(answer_path, "r") as answer_file:
            answer_json = answer_file.read()
            answer = json.loads(answer_json)
            assert Counter(answer) == Counter(parsed_config)
