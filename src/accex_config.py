from _collections_abc import dict_items, dict_keys, dict_values
import json
import os
import sys
import logging
import pyparsing as pp
from typing import Any, Callable, TypeVar, ItemsView, KeysView, ValuesView
import re
import dotenv
import argparse

target_default_schema = "public"

arg_parser: argparse.ArgumentParser | None = None

def to_str(t: Any) -> str:
    return t.to_str() if callable(getattr(t, 'to_str', None)) else str(t) + '\n'

def get_target_default_schema() -> str:
    return target_default_schema

class TargetTablePointer:
    def __init__(self, table_name: str, schema_name: str = get_target_default_schema()) -> None:
        self.schema_name = schema_name
        self.table_name = table_name
    
    def __str__(self) -> str:
        return "TargetTablePointer<" + ((self.schema_name and f"{self.schema_name}.") or "") + self.table_name + ">"
    
    def __eq__(self, __value: object) -> bool:
        return self.schema_name == __value.schema_name and self.table_name == __value.table_name
    
    def __hash__(self) -> int:
        return hash((self.schema_name, self.table_name))

class TargetColumnPointer:
    def __init__(self, column_name: str, table_name: str, schema_name: str = get_target_default_schema()) -> None:
        self.table = TargetTablePointer(table_name, schema_name)
        self.column_name = column_name
    
    def __init__(self, column_name: str, table: TargetTablePointer) -> None:
        self.table = table
        self.column_name = column_name

    @property
    def schema_name(self) -> str:
        return self.table.schema_name
    @schema_name.setter
    def schema_name(self, value):
        self.table.schema_name = value

    @property
    def table_name(self) -> str:
        return self.table.table_name
    @table_name.setter
    def table_name(self, value):
        self.table.table_name = value

    def __str__(self) -> str:
        return "TargetColumnPointer<" + ((self.schema_name and f"{self.schema_name}.") or "") + (self.table_name and f"{self.table_name}." or "?.") + self.column_name + ">"

    # def __eq__(self, __value: object) -> bool:
    #     return self.sc

class TargetRowPointer:
    match_directives = {
        "value": True
    }

    def __init__(self, select_column: TargetColumnPointer, match_directive: str) -> None:
        self.select_column = select_column
        self.match_directive = match_directive

    def __str__(self) -> str:
        return f"TargetRowPointer<{str(self.select_column)}, match {self.match_directive}>"

TRecordValue = TypeVar('TRecordValue', bound='RecordValue')

class RecordValue(str):
    def __init__(self, v, key : str = '') -> None:
        self.key = key
        self.value = v
        self.original_value = v

    def __str__(self) -> str:
        return str(self.value)

    def to_str(self) -> str:
        return f'{self.key}: ' + self.__str__()

    def set_key(self, key: str) -> TRecordValue:
        self.key = key
        return self
    
    def set_value(self, value) -> TRecordValue:
        self.value = value
        return self

TBlockValue = TypeVar('TBlockValue', bound='BlockValue')

class BlockValue(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.key = ''
    
    def to_str(self, indents: int = 4) -> str:
        prev_space = ''.join([' ' for _ in range(max(0, indents - 4))])
        space = ''.join([' ' for _ in range(indents)])
        s = f"{self.key} {'{'}\n" + "\n".join(
            [
                f'{space}{to_str(value) if not isinstance(value, BlockValue) else value.to_str(indents + 4)}' for key, value in self.items()
            ]
        ) + f"\n{prev_space}{'}'}"
        return s
    
    def set_key(self: TBlockValue, key: str) -> TBlockValue:
        self.key = key
        return self

TSourceTableBlockColumns = TypeVar('TSourceTableBlockColumns', bound='SourceTableBlock.Columns')

# contains database, target, and columns
class SourceTableBlock(BlockValue):
    class Columns(BlockValue):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.target_table : TargetTablePointer | None = None
            self.target_table_deps = set()
            for k, v in self.items():
                self[k] = v

        def __getitem__(self, __key: str) -> RecordValue:
            return super().__getitem__(__key)

        def __setitem__(self, __key: Any, __value: Any) -> None:
            # parse value of each record
            if not isinstance(__value, RecordValue):
                __value = RecordValue(__value, key=__key)
            super().__setitem__(__key, __value)

        def items(self) -> ItemsView[str, RecordValue]:
            return super().items()
        
        def values(self) -> ValuesView[RecordValue]:
            return super().values()

        def process_columns(self) -> None:
            self.target_table_deps.clear()
            for r in self.values():
                if isinstance(r.value, TargetColumnPointer):
                    r.value.table = self.target_table
                elif isinstance(r.value, SourceColumnMapFunction):
                    r.value.to_column.table = self.target_table
                    self.target_table_deps.add(self.target_table)
                else:
                    parsed_source_column_value = parse_source_column_function(r.value)[0]
                    r.value = parsed_source_column_value
                    # check dependent tables
                    if isinstance(r.value, SourceColumnMapFunction):
                        r.value.to_column.table = self.target_table
                        self.target_table_deps.add(r.value.with_column.table)
                    elif isinstance(r.value, TargetColumnPointer):
                        r.value.table = self.target_table

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for k, v in self.items():
            self[k] = v
    
    def __setitem__(self, __key: Any, __value: Any) -> None:
        if __key == "COLUMNS":
            if not isinstance(__value, SourceTableBlock.Columns):
                __value : SourceTableBlock.Columns = SourceTableBlock.Columns(__value)
                if self.target:
                    __value.target_table = self.target
                    __value.process_columns()
            super().__setitem__(__key, __value)
        elif __key == "TARGET_TABLE":
            if isinstance(__value, RecordValue):
                if not isinstance(__value.value, TargetTablePointer):
                    __value.value = TargetTablePointer(__value.value)
                super().__setitem__(__key, __value)
            elif isinstance(__value, TargetTablePointer):
                super().__setitem__(__key, RecordValue(__value, key=__key))
            else:
                # assuming string
                __value = TargetTablePointer(__value)
                super().__setitem__(__key, RecordValue(__value, key=__key))
            # fix columns
            if "COLUMNS" in self:
                c = self["COLUMNS"]
                if not isinstance(c, SourceTableBlock.Columns):
                    # c = SourceTableBlock.Columns(__value, self.target_table)
                    self["COLUMNS"] = c
                else:
                    c.target_table = self.target
                    c.process_columns()
        elif __key == "DSN_PARAMS":
            super().__setitem__(__key, __value if isinstance(__value, BlockValue) else BlockValue(__value))
        else:
            super().__setitem__(__key, __value)

    @property
    def target(self) -> TargetTablePointer:
        return None if "TARGET_TABLE" not in self else self["TARGET_TABLE"].value

    @property
    def columns(self) -> TSourceTableBlockColumns:
        return self['COLUMNS']

    @property
    def dsn_params(self) -> BlockValue:
        return self.get('DSN_PARAMS') or BlockValue()
    
    @property
    def target_table_deps(self) -> set[TargetTablePointer]:
        return self.columns.target_table_deps

class SourcesBlock(BlockValue):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for k, v in super().items():
            self[k] = v
    
    def __setitem__(self, __key: Any, __value: Any) -> None:
        if isinstance(__value, SourceTableBlock):
            super().__setitem__(__key, __value)
        else:
            super().__setitem__(__key, SourceTableBlock(__value))

    def __getitem__(self, __key: Any) -> SourceTableBlock:
        return super().__getitem__(__key)

    def items(self) -> ItemsView[str, SourceTableBlock]:
        return super().items()

    def values(self) -> ValuesView[SourceTableBlock]:
        return super().values()

class SourceColumnMapFunction:
    """Source column function that transforms a source column value to another value before inserting to a target
    """
    def __init__(self, to_column: TargetColumnPointer, with_column: TargetColumnPointer, from_row: TargetRowPointer) -> None:
        self.to_column = to_column
        self.with_column = with_column
        self.from_row = from_row
    
    def __str__(self) -> str:
        return f"SourceColumnMapFunction<to {str(self.to_column)} WITH {str(self.with_column)} FROM {str(self.from_row)}>"

    @property
    def table(self) -> TargetTablePointer:
        return self.to_column.table

class TargetTableBlock(BlockValue):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def columns(self) -> dict:
        return self['COLUMNS']

    @property
    def dsn_params(self) -> dict:
        return self.get('DSN_PARAMS') or dict()

class TargetsBlock(BlockValue):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for k, v in super().items():
            self[k] = v
    
    def __setitem__(self, __key: Any, __value: Any) -> None:
        if isinstance(__value, TargetTableBlock):
            super().__setitem__(__key, __value)
        else:
            super().__setitem__(__key, TargetTableBlock(__value))
    
    def __getitem__(self, __key: Any) -> TargetTableBlock:
        return super().__getitem__(__key)

    def items(self) -> ItemsView[str, TargetTableBlock]:
        return [(k, v) for k, v in super().items()]


class Config(dict):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # update each item through the __setitem__ method
        for k, v in self.items():
            self[k] = v

    def __setitem__(self, __key: Any, __value: Any) -> None:
        if __key == "SOURCES":
            super().__setitem__(__key, SourcesBlock(__value))
        elif __key == "TARGETS":
            super().__setitem__(__key, TargetsBlock(__value))
        else:
            super().__setitem__(__key, __value)

    @property
    def sources(self) -> SourcesBlock:
        return self["SOURCES"]
    @sources.setter
    def sources(self, value) -> None:
        self["SOURCES"] = value

    @property
    def targets(self) -> TargetsBlock:
        return self["TARGETS"]
    @targets.setter
    def targets(self, value) -> None:
        self["TARGETS"] = value

    @property
    def target_dsn_params(self) -> dict:
        return self["TARGET_DSN_PARAMS"]
    @target_dsn_params.setter
    def target_dsn_params(self, value) -> None:
        self["TARGET_DSN_PARAMS"] = value
    
    @property
    def source_dsn_params(self) -> dict:
        return self["SOURCE_DSN_PARAMS"]
    @source_dsn_params.setter
    def source_dsn_params(self, value) -> None:
        self["SOURCE_DSN_PARAMS"] = value
    
    def to_str(self) -> str:
        s: str = ''
    
        for key, value in self.items():

            to_str_method = getattr(value, 'to_str', None)

            s += (value.to_str() if callable(to_str_method) else str(value)) + '\n'

            s += '\n'
        
        return s


def remove_comments(s: str) -> str:
    pattern = r"(?<!\\)#.*"
    s = re.sub(pattern, '', s)
    return re.sub(r"\\#", '#', s)

def replace_env_vars(s: str) -> str:
    pattern = r"\$(?:{([a-zA-Z_]*)}|([a-zA-Z_]*))"

    def replace_env_var(match: re.Match) -> str:
        env_var_name = match.group(1) or match.group(2)
        if not env_var_name or len(env_var_name) == 0:
            raise ValueError("invalid environment variable name")
        env_var_value = os.getenv(env_var_name)
        if not env_var_value:
            raise ValueError(f"failed to get environment variable by name [{env_var_name}]")
        return env_var_value

    return re.sub(pattern, replace_env_var, s)


source_column_function_parser: pp.ParserElement | None = None

def parse_source_column_function(s: str):
    # example "customer_id WITH customers(id) FROM ROW(old_id, @value)"
    id_chars = pp.alphas + pp.nums + '_'
    # create source column parser if not created
    global source_column_function_parser
    if not source_column_function_parser:
        target_table_name = pp.Word(id_chars).set_name("target_table_name")
        target_column_name = pp.Word(id_chars).set_name("target_column_name")
        
        target_column_pointer = (pp.Opt(target_table_name + pp.Suppress(".")) + target_column_name).set_name("target_column_pointer")
        def target_column_pointer_parse_action(toks: pp.ParseResults):
            if len(toks) > 1:
                # target column name and target table name
                return TargetColumnPointer(toks[1], TargetTablePointer(toks[0]))
            else:
                # target column name only
                return TargetColumnPointer(toks[0], TargetTablePointer(""))
        target_column_pointer.add_parse_action(target_column_pointer_parse_action)

        # source column's value
        # FIXME: consider renaming this
        source_value_pointer = (pp.Suppress('@') + pp.oneOf(TargetRowPointer.match_directives.keys())).set_name("source_value_pointer")
        
        target_row_pointer = (pp.Suppress(pp.Keyword("ROW")) + pp.Suppress("(") + target_column_pointer + pp.Suppress(",") + source_value_pointer + pp.Suppress(")")).set_name("row_function")
        def target_row_pointer_parse_action(toks: pp.ParseResults):
            # return [TargetColumnPointer(toks[0]), toks[1]]
            return TargetRowPointer(select_column=toks[0], match_directive=source_value_pointer)
        target_row_pointer.add_parse_action(target_row_pointer_parse_action)

        
        map_function = (target_column_pointer + pp.Suppress(pp.Keyword("WITH")) + target_column_pointer + pp.Suppress(pp.Keyword("FROM")) + target_row_pointer).set_name("map_function")
        def map_function_parse_action(toks: pp.ParseResults):
            # update row function pointer
            tc: TargetColumnPointer = toks[0]
            wc: TargetColumnPointer = toks[1]
            rp: TargetRowPointer = toks[2]
            rp.select_column.table = wc.table
            return SourceColumnMapFunction(to_column=tc, with_column=wc, from_row=rp)
        map_function.add_parse_action(map_function_parse_action)
        def map_function_condition(toks: pp.ParseResults):
            m: SourceColumnMapFunction = toks[0]
            return m.with_column.table == m.from_row.select_column.table
        map_function.add_condition(map_function_condition)

        def parse_action(toks: pp.ParseResults):
            return toks
    
        source_column_function_parser = (map_function | target_column_pointer) + (pp.StringEnd() | pp.LineEnd())
        # source_column_function_parser.add_parse_action(parse_action)
    
    return source_column_function_parser.parse_string(s)


def parse_config(config_text: str) -> Config:

    config_text = remove_comments(config_text)

    dotenv.load_dotenv(override=True)

    config_text = replace_env_vars(config_text)

    id_chars = pp.alphas + pp.nums + '_'
    type_chars = pp.alphas + pp.nums + '()'
    path_chars = pp.identbodychars + '-./\\'
    identifier = pp.Word(id_chars).set_name('identifier')
    special_identifier = pp.Word(id_chars + "$").set_name('special_identifier')
    column_name = identifier.copy().set_name('column_name')
    column_type = pp.Word(type_chars + ' ').set_name('column_type')

    TKey = TypeVar('TKey', str, pp.ParserElement)

    def record(key: TKey, value: str | pp.ParserElement, key_name: str | Callable[[TKey], str] = None):
        r = key + pp.Suppress(':') + value + pp.Suppress(pp.LineEnd() | pp.StringEnd())

        def record_parse_action(toks: pp.ParseResults) -> Any:
            kn = key_name if key_name is str else (key_name(toks[0]) if callable(key_name) else toks[0])
            return (kn, RecordValue(toks[1]).set_key(kn))

        r.add_parse_action(record_parse_action)
        return r

    def block(key: str | pp.ParserElement, item: str | pp.ParserElement, parse_action: pp.ParseAction = None, key_name: str = None):
        t = pp.Group(key).set_name('block_key') + pp.Suppress('{').set_name('{') + pp.Group(pp.ZeroOrMore(item)).set_name('block_items') + pp.Suppress('}').set_name('}')
        
        def block_parse_action(toks: pp.ParseResults) -> Any:
            kn = key_name if key_name is str else (key_name(toks[0][0]) if callable(key_name) else toks[0][0])
            return (kn, BlockValue(toks[1].as_list()).set_key(kn))
        
        t.add_parse_action(parse_action if parse_action is not None else block_parse_action)
        return t

    ## target and source tables unused rn
    
    # target_column_record = record(column_name, column_type)
    # target_table_name = identifier.copy().set_name('target_table_name')
    # target_table = block(target_table_name, target_column_record)
    # target_tables = block('TARGETS', target_table)
    # target_tables.add_parse_action(lambda toks: (toks[0][0], TargetsBlock(toks[0][1]).set_key('TARGETS')))

    # # source tables
    # source_column_name = identifier.copy().set_name('source_column_name')
    # source_column_target = identifier.copy().set_name('source_column_target')
    # source_column_record = record(source_column_name, source_column_target)
    # source_table_record = identifier + pp.Suppress(':') + pp.Word(path_chars + ' ') + pp.Suppress('>') + target_table_name
    # def source_table_parse_action(toks: pp.ParseResults) -> Any:
    #     kn = toks[0][0]
    #     return (kn, SourceTableBlock(database=toks[0][1].strip(), target=toks[0][2], columns=dict(toks[1].as_list())).set_key(kn))
    # source_table = block(source_table_record, source_column_record, source_table_parse_action)
    # source_tables = block('SOURCES', source_table)
    # source_tables.add_parse_action(lambda toks: (toks[0][0], SourcesBlock(toks[0][1]).set_key('SOURCES')))


    misc_record = record(identifier.copy().set_name('record_key'), pp.Word(pp.printables + ' '))

    misc_block_recursive = pp.Forward()
    misc_block = block(identifier.copy().set_name('block_key'), misc_block_recursive | misc_record)
    misc_block_recursive <<= misc_block

    # comment
    comment = pp.Suppress('#') + pp.Word(pp.printables + ' ')
    comment.set_name('comment')

    # access_transform_spec = pp.ZeroOrMore(comment.suppress() | misc_record | source_tables | target_tables | misc_block)
    access_transform_spec = pp.ZeroOrMore(comment.suppress() | misc_record | misc_block)
    access_transform_spec.add_parse_action(lambda toks: Config(toks.as_list()))
    access_transform_spec.set_fail_action(lambda s, loc, expr, err: logging.error('failed to parse config\nexpr[%s]\nloc[%s]\nerr[%s]', expr, loc, err))

    config: Config = access_transform_spec.parse_string(config_text)[0]

    

    return config

def parse_config_file(config_file_path: str) -> Config:
    config_file = open(config_file_path, 'r')
    s = config_file.read()
    config_file.close()
    parsed_config = parse_config(s)
    return parsed_config

def write_config(config: Config) -> str:
    config_str: str = to_str(config).strip()
    
    return config_str

def write_config_file(config: Config, config_file_path: str):
    config_str = write_config(config)

    with open(config_file_path, 'w') as file:
        file.write(config_str)

def find_config_path() -> str | None:
    extension = ".accex"
    for file_name in os.listdir():
        if file_name.endswith(extension):
            return os.path.abspath(file_name)
    return None

def resolve_config_path() -> str | None:
    config_path = None
    args = parse_args()
    if args.config_path:
        config_path_arg = args.config_path
        if os.path.exists(config_path_arg):
            config_path = os.path.abspath(config_path_arg)
        else:
            raise ValueError(f"[{config_path_arg}] does not exist")
    if not config_path:
        config_path = find_config_path()
    return config_path

def setup_argparse() -> argparse.ArgumentParser:
    global arg_parser
    if arg_parser:
        return arg_parser
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "config_path",
        type=str,
        help="path to a config file",
        nargs="?"
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="parse config and output json",
    )
    parser.add_argument(
        "--json-format",
        action="store_true",
        help="if json output is enabled, format it",
    )
    parser.add_argument(
        "--out-file",
        action="store",
        type=str,
        help="write output to specified file path",
    )

    arg_parser = parser
    return parser

def parse_args() -> argparse.Namespace:
    return setup_argparse().parse_args()

def main():
    args = parse_args()
    config_path = resolve_config_path()
    if not config_path:
        logging.info("no config file specified/found")
        sys.exit(1)
    config = parse_config_file(config_path)
    out = ''
    if args.json:
        if args.json_format:
            out = json.dumps(config, indent=4)
        else:
            out = json.dumps(config)
    else:
        out = write_config(config)

    if args.out_file:
        with open(args.out_file, 'w') as file:
            file.write(out)
    else:
        print(out)

def init():
    if __name__ == "__main__":
        main()

init()