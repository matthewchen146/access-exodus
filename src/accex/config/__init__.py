import json
import os
import sys
import logging
import pyparsing as pp
from typing import Any, Callable, TypeVar, ItemsView, KeysView, ValuesView, Generic, Self
import re
import dotenv
import argparse

class ValidationError(Exception):
    """Raised during config validation from config.validate()"""
    pass

TConfig = TypeVar("TConfig", bound="Config")
# FIXME: rename these
TRecordValueValue = TypeVar('TRecordValueValue')

class RecordValue(str, Generic[TRecordValueValue]):
    def __init__(self, v : TRecordValueValue, key : str = '') -> None:
        self.key = key
        self.value = v
        self.original_value = v

    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, RecordValue) and self.value == __value.value

    def __ne__(self, __value: object) -> bool:
        return not self.__eq__(__value)

    def __str__(self) -> str:
        return str(self.value)

    def to_str(self) -> str:
        return f"{self.key}: {to_str(self.value)}"

    def set_key(self, key: str) -> Self:
        self.key = key
        return self
    
    def set_value(self, value) -> Self:
        self.value = value
        return self

# FIXME: rename this
TBlockValueValue = TypeVar("TBlockValueValue")

class BlockValue(dict, Generic[TBlockValueValue]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.key = ''
    
    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, BlockValue):
            return False
        for k, v in self.items():
            if k not in __value:
                return False
            # print("  CHECK EQUAL", to_str(__value[k]), to_str(v))
            if not __value[k] == v:
                # print("  NOT EQUAL!")
                return False
        return True

    def __getitem__(self, __key: Any) -> TBlockValueValue:
        return super().__getitem__(__key)

    def items(self) -> ItemsView[str, TBlockValueValue]:
        return super().items()
    
    def keys(self) -> KeysView[str]:
        return super().keys()

    def values(self) -> ValuesView[TBlockValueValue]:
        return super().values()

    def to_str(self, indents: int = 4) -> str:
        prev_space = ''.join([' ' for _ in range(max(0, indents - 4))])
        space = ''.join([' ' for _ in range(indents)])
        s = f"{self.key} {'{'}\n" + "\n".join(
            [
                f'{space}{to_str(value) if not isinstance(value, BlockValue) else value.to_str(indents + 4)}' for value in self.values()
            ]
        ) + f"\n{prev_space}{'}'}"
        return s
    
    def set_key(self, key: str) -> Self:
        self.key = key
        return self

target_default_schema = "public"

arg_parser: argparse.ArgumentParser | None = None

def to_str(obj: Any) -> str:
    """Checks object for a special string method that writes a config string"""
    return obj.to_str() if callable(getattr(obj, 'to_str', None)) else str(obj)

def get_target_default_schema() -> str:
    """Gets the default schema name for target tables, set in target_default_schema"""
    return target_default_schema

class TargetTablePointer:
    """_summary_
    """
    def __init__(self, table_name: str, schema_name: str = get_target_default_schema()) -> None:
        self.schema_name = schema_name
        self.table_name = table_name
    
    def __str__(self) -> str:
        return f"TargetTablePointer<{self.to_sql_str()}>"
    
    def to_str(self) -> str:
        # FIXME: add schema back when source block handles target table
        return self.to_sql_str(with_schema=True)

    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, TargetTablePointer) and self.schema_name == __value.schema_name and self.table_name == __value.table_name

    def __hash__(self) -> int:
        return hash((self.schema_name, self.table_name))
    
    def to_sql_str(self, with_schema: bool = True) -> str:
        return ((with_schema and self.schema_name and f"{self.schema_name}.") or "") + self.table_name
    
    def get_columns_block(self, config: TConfig) -> BlockValue:
        cols = config.targets[self.table_name].columns
        return cols

    def validate(self, config: TConfig):
        """_summary_

        :param config: _description_
        :type config: TConfig
        :raises ValidationError: _description_
        :raises ValidationError: _description_
        :raises ValidationError: _description_
        """
        if not self.table_name: raise ValidationError("Missing table name")
        if not self.schema_name: raise ValidationError("Missing schema name")
        # FIXME: take into account schema
        if self.table_name not in config.targets: raise ValidationError(f"{str(self)} does not point to a table in TARGETS")

class TargetColumnPointer:
    def __init__(self, column_name: str, table_name: str, schema_name: str = get_target_default_schema()) -> None:
        self.table = TargetTablePointer(table_name, schema_name)
        self.column_name = column_name
    
    def __init__(self, column_name: str, table: TargetTablePointer) -> None:
        self.table = table
        self.column_name = column_name
    
    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, TargetColumnPointer) and self.table == __value.table and self.column_name == __value.column_name

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
        return f"TargetColumnPointer<{self.to_str()}>"

    def to_str(self) -> str:
        return ((self.schema_name and f"{self.schema_name}.") or "") + (self.table_name and f"{self.table_name}." or "?.") + self.column_name

    # def __eq__(self, __value: object) -> bool:
    #     return self.sc
    def validate(self, config: TConfig):
        tgt_table_cols = self.table.get_columns_block(config)
        # assert column name in target table
        if self.column_name not in tgt_table_cols:
            raise ValidationError(f'{str(self.table)} deos not have a column named [{self.column_name}]')

class TargetRowPointer:
    match_directives = {
        "value": True
    }

    def __init__(self, select_column: TargetColumnPointer, match_directive: str) -> None:
        self.select_column = select_column
        self.match_directive = match_directive

    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, TargetRowPointer) and self.select_column == __value.select_column and self.match_directive == __value.match_directive

    def __str__(self) -> str:
        return f"TargetRowPointer<{str(self.select_column)}, match {self.match_directive}>"
    
    def to_str(self) -> str:
        return f"ROW({to_str(self.select_column)}, @{self.match_directive})"

    def validate(self, config: TConfig):
        self.select_column.validate(config)
        if self.match_directive not in TargetRowPointer.match_directives:
            raise ValidationError(f"Invalid match directive @{self.match_directive}, must be one of [{', '.join(k for k in TargetRowPointer.match_directives)}]")

class SourceColumnMapFunction:
    """Source column function that transforms a source column value to another value before inserting to a target
    """
    def __init__(self, to_column: TargetColumnPointer, with_column: TargetColumnPointer, from_row: TargetRowPointer) -> None:
        self.to_column = to_column
        self.with_column = with_column
        self.from_row = from_row
    
    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, SourceColumnMapFunction) and \
            self.to_column == __value.to_column and \
            self.with_column == __value.with_column and \
            self.from_row == __value.from_row

    def __str__(self) -> str:
        return f"SourceColumnMapFunction<to {str(self.to_column)} WITH {str(self.with_column)} FROM {str(self.from_row)}>"

    def to_str(self) -> str:
        return f"{to_str(self.to_column)} WITH {to_str(self.with_column)} FROM {to_str(self.from_row)}"

    @property
    def table(self) -> TargetTablePointer:
        return self.to_column.table
    
    def validate(self, config: TConfig):
        self.with_column.validate(config)
        self.from_row.validate(config)
        self.to_column.validate(config)


class SourceTableBlockColumns(BlockValue[RecordValue[TargetColumnPointer | SourceColumnMapFunction]]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.target_table : TargetTablePointer | None = None
        self.target_table_deps = set()
        for k, v in self.items():
            self[k] = v

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, SourceTableBlockColumns):
            return False
        return super().__eq__(__value)

    def __setitem__(self, __key: Any, __value: Any) -> None:
        # parse value of each record
        if not isinstance(__value, RecordValue):
            __value = RecordValue(__value, key=__key)
        super().__setitem__(__key, __value)

    def process_columns(self) -> None:
        self.target_table_deps.clear()
        for r in self.values():
            if isinstance(r.value, TargetColumnPointer):
                r.value.table = self.target_table
            elif isinstance(r.value, SourceColumnMapFunction):
                r.value.to_column.table = self.target_table
                self.target_table_deps.add(self.target_table)
            else:
                parsed_source_column_value = parse_source_column_function(r.value)
                r.value = parsed_source_column_value
                # check dependent tables
                if isinstance(r.value, SourceColumnMapFunction):
                    r.value.to_column.table = self.target_table
                    self.target_table_deps.add(r.value.with_column.table)
                elif isinstance(r.value, TargetColumnPointer):
                    r.value.table = self.target_table

# contains database, target, and columns
class SourceTableBlock(BlockValue[RecordValue | BlockValue]):
    Columns = SourceTableBlockColumns

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for k, v in self.items():
            self[k] = v
    
    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, SourceTableBlock):
            return False
        return super().__eq__(__value)

    def __setitem__(self, __key: Any, __value: Any) -> None:
        if __key == "COLUMNS":
            if not isinstance(__value, SourceTableBlockColumns):
                __value : SourceTableBlockColumns = SourceTableBlockColumns(__value).set_key(__key)
                if self.target:
                    __value.target_table = self.target
                    __value.process_columns()
            super().__setitem__(__key, __value)
        elif __key == "TARGET_TABLE":
            if isinstance(__value, RecordValue):
                if not isinstance(__value.value, TargetTablePointer):
                    __value.value = parse_target_table_pointer(__value.value)
                super().__setitem__(__key, __value)
            elif isinstance(__value, TargetTablePointer):
                super().__setitem__(__key, RecordValue(__value, key=__key))
            else:
                # assuming string
                __value = parse_target_table_pointer(__value)
                super().__setitem__(__key, RecordValue(__value, key=__key))
            # fix columns
            if "COLUMNS" in self:
                c = self["COLUMNS"]
                if not isinstance(c, SourceTableBlockColumns):
                    # c = SourceTableBlockColumns(__value, self.target_table)
                    self["COLUMNS"] = c
                else:
                    c.target_table = self.target
                    c.process_columns()
        elif __key == "DSN_PARAMS":
            super().__setitem__(__key, __value if isinstance(__value, BlockValue) else BlockValue(__value).set_key(__key))
        else:
            super().__setitem__(__key, __value)

    @property
    def target(self) -> TargetTablePointer:
        return None if "TARGET_TABLE" not in self else self["TARGET_TABLE"].value

    @property
    def columns(self) -> SourceTableBlockColumns:
        return self['COLUMNS']

    @property
    def dsn_params(self) -> BlockValue[RecordValue[str]]:
        return self.get('DSN_PARAMS') or BlockValue()
    
    @property
    def target_table_deps(self) -> set[TargetTablePointer]:
        return self.columns.target_table_deps

    def validate(self, config: TConfig):
        if "COLUMNS" not in self: raise ValidationError("Missing COLUMNS")
        for column in ((v.value) for v in self.columns.values()):
            if isinstance(column, TargetColumnPointer):
                column.validate(config)
            elif isinstance(column, SourceColumnMapFunction):
                column.validate(config)
            else:
                raise ValidationError(f"Column should be a TargetColumnPointer or Function, got [{column}]")
        if "TARGET_TABLE" in self:
            r: RecordValue = self["TARGET_TABLE"]
            if not isinstance(r, RecordValue): raise ValidationError(f"TARGET_TABLE should be a RecordValue, got [{r}]")
            if not isinstance(r.value, TargetTablePointer): raise ValidationError(f"TARGET_TABLE record should be a TargetTablePointer, got [{r.value}]")
            r.value.validate(config)

class SourcesBlock(BlockValue[SourceTableBlock]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for k, v in super().items():
            self[k] = v
    
    def __setitem__(self, __key: Any, __value: Any) -> None:
        if isinstance(__value, SourceTableBlock):
            super().__setitem__(__key, __value)
        else:
            super().__setitem__(__key, SourceTableBlock(__value).set_key(__key))

    def validate(self, config: TConfig):
        for k, v in self.items():
            v.validate(config)


class TargetTableBlockColumns(BlockValue[RecordValue]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class TargetTableBlock(BlockValue[RecordValue]):
    Columns = TargetTableBlockColumns

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def columns(self) -> TargetTableBlockColumns:
        return self['COLUMNS']

    @property
    def dsn_params(self) -> BlockValue[RecordValue[str]]:
        return self.get('DSN_PARAMS') or BlockValue()


class TargetsBlock(BlockValue[TargetTableBlock]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for k, v in super().items():
            self[k] = v
    
    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, TargetsBlock):
            return False
        return super().__eq__(__value)

    def __setitem__(self, __key: Any, __value: Any) -> None:
        if isinstance(__value, TargetTableBlock):
            super().__setitem__(__key, __value)
        else:
            super().__setitem__(__key, TargetTableBlock(__value).set_key(__key))


class Config(dict):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # update each item through the __setitem__ method
        for k, v in self.items():
            self[k] = v

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, Config):
            return False
        for k, v in self.items():
            if k not in __value:
                return False
            if __value[k] != v:
                return False
        return True

    def __setitem__(self, __key: Any, __value: Any) -> None:
        if __key == "SOURCES":
            super().__setitem__(__key, SourcesBlock(__value).set_key(__key))
        elif __key == "TARGETS":
            super().__setitem__(__key, TargetsBlock(__value).set_key(__key))
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

            s += to_str(value) + '\n'

            s += '\n'
        
        return s
    
    def validate(self):
        if "SOURCES" not in self: raise ValidationError("Missing SOURCES")
        self.sources.validate(self)
        if "TARGETS" not in self: raise ValidationError("Missing TARGETS")


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

id_chars = pp.alphas + pp.nums + '_'
target_schema_name = pp.Word(id_chars).set_name("target_schema_name")
target_table_name = pp.Word(id_chars).set_name("target_table_name")
target_table_parser = (pp.Opt(target_schema_name + pp.Suppress(".")) + target_table_name).set_name("target_table")

def parse_target_table_pointer(s: str) -> TargetTablePointer:
    global target_table_parser
    toks = target_table_parser.parse_string(s)
    if len(toks) == 2:
        return TargetTablePointer(toks[1], toks[0])
    else:
        return TargetTablePointer(toks[0])

source_column_function_parser: pp.ParserElement | None = None

def parse_source_column_function(s: str):
    # example "customer_id WITH customers(id) FROM ROW(old_id, @value)"
    global target_table_parser
    # create source column parser if not created
    global source_column_function_parser
    if not source_column_function_parser:
        
        target_column_name = pp.Word(id_chars).set_name("target_column_name")
        
        target_column_pointer = (
            (target_schema_name + pp.Suppress(".") + target_table_name + pp.Suppress(".") + target_column_name) |\
            (target_table_name + pp.Suppress(".") + target_column_name) |\
            (target_column_name)
        ).set_name("target_column_pointer")
        def target_column_pointer_parse_action(toks: pp.ParseResults):
            if len(toks) == 3:
                return TargetColumnPointer(toks[2], TargetTablePointer(toks[1], toks[0]))
            elif len(toks) == 2:
                # target column name and target table name
                return TargetColumnPointer(toks[1], TargetTablePointer(toks[0]))
            else:
                # target column name only
                return TargetColumnPointer(toks[0], TargetTablePointer(""))
        target_column_pointer.add_parse_action(target_column_pointer_parse_action)

        # source column's value
        # FIXME: consider renaming this
        source_value_pointer = (pp.Suppress('@') + pp.oneOf(TargetRowPointer.match_directives.keys())).set_name("source_value_pointer")
        
        target_row_pointer = (pp.Suppress(pp.Keyword("ROW")) + pp.Suppress("(") + target_column_pointer + pp.Suppress(",") + source_value_pointer + pp.Suppress(")")).set_name("row_pointer")
        def target_row_pointer_parse_action(toks: pp.ParseResults):
            return TargetRowPointer(select_column=toks[0], match_directive=toks[1])
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
    
        source_column_function_parser = (map_function | target_column_pointer) + (pp.StringEnd() | pp.LineEnd())
    
    return source_column_function_parser.parse_string(s)[0]


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

    misc_record = record(identifier.copy().set_name('record_key'), pp.Word(pp.printables + ' '))

    misc_block_recursive = pp.Forward()
    misc_block = block(identifier.copy().set_name('block_key'), misc_block_recursive | misc_record)
    misc_block_recursive <<= misc_block

    access_transform_spec = pp.ZeroOrMore(misc_record | misc_block)
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
    parser = argparse.ArgumentParser(prog="accex")
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

