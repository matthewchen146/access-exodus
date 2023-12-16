import os, sys, argparse, logging
import pyparsing as pp
from typing import Any, TypeVar, Generic, Self
import dotenv
import yaml

class ValidationError(Exception):
    """Raised during config validation from config.validate()"""
    pass

_TConfig = TypeVar("_TConfig", bound="Config")

source_default_catalog = ""

target_default_schema = "public"

def get_target_default_schema() -> str:
    """Gets the default schema name for target tables, set in target_default_schema"""
    return target_default_schema

_TSerializeableContext = TypeVar("_TSerializeableContext")

class Serializeable(Generic[_TSerializeableContext]):
    def __serial_repr__(self, dumper: yaml.Dumper, context: _TSerializeableContext) -> object:
        if isinstance(self, dict):
            return dumper.represent_dict(self)
        elif isinstance(self, list):
            return dumper.represent_list(self)
        return dumper.represent_str(f"unhandled {self.__class__.__name__}")
        # raise TypeError(f"{self.__class__} is not Serializeable")

class SourceTablePointer(Serializeable[_TConfig]):

    @classmethod
    def from_str(self, s: str) -> Self:
        return source_table_pointer.parse_string(s)[0]

    def __init__(self, obj: object = None, /, *, table_name: str = "", catalog_name: str = source_default_catalog) -> None:
        self.catalog_name = ""
        self.table_name = ""

        if obj:
            if isinstance(obj, str):
                self.set_from_pointer(SourceTablePointer.from_str(obj))
            elif isinstance(obj, SourceTablePointer):
                self.set_from_pointer(obj)
            else:
                raise ValueError(f"{self.__class__.__name__} cannot be initialized with {obj}")

        self.catalog_name = self.catalog_name or catalog_name
        self.table_name = self.table_name or table_name
    
    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, SourceTablePointer) and \
            __value.catalog_name == self.catalog_name and \
            __value.table_name == self.table_name

    def __str__(self) -> str:
        return f"{self.catalog_name and self.catalog_name + '.' or ''}{self.table_name}"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.__str__()})"

    def __serial_repr__(self, dumper, context) -> object:
        return dumper.represent_str(self.__str__())

    def set_from_pointer(self, p: Self) -> None:
        self.catalog_name = p.catalog_name
        self.table_name = p.table_name

    def validate(self, config: _TConfig):
        if self.catalog_name != source_default_catalog:
            if self.catalog_name not in config.source_databases:
                raise ValidationError(f"Source catalog \"{self.catalog_name}\" not defined")

class TargetTablePointer(Serializeable[_TConfig]):
    """_summary_
    """

    @classmethod
    def from_str(self, s: str) -> Self:
        return target_table_pointer.parse_string(s)[0]

    def __init__(self, obj: object = None, /, *, table_name: str = "", schema_name: str = get_target_default_schema(), catalog_name: str = "") -> None:
        self.catalog_name = ""
        self.schema_name = ""
        self.table_name = ""

        if obj is None:
            pass
            # if not table_name:
            #     raise ValueError(f"TargetTablePointer requires table_name at least if no positional arguments")
        else:
            if isinstance(obj, str):
                self.set_from_pointer(parse_target_table_pointer(obj))
            elif isinstance(obj, TargetTableBlock):
                self.set_from_pointer(parse_target_table_pointer(obj.name))
            elif isinstance(obj, tuple):
                self.set_from_pointer(parse_target_table_pointer(".".join(obj)))
            else:
                raise ValueError(f"{self.__class__.__name__} cannot be initialized with {obj}")

        self.catalog_name = self.catalog_name or catalog_name
        self.schema_name = self.schema_name or schema_name
        self.table_name = self.table_name or table_name
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.to_sql_str()})"

    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, TargetTablePointer) and self.schema_name == __value.schema_name and self.table_name == __value.table_name

    def __hash__(self) -> int:
        return hash((self.catalog_name, self.schema_name, self.table_name))

    def __str__(self) -> str:
        return self.to_sql_str()
    
    def __serial_repr__(self, dumper, context: _TConfig) -> object:
        return dumper.represent_str(self.to_sql_str())

    def to_sql_str(self, with_schema: bool = True) -> str:
        return ((with_schema and self.schema_name and f"{self.schema_name}.") or "") + (self.table_name or "?")
    
    def set_from_pointer(self, p: Self) -> None:
        if not isinstance(p, TargetTablePointer):
            raise ValueError(f"from_pointer requires a TargetTablePointer, got {p}")
        self.catalog_name = p.catalog_name
        self.schema_name = p.schema_name
        self.table_name = p.table_name

    def get_columns_block(self, config: _TConfig) -> dict:
        cols = config.targets[self.table_name].columns
        return cols

    def validate(self, config: _TConfig):
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

class TargetColumnPointer(Serializeable[_TConfig]):
    def __init__(self, column_name: str, table_name: str, schema_name: str = get_target_default_schema()) -> None:
        self.table = TargetTablePointer(table_name, schema_name)
        self.column_name = column_name
    
    def __init__(self, column_name: str, table: TargetTablePointer) -> None:
        self.table = table
        self.column_name = column_name
    
    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, TargetColumnPointer) and self.table == __value.table and self.column_name == __value.column_name

    def __str__(self) -> str:
        return f"{self.table.to_sql_str()}.{self.column_name}"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.__str__()})"

    def __serial_repr__(self, dumper, context) -> object:
        # return dumper.represent_str(self.__str__())
        return dumper.represent_str(self.column_name)

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

    def validate(self, config: _TConfig):
        if self.table not in config.targets:
            raise ValidationError(f"{self.table} does not exist in config TARGETS")
        tgt_table_cols = self.table.get_columns_block(config)
        # assert column name in target table
        if self.column_name not in tgt_table_cols:
            raise ValidationError(f'{self.table} does not have a column named \"{self.column_name}\"')

class TargetRowPointer(Serializeable[_TConfig]):
    match_directives = {
        "value": True
    }

    def __init__(self, select_column: TargetColumnPointer, match_directive: str) -> None:
        self.select_column = select_column
        self.match_directive = match_directive

    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, TargetRowPointer) and self.select_column == __value.select_column and self.match_directive == __value.match_directive

    def __str__(self) -> str:
        return f"ROW({str(self.select_column)}, @{self.match_directive})"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({str(self.select_column)}, match {self.match_directive})"
    
    def __serial_repr__(self, dumper, context) -> object:
        return dumper.represent_str(self.__str__())

    def validate(self, config: _TConfig):
        self.select_column.validate(config)
        if self.match_directive not in TargetRowPointer.match_directives:
            raise ValidationError(f"Invalid match directive @{self.match_directive}, must be one of [{', '.join(k for k in TargetRowPointer.match_directives)}]")

class SourceColumnMapFunction(Serializeable[_TConfig]):
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
        return f"{str(self.to_column)} WITH {str(self.with_column)} FROM {str(self.from_row)}"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.__str__()})"

    def __serial_repr__(self, dumper, context) -> object:
        return dumper.represent_str(self.__str__())

    @property
    def table(self) -> TargetTablePointer:
        return self.to_column.table
    
    def validate(self, config: _TConfig):
        self.with_column.validate(config)
        self.from_row.validate(config)
        self.to_column.validate(config)


class SourceTableBlockColumns(dict[str, TargetColumnPointer | SourceColumnMapFunction], Serializeable[_TConfig]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._target_table: TargetTablePointer | None = None
        self.target_table_deps = set()
        for k, v in self.items():
            self[k] = v

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, SourceTableBlockColumns):
            return False
        return super().__eq__(__value)

    def __setitem__(self, __key: Any, __value: Any) -> None:
        # parse value of each record
        __value = self.process_column(__value)
        super().__setitem__(__key, __value)

    def process_column(self, v: Any) -> Any:
        if isinstance(v, TargetColumnPointer):
            v.table = self._target_table
        elif isinstance(v, SourceColumnMapFunction):
            v.to_column.table = self._target_table
            self.target_table_deps.add(self._target_table)
        else:
            parsed_source_column_value = parse_source_column_function(v)
            v = parsed_source_column_value
            # check dependent tables
            if isinstance(v, SourceColumnMapFunction):
                v.to_column.table = self._target_table
                self.target_table_deps.add(v.with_column.table)
            elif isinstance(v, TargetColumnPointer):
                v.table = self._target_table
        return v

    def process_columns(self) -> None:
        self.target_table_deps.clear()
        for v in self.values():
            self.process_column(v)
    
    @property
    def target_table(self) -> TargetTablePointer | None:
        return self._target_table
    
    @target_table.setter
    def target_table(self, value: Any) -> None:
        self._target_table = value
        self.process_columns()

class SourceDatabase(dict[str, object], Serializeable[_TConfig]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def dsn_params(self) -> dict[str, str]:
        return self.get("DSN_PARAMS") or dict()

    def validate(self, config: _TConfig):
        pass

class SourceDatabases(dict[str, SourceDatabase], Serializeable[_TConfig]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for k, v in self.items():
            self[k] = v
    
    def __setitem__(self, __key: str, __value: object) -> None:
        if not isinstance(__value, SourceDatabase):
            __value = SourceDatabase(__value)
        return super().__setitem__(__key, __value)

    def validate(self, config: _TConfig):
        for k, v in self.items():
            v.validate(config)

# contains database, target, and columns
class SourceTableBlock(dict[str, object], Serializeable[_TConfig]):
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
        if __key == "TABLE":
            if not isinstance(__value, SourceTablePointer):
                __value = SourceTablePointer(__value)
            super().__setitem__(__key, __value)
        elif __key == "COLUMNS":
            if not isinstance(__value, SourceTableBlockColumns):
                __value: SourceTableBlockColumns = SourceTableBlockColumns(__value)
                if self.target_pointer:
                    __value.target_table = self.target_pointer
                    __value.process_columns()
            super().__setitem__(__key, __value)
        elif __key == "TARGET_TABLE":
            if isinstance(__value, TargetTablePointer):
                super().__setitem__(__key, __value)
            else:
                # assuming string
                __value = parse_target_table_pointer(__value)
                super().__setitem__(__key, __value)
            # fix columns
            if "COLUMNS" in self:
                c = self["COLUMNS"]
                if not isinstance(c, SourceTableBlockColumns):
                    # c = SourceTableBlockColumns(__value, self.target_table)
                    self["COLUMNS"] = c
                else:
                    c.target_table = self.target_pointer
                    c.process_columns()
        elif __key == "DSN_PARAMS":
            super().__setitem__(__key, __value if isinstance(__value, dict) else dict(__value))
        else:
            super().__setitem__(__key, __value)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(table={self.table_pointer} ,target={self.target_pointer})"

    def __serial_repr__(self, dumper: yaml.Dumper, context: _TConfig) -> object:
        return dumper.represent_dict(sort_dict_with_list(self, ["TABLE", "TARGET_TABLE", "DSN_PARAMS", "COLUMNS"]))

    @property
    def table_pointer(self) -> SourceTablePointer:
        return self["TABLE"]

    @property
    def target_pointer(self) -> TargetTablePointer | None:
        return self.get("TARGET_TABLE")

    @property
    def columns(self) -> SourceTableBlockColumns:
        return self['COLUMNS']
    
    @property
    def dsn_params(self) -> dict[str, str]:
        return self.get('DSN_PARAMS') or dict()

    @property
    def target_table_deps(self) -> set[TargetTablePointer]:
        return self.columns.target_table_deps

    def validate(self, config: _TConfig):
        if "TABLE" not in self: raise ValidationError("Missing TABLE")
        self.table_pointer.validate(config)

        if "COLUMNS" not in self: raise ValidationError("Missing COLUMNS")
        for column in self.columns.values():
            if isinstance(column, TargetColumnPointer):
                column.validate(config)
            elif isinstance(column, SourceColumnMapFunction):
                column.validate(config)
            else:
                raise ValidationError(f"Column should be a TargetColumnPointer or Function, got {column}")
        if "TARGET_TABLE" in self:
            r = self["TARGET_TABLE"]
            if not isinstance(r, TargetTablePointer): raise ValidationError(f"TARGET_TABLE should be a TargetTablePointer, got {r}")
            r.validate(config)

class SourcesBlock(list[SourceTableBlock], Serializeable[_TConfig]):
    def __init__(self, *args, **kwargs):
        largs = list(args)
        for i in range(len(largs)):
            a = largs[i]
            if isinstance(a, list):
                na = a.copy()
                for j in range(len(na)):
                    if not isinstance(na[j], SourceTableBlock):
                        na[j] = SourceTableBlock(na[j])
                largs[i] = na
        args = tuple(largs)
        super().__init__(*args, **kwargs)

    def __getitem__(self, index: object) -> SourceTableBlock:
        if not isinstance(index, int):
            if isinstance(index, str):
                index = SourceTablePointer.from_str(index)
            if isinstance(index, SourceTablePointer):
                i = 0
                for v in self:
                    if v.table_pointer == index:
                        index = i
                        break 
                    i += 1
        return super().__getitem__(index)

    def __contains__(self, __key: object) -> bool:
        if not isinstance(__key, SourceTablePointer):
            __key = SourceTablePointer(__key)
        if isinstance(__key, SourceTablePointer):
            for v in self:
                if v.table_pointer == __key:
                    return True
            return False
        return super().__contains__(__key)

    def append(self, __object: object) -> None:
        if not isinstance(__object, SourcesBlock):
            __object = SourcesBlock(__object)
        return super().append(__object)

    def validate(self, config: _TConfig):
        for v in self:
            v.validate(config)




class TargetTableBlockColumns(dict[str, str], Serializeable[_TConfig]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class TargetTableBlock(dict[str, str | dict], Serializeable[_TConfig]):
    Columns = TargetTableBlockColumns

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __hash__(self) -> int:
        return hash(parse_target_table_pointer(self.name))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(table={self.name}, columns={self.columns and self.columns.keys()})"

    @property
    def name(self) -> str:
        return self["TABLE"]

    @property
    def columns(self) -> TargetTableBlockColumns:
        return self.get('COLUMNS')

    @property
    def dsn_params(self) -> dict[str, str]:
        return self.get('DSN_PARAMS') or dict()
    
    def validate(self, config: _TConfig):
        if "TABLE" not in self:
            raise ValidationError("Missing TABLE")
        if "COLUMNS" not in self:
            raise ValidationError("Missing COLUMNS")


class TargetsBlock(dict[TargetTablePointer, TargetTableBlock], Serializeable[_TConfig]):
    def __init__(self, *args, **kwargs):
        largs = list(args)
        for i in range(len(largs)):
            a = largs[i]
            if isinstance(a, list):
                na = a.copy()
                keys = set()
                for j in range(len(na)):
                    if not isinstance(na[j], tuple):
                        t = na[j]
                        if not isinstance(t, TargetTableBlock):
                            t = TargetTableBlock(t)
                        k = self.create_key(t)
                        if k in keys:
                            raise ValueError(f"{self.__class__.__name__} cannot have duplicate tables, such as {k}")
                        keys.add(k)
                        na[j] = (k, t)
                largs[i] = na
            else:
                raise ValueError(f"{self.__class__.__name__} cannot be initialized with {args[i]}")
        args = tuple(largs)
        super().__init__(*args, **kwargs)
    
    def __setitem__(self, __key: object, __value: object) -> None:
        if not isinstance(__value, TargetTableBlock):
            __value = TargetTableBlock(__value)
        __key = self.create_key(__value)
        return super().__setitem__(__key, __value)

    def __getitem__(self, __key: object) -> TargetTableBlock:
        return super().__getitem__(self.create_key(__key))

    def __contains__(self, __key: object) -> bool:
        return super().__contains__(self.create_key(__key))

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, TargetsBlock):
            return False
        return super().__eq__(__value)

    def __serial_repr__(self, dumper, context: _TConfig) -> object:
        return dumper.represent_list(list(self.values()))

    def add(self, __element: object) -> None:
        self.__setitem__("", __element)

    def create_key(self, __key: object) -> TargetTablePointer:
        
        if not isinstance(__key, TargetTablePointer):
            if isinstance(__key, str):
                __key = TargetTablePointer.from_str(__key)
            else:
                __key = TargetTablePointer(__key)
        return __key
    
    def validate(self, config: _TConfig):
        for k, v in self.items():
            # validate key type
            if not isinstance(k, TargetTablePointer):
                raise ValidationError(f"Target {k} must be a {TargetTablePointer.__name__}")
            v.validate(config)


class Config(dict, Serializeable[_TConfig]):

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
            __value = SourcesBlock(__value)
        elif __key == "SOURCE_DATABASES":
            __value = SourceDatabases(__value)
        elif __key == "TARGETS":
            __value = TargetsBlock(__value)
        super().__setitem__(__key, __value)

    # def __serial_repr__(self, context: _TConfig) -> object:
    #     return super().__serial_repr__(context)

    @property
    def sources(self) -> SourcesBlock:
        return self["SOURCES"]
    @sources.setter
    def sources(self, value) -> None:
        self["SOURCES"] = value

    @property
    def source_databases(self) -> SourceDatabases:
        return self.get("SOURCE_DATABASES") or SourceDatabases()
    @source_databases.setter
    def source_databases(self, value) -> None:
        self["SOURCE_DATABASES"] = value

    @property
    def targets(self) -> TargetsBlock:
        return self["TARGETS"]
    @targets.setter
    def targets(self, value) -> None:
        self["TARGETS"] = value

    @property
    def target_dsn_params(self) -> dict[str, str]:
        return self["TARGET_DSN_PARAMS"]
    @target_dsn_params.setter
    def target_dsn_params(self, value) -> None:
        self["TARGET_DSN_PARAMS"] = value
    
    @property
    def source_dsn_params(self) -> dict[str, str]:
        return self["SOURCE_DSN_PARAMS"]
    @source_dsn_params.setter
    def source_dsn_params(self, value) -> None:
        self["SOURCE_DSN_PARAMS"] = value       
    
    def get_source_dsn_params_with_catalog(self, catalog_name: str) -> dict[str, str]:
        # SOURCE_DSN_PARAMS + defined source database DSN_PARAMS + ( FIXME: source table DSN_PARAMS)
        return {**self.source_dsn_params, **((catalog_name and catalog_name != source_default_catalog and self.source_databases[catalog_name].dsn_params) or dict())}

    def validate(self):
        if "TARGETS" not in self: raise ValidationError("Missing TARGETS")
        self.targets.validate(self)
        # if "SOURCE_DATABASES" not in self: raise ValidationError("Missing SOURCE_DATABASES")
        self.source_databases.validate(self)
        if "SOURCES" not in self: raise ValidationError("Missing SOURCES")
        self.sources.validate(self)

def sort_dict_with_list(d: dict, l: list) -> dict:
    order_map = dict((l[i], i) for i in range(len(l)))
    return dict(sorted([i for i in d.items()], key=lambda a: order_map.get(a[0]) or 0))

def _remove_comments(s: str) -> str:
    import re
    pattern = r"(?<!\\)#.*"
    s = re.sub(pattern, '', s)
    return re.sub(r"\\#", '#', s)

def _replace_env_vars(s: str) -> str:
    import re
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

source_catalog_name = pp.Word(id_chars).set_name("source_catalog_name")
source_table_name = pp.Word(id_chars).set_name("source_table_name")
source_table_pointer = (
    (source_catalog_name + pp.Suppress(".") + source_table_name) |\
    (source_table_name)
)
def _source_table_pointer_parse_action(toks: pp.ParseResults) -> SourceTablePointer:
    if len(toks) == 2:
        return SourceTablePointer(table_name=toks[1], catalog_name=toks[0])
    else:
        return SourceTablePointer(table_name=toks[0])
source_table_pointer.add_parse_action(_source_table_pointer_parse_action)

target_catalog_name = pp.Word(id_chars).set_name("target_catalog_name")
target_schema_name = pp.Word(id_chars).set_name("target_schema_name")
target_table_name = pp.Word(id_chars).set_name("target_table_name")
target_column_name = pp.Word(id_chars).set_name("target_column_name")
target_table_pointer = (
    (target_catalog_name + pp.Suppress(".") + target_schema_name + pp.Suppress(".") + target_table_name) |\
    (target_schema_name + pp.Suppress(".") + target_table_name) |\
    (target_table_name)
).set_name("target_table_pointer")
def _target_table_pointer_parse_action(toks: pp.ParseResults) -> TargetTablePointer:
    if len(toks) == 3:
        return TargetTablePointer(table_name=toks[2], schema_name=toks[1], catalog_name=toks[0])
    if len(toks) == 2:
        return TargetTablePointer(table_name=toks[1], schema_name=toks[0])
    else:
        return TargetTablePointer(table_name=toks[0])
target_table_pointer.add_parse_action(_target_table_pointer_parse_action)
target_column_pointer = (
    (target_schema_name + pp.Suppress(".") + target_table_name + pp.Suppress(".") + target_column_name) |\
    (target_table_name + pp.Suppress(".") + target_column_name) |\
    (target_column_name)
).set_name("target_column_pointer")
def target_column_pointer_parse_action(toks: pp.ParseResults):
    if len(toks) == 3:
        return TargetColumnPointer(toks[2], TargetTablePointer(table_name=toks[1], schema_name=toks[0]))
    elif len(toks) == 2:
        # target column name and target table name
        return TargetColumnPointer(toks[1], TargetTablePointer(table_name=toks[0]))
    elif len(toks) == 1:
        # target column name only
        return TargetColumnPointer(toks[0], TargetTablePointer(table_name=""))
target_column_pointer.add_parse_action(target_column_pointer_parse_action)

def parse_target_table_pointer(s: str) -> TargetTablePointer:
    global target_table_pointer
    return target_table_pointer.parse_string(s)[0]

def _create_source_column_function_parser() -> pp.ParserElement:
    global target_column_pointer

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

    return (map_function | target_column_pointer) + (pp.StringEnd() | pp.LineEnd())
_source_column_function_parser: pp.ParserElement = _create_source_column_function_parser()

def parse_source_column_function(s: str):
    global _source_column_function_parser
    return _source_column_function_parser.parse_string(s)[0]


def parse_config(config_text: str) -> Config:

    config_text = _remove_comments(config_text)

    dotenv.load_dotenv(override=True)

    config_text = _replace_env_vars(config_text)

    config = Config(yaml.load(config_text, yaml.Loader))
    return config

def parse_config_file(config_file_path: str) -> Config:
    config_file = open(config_file_path, 'r')
    s = config_file.read()
    config_file.close()
    parsed_config = parse_config(s)
    return parsed_config


def create_dumper(config: Config):
    import inspect
    class CustomDumper(yaml.Dumper):
        pass
    serializeable_classes = inspect.getmembers(
        inspect.getmodule(Serializeable),
        lambda member: inspect.isclass(member) and issubclass(member, Serializeable)
    )
    for _, c in serializeable_classes:
        if issubclass(c, Serializeable):
            CustomDumper.add_representer(c, lambda dumper, data: data.__serial_repr__(dumper, config))
    return CustomDumper

def write_config(config: Config) -> str:
    """Writes config to a string

    :param config: config
    :type config: Config
    :return: the config as a string
    :rtype: str
    """
    config_str = yaml.dump(config, default_flow_style=False, width=float("inf"), sort_keys=False, Dumper=create_dumper(config))
    return config_str

def write_config_file(config: Config, config_file_path: str):
    """Writes config to a file

    :param config: config
    :type config: Config
    :param config_file_path: file path
    :type config_file_path: str
    """
    config_str = write_config(config)

    with open(config_file_path, 'w') as file:
        file.write(config_str)

def find_config_path() -> str | None:
    extension = ".accex"
    for file_name in os.listdir():
        if file_name.endswith(extension):
            return os.path.abspath(file_name)
    return None

def resolve_config_path(config_path_arg: str = "") -> str | None:
    config_path = None
    if config_path_arg:
        if os.path.exists(config_path_arg):
            config_path = os.path.abspath(config_path_arg)
        else:
            raise ValueError(f"[{config_path_arg}] does not exist")
    if not config_path:
        config_path = find_config_path()
    return config_path

def populate_arg_parser(parser: argparse.ArgumentParser, main: bool = False) -> argparse.ArgumentParser:
    parser.add_argument(
        "config_path",
        type=str,
        help="path to a config file",
        nargs="?"
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="log level for logging"
    )
    if main:
        parser.add_argument(
            "--validate",
            action="store_true",
            help="parse and validate the config"
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
    return parser

def _main():
    import json
    logger = logging.getLogger("config")

    arg_parser = argparse.ArgumentParser(prog="accex")
    populate_arg_parser(arg_parser, True)
    args = arg_parser.parse_args()
    logging.basicConfig(level=logging.getLevelNamesMapping()[args.log_level])
    config_path = resolve_config_path(args.config_path)
    if not config_path:
        logger.info("no config file specified and no config file found")
        sys.exit(1)
    config = parse_config_file(config_path)
    config.validate()
    out = ''
    if args.json:
        if args.json_format:
            out = json.dumps(yaml.load(write_config(config), Loader=yaml.Loader), indent=2)
            # out = json.dumps(config, indent=4)
        else:
            out = json.dumps(yaml.load(write_config(config), Loader=yaml.Loader))
    else:
        out = write_config(config)

    if args.out_file:
        with open(args.out_file, 'w') as file:
            file.write(out)
    else:
        print(out)
    
    if args.validate:
        config.validate()