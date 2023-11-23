from collections.abc import Iterator
import pyparsing as pp
from typing import Any, Callable, TypeVar, ItemsView
from collections import Counter
import json

def to_str(t: Any) -> str:
    return t.to_str() if callable(getattr(t, 'to_str', None)) else str(t) + '\n'

TRecordValue = TypeVar('TRecordValue', bound='RecordValue')

class RecordValue(str):       

    def __new__(self, s):
        self.key = ''
        return super().__new__(self, s)

    # def __str__(self) -> str:
    #     return self.to_str()
    
    def to_str(self) -> str:
        return f'{self.key}: ' + super().__str__()
    
    @property
    def value(self):
        return super().__str__()

    def set_key(self: TRecordValue, key: str) -> TRecordValue:
        self.key = key
        return self

TBlockValue = TypeVar('TBlockValue', bound='BlockValue')

class BlockValue(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.key = ''
    
    def __str__(self):
        return self.to_str(4)
    
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

# contains database, target, and columns
class SourceTableBlock(BlockValue):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    @property
    def database(self) -> str:
        return self['database']
    
    @property
    def target(self) -> str:
        return self['TARGET_TABLE']

    @property
    def columns(self) -> dict:
        return self['COLUMNS']

    @property
    def dsn_params(self) -> dict:
        return self.get('DSN_PARAMS') or dict()

    def to_str(self, indents: int = 4) -> str:
        prev_space = ''.join([' ' for _ in range(max(0, indents - 4))])
        space = ''.join([' ' for _ in range(indents)])
        columns: dict = self['columns']
        print('### converting SourceTables to string')
        print('### columns', columns)
        s = f"{self.key}: {self['database']} > {self['target']} {'{'}\n" + "\n".join(
            [f"{space}{columns[column_name]}" for column_name in columns.keys()]
        ) + f"\n{prev_space}{'}'}"
        print('### output tables', s)
        return s

class SourcesBlock(BlockValue):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def __getitem__(self, __key: Any) -> SourceTableBlock:
        return SourceTableBlock(super().__getitem__(__key))

    def items(self) -> ItemsView[str, SourceTableBlock]:
        return [(k, self.__getitem__(k)) for k in super().keys()]

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
    
    def __getitem__(self, __key: Any) -> TargetTableBlock:
        return TargetTableBlock(super().__getitem__(__key))
    
class Config(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def sources(self) -> SourcesBlock:
        return SourcesBlock(self['SOURCES'])

    @property
    def targets(self) -> TargetsBlock:
        return TargetsBlock(self['TARGETS'])

    @property
    def source_dsn(self) -> str:
        return self['SOURCE_DSN'].value

    @property
    def target_dsn_params(self) -> dict:
        return self['TARGET_DSN_PARAMS']
    
    @property
    def source_dsn_params(self) -> dict:
        return self['SOURCE_DSN_PARAMS']
    
    def to_str(self) -> str:
        s: str = ''
    
        for key, value in self.items():

            to_str_method = getattr(value, 'to_str', None)

            s += (value.to_str() if callable(to_str_method) else str(value)) + '\n'

            s += '\n'
        
        return s


# pp.ParserElement.set_default_whitespace_chars(' \t')
def parse_config(config_text: str) -> Config:

    id_chars = pp.alphas + pp.nums + '_'
    type_chars = pp.alphas + pp.nums + '()'
    path_chars = pp.identbodychars + '-./\\'
    identifier = pp.Word(id_chars).set_name('identifier')
    special_identifier = pp.Word(id_chars + "$").set_name('special_identifier')
    column_name = identifier.copy().set_name('column_name')
    column_type = pp.Word(type_chars + ' ').set_name('column_type')

    TKey = TypeVar('TKey', str, pp.ParserElement)

    def record(key: TKey, value: str | pp.ParserElement, key_name: str | Callable[[TKey], str] = None):
        r = key + pp.Suppress(':') + value + pp.Suppress(pp.White(ws='\n'))

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

    target_column_record = record(column_name, column_type)
    target_table_name = identifier.copy().set_name('target_table_name')
    target_table = block(target_table_name, target_column_record)
    target_tables = block('TARGETS', target_table)
    target_tables.add_parse_action(lambda toks: (toks[0][0], TargetsBlock(toks[0][1]).set_key('TARGETS')))

    # source tables
    source_column_name = identifier.copy().set_name('source_column_name')
    source_column_target = identifier.copy().set_name('source_column_target')
    source_column_record = record(source_column_name, source_column_target)
    source_table_record = identifier + pp.Suppress(':') + pp.Word(path_chars + ' ') + pp.Suppress('>') + target_table_name
    def source_table_parse_action(toks: pp.ParseResults) -> Any:
        kn = toks[0][0]
        return (kn, SourceTableBlock(database=toks[0][1].strip(), target=toks[0][2], columns=dict(toks[1].as_list())).set_key(kn))
    source_table = block(source_table_record, source_column_record, source_table_parse_action)
    source_tables = block('SOURCES', source_table)
    source_tables.add_parse_action(lambda toks: (toks[0][0], SourcesBlock(toks[0][1]).set_key('SOURCES')))
    # source_tables.set_fail_action(lambda s, loc, expr, err: print('source_tables_FAIL', expr, loc, err))
    # source_tables.add_parse_action(lambda toks: [('sources', toks[0][1])])

    misc_record = record(identifier.copy().set_name('record_key') | special_identifier.copy().set_name('special_record_key'), pp.Word(pp.printables + ' '))

    misc_block_recursive = pp.Forward()
    misc_block = block(identifier.copy().set_name('block_key') | special_identifier.copy().set_name('special_block_key'), misc_block_recursive | misc_record)
    misc_block_recursive <<= misc_block

    # comment
    comment = pp.Suppress('#') + pp.Word(pp.printables + ' ')
    comment.set_name('comment')

    # access_transform_spec = pp.ZeroOrMore(comment.suppress() | misc_record | source_tables | target_tables | misc_block)
    access_transform_spec = pp.ZeroOrMore(comment.suppress() | misc_record | target_tables | misc_block)
    # access_transform_spec.add_parse_action(lambda toks: print('\n'.join([f'count: {str(len(tok))} - {str(tok)}' for tok in toks.as_list()])))
    access_transform_spec.add_parse_action(lambda toks: Config(toks.as_list()))
    access_transform_spec.set_fail_action(lambda s, loc, expr, err: print('CONFIG_FAIL', expr, loc, err))

    return access_transform_spec.parse_string(config_text)[0]

def parse_config_file(config_file_path: str) -> Config:
    config_file = open(config_file_path, 'r')
    parsed_config = parse_config(config_file.read())
    config_file.close()
    return parsed_config

def write_config(config: Config) -> str:
    config_str: str = to_str(config)
    
    return config_str

def write_config_file(config: Config, config_file_path: str):
    config_str = write_config(config)

    with open(config_file_path, 'w') as file:
        file.write(config_str)

if __name__ == '__main__':

    print('------ testing accex config parser')

    with open('./config.accex', 'r') as file:

        parsed_config = parse_config(file.read())

    print('------ intitial parsed config\n', json.dumps(parsed_config, indent=4))

    out_config = write_config(parsed_config)

    print('------ written config\n', out_config)

    print('------ reparsing written config')

    parsed_config_2 = parse_config(out_config)

    print('------ reparsed config\n', json.dumps(parsed_config_2, indent=4))

    print('------ asserting equality')

    assert(Counter(parsed_config) == Counter(parsed_config_2))

    print('------ assert successful')

    print('------ writing config to file')

    write_config_file(parsed_config_2, 'misc/out_config.accex')

    print('------ test finished')
