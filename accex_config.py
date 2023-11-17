import pyparsing as pp
from typing import Any, Callable, TypeVar
from collections import Counter

class RecordValue(str):
    def __new__(self, s):
        return super().__new__(self, s)

class BlockValue(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def __str__(self):
        return self.to_str(4)
    
    def to_str(self, indents: int = 4) -> str:
        prev_space = ''.join([' ' for _ in range(max(0, indents - 4))])
        space = ''.join([' ' for _ in range(indents)])
        s = '{\n' + '\n'.join(
            [
                f'{space}{key}{':' if not isinstance(self[key], BlockValue) else ''}'\
                + (' ' if isinstance(self[key], BlockValue) and not isinstance(self[key], SourceTableBlock) else '')\
                + f'{self[key].to_str(indents + 4) if isinstance(self[key], BlockValue) else str(self[key])}' for key in self.keys()
            ]
        ) + f'\n{prev_space}{'}'}'
        return s

# contains database, target, and columns
class SourceTableBlock(BlockValue):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    @property
    def database(self) -> str:
        return self['database']
    
    @property
    def target(self) -> str:
        return self['target']

    @property
    def columns(self) -> dict:
        return self['columns']

    def to_str(self, indents: int = 4) -> str:
        prev_space = ''.join([' ' for _ in range(max(0, indents - 4))])
        space = ''.join([' ' for _ in range(indents)])
        columns: dict = self['columns']
        s = f': {self['database']} > {self['target']} {'{'}\n' + '\n'.join(
            [f'{space}{key}: {columns[key]}' for key in columns.keys()]
        ) + f'\n{prev_space}{'}'}'
        return s

class SourcesBlock(BlockValue):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def __getitem__(self, __key: Any) -> SourceTableBlock:
        return super().__getitem__(__key)

class TargetTableBlock(BlockValue):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

class TargetsBlock(BlockValue):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def __getitem__(self, __key: Any) -> TargetTableBlock:
        return super().__getitem__(__key)
    
class Config(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def sources(self) -> SourcesBlock:
        return self['sources']

    @property
    def targets(self) -> TargetsBlock:
        return self['targets']

    @property
    def source_dsn(self) -> str:
        return self['source_dsn']


# pp.ParserElement.set_default_whitespace_chars(' \t')
def parse_config(config_text: str) -> Config:

    id_chars = pp.alphas + pp.nums + '_'
    type_chars = pp.alphas + pp.nums + '()'
    path_chars = pp.identbodychars + '-./\\'
    identifier = pp.Word(id_chars).set_name('identifier')
    column_name = identifier.copy().set_name('column_name')
    column_type = pp.Word(type_chars + ' ').set_name('column_type')

    TKey = TypeVar('TKey', str, pp.ParserElement)

    def record(key: TKey, value: str | pp.ParserElement, key_name: str | Callable[[TKey], str] = None):
        r = key + pp.Suppress(':') + value + pp.Suppress(pp.White(ws='\n'))
        r.add_parse_action(lambda toks: (key_name if key_name is str else (key_name(toks[0]) if callable(key_name) else toks[0]), RecordValue(toks[1])))
        return r
    
    def block(key: str | pp.ParserElement, item: str | pp.ParserElement, parse_action: pp.ParseAction = None, key_name: str = None):
        t = pp.Group(key).set_name('block_key') + pp.Suppress('{').set_name('{') + pp.Group(pp.OneOrMore(item)).set_name('block_items') + pp.Suppress('}').set_name('}')
        t.add_parse_action(parse_action if parse_action is not None else lambda toks: (key_name or toks[0][0], BlockValue(toks[1].as_list())))
        return t

    target_column_record = record(column_name, column_type)
    target_table_name = identifier.copy().set_name('target_table_name')
    target_table = block(target_table_name, target_column_record)
    target_tables = block('TARGETS', target_table, key_name='targets')
    target_tables.add_parse_action(lambda toks: (toks[0][0], TargetsBlock(toks[0][1])))

    # source tables
    source_column_name = identifier.copy().set_name('source_column_name')
    source_column_target = identifier.copy().set_name('source_column_target')
    source_column_record = record(source_column_name, source_column_target)
    source_table_record = identifier + pp.Suppress(':') + pp.Word(path_chars + ' ') + pp.Suppress('>') + target_table_name
    source_table = block(source_table_record, source_column_record, lambda toks: (toks[0][0], SourceTableBlock(database=toks[0][1].strip(), target=toks[0][2], columns=dict(toks[1].as_list()))))
    source_tables = block('SOURCES', source_table, key_name='sources')
    source_tables.add_parse_action(lambda toks: (toks[0][0], SourcesBlock(toks[0][1])))
    # source_tables.add_parse_action(lambda toks: [('sources', toks[0][1])])

    source_connection_string = record('SOURCE_CONNECTION_STRING', pp.Word(pp.printables + ' '), key_name='source_connection_string')

    misc_record = record(identifier.copy(), pp.Word(pp.printables + ' '), key_name=lambda key: str(key).lower())

    access_transform_spec = pp.ZeroOrMore(target_tables | source_tables | misc_record)
    access_transform_spec.set_parse_action(lambda toks: Config(toks.as_list()))

    return access_transform_spec.parse_string(config_text)[0]

def parse_config_file(config_file_path: str) -> Config:
    config_file = open(config_file_path, 'r')
    parsed_config = parse_config(config_file.read())
    config_file.close()
    return parsed_config

def write_config(config: Config) -> str:
    config_str: str = ''
    
    for key in config.keys():
        
        value = config[key]

        if isinstance(value, RecordValue):
        
            config_str += key.upper() + ': ' + value + '\n'
        
        elif isinstance(value, TargetsBlock):

            config_str += key.upper() + ' ' + str(value) + '\n'

        elif isinstance(value, SourcesBlock):

            config_str += key.upper() + ' ' + str(value) + '\n'

        config_str += '\n'
    
    return config_str

def write_config_file(config: Config, config_file_path: str):
    config_str = write_config(config)

    with open(config_file_path, 'w') as file:
        file.write(config_str)

if __name__ == '__main__':

    print('------ testing accex config parser')

    with open('./config.accex', 'r') as file:

        parsed_config = parse_config(file.read())

    print('------ intitial parsed config\n', parsed_config)

    out_config = write_config(parsed_config)

    print('------ written config\n', out_config)

    print('------ reparsing written config')

    parsed_config_2 = parse_config(out_config)

    print('------ reparsed config\n', parsed_config_2)

    print('------ asserting equality')

    assert(Counter(parsed_config) == Counter(parsed_config_2))

    print('------ assert successful')

    print('------ writing config to file')

    write_config_file(parsed_config_2, 'out_config.accex')

    print('------ test finished')
