import asyncio
import logging
from functools import cmp_to_key
import pyodbc
import aioodbc
from ..config import core as ac

async def _conn_attributes(conn: pyodbc.Connection):
    pass
    # postgres settings
    # conn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
    # conn.setencoding(encoding='utf-8')

_src_conn_str: str = ''
_src_conn: aioodbc.Connection | None = None
_src_cur: aioodbc.Cursor | None = None
_tgt_conn_str: str = ''
_tgt_conn: aioodbc.Connection | None = None
_tgt_cur: aioodbc.Cursor | None = None

async def open_src_connection(new_src_conn_str: str) -> aioodbc.Cursor:
    global _src_conn_str
    global _src_conn
    global _src_cur

    if _src_conn is None or new_src_conn_str != _src_conn_str:
        _src_conn_str = new_src_conn_str
        await close_src_connection()
        logging.info(f'connecting to source via connection string [{_src_conn_str}]')
        _src_conn = await aioodbc.connect(dsn=_src_conn_str)
        _src_cur = await _src_conn.cursor()
    return _src_cur

async def open_tgt_connection(new_tgt_conn_str: str) -> aioodbc.Cursor:
    global _tgt_conn_str
    global _tgt_conn
    global _tgt_cur

    if _tgt_conn is None or new_tgt_conn_str != _tgt_conn_str:
        _tgt_conn_str = new_tgt_conn_str
        await close_tgt_connection()
        logging.info(f'connecting to target via connection string [{_tgt_conn_str}]')
        _tgt_conn = await aioodbc.connect(dsn=_tgt_conn_str, after_created=_conn_attributes, autocommit=True)
        _tgt_cur = await _tgt_conn.cursor()
    return _tgt_cur

async def close_src_connection():
    global _src_conn
    global _src_cur

    if _src_cur: await _src_cur.close()
    if _src_conn: await _src_conn.close()
    _src_cur = None
    _src_conn = None

async def close_tgt_connection():
    global _tgt_conn
    global _tgt_cur

    if _tgt_cur: await _tgt_cur.close()
    if _tgt_conn: await _tgt_conn.close()
    _tgt_cur = None
    _tgt_conn = None

async def close_connections():
    await asyncio.gather(
        close_src_connection(),
        close_tgt_connection()
    )

async def get_table_name_dict(cur: aioodbc.Cursor, table_type='TABLE') -> dict:
    await cur.tables(tableType=table_type)
    return dict([(row.table_name, row) for row in await cur.fetchall()])

async def get_column_name_dict(cur: aioodbc.Cursor, table: str, schema: str = None) -> dict:
    await cur.columns(table=table, schema=schema)
    return dict([(row.column_name, row) for row in await cur.fetchall()])

def create_conn_str(params: dict) -> str:
    return ";".join([f"{k}={v}" for k, v in params.items()])

def get_src_conn_str(config: ac.Config, table_name: str = "") -> str:
    if not table_name:
        return create_conn_str(config.source_dsn_params)
    if table_name not in config.sources:
        return ""
    return create_conn_str({ **config.source_dsn_params, **config.sources[table_name].dsn_params })

def get_tgt_conn_str(config: ac.Config, table_name: str = "") -> str:
    if not table_name:
        return create_conn_str(config.target_dsn_params)
    if table_name not in config.targets:
        return ""
    return create_conn_str({ **config.target_dsn_params, **config.targets[table_name].dsn_params })

async def transfer_table(config: ac.Config, src_table_name: str, tgt_table_name: str) -> bool:
    global _src_cur
    global _tgt_cur

    try:
        src_table = config.sources[src_table_name]

        tgt_table = config.targets[tgt_table_name]
        
        src_table_columns = src_table.columns

        true_tgt_table_columns = tgt_table.columns

        src_dsn_params = {**config.source_dsn_params, **src_table.dsn_params}
        new_src_conn_str = create_conn_str(src_dsn_params)
        logging.info(f"connecting to source database")
        await open_src_connection(new_src_conn_str)
        
        tgt_dsn_params = {**config.target_dsn_params, **tgt_table.dsn_params}
        new_tgt_conn_str = create_conn_str(tgt_dsn_params)
        logging.info(f'connecting to target database')
        await open_tgt_connection(new_tgt_conn_str)
        
        logging.info(f'validating source table [{src_table_name}]')
        # check if table exists in source
        source_table_name_dict = await get_table_name_dict(_src_cur)
        if src_table_name not in source_table_name_dict:
            raise ac.ValidationError(f'Source database deos not have a table named [{src_table_name}]')

        # once validation is finished, create tables in the target
        # drop original table if that is in the settings
        await _tgt_cur.execute(f'DROP TABLE IF EXISTS {tgt_table_name} CASCADE')
        logging.info(f'creating target table [{tgt_table_name}]')
        
        await _tgt_cur.execute(f'CREATE TABLE IF NOT EXISTS {tgt_table_name} ({",".join([f"{cname} {ctype}" for cname, ctype in true_tgt_table_columns.items()])})')

        # once table is created, get rows form source to insert
        logging.info(f'fetching rows from source table [{src_table_name}] with columns [{",".join([c for c in src_table_columns.keys()])}]')
        await _src_cur.execute(f'SELECT {",".join(c for c in src_table_columns.keys())} FROM {src_table_name}')

        src_rows = await _src_cur.fetchall()

        tgt_table_column_names = []

        # get target columns and functions
        col_index = 0
        for k, v in src_table_columns.items():
            if isinstance(v.value, ac.TargetColumnPointer):
                tgt_table_column_names.append(v.value.column_name)
            if isinstance(v.value, ac.SourceColumnMapFunction):
                f = v.value
                tgt_table_column_names.append(f.to_column.column_name)
                await _tgt_cur.execute(f"SELECT {f.from_row.select_column.column_name}, {f.with_column.column_name} FROM {f.with_column.table.to_sql_str()}")
                with_rows = await _tgt_cur.fetchall()
                match_dict = dict(with_rows)
                # print(json.dumps(match_dict, indent=2))
                for row_index in range(len(src_rows)):
                    # replace column in source row with a match using the source column value as key
                    src_rows[row_index][col_index] = match_dict[src_rows[row_index][col_index]]
                
            col_index += 1

        # enable bulk insert. this may or may not work
        _tgt_cur._impl.fast_executemany = True
        await _tgt_cur.executemany(
            f'INSERT INTO {tgt_table_name} ({",".join(c for c in tgt_table_column_names)}) VALUES ({",".join(["?" for _ in range(len(src_table_columns))])})',
            src_rows
        )

        logging.info(f'finished insert into [{tgt_table_name}] from [{src_table_name}]')

        return True
    except ValueError as e:
        logging.error("transfer failed - %s", e)
        return False
    except ac.ValidationError as e:
        logging.error("validation failed - %s", e)
        return False
    except Exception as e:
        logging.error("unknown exception - %s", e)
        return False


async def transfer(config: ac.Config, allow_prompts: bool = False):

    try:
        logging.info("validating config")
        config.validate()
    except ac.ValidationError as e:
        logging.error(f"config failed validation with error: [{e}]")
        return
    logging.info("validated config")

    def source_table_order_compare(a, b):
        ta: ac.SourceTableBlock = a[1]
        tb: ac.SourceTableBlock = b[1]
        # if ta depends on tb
        # if so, tb should go before ta
        if tb.target in ta.target_table_deps:
            return 1
        if ta.target in tb.target_table_deps:
            return -1
        return 0

    source_tables = sorted(config.sources.items(), key=cmp_to_key(source_table_order_compare))

    try:
        for src_table_name, src_table in source_tables:
            tgt_table = src_table.target
            tgt_table_name = tgt_table.table_name
            success = await transfer_table(config, src_table_name, tgt_table_name)
            if not success:
                logging.warn(f"transfer from [{src_table_name}] to {tgt_table} failed")
                if allow_prompts:
                    user_input = input(f"skip table? (y/N)")
                    if user_input.lower() == 'y':
                        logging.warn(f"skipping [{src_table_name}]")
                    else:
                        logging.warn('cancelling transfer')
                        break
                else:
                    logging.warn("cancelling transfer")
                    break
                    

    finally:

        # close connections
        logging.info('closing connections')
        await close_connections()


async def _main():
    config_path: str | None = ac.resolve_config_path()

    if not config_path:
        raise ValueError("no config file could be found")

    config = ac.parse_config_file(config_path)

    logging.info('transfering tables')

    await transfer(config)

    logging.info('finished')