import argparse, time, asyncio, logging
from functools import cmp_to_key
import pyodbc
import aioodbc
from ..config import core as ac

class TransferError(Exception):
    pass


MAX_PARAM_COUNT_DEFAULT = 1000
"""
The default max amount of parameters that can be used at once in a query.
"""
MAX_PARAM_COUNTS = {
    "PostgreSQL Unicode": 7499
}
"""
Maps the driver name to the amount of max parameters that can be used at once in a query.
This is the the number of ``?`` markers in the query string, also known as parameter markers or bind variables.
It probably depends on the specific ODBC driver.

Currently the number is derived from experimentation, not any documentation.
In order to handle untested counts, a default ``MAX_PARAM_COUNT_DEFAULT`` exists.
"""

_LOG_DIVIDER = "============================================================"

def get_max_param_count(driver_name: str) -> int:
    """Gets the max param count based on the driver name.
    If the driver is not handled, the default is returned instead.

    :param driver_name: name of the driver
    :type driver_name: str
    :return: max param count
    :rtype: int
    """
    return MAX_PARAM_COUNTS.get(driver_name) or MAX_PARAM_COUNT_DEFAULT

async def _conn_attributes(conn: pyodbc.Connection) -> None:
    """Applies settings to connection based on specific driver.

    :param conn: connection
    :type conn: pyodbc.Connection
    """
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
    """Creates connection to source database using connection string.
    If the connection is the same as the previous connection, the connection is kept open.

    :param new_src_conn_str: source connection string
    :type new_src_conn_str: str
    :return: cursor to the source connection
    :rtype: aioodbc.Cursor
    """
    global _src_conn_str
    global _src_conn
    global _src_cur

    logger = logging.getLogger("process")

    if _src_conn is None or new_src_conn_str != _src_conn_str:
        _src_conn_str = new_src_conn_str
        await close_src_connection()
        logger.info(f'connecting to source via connection string \"{_src_conn_str}\"')
        _src_conn = await aioodbc.connect(dsn=_src_conn_str)
        _src_cur = await _src_conn.cursor()
        logger.info("connected to source")
    return _src_cur

async def open_tgt_connection(new_tgt_conn_str: str) -> aioodbc.Cursor:
    """Creates connection to target database using connection string.
    If the connection is the same as the previous connection, the connection is kept open.

    :param new_tgt_conn_str: target connection string
    :type new_tgt_conn_str: str
    :return: cursor to the target connection
    :rtype: aioodbc.Cursor
    """
    global _tgt_conn_str
    global _tgt_conn
    global _tgt_cur

    logger = logging.getLogger("process")

    if _tgt_conn is None or new_tgt_conn_str != _tgt_conn_str:
        _tgt_conn_str = new_tgt_conn_str
        await close_tgt_connection()
        logger.info(f'connecting to target via connection string \"{_tgt_conn_str}\"')
        _tgt_conn = await aioodbc.connect(dsn=_tgt_conn_str, after_created=_conn_attributes, autocommit=True)
        _tgt_cur = await _tgt_conn.cursor()
        logger.info("connected to target")
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

def create_conn_str(*args: list[dict]) -> str:
    params = {}
    for p in args:
        params = {**params, **p}
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

    logger = logging.getLogger("process.transfer_table")
    start_time = time.time()

    try:
        src_table = config.sources[src_table_name]

        tgt_table = config.targets[tgt_table_name]
        
        src_table_columns = src_table.columns

        true_tgt_table_columns = tgt_table.columns

        src_dsn_params = {**config.source_dsn_params, **src_table.dsn_params}
        new_src_conn_str = create_conn_str(src_dsn_params)
        logger.info(f"connecting to source database")
        await open_src_connection(new_src_conn_str)
        
        tgt_dsn_params = {**config.target_dsn_params, **tgt_table.dsn_params}
        new_tgt_conn_str = create_conn_str(tgt_dsn_params)
        logger.info(f'connecting to target database')
        await open_tgt_connection(new_tgt_conn_str)
        
        logger.info(f'validating source table [{src_table_name}]')
        # check if table exists in source
        source_table_name_dict = await get_table_name_dict(_src_cur)
        if src_table_name not in source_table_name_dict:
            raise ac.ValidationError(f'Source database deos not have a table named [{src_table_name}]')

        # once validation is finished, create tables in the target
        # drop original table if that is in the settings
        await _tgt_cur.execute(f'DROP TABLE IF EXISTS {tgt_table_name} CASCADE')
        logger.info(f'creating target table [{tgt_table_name}]')
        
        await _tgt_cur.execute(f'CREATE TABLE IF NOT EXISTS {tgt_table_name} ({",".join([f"{cname} {ctype}" for cname, ctype in true_tgt_table_columns.items()])})')

        driver_name = ""
        for k, v in tgt_dsn_params.items():
            if k.lower() == "driver":
                driver_name = v.value

        total_src_row_count: int = (await (await _src_cur.execute(f"SELECT COUNT(*) FROM {src_table_name}")).fetchone())[0]
        total_src_row_count_strlen = len(str(total_src_row_count))

        max_param_count = get_max_param_count(driver_name)
        row_size = len(src_table_columns)
        chunk_size = int(max_param_count / row_size)
        
        if logger.isEnabledFor(logging.INFO):
            print(
                _LOG_DIVIDER + "\n"
                f"source:         {src_table_name}\n"
                f"target:         {tgt_table_name}\n"
                f"max params:     {max_param_count}\n"
                f"col count:      {row_size}\n"
                f"chunk size:     {chunk_size}\n"
                f"row count:      {total_src_row_count}\n"
                + _LOG_DIVIDER
            )

        total_inserted = 0

        # once table is created, get rows form source to insert
        logger.info(f'fetching rows from source table [{src_table_name}] with columns [{", ".join([c for c in src_table_columns.keys()])}]')
        await _src_cur.execute(f'SELECT {",".join(c for c in src_table_columns.keys())} FROM {src_table_name}')

        logger.debug("selected source rows")

        if logger.isEnabledFor(logging.INFO):
            print(_LOG_DIVIDER)

        while True:
            src_rows = await _src_cur.fetchmany(chunk_size)

            src_row_count = len(src_rows)

            if src_row_count == 0:
                print("")
                logger.debug("no more source rows to fetch, breaking")
                break

            tgt_table_column_names = []

            # get target columns and functions
            col_index = 0
            for k, v in src_table_columns.items():
                try:
                    if isinstance(v.value, ac.TargetColumnPointer):
                        tgt_table_column_names.append(v.value.column_name)
                    if isinstance(v.value, ac.SourceColumnMapFunction):
                        f = v.value
                        tgt_table_column_names.append(f.to_column.column_name)
                        await _tgt_cur.execute(
                            f"SELECT {f.from_row.select_column.column_name}, {f.with_column.column_name} " +\
                            f"FROM {f.with_column.table.to_sql_str()} " +\
                            f"WHERE {f.from_row.select_column.column_name} IN ({','.join('?' * src_row_count)})",
                            [row[col_index] for row in src_rows]
                        )
                        with_rows = await _tgt_cur.fetchall()
                        match_dict = dict(with_rows)
                        # print(json.dumps(match_dict, indent=2))
                        for row_index in range(src_row_count):
                            # replace column in source row with a match using the source column value as key
                            src_rows[row_index][col_index] = match_dict.get(src_rows[row_index][col_index])
                except Exception as e:
                    raise TransferError(f"source column read failed for {k} - {e}")
                col_index += 1
    
            logger.debug("finished src col check, beginning insert")

            # # pyodbc fast executemany method buggy
            # try:
            #     # enable bulk insert. this may or may not work
            #     _tgt_cur._impl.fast_executemany = True
            #     await _tgt_cur.executemany(
            #         f'INSERT INTO {tgt_table_name} ({",".join(c for c in tgt_table_column_names)}) VALUES ({",".join("?" for _ in range(len(src_table_columns)))})',
            #         src_rows
            #     )
            # except Exception as e:
            #     logging.warn("executemany failed, falling back - %s", e)
            #     _tgt_cur._impl.fast_executemany = False
            #     await _tgt_cur.executemany(
            #         f'INSERT INTO {tgt_table_name} ({",".join(c for c in tgt_table_column_names)}) VALUES ({",".join("?" for _ in range(len(src_table_columns)))})',
            #         src_rows
            #     )
            
            await _tgt_cur.execute(
                f"INSERT INTO {tgt_table_name} ({','.join(c for c in tgt_table_column_names)}) VALUES " + \
                    ",".join(f"({','.join('?' * row_size)})" for _ in range(src_row_count)),
                [value for row in src_rows for value in row]
            )

            total_inserted += src_row_count
            total_percent = total_inserted / total_src_row_count
            if logger.isEnabledFor(logging.INFO):
                if logger.level != logging.DEBUG:
                    print(
                        f"    inserted {total_inserted:>{total_src_row_count_strlen}} / {total_src_row_count} " +\
                        f"{'█' * int(total_percent * 20):<{20}} {int(100 * total_percent):3}%",
                        end="\r"
                    )
                else:
                    logger.debug(f"inserted [{src_row_count:<{total_src_row_count_strlen}}] {total_inserted:>{total_src_row_count_strlen}} / {total_src_row_count}")

        tgt_count = (await (await _tgt_cur.execute(f"SELECT COUNT(*) FROM {tgt_table_name}")).fetchone())[0]

        if logger.isEnabledFor(logging.INFO):
            print(
                _LOG_DIVIDER + "\n"
                f"finished transfer from [{src_table_name}] to [{tgt_table_name}]\n"
                f"source count:   {total_src_row_count}\n"
                f"target count:   {tgt_count}\n"
                f"duration:       {time.time() - start_time:.2f}s\n"
                + _LOG_DIVIDER
            )

        return True
    except ValueError as e:
        logger.error("transfer failed - %s", e)
        return False
    except ac.ValidationError as e:
        logger.error("validation failed - %s", e)
        return False
    except Exception as e:
        logger.error("unhandled exception - %s", e)
        return False


async def transfer(config: ac.Config, allow_prompts: bool = False):

    logger = logging.getLogger("process.transfer")

    try:
        logger.info("validating config")
        config.validate()
    except ac.ValidationError as e:
        logger.error("config failed validation - %s", e)
        return
    logger.info("validated config")

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

    # sort source tables based on their dependencies
    source_tables = sorted(config.sources.items(), key=cmp_to_key(source_table_order_compare))

    try:
        for src_table_name, src_table in source_tables:
            tgt_table = src_table.target
            tgt_table_name = tgt_table.table_name
            success = await transfer_table(config, src_table_name, tgt_table_name)
            if not success:
                logger.warn(f"transfer from [{src_table_name}] to {tgt_table} failed")
                if allow_prompts:
                    user_input = input(f"skip table? (y/N)")
                    if user_input.lower() == 'y':
                        logger.warn(f"skipping [{src_table_name}]")
                    else:
                        logger.warn('cancelling transfer')
                        break
                else:
                    logger.warn("cancelling transfer")
                    break
    finally:
        # close connections
        logger.info('closing connections')
        await close_connections()


async def _main():
    arg_parser = argparse.ArgumentParser(prog="accex")
    ac.populate_arg_parser(arg_parser)
    args = arg_parser.parse_args()
    logging.basicConfig(level=logging.getLevelNamesMapping()[args.log_level])
    config_path = ac.resolve_config_path(args.config_path)

    if not config_path:
        raise ValueError("no config file could be found")

    config = ac.parse_config_file(config_path)

    logger = logging.getLogger("process")

    logger.info('transfering tables')

    await transfer(config)

    logger.info('finished')