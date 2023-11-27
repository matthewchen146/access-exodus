import asyncio
import logging
import json
import pyodbc
import aioodbc
import accex_config
from typing import Callable

async def conn_attributes(conn: pyodbc.Connection):
    pass
    # postgres settings
    # conn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
    # conn.setencoding(encoding='utf-8')

src_conn_str: str = ''
src_conn: aioodbc.Connection = None
src_cur: aioodbc.Cursor = None
tgt_conn_str: str = ''
tgt_conn: aioodbc.Connection = None
tgt_cur: aioodbc.Cursor = None

async def open_src_connection(new_src_conn_str: str) -> aioodbc.Connection:
    global src_conn_str
    global src_conn
    global src_cur

    if src_conn is None or new_src_conn_str != src_conn_str:
        src_conn_str = new_src_conn_str
        await close_src_connection()
        logging.info(f'connecting via connection string [{src_conn_str}]')
        src_conn = await aioodbc.connect(dsn=src_conn_str)
        src_cur = await src_conn.cursor()
    return src_conn

async def open_tgt_connection(new_tgt_conn_str: str) -> aioodbc.Connection:
    global tgt_conn_str
    global tgt_conn
    global tgt_cur

    if tgt_conn is None or new_tgt_conn_str != tgt_conn_str:
        tgt_conn_str = new_tgt_conn_str
        await close_tgt_connection()
        logging.info(f'connecting via connection string [{tgt_conn_str}]')
        tgt_conn = await aioodbc.connect(dsn=tgt_conn_str, after_created=conn_attributes, autocommit=True)
        tgt_cur = await tgt_conn.cursor()
    return tgt_conn

async def close_src_connection():
    global src_conn
    global src_cur

    if src_cur: await src_cur.close()
    if src_conn: await src_conn.close()

async def close_tgt_connection():
    global tgt_conn
    global tgt_cur

    if tgt_cur: await tgt_cur.close()
    if tgt_conn: await tgt_conn.close()

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

async def transfer_table(config: accex_config.Config, src_table_name: str, tgt_table_name: str) -> bool:
    global src_cur
    global tgt_cur

    try:
        src_table = config.sources[src_table_name]

        tgt_table = config.targets[tgt_table_name]

        logging.info(f'validating source table [{src_table_name}]')
        
        src_table_columns: dict = src_table.columns

        tgt_table_columns: dict = tgt_table.columns

        src_dsn_params = {**config.source_dsn_params, **src_table.dsn_params}
        new_src_conn_str = create_conn_str(src_dsn_params)
        logging.info(f"connecting to source database")
        await open_src_connection(new_src_conn_str)
        
        tgt_dsn_params = {**config.target_dsn_params, **tgt_table.dsn_params}
        new_tgt_conn_str = create_conn_str(tgt_dsn_params)
        logging.info(f'connecting to target database')
        await open_tgt_connection(new_tgt_conn_str)
        
        # check if table exists in source
        source_table_name_dict = await get_table_name_dict(src_cur)
        if src_table_name not in source_table_name_dict:
            raise ValueError(f'Source database deos not have a table named [{src_table_name}]')

        # check if source columns are valid
        # await src_cur.columns(table=source_table_name)
        true_src_columns = await get_column_name_dict(src_cur, table=src_table_name)# dict([(row.column_name, row) for row in await src_cur.fetchall()])
        for column_name in src_table_columns.keys():
            # check if column exists in access
            if column_name not in true_src_columns:
                raise ValueError(f'Source [{src_table_name}] deos not have a column named [{column_name}]')
            # check if column name is in the target table
            tgt_column_name = src_table_columns[column_name]
            if tgt_column_name not in tgt_table_columns:
                raise ValueError(f'Target [{tgt_table_name}] deos not have a column named [{tgt_column_name}]')

        # once validation is finished, create tables in the target
        # drop original table if that is in the settings
        await tgt_cur.execute(f'DROP TABLE IF EXISTS {tgt_table_name}')
        logging.info(f'creating target table [{tgt_table_name}]')
        
        await tgt_cur.execute(f'CREATE TABLE IF NOT EXISTS {tgt_table_name} ({",".join([f"{cname} {ctype}" for cname, ctype in tgt_table_columns.items()])})')

        # once table is created, get rows form source to insert
        logging.info(f'fetching rows from source table [{src_table_name}] with columns [{",".join([c for c in src_table_columns.keys()])}]')
        await src_cur.execute(f'SELECT {",".join([c for c in src_table_columns.keys()])} FROM {src_table_name}')

        src_rows = await src_cur.fetchall()

        # enable bulk insert. this may or may not work
        tgt_cur._impl.fast_executemany = True
        await tgt_cur.executemany(
            f'INSERT INTO {tgt_table_name} ({",".join([c for c in src_table_columns.values()])}) VALUES ({",".join(["?" for _ in range(len(src_table_columns))])})',
            src_rows
        )

        logging.info(f'finished insert into [{tgt_table_name}] from [{src_table_name}]')

        return True
    except ValueError as error:
        logging.error('transfer failed - %s', error)
        return False


async def transfer(config: accex_config.Config):

    try:
        for src_table_name, src_table in config.sources.items():
            tgt_table_name = src_table.target
            success = await transfer_table(config, src_table_name, tgt_table_name)
            if not success:
                user_input = input(f"IN- transfer from [{src_table_name}] to [{tgt_table_name}] failed. skip table? (y/N)")
                if user_input.lower() == 'y':
                    logging.info(f"skipping [{src_table_name}]")
                else:
                    logging.info('cancelling transfer')
                    break
                    

    finally:

        # close connections
        logging.info('closing connections')
        await close_connections()


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    async def main():
        config_path: str | None = accex_config.resolve_config_path()
    
        if not config_path:
            raise ValueError("no config file could be found")
    
        config = accex_config.parse_config_file(config_path)

        logging.info('config\n', json.dumps(config, indent=4))

        logging.info('transfering tables')

        await transfer(config)

        logging.info('finished')

    asyncio.run(main())