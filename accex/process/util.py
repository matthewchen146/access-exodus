import logging

def generate_max_param_count(conn_str: str, logger: logging.Logger = None):
    import pyodbc
    
    logger = (logger and logger.getChild("util.maxparam")) or logging.getLogger("util.maxparam")

    print(f"connecting via connection string [{conn_str}]")
    conn = pyodbc.connect(conn_str)
    cur = conn.cursor()
    test_table_name = "__test"

    min_count = 1000
    max_count = 10000
    while min_count < max_count:                
        cur.execute(f"CREATE TABLE IF NOT EXISTS {test_table_name} (id serial primary key, v int)")
        cur.execute(f"DELETE FROM {test_table_name}")
        mid_count = (max_count + min_count) // 2
        try:
            logger.debug(f"inserting [{mid_count}]")
            cur.execute(f"INSERT INTO {test_table_name} (v) VALUES {','.join('(?)' for _ in range(mid_count))}", [i for i in range(mid_count)])
            min_count = mid_count + 1
            logger.debug(f"success")
        except:
            max_count = mid_count
            logger.debug(f"failure")
            # reconnect
            conn = pyodbc.connect(conn_str)
            cur = conn.cursor()

    cur.execute(f"DROP TABLE {test_table_name}")

    cur.close()
    conn.close()

    logger.info(f"max param count: {min_count - 1}")

    return min_count - 1