import os, winreg, argparse
from pathlib import Path

ODBCINST = "SOFTWARE\\ODBC\\ODBCINST.INI"

def main():

    arg_parser = argparse.ArgumentParser()

    sub_parsers = arg_parser.add_subparsers(title="sub commands", dest="cmd")

    ls_parser = sub_parsers.add_parser("ls", help="list all drivers")
    ls_parser.add_argument(
        "--info",
        action="store_true",
        help="show driver info"
    )

    path_parser = sub_parsers.add_parser("path", help="get driver path")
    path_parser.add_argument(
        "name",
        type=str,
        help="name of the driver"
    )

    register_parser = sub_parsers.add_parser("register", help="register a driver in ODBCINST")
    register_parser.add_argument(
        "name",
        type=str,
        help="name of the driver"
    )
    register_parser.add_argument(
        "path",
        type=str,
        help="path to the driver"
    )

    maxparam_parser = sub_parsers.add_parser("maxparam", help="test for the max param markers of a driver/database")
    maxparam_parser.add_argument(
        "conn_str",
        type=str,
        help="connection string to connect to the database"
    )

    gencols_parser = sub_parsers.add_parser("gencols", help="generate source and target columns for config from source database")
    gencols_parser.add_argument(
        "conn_str",
        type=str,
        help="connection string to connect to the source database"
    )
    gencols_parser.add_argument(
        "table_name",
        type=str,
        help="name of the table to get columns from"
    )

    args = arg_parser.parse_args()

    if args.cmd == 'ls':
        import json
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, ODBCINST) as odbc_key:
            odbc_info = winreg.QueryInfoKey(odbc_key)
            drivers = dict()
            for i in range(odbc_info[0]):
                driver_name = winreg.EnumKey(odbc_key, i)
                with winreg.OpenKey(odbc_key, driver_name) as driver_key:
                    try:
                        driver_path = winreg.QueryValueEx(driver_key, "Driver")[0]
                        drivers[driver_name] = {
                            "name": driver_name,
                            "path": str(Path(driver_path).resolve())
                        }
                    except OSError as e:
                        pass
        if args.info:
            print(json.dumps(drivers, indent=2))
        else:
            print(json.dumps(list(drivers.keys()), indent=2))
    elif args.cmd == 'path':
        driver_name: str = args.name
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, ODBCINST) as odbc_key:
            try:
                with winreg.OpenKey(odbc_key, driver_name) as driver_key:
                    path: str = winreg.QueryValueEx(driver_key, "Driver")[0]
                    print(Path(path).resolve())
            except OSError as e:
                print(f"failed to find driver \"{driver_name}\"")
                exit(1)
    elif args.cmd == 'register':
        import ctypes
        if ctypes.windll.shell32.IsUserAnAdmin() == 0:
            print(f"admin privileges required to register a driver")
            exit(1)
        driver_name: str = args.name
        driver_path = str(Path(args.path).resolve())
        if not os.path.exists(driver_path):
            print(f"path \"{driver_path}\" does not exist")
            exit(1)
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, ODBCINST, 0, winreg.KEY_CREATE_SUB_KEY) as odbc_key:
            try:
                with winreg.OpenKey(odbc_key, driver_name) as driver_key:
                    print(f"driver \"{driver_name}\" already registered")
                    exit(1)
            except OSError as e:    
                with winreg.CreateKey(odbc_key, driver_name) as driver_key:
                    winreg.SetValueEx(driver_key, "Driver", 0, winreg.REG_SZ, driver_path)
                    winreg.SetValueEx(driver_key, "Setup", 0, winreg.REG_SZ, driver_path)
                print(f"successfully registered \"{driver_name}\"")
    elif args.cmd == 'maxparam':
        import pyodbc
        conn_str = args.conn_str
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
                print(f"inserting [{mid_count}]")
                cur.execute(f"INSERT INTO {test_table_name} (v) VALUES {','.join('(?)' for _ in range(mid_count))}", [i for i in range(mid_count)])
                min_count = mid_count + 1
                print(f"success")
            except:
                max_count = mid_count
                print(f"failure")
                # reconnect
                conn = pyodbc.connect(conn_str)
                cur = conn.cursor()
        print(f"max params [{min_count - 1}]")

        cur.execute(f"DROP TABLE {test_table_name}")

        cur.close()
        conn.close()
    elif args.cmd == 'gencols':
        import re, textwrap
        import pyodbc

        def pascal_to_snake(s: str) -> str:
            return re.sub(r'(?<!^)(?=[A-Z])', '_', s).lower()

        TYPE_TO_NAME = {
            pyodbc.SQL_UNKNOWN_TYPE: "SQL_UNKNOWN_TYPE",
            pyodbc.SQL_CHAR: "SQL_CHAR",
            pyodbc.SQL_VARCHAR: "SQL_VARCHAR",
            pyodbc.SQL_LONGVARCHAR: "SQL_LONGVARCHAR",
            pyodbc.SQL_WCHAR: "SQL_WCHAR",
            pyodbc.SQL_WVARCHAR: "SQL_WVARCHAR",
            pyodbc.SQL_WLONGVARCHAR: "SQL_WLONGVARCHAR",
            pyodbc.SQL_DECIMAL: "SQL_DECIMAL",
            pyodbc.SQL_NUMERIC: "SQL_NUMERIC",
            pyodbc.SQL_SMALLINT: "SQL_SMALLINT",
            pyodbc.SQL_INTEGER: "SQL_INTEGER",
            pyodbc.SQL_REAL: "SQL_REAL",
            pyodbc.SQL_FLOAT: "SQL_FLOAT",
            pyodbc.SQL_DOUBLE: "SQL_DOUBLE",
            pyodbc.SQL_BIT: "SQL_BIT",
            pyodbc.SQL_TINYINT: "SQL_TINYINT",
            pyodbc.SQL_BIGINT: "SQL_BIGINT",
            pyodbc.SQL_BINARY: "SQL_BINARY",
            pyodbc.SQL_VARBINARY: "SQL_VARBINARY",
            pyodbc.SQL_LONGVARBINARY: "SQL_LONGVARBINARY",
            pyodbc.SQL_TYPE_DATE: "SQL_TYPE_DATE",
            pyodbc.SQL_TYPE_TIME: "SQL_TYPE_TIME",
            pyodbc.SQL_TYPE_TIMESTAMP: "SQL_TYPE_TIMESTAMP",
            pyodbc.SQL_SS_TIME2: "SQL_SS_TIME2",
            pyodbc.SQL_SS_XML: "SQL_SS_XML",
            pyodbc.SQL_INTERVAL_MONTH: "SQL_INTERVAL_MONTH",
            pyodbc.SQL_INTERVAL_YEAR: "SQL_INTERVAL_YEAR",
            pyodbc.SQL_INTERVAL_YEAR_TO_MONTH: "SQL_INTERVAL_YEAR_TO_MONTH",
            pyodbc.SQL_INTERVAL_DAY: "SQL_INTERVAL_DAY",
            pyodbc.SQL_INTERVAL_HOUR: "SQL_INTERVAL_HOUR",
            pyodbc.SQL_INTERVAL_MINUTE: "SQL_INTERVAL_MINUTE",
            pyodbc.SQL_INTERVAL_SECOND: "SQL_INTERVAL_SECOND",
            pyodbc.SQL_INTERVAL_DAY_TO_HOUR: "SQL_INTERVAL_DAY_TO_HOUR",
            pyodbc.SQL_INTERVAL_DAY_TO_MINUTE: "SQL_INTERVAL_DAY_TO_MINUTE",
            pyodbc.SQL_INTERVAL_DAY_TO_SECOND: "SQL_INTERVAL_DAY_TO_SECOND",
            pyodbc.SQL_INTERVAL_HOUR_TO_MINUTE: "SQL_INTERVAL_HOUR_TO_MINUTE",
            pyodbc.SQL_INTERVAL_HOUR_TO_SECOND: "SQL_INTERVAL_HOUR_TO_SECOND",
            pyodbc.SQL_INTERVAL_MINUTE_TO_SECOND: "SQL_INTERVAL_MINUTE_TO_SECOND",
            pyodbc.SQL_GUID: "SQL_GUID"
        }

        TYPE_MAP = {
            "postgres": {
                pyodbc.SQL_UNKNOWN_TYPE: "UKN",
                pyodbc.SQL_CHAR: "char",
                pyodbc.SQL_VARCHAR: "text",
                pyodbc.SQL_LONGVARCHAR: "text",
                pyodbc.SQL_WCHAR: "text",
                pyodbc.SQL_WVARCHAR: "text",
                pyodbc.SQL_WLONGVARCHAR: "text",
                pyodbc.SQL_DECIMAL: "numeric",
                pyodbc.SQL_NUMERIC: "numeric",
                pyodbc.SQL_SMALLINT: "int",
                pyodbc.SQL_INTEGER: "int",
                pyodbc.SQL_REAL: "real",
                pyodbc.SQL_FLOAT: "numeric",
                pyodbc.SQL_DOUBLE: "numeric",
                pyodbc.SQL_BIT: "bit",
                pyodbc.SQL_TINYINT: "int",
                pyodbc.SQL_BIGINT: "bigint",
                pyodbc.SQL_BINARY: "",
                pyodbc.SQL_VARBINARY: "",
                pyodbc.SQL_LONGVARBINARY: "",
                pyodbc.SQL_TYPE_DATE: "date",
                pyodbc.SQL_TYPE_TIME: "time",
                pyodbc.SQL_TYPE_TIMESTAMP: "timestamp",
                pyodbc.SQL_SS_TIME2: "",
                pyodbc.SQL_SS_XML: "",
                pyodbc.SQL_INTERVAL_MONTH: "",
                pyodbc.SQL_INTERVAL_YEAR: "",
                pyodbc.SQL_INTERVAL_YEAR_TO_MONTH: "",
                pyodbc.SQL_INTERVAL_DAY: "",
                pyodbc.SQL_INTERVAL_HOUR: "",
                pyodbc.SQL_INTERVAL_MINUTE: "",
                pyodbc.SQL_INTERVAL_SECOND: "",
                pyodbc.SQL_INTERVAL_DAY_TO_HOUR: "",
                pyodbc.SQL_INTERVAL_DAY_TO_MINUTE: "",
                pyodbc.SQL_INTERVAL_DAY_TO_SECOND: "",
                pyodbc.SQL_INTERVAL_HOUR_TO_MINUTE: "",
                pyodbc.SQL_INTERVAL_HOUR_TO_SECOND: "",
                pyodbc.SQL_INTERVAL_MINUTE_TO_SECOND: "",
                pyodbc.SQL_GUID: "uuid"
            }
        }

        def infer_new_type(t: int, db: str = "postgres") -> str:
            if db not in TYPE_MAP:
                print(f"database \"{db}\" is unhandled, defaulting to postgres")
                db = "postgres"
            return TYPE_MAP[db].get(t) or "UKN"
        
        "Driver=Microsoft Access Driver (*.mdb, *.accdb);DBQ=./databases/SHOPDATA.ACCDB"
        conn_str = args.conn_str
        print(f"connecting via connection string [{conn_str}]")
        with pyodbc.connect(conn_str) as conn:
            with conn.cursor() as cur:
                table_name = args.table_name
                cur.columns(table_name)
                rows = cur.fetchall()
                print("\n".join(str(row) for row in rows))
                # create targets
                sources = "\n".join(f"{row.column_name}: {pascal_to_snake(row.column_name)}" for row in rows)
                targets = "\n".join(f"{pascal_to_snake(row.column_name)}: {infer_new_type(row.sql_data_type)}" for row in rows)
                print(f"{table_name} " + "{\n" + textwrap.indent("COLUMNS {\n" + textwrap.indent(sources, "    ") + "\n}", "    ") + "\n}")
                print(f"{pascal_to_snake(table_name)} " + "{\n" + textwrap.indent("COLUMNS {\n" + textwrap.indent(targets, "    ") + "\n}", "    ") + "\n}")

if __name__ == "__main__":

    main()