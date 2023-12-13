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
        # "Driver=PostgreSQL Unicode;Server=localhost;Port=8000;Database=postgres;Uid=postgres;Pwd=123"
        conn_str = args.conn_str
        print(f"connecting via connection string [{conn_str}]")
        conn = pyodbc.connect(conn_str)
        cur = conn.cursor()
        test_table_name = "__test"
        # cur.execute(f"CREATE TABLE IF NOT EXISTS {test_table_name} (id serial primary key, v int)")

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

if __name__ == "__main__":
    main()