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
                            "path": Path(driver_path).resolve()
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
        driver_path: str = Path(args.path).resolve()
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

if __name__ == "__main__":
    main()