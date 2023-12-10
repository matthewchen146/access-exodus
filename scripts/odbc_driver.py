import winreg, argparse, json

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
        "driver_name",
        type=str,
        help="name of the driver"
    )

    args = arg_parser.parse_args()

    if args.cmd == 'ls':
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
                            "path": driver_path
                        }
                    except OSError as e:
                        pass
        if args.info:
            print(json.dumps(drivers, indent=2))
        else:
            print(json.dumps(list(drivers.keys()), indent=2))
    elif args.cmd == 'path':
        driver_name: str = args.driver_name
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, ODBCINST) as odbc_key:
            try:
                with winreg.OpenKey(odbc_key, driver_name) as driver_key:
                    path: str = winreg.QueryValueEx(driver_key, "Driver")[0]
                    print(path)
            except OSError as e:
                print(f"failed to find driver \"{driver_name}\"")
                exit(1)

if __name__ == "__main__":
    main()