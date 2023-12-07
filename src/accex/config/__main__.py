from . import parse_args, resolve_config_path, parse_config_file, write_config
import sys, logging, json

def main():
    args = parse_args()
    config_path = resolve_config_path()
    if not config_path:
        logging.info("no config file specified/found")
        sys.exit(1)
    config = parse_config_file(config_path)
    out = ''
    if args.json:
        if args.json_format:
            out = json.dumps(config, indent=4)
        else:
            out = json.dumps(config)
    else:
        out = write_config(config)

    if args.out_file:
        with open(args.out_file, 'w') as file:
            file.write(out)
    else:
        print(out)

def init():
    if __name__ == "__main__":
        main()

init()