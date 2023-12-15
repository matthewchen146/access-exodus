import os, sys
import argparse
import re
import importlib
import inspect
import logging
import ast
from enum import Enum

class ExportType(str, Enum):
    IMPORT = "IMPORT"
    ASSIGN = "ASSIGN"
    FUNCTION = "FUNCTION"
    CLASS = "CLASS"

def get_top_level_exports(file_path) -> dict[str, ExportType]:
    
    top_level_exports = dict()
    with open(file_path, "r") as file:
        tree = ast.parse(file.read())
        tree.body
        for node in tree.body:

            if isinstance(node, ast.Import):
                logging.info("import", " ".join(a.asname or a.name for a in node.names))
                for a in node.names:
                    top_level_exports[a.asname or a.name] = ExportType.IMPORT
            
            elif isinstance(node, ast.ImportFrom):
                logging.info("import", " ".join(a.name for a in node.names))
                for a in node.names:
                    top_level_exports[a.asname or a.name] = ExportType.IMPORT
            
            elif isinstance(node, ast.Assign):
                logging.info("var", ", ".join(v.id for v in node.targets if isinstance(v, ast.Name)))
                for v in node.targets:
                    if isinstance(v, ast.Name):
                        top_level_exports[v.id] = ExportType.ASSIGN
            
            elif isinstance(node, ast.AnnAssign):
                if isinstance(node.target, ast.Name):
                    logging.info("vart", node.target.id)
                    top_level_exports[node.target.id] = ExportType.ASSIGN
            
            elif isinstance(node, ast.FunctionDef):
                logging.info("func", node.name)
                top_level_exports[node.name] = ExportType.FUNCTION
            
            elif isinstance(node, ast.AsyncFunctionDef):
                logging.info("class", node.name)
                top_level_exports[node.name] = ExportType.FUNCTION

            elif isinstance(node, ast.ClassDef):
                logging.info("class", node.name)
                top_level_exports[node.name] = ExportType.CLASS
    
    return top_level_exports

arg_parser: argparse.ArgumentParser | None = None

def setup_argparse(force: bool = False) -> argparse.ArgumentParser:
    global arg_parser
    if arg_parser and not force:
        return arg_parser
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "module_path",
        type=str,
        nargs="+",
        help="modules to import, can be multiple"
    )
    parser.add_argument(
        "--include-path",
        action="store",
        type=str,
        help="path to include for imports, can define multiple with comma separated list"
    )
    parser.add_argument(
        "--ignore-pattern",
        action="store",
        type=str,
        default="^\\s*_",
        help="ignore any symbols that match this regex pattern, by default ignores symbols starting with an _"
    )
    parser.add_argument(
        "--no-all",
        action="store_true",
        help="do not create __all__ list",
    )
    parser.add_argument(
        "--out-file",
        action="store",
        type=str,
        help="write to file from path",
    )

    arg_parser = parser
    return parser

def main():
    args = setup_argparse().parse_args()   
    ignore_pattern = re.compile(args.ignore_pattern)

    def is_valid(s: str, t: ExportType) -> bool:
        nonlocal args
        nonlocal ignore_pattern

        if t == ExportType.IMPORT:
            return False
        
        if ignore_pattern.match(s):
            return False
        
        return True

    indent = "    "
    output_imports = ""
    output_all = ""

    if args.include_path:
        for path in str.split(args.include_path, ","):
            sys.path.append(os.path.abspath(path))

    for mod_path in args.module_path:
        mod = importlib.import_module(mod_path)
        file_path = inspect.getfile(mod)
        top_level_exports = get_top_level_exports(file_path)
        symbols = [s for s, t in top_level_exports.items() if is_valid(s, t)]
        import_string = f"from {mod_path} import (\n" + ",\n".join(f"{indent}{s}" for s in symbols) + "\n)\n"
        output_imports += import_string
        if not args.no_all:
            output_all += ",\n".join(f"{indent}\"{s}\"" for s in symbols)
    
    output_all = f"__all__ = [\n" + output_all + "\n]\n"
    output = output_imports + output_all

    if args.out_file:
        p = os.path.abspath(args.out_file)
        if os.path.isdir(p):
            p = os.path.abspath(p + "/__init__.py")
        if os.path.exists(p):
            user_input = input(f"File at \"{p}\" already exists. Overwrite? [Y/n] ")
            if not user_input or user_input.lower() != "n":
                with open(p, 'w') as file:
                    print(f"Writing to {p}")
                    file.write(output)
            else:
                print("Cancelled")
    else:
        print(output)

if __name__ == "__main__":
    main()