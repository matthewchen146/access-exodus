# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
import regex

project = "Access Exodus"
copyright = "2023, Matthew C"
author = "Matthew C"
release = "0.1.0"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration


extensions = ["sphinx.ext.autodoc", "sphinx.ext.autosummary"]

templates_path = ["_templates"]
exclude_patterns = []


docstring = regex.compile(
    r"(?:def|class)[\S\s]*?:\s*f*\"\"\"([\S\s]*?)\"\"\"", regex.RegexFlag.MULTILINE
)


def skip_member(app, what, name, obj, skip, options):
    import inspect

    # FIXME: ``what`` is broken and will display as class for methods and functions
    try:
        if not hasattr(obj, "__doc__"):
            return True
        if not obj.__doc__:
            print(obj, name, "__doc__ not set, searching")
            d = docstring.findall(inspect.getsource(obj))
            if len(d) > 0:
                print("not skipping, found docstring for", obj, name)
                return False
            print("skipping, no docstring found for", obj, name)
            return True
    except Exception as e:
        print("skip member raised an exception -", e)
        return True


def process_docstring(app, what, name, obj, options, lines):
    import inspect
    # Modify docstrings based on 'what' (e.g., 'module', 'class', 'method', 'function')
    # Modify 'lines' list in place to change the docstring content

    # Example: Add a prefix to docstrings of functions and methods
    if what in ["method"]:
        if not obj.__doc__:
            print(name, "__doc__ not set, searching")
            d = docstring.findall(inspect.getsource(obj))
            if len(d) > 0:
                r = regex.sub(
                    r"{((?>[^{}]+|(?R))*)}",
                    lambda match: [str(i) for i in eval(match[0], obj.__globals__)][0],
                    d[0],
                )
                lines.extend([str.strip(line) for line in r.splitlines()])
            print("docstring result", "\n".join(lines))


def setup(app):
    app.connect("autodoc-skip-member", skip_member)
    app.connect("autodoc-process-docstring", process_docstring)


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "groundwork"
html_static_path = ["_static"]
html_style = "styles.css"
