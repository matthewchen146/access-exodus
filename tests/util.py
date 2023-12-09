import os, shutil

class CWDContext:
    def __init__(self, path, remove_dir: bool | None = None) -> None:
        self._original_cwd = os.getcwd()
        self._temp_cwd = os.path.abspath(path)
        if remove_dir == None:
            self._remove_dir = False if os.path.exists(self._temp_cwd) else True
        else:
            self._remove_dir = remove_dir

    def __enter__(self):
        if not os.path.exists(self._temp_cwd):
            os.mkdir(self._temp_cwd)
        os.chdir(self._temp_cwd)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        os.chdir(self._original_cwd)
        if self._remove_dir:
            shutil.rmtree(self._temp_cwd, ignore_errors=True)