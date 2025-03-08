"""It's not a real project until you have a junk-drawer `utils.py` file :-)"""

import os.path


def make_rel_path(base: str, file: str) -> str:
    """Make a file path relative to a base path

    Example: make_rel_path('/a/b', '/a/b/asdf/file.txt') -> 'asdf/file.txt'
    """
    abs_base = os.path.abspath(base)
    abs_file = os.path.abspath(file)
    return abs_file.removeprefix(abs_base).removeprefix("/")
