import os
import os.path
import sqlite3
from typing import List

from .utils import make_rel_path


def build(dataset_path: str, db_path: str, absolute_paths: bool) -> None:
    found_files: List[str] = []

    for root, dirs, files in os.walk(dataset_path):
        if absolute_paths:
            file_paths = [os.path.abspath(os.path.join(root, file)) for file in files]
        else:
            file_paths = [
                make_rel_path(dataset_path, os.path.join(root, file)) for file in files
            ]
        found_files += file_paths

    con = sqlite3.connect(db_path)

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS files(
            filename TEXT PRIMARY KEY,
            extension TEXT
        )
    """
    )

    con.executemany(
        "INSERT OR IGNORE INTO files (filename, extension) VALUES (?, ?)",
        [(file, os.path.splitext(file)[1][1:].lower()) for file in found_files],
    )

    con.commit()
    con.close()

    print("Created and populated files table in", db_path)
