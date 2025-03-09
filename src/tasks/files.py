import os
import os.path
import sqlite3
import mimetypes
from typing import List

from .utils import make_rel_path


def get_file_size(file_path: str, dataset_path: str, absolute_paths: bool) -> int:
    if absolute_paths:
        return os.path.getsize(file_path)

    return os.path.getsize(os.path.join(dataset_path, file_path))


def get_mime_type(file_path: str) -> str:
    return mimetypes.guess_type(file_path)[0] or "unknown"


def get_file_extension(file_path: str) -> str:
    return os.path.splitext(file_path)[1][1:].lower()


def build_files_db(
    conn: sqlite3.Connection,
    cur: sqlite3.Cursor,
    dataset_path: str,
    absolute_paths: bool,
) -> None:
    print("Building files database...")

    found_files: List[str] = []

    for root, dirs, files in os.walk(dataset_path):
        if absolute_paths:
            file_paths = [os.path.abspath(os.path.join(root, file)) for file in files]
        else:
            file_paths = [
                make_rel_path(dataset_path, os.path.join(root, file)) for file in files
            ]
        found_files += file_paths

    cur.executemany(
        "INSERT OR IGNORE INTO files (filename, size, mime_type, extension) VALUES (?, ?, ?, ?)",
        [
            (
                file,
                get_file_size(file, dataset_path, absolute_paths),
                get_mime_type(file),
                get_file_extension(file),
            )
            for file in found_files
        ],
    )
    conn.commit()
