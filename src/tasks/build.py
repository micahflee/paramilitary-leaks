import os

from .db import db_connect
from .telegram import build_telegram_db
from .files import build_files_db


def build(dataset_path: str, absolute_paths: bool) -> None:
    # Initialize the database
    output_path = os.path.join(os.getcwd(), "output")
    os.makedirs(output_path, exist_ok=True)
    db_file_path = os.path.join(output_path, "data.db")
    print(f"Using database file: {db_file_path}")

    conn, cur = db_connect(db_file_path)

    # Build telegram data
    build_telegram_db(cur, dataset_path, absolute_paths)

    # Build files data
    build_files_db(conn, cur, dataset_path, absolute_paths)

    cur.connection.close()
