import sqlite3
from typing import List


def initialize_database(cur: sqlite3.Cursor) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS group_chats(
            id INTEGER NOT NULL,
            title TEXT NOT NULL,
            PRIMARY KEY (id, title)
        )
    """
    )


def db_connect(db_name: str) -> sqlite3.Cursor:
    cur = sqlite3.connect(db_name).cursor()
    initialize_database(cur)
    return cur


def next_group_chats_id(cur: sqlite3.Cursor) -> int:
    cur.execute("SELECT MAX(id) FROM group_chats")
    max_id = cur.fetchone()[0]
    return 1 if max_id is None else max_id + 1


def insert_group_chats(cur: sqlite3.Cursor, titles: List[str]) -> int:
    # This would be sketchy in a multi-user or multi-threaded environment but,
    # for the purposes of this script, we shouldn't run into any race conditions
    # related to the way we're finding the next id.

    placeholders = ", ".join(list((len(titles) * "?")))
    cur.execute(f"SELECT id FROM group_chats WHERE title IN ({placeholders})", titles)
    ids = cur.fetchall()
    if ids:
        id = ids[0][0]
    else:
        id = next_group_chats_id(cur)

    # TODO Batch these inserts
    for title in titles:
        with cur.connection as con:
            con.execute(
                "INSERT OR IGNORE INTO group_chats (id, title) VALUES (?, ?)",
                (id, title),
            )

    return id
