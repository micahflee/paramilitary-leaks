import os.path
import sqlite3
from typing import List

from .datatypes import Message


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

    cur.execute(
        """
      CREATE TABLE IF NOT EXISTS messages(
        id TEXT PRIMARY KEY,
        timestamp TEXT,
        sender TEXT,
        text TEXT,
        media_note TEXT,
        media_filename TEXT,
        filename TEXT,
        group_chat_id INTEGER
      )
    """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS files(
            filename TEXT PRIMARY KEY,
            size INTEGER,
            mime_type TEXT,
            extension TEXT
        )
    """
    )


def db_connect(db_name: str) -> tuple[sqlite3.Connection, sqlite3.Cursor]:
    conn = sqlite3.connect(db_name, autocommit=True)
    cur = conn.cursor()
    initialize_database(cur)
    return conn, cur


def next_group_chats_id(cur: sqlite3.Cursor) -> int:
    # This would be sketchy in a multi-user or multi-threaded environment but,
    # for the purposes of this script, we shouldn't run into any race conditions
    # related to the way we're finding the next id.

    cur.execute("SELECT MAX(id) FROM group_chats")
    max_id = cur.fetchone()[0]
    return 1 if max_id is None else max_id + 1


def insert_group_chats(cur: sqlite3.Cursor, titles: List[str]) -> int:
    placeholders = ", ".join(list((len(titles) * "?")))
    cur.execute(f"SELECT id FROM group_chats WHERE title IN ({placeholders})", titles)
    ids = cur.fetchall()
    if ids:
        id = ids[0][0]
    else:
        id = next_group_chats_id(cur)

    cur.executemany(
        "INSERT OR IGNORE INTO group_chats (id, title) VALUES (?, ?)",
        [(id, title) for title in titles],
    )

    return id


def insert_messages(
    cur: sqlite3.Cursor, group_chat_id: int, filename: str, messages: List[Message]
) -> None:
    params = [
        (
            message.id,
            message.timestamp,
            message.sender,
            message.text,
            message.media_note,
            (
                os.path.join(filename, message.media_filename)
                if message.media_filename
                else None
            ),
            filename,
            group_chat_id,
        )
        for message in messages
    ]
    cur.executemany(
        """
    INSERT OR IGNORE INTO messages (
        id,
        timestamp,
        sender,
        text,
        media_note,
        media_filename,
        filename,
        group_chat_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """,
        params,
    )
