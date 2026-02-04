from __future__ import annotations

import os
import sqlite3

from config import DB_PATH

# Ограничение на размер таблицы events (защита от бесконечного роста логов событий).
MAX_EVENTS_ROWS = int(os.getenv("MAX_EVENTS_ROWS", "20000"))

def _init_conn(conn: sqlite3.Connection) -> sqlite3.Connection:
    # Reduce "database is locked" under concurrency
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
    except Exception:
        pass
    return conn


def connect() -> sqlite3.Connection:
    return _init_conn(sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30))

def connect_ctx() -> sqlite3.Connection:
    """Отдельное соединение (для with/коротких операций)."""
    return _init_conn(sqlite3.connect(DB_PATH, timeout=30))

conn = connect()
cur = conn.cursor()
