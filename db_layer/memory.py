from __future__ import annotations

import datetime as dt
import json
import sqlite3
from typing import Tuple, Optional, Any, Dict, List

from config import (
    DB_PATH,
    FREE_TEXT_LIMIT_PER_DAY,
    FREE_PHOTO_LIMIT_PER_DAY,
    today_iso,
    FREE_TAROT_LIFETIME,
    FREE_TAROT_LIMIT_PER_DAY,
    MAX_DB_MESSAGES_PER_CHAT,
    MAX_TAROT_HISTORY_PER_USER,
)

from .connection import conn, cur, MAX_EVENTS_ROWS
from .users import get_user


# MEMORY (users table) - как было
# =========================================================
def set_last_context(
    user_id: int,
    *,
    topic: Optional[str] = None,
    last_user_message: Optional[str] = None,
    last_bot_message: Optional[str] = None,
) -> None:
    get_user(user_id)

    def _cut(s: Optional[str], n: int) -> Optional[str]:
        if s is None:
            return None
        s = (s or "").strip()
        if not s:
            return None
        return s[:n]

    topic = _cut(topic, 64)
    last_user_message = _cut(last_user_message, 500)
    last_bot_message = _cut(last_bot_message, 500)

    cur.execute(
        """
        UPDATE users
        SET last_topic = COALESCE(?, last_topic),
            last_user_message = COALESCE(?, last_user_message),
            last_bot_message = COALESCE(?, last_bot_message)
        WHERE user_id = ?
        """,
        (topic, last_user_message, last_bot_message, user_id),
    )
    conn.commit()


def set_last_followup_text(user_id: int, text: str) -> None:
    get_user(user_id)
    txt = (text or "").strip()
    if len(txt) > 600:
        txt = txt[:600]
    cur.execute(
        "UPDATE users SET last_followup_text = ? WHERE user_id = ?",
        (txt, user_id),
    )
    conn.commit()


def set_last_followup_meta(
    user_id: int,
    *,
    followup_type: Optional[str] = None,
    followup_topic: Optional[str] = None,
) -> None:
    get_user(user_id)
    t = (followup_type or "").strip()[:64] if followup_type else None
    topic = (followup_topic or "").strip()[:64] if followup_topic else None
    cur.execute(
        """
        UPDATE users
        SET last_followup_type = COALESCE(?, last_followup_type),
            last_followup_topic = COALESCE(?, last_followup_topic)
        WHERE user_id = ?
        """,
        (t, topic, user_id),
    )
    conn.commit()


def set_last_limit_info(user_id: int, *, topic: Optional[str], limit_type: str) -> None:
    get_user(user_id)
    t = (topic or "").strip()[:64] if topic else None
    limit_type = (limit_type or "").strip()[:16]
    now_iso = dt.datetime.utcnow().isoformat()
    cur.execute(
        """
        UPDATE users
        SET last_limit_topic = ?,
            last_limit_type = ?,
            last_limit_at = ?
        WHERE user_id = ?
        """,
        (t, limit_type, now_iso, user_id),
    )
    conn.commit()


def set_last_paywall_text(user_id: int, text: str) -> None:
    get_user(user_id)
    txt = (text or "").strip()
    if len(txt) > 900:
        txt = txt[:900]
    now_iso = dt.datetime.utcnow().isoformat()
    cur.execute(
        """
        UPDATE users
        SET last_paywall_text = ?,
            last_paywall_at = ?
        WHERE user_id = ?
        """,
        (txt, now_iso, user_id),
    )
    conn.commit()


def get_user_memory_snapshot(user_id: int) -> Dict[str, Any]:
    get_user(user_id)
    cur.execute(
        """
        SELECT last_topic, last_user_message, last_bot_message,
               last_followup_text, last_followup_type, last_followup_topic, last_limit_topic,
               last_limit_type, last_limit_at,
               last_paywall_text, last_paywall_at,
               last_tarot_meta, last_tarot_at
        FROM users
        WHERE user_id = ?
        """,
        (user_id,),
    )
    row = cur.fetchone()
    if not row:
        return {}
    return {
        "last_topic": row[0],
        "last_user_message": row[1],
        "last_bot_message": row[2],
        "last_followup_text": row[3],
        "last_followup_type": row[4],
        "last_followup_topic": row[5],
        "last_limit_topic": row[6],
        "last_limit_type": row[7],
        "last_limit_at": row[8],
        "last_paywall_text": row[9],
        "last_paywall_at": row[10],
        "last_tarot_meta": row[11],
        "last_tarot_at": row[12],
    }


# =========================================================
