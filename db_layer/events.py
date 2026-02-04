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

from .users import get_user, _pro_active
from .profiles import update_user_profile_on_event


# EVENTS + PAYMENTS (legacy) - как было
# + добавили optional chat_id, чтобы можно было логировать без смешивания
# =========================================================
def log_event(
    user_id: int,
    event_type: str,
    *,
    tokens: Optional[int] = None,
    meta: Optional[str] = None,
    last_limit_type: Optional[str] = None,
    lang: Optional[str] = None,
    topic: Optional[str] = None,
    segments: Optional[List[str]] = None,
    segment_scores: Optional[Dict[str, float]] = None,
    chat_id: Optional[int] = None,  # NEW optional
) -> None:
    (
        _uid, _used_text, _last_date, _is_pro, _used_photos,
        pro_until, *_rest
    ) = get_user(user_id)
    is_pro_active = 1 if _pro_active(pro_until) else 0

    created_at = dt.datetime.utcnow().isoformat()

    cur.execute(
        """
        INSERT INTO events (user_id, event_type, tokens, is_pro, meta, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (user_id, event_type, tokens, is_pro_active, meta, created_at),
    )
    conn.commit()

    # prune events table (bounded growth)
    try:
        cur.execute("DELETE FROM events WHERE id NOT IN (SELECT id FROM events ORDER BY id DESC LIMIT ?)", (MAX_EVENTS_ROWS,))
        conn.commit()
    except Exception:
        pass

    update_user_profile_on_event(
        user_id,
        event_type,
        lang=lang,
        segments=segments,
        segment_scores=segment_scores,
        topic=topic,
        last_limit_type=last_limit_type,
        chat_id=chat_id,
    )


def log_pro_payment(user_id: int, stars: int, days: int) -> None:
    cur.execute("SELECT traffic_source FROM users WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    traffic_source = row[0] if row and row[0] is not None else None

    created_at = dt.datetime.utcnow().isoformat()

    cur.execute(
        """
        INSERT INTO pro_payments (user_id, stars, days, traffic_source, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (user_id, stars, days, traffic_source, created_at),
    )
    conn.commit()

    # prune events table (bounded growth)
    try:
        cur.execute("DELETE FROM events WHERE id NOT IN (SELECT id FROM events ORDER BY id DESC LIMIT ?)", (MAX_EVENTS_ROWS,))
        conn.commit()
    except Exception:
        pass

    update_user_profile_on_event(
        user_id,
        "payment",
        pro_payment_increment=1,
    )


# =========================================================
