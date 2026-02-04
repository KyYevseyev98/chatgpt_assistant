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
from .events import log_event


# TAROT LIMITS + MEMORY (как было)
# =========================================================
def set_last_tarot_meta(user_id: int, meta: Dict[str, Any]) -> None:
    get_user(user_id)
    now_iso = dt.datetime.utcnow().isoformat()
    try:
        txt = json.dumps(meta or {}, ensure_ascii=False)
    except Exception:
        txt = "{}"
    if len(txt) > 2000:
        txt = txt[:2000]
    cur.execute(
        """
        UPDATE users
        SET last_tarot_meta = ?,
            last_tarot_at = ?
        WHERE user_id = ?
        """,
        (txt, now_iso, user_id),
    )
    conn.commit()


def _count_tarot_readings_lifetime(user_id: int) -> int:
    cur.execute(
        """
        SELECT COUNT(1)
        FROM events
        WHERE user_id = ?
          AND event_type = 'tarot_reading'
          AND is_pro = 0
        """,
        (user_id,),
    )
    row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def _count_tarot_readings_today(user_id: int) -> int:
    today = today_iso()
    cur.execute(
        """
        SELECT COUNT(1)
        FROM events
        WHERE user_id = ?
          AND event_type = 'tarot_reading'
          AND is_pro = 0
          AND substr(created_at, 1, 10) = ?
        """,
        (user_id, today),
    )
    row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def get_tarot_limits_snapshot(user_id: int, chat_id: int) -> Dict[str, Any]:
    from .billing import get_billing_snapshot
    snap = get_billing_snapshot(user_id, chat_id)
    return {
        "tarot_free_lifetime_used": snap.get("tarot_free_used", 0),
        "tarot_free_lifetime_left": snap.get("tarot_free_left", 0),
        "tarot_credits": snap.get("tarot_credits", 0),
    }


def check_tarot_limits(user_id: int, chat_id: int) -> Tuple[bool, str]:
    from .billing import can_start_tarot, get_billing_snapshot
    if can_start_tarot(user_id, chat_id):
        return True, ""
    snap = get_billing_snapshot(user_id, chat_id)
    left = int(snap.get("tarot_free_left") or 0)
    credits = int(snap.get("tarot_credits") or 0)
    if credits <= 0 and left <= 0:
        return False, "Бесплатные расклады уже использованы 🙏"
    return False, "Лимит раскладов исчерпан 🙏"


def log_tarot_reading(
    user_id: int,
    *,
    question: str,
    spread_name: str,
    cards_meta: List[Dict[str, Any]],
    lang: str = "ru",
    meta_extra: Optional[str] = None,
) -> None:
    meta_obj = {"spread": spread_name, "cards": cards_meta}
    if meta_extra:
        meta_obj["extra"] = meta_extra

    try:
        meta_str = json.dumps(meta_obj, ensure_ascii=False)
    except Exception:
        meta_str = meta_extra or None

    log_event(
        user_id,
        "tarot_reading",
        tokens=min(9999, len((question or "").strip())),
        meta=meta_str,
        lang=lang,
        topic="tarot",
        segments=["tarot"],
        segment_scores={"tarot": 1.0},
    )

    set_last_tarot_meta(
        user_id,
        {
            "question": (question or "").strip()[:500],
            "spread": (spread_name or "").strip()[:64],
            "cards": (cards_meta or [])[:10],
        },
    )


# =========================================================
