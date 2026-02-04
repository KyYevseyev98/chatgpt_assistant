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


# SALES TRIGGERS + PAYWALL DEDUP
# =========================================================
def should_soft_upsell(user_id: int) -> bool:
    prof = get_user_profile_snapshot(user_id)
    if not prof:
        return False
    if prof.get("pro_payments_count", 0) > 0:
        return False
    total_msgs = prof.get("total_messages", 0)
    return total_msgs >= 20 and total_msgs % 5 == 0


def should_send_limit_paywall(user_id: int, new_text: str) -> bool:
    """
    Защита от спама: если paywall тот же самый недавно — не дублируем.
    """
    mem = get_user_memory_snapshot(user_id)
    last_text = (mem.get("last_paywall_text") or "").strip()
    last_at = mem.get("last_paywall_at")

    if not new_text:
        return False

    if last_text and last_text == new_text.strip():
        return False

    if last_at:
        try:
            dt_last = dt.datetime.fromisoformat(last_at)
            if (dt.datetime.utcnow() - dt_last).total_seconds() < 120:
                return False
        except Exception:
            pass

    return True


# =========================================================
