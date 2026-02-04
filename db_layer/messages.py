from __future__ import annotations

import datetime as dt
import json
import logging
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

from config import (
    DB_PATH,
    FREE_TEXT_LIMIT_PER_DAY,
    FREE_PHOTO_LIMIT_PER_DAY,
    today_iso,
    FREE_TAROT_LIFETIME,
    FREE_TAROT_LIMIT_PER_DAY,
    MAX_DB_MESSAGES_PER_CHAT,
    MAX_TAROT_HISTORY_PER_USER,
    MAX_MESSAGE_CHARS_DB,
)

from .connection import conn, cur, MAX_EVENTS_ROWS
from .personalization import prune_messages

logger = logging.getLogger(__name__)


def _log_exception(message: str) -> None:
    logger.debug(message, exc_info=True)


# NEW (ТЗ): MESSAGES TABLE
# =========================================================
def add_message(
    user_id: int,
    chat_id: int,
    role: str,
    text: str,
    ts_iso: Optional[str] = None,
) -> None:
    """
    Сохраняет единичное сообщение в messages (ТЗ).
    role: "user" | "assistant" | "system"
    """
    try:
        user_id = int(user_id)
        chat_id = int(chat_id)
    except Exception:
        return

    role = (role or "").strip().lower()[:16] or "user"
    if role not in ("user", "assistant", "system"):
        role = "user"

    txt = (text or "").strip()
    if not txt:
        return
    if MAX_MESSAGE_CHARS_DB:
        txt = txt[: int(MAX_MESSAGE_CHARS_DB)]

    ts = ts_iso or dt.datetime.utcnow().isoformat()

    try:
        cur.execute(
            """
            INSERT INTO messages (user_id, chat_id, role, text, ts)
            VALUES (?, ?, ?, ?, ?)
            """,
            (user_id, chat_id, role, txt, ts),
        )
        conn.commit()

        # удерживаем размер messages под контролем
        try:
            prune_messages(user_id, chat_id, keep=MAX_DB_MESSAGES_PER_CHAT)
        except Exception:
            _log_exception("suppressed exception")
    except Exception:
        _log_exception("suppressed exception")


def get_last_messages(
    user_id: int,
    chat_id: int,
    limit: int = 20,
) -> List[Dict[str, str]]:
    """
    Возвращает историю для модели: [{"role":"user","content":"..."}...]
    """
    try:
        user_id = int(user_id)
        chat_id = int(chat_id)
        limit = max(1, min(int(limit or 20), 200))
    except Exception:
        return []

    try:
        cur.execute(
            """
            SELECT role, text
            FROM messages
            WHERE user_id = ? AND chat_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (user_id, chat_id, limit),
        )
        rows = cur.fetchall() or []
    except Exception:
        return []

    # в модель — в правильном порядке (старые -> новые)
    rows.reverse()
    out: List[Dict[str, str]] = []
    for role, text in rows:
        r = (role or "").strip().lower()
        if r not in ("user", "assistant", "system"):
            r = "user"
        out.append({"role": r, "content": (text or "").strip()})
    return out


# =========================================================
