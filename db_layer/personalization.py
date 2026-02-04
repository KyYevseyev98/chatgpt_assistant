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
from .memory import get_user_memory_snapshot
from .profiles import get_user_profile_snapshot
from .tarot_limits import get_tarot_limits_snapshot


# PERSONALIZATION SNAPSHOT (как было) + добавили chat_id
# =========================================================
def get_followup_personalization_snapshot(user_id: int, chat_id: Optional[int] = None) -> Dict[str, Any]:
    """
    Возвращает общую "сборку" для персонализации:
      - память users (last_* поля)
      - legacy профайл user_profiles (сегменты/счетчики)
      - tarot лимиты
      - NEW ТЗ-профиль по (user_id, chat_id), если chat_id указан
    """
    mem = get_user_memory_snapshot(user_id)
    prof = get_user_profile_snapshot(user_id, chat_id=chat_id)
    tarot = get_tarot_limits_snapshot(user_id, chat_id or 0)

    out: Dict[str, Any] = {}
    out.update(mem or {})
    out.update(prof or {})
    out.update(tarot or {})

    if chat_id is not None:
        try:
            out["chat_profile"] = get_user_profile_chat(user_id, int(chat_id)) or {}
        except Exception:
            out["chat_profile"] = {}

    return out

def prune_messages(user_id: int, chat_id: int, keep: int = None) -> None:
    """Ограничиваем рост таблицы messages: оставляем последние keep сообщений на (user_id,chat_id)."""
    if keep is None:
        keep = int(MAX_DB_MESSAGES_PER_CHAT or 200)
    keep = max(50, int(keep))
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            DELETE FROM messages
            WHERE id NOT IN (
                SELECT id FROM messages
                WHERE user_id=? AND chat_id=?
                ORDER BY id DESC
                LIMIT ?
            )
            AND user_id=? AND chat_id=?
            """,
            (user_id, chat_id, keep, user_id, chat_id),
        )
        conn.commit()


def add_tarot_history(
    user_id: int,
    chat_id: int,
    question: str,
    spread_name: str,
    cards_meta: list,
    answer_excerpt: str = "",
) -> None:
    """Пишем запись расклада в tarot_history и подрезаем до MAX_TAROT_HISTORY_PER_USER."""
    # В проекте datetime импортируется как dt, поэтому используем dt.datetime
    created_at = dt.datetime.utcnow().isoformat(timespec="seconds")
    meta_json = json.dumps(cards_meta or [], ensure_ascii=False)
    excerpt = (answer_excerpt or "")[:1200]

    with _connect() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO tarot_history(user_id, chat_id, created_at, question, spread_name, cards_meta, answer_excerpt)
            VALUES(?, ?, ?, ?, ?, ?, ?)
            """,
            (user_id, chat_id, created_at, question or "", spread_name or "", meta_json, excerpt),
        )
        conn.commit()

    prune_tarot_history(user_id, keep=int(MAX_TAROT_HISTORY_PER_USER or 100))


def prune_tarot_history(user_id: int, keep: int = None) -> None:
    if keep is None:
        keep = int(MAX_TAROT_HISTORY_PER_USER or 100)
    keep = max(10, int(keep))
    with _connect() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            DELETE FROM tarot_history
            WHERE id NOT IN (
                SELECT id FROM tarot_history
                WHERE user_id=?
                ORDER BY id DESC
                LIMIT ?
            )
            AND user_id=?
            """,
            (user_id, keep, user_id),
        )
        conn.commit()


def get_last_tarot_history(user_id: int, chat_id: int = None, limit: int = None):
    """Возвращает последние расклады (для контекста)."""
    if limit is None:
        limit = int(MAX_TAROT_HISTORY_PER_USER or 100)
    limit = max(1, int(limit))
    with _connect() as conn:
        cur = conn.cursor()
        if chat_id is None:
            cur.execute(
                """
                SELECT created_at, question, spread_name, cards_meta, answer_excerpt
                FROM tarot_history
                WHERE user_id=?
                ORDER BY id DESC
                LIMIT ?
                """,
                (user_id, limit),
            )
        else:
            cur.execute(
                """
                SELECT created_at, question, spread_name, cards_meta, answer_excerpt
                FROM tarot_history
                WHERE user_id=? AND chat_id=?
                ORDER BY id DESC
                LIMIT ?
                """,
                (user_id, chat_id, limit),
            )
        rows = cur.fetchall() or []

    out = []
    for r in rows:
        out.append(
            {
                "created_at": r[0],
                "question": r[1],
                "spread_name": r[2],
                "cards_meta": r[3],
                "answer_excerpt": r[4],
            }
        )
    return out
