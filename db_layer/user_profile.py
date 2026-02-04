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


# NEW (ТЗ): USER PROFILE per (user_id, chat_id)
# =========================================================
def _safe_load_json(s: Optional[str], fallback):
    if not s:
        return fallback
    try:
        return json.loads(s)
    except Exception:
        return fallback


def get_user_profile_chat(
    user_id: int,
    chat_id: int,
) -> Dict[str, Any]:
    """
    ТЗ-профиль: хранится отдельно по каждому chat_id.
    Возвращает dict (json_profile) или {}.
    """
    try:
        user_id = int(user_id)
        chat_id = int(chat_id)
    except Exception:
        return {}

    try:
        cur.execute(
            """
            SELECT json_profile
            FROM user_profile
            WHERE user_id = ? AND chat_id = ?
            """,
            (user_id, chat_id),
        )
        row = cur.fetchone()
    except Exception:
        row = None

    if not row or not row[0]:
        return {}

    return _safe_load_json(row[0], {}) or {}


def patch_user_profile_chat(
    user_id: int,
    chat_id: int,
    patch: Optional[Dict[str, Any]] = None,
    delete_keys: Optional[List[str]] = None,
) -> None:
    """Частично обновляет json_profile для (user_id, chat_id).

    Удобно для хранения краткоживущего состояния (например, «ждём уточнение»).
    """
    patch = patch or {}
    delete_keys = delete_keys or []

    profile = get_user_profile_chat(user_id, chat_id) or {}

    for k in delete_keys:
        if k in profile:
            del profile[k]

    for k, v in patch.items():
        profile[k] = v

    upsert_user_profile_chat(user_id, chat_id, profile)


def upsert_user_profile_chat(
    user_id: int,
    chat_id: int,
    profile: Dict[str, Any],
) -> None:
    """
    Полная запись json_profile для (user_id, chat_id).
    """
    try:
        user_id = int(user_id)
        chat_id = int(chat_id)
    except Exception:
        return

    now_iso = dt.datetime.utcnow().isoformat()

    try:
        txt = json.dumps(profile or {}, ensure_ascii=False)
    except Exception:
        txt = "{}"

    try:
        cur.execute(
            """
            INSERT INTO user_profile (user_id, chat_id, json_profile, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id, chat_id) DO UPDATE SET
                json_profile = excluded.json_profile,
                updated_at = excluded.updated_at
            """,
            (user_id, chat_id, txt, now_iso),
        )
        conn.commit()
    except Exception:
        pass


def update_user_profile_chat_if_new_facts(
    user_id: int,
    chat_id: int,
    new_facts: Dict[str, Any],
) -> bool:
    """
    ТЗ: профиль обновляется НЕ на каждый чих, а когда появились новые факты.
    Логика:
      - если ключа не было -> добавить
      - если значение изменилось -> обновить
      - пустые/None -> игнор
    Возвращает True если реально обновили.
    """
    if not new_facts or not isinstance(new_facts, dict):
        return False

    cur_prof = get_user_profile_chat(user_id, chat_id) or {}
    changed = False

    for k, v in new_facts.items():
        if not k:
            continue
        if v is None:
            continue
        # убираем пустые строки
        if isinstance(v, str) and not v.strip():
            continue

        if k not in cur_prof:
            cur_prof[k] = v
            changed = True
        else:
            if cur_prof.get(k) != v:
                cur_prof[k] = v
                changed = True

    if changed:
        upsert_user_profile_chat(user_id, chat_id, cur_prof)
    return changed


# =========================================================
