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
from .users import _ensure_user_profile
from .user_profile import _safe_load_json


# PROFILES (legacy user_profiles) - как было
# (оставляем, чтобы не ломать текущие вызовы)
# + добавили optional chat_id, чтобы дальше ты мог разрулить "не мешать чаты"
# =========================================================
def update_user_profile_on_event(
    user_id: int,
    event_type: str,
    *,
    lang: Optional[str] = None,
    segments: Optional[List[str]] = None,
    segment_scores: Optional[Dict[str, float]] = None,
    topic: Optional[str] = None,
    pro_payment_increment: int = 0,
    last_limit_type: Optional[str] = None,
    chat_id: Optional[int] = None,  # NEW optional (не ломает старые импорты)
) -> None:
    """
    legacy-аналитика/сегменты. НЕ ТЗ-профиль.
    chat_id здесь пока не используется (оставлен под будущую развязку по чатам).
    """
    _ensure_user_profile(user_id)

    cur.execute(
        """
        SELECT segments, segments_json, topic_counts_json,
               total_messages, total_photos, total_voice,
               pro_payments_count,
               last_limit_type, last_lang,
               profile_updated_at, messages_since_profile_update
        FROM user_profiles
        WHERE user_id = ?
        """,
        (user_id,),
    )
    row = cur.fetchone()
    if not row:
        return

    (
        segments_str,
        segments_json_str,
        topic_counts_json_str,
        total_messages,
        total_photos,
        total_voice,
        pro_payments_count,
        last_limit_type_db,
        last_lang_db,
        profile_updated_at,
        msgs_since,
    ) = row

    total_messages = total_messages or 0
    total_photos = total_photos or 0
    total_voice = total_voice or 0
    pro_payments_count = pro_payments_count or 0
    msgs_since = msgs_since or 0

    if event_type == "text":
        total_messages += 1
        msgs_since += 1
    elif event_type == "photo":
        total_photos += 1
    elif event_type == "voice":
        total_voice += 1

    if pro_payment_increment:
        pro_payments_count += pro_payment_increment

    if last_limit_type is not None:
        last_limit_type_db = last_limit_type

    if lang:
        last_lang_db = lang

    existing_segments = [s for s in (segments_str or "").split(",") if s.strip()]
    if segments:
        for s in segments:
            s = (s or "").strip()
            if s and s not in existing_segments:
                existing_segments.append(s)
    new_segments_str = ",".join(existing_segments)

    seg_map: Dict[str, float] = _safe_load_json(segments_json_str, {})
    if segment_scores:
        for k, v in segment_scores.items():
            if not k:
                continue
            try:
                val = float(v)
            except Exception:
                continue
            seg_map[k] = max(seg_map.get(k, 0.0), min(1.0, val))

    topic_counts: Dict[str, int] = _safe_load_json(topic_counts_json_str, {})
    if topic and event_type == "text":
        t = topic.strip()
        if t:
            topic_counts[t] = int(topic_counts.get(t, 0) or 0) + 1

    cur.execute(
        """
        UPDATE user_profiles
        SET segments = ?,
            segments_json = ?,
            topic_counts_json = ?,
            total_messages = ?,
            total_photos = ?,
            total_voice = ?,
            pro_payments_count = ?,
            last_limit_type = ?,
            last_lang = ?,
            profile_updated_at = ?,
            messages_since_profile_update = ?
        WHERE user_id = ?
        """,
        (
            new_segments_str,
            json.dumps(seg_map, ensure_ascii=False) if seg_map else None,
            json.dumps(topic_counts, ensure_ascii=False) if topic_counts else None,
            total_messages,
            total_photos,
            total_voice,
            pro_payments_count,
            last_limit_type_db,
            last_lang_db,
            profile_updated_at,
            msgs_since,
            user_id,
        ),
    )
    conn.commit()


def get_user_profile_snapshot(user_id: int, chat_id: Optional[int] = None) -> Dict[str, Any]:
    """
    legacy-снимок (сегменты/счетчики) + traffic_source.
    chat_id пока не участвует — добавлен для совместимости с будущей развязкой.
    """
    _ensure_user_profile(user_id)

    cur.execute(
        """
        SELECT segments,
               segments_json,
               topic_counts_json,
               total_messages,
               total_photos,
               total_voice,
               pro_payments_count,
               last_limit_type,
               last_lang,
               profile_updated_at,
               messages_since_profile_update
        FROM user_profiles
        WHERE user_id = ?
        """,
        (user_id,),
    )
    row = cur.fetchone()
    if not row:
        return {}

    (
        segments_str,
        segments_json_str,
        topic_counts_json_str,
        total_messages,
        total_photos,
        total_voice,
        pro_payments_count,
        last_limit_type,
        last_lang,
        profile_updated_at,
        messages_since_profile_update,
    ) = row

    cur.execute("SELECT traffic_source FROM users WHERE user_id = ?", (user_id,))
    row2 = cur.fetchone()
    traffic_source = row2[0] if row2 else None

    segments_list = [s for s in (segments_str or "").split(",") if s.strip()]
    segments_json = _safe_load_json(segments_json_str, {})
    topic_counts = _safe_load_json(topic_counts_json_str, {})

    return {
        "segments": segments_list,
        "segments_json": segments_json,
        "topic_counts": topic_counts,
        "total_messages": total_messages or 0,
        "total_photos": total_photos or 0,
        "total_voice": total_voice or 0,
        "pro_payments_count": pro_payments_count or 0,
        "last_limit_type": last_limit_type,
        "last_lang": last_lang,
        "traffic_source": traffic_source,
        "profile_updated_at": profile_updated_at,
        "messages_since_profile_update": messages_since_profile_update or 0,
    }


# =========================================================
