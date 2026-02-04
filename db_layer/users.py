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


# USERS (legacy) + LIMITS
# =========================================================
def _ensure_user_profile(user_id: int) -> None:
    cur.execute(
        """
        INSERT OR IGNORE INTO user_profiles (
            user_id,
            segments, segments_json,
            topic_counts_json,
            total_messages, total_photos, total_voice,
            pro_payments_count,
            last_limit_type, last_lang,
            profile_updated_at,
            messages_since_profile_update
        )
        VALUES (?, '', NULL, NULL, 0, 0, 0, 0, NULL, NULL, NULL, 0)
        """,
        (user_id,),
    )
    conn.commit()


def get_user(
    user_id: int,
) -> Tuple[
    int, int, str, int, int,
    Optional[str], Optional[str], Optional[str], int
]:
    """
    Возвращает:
    (user_id, free_used_today, last_reset_date, is_pro, free_photos_used_today,
     pro_until, last_activity_at, last_followup_at, followup_stage)
    """
    if user_id is None:
        raise ValueError("user_id is required")
    user_id = int(user_id)

    SELECT_SQL = """
        SELECT user_id,
               free_used_today,
               last_reset_date,
               is_pro,
               free_photos_used_today,
               pro_until,
               last_activity_at,
               last_followup_at,
               followup_stage
        FROM users
        WHERE user_id = ?
    """

    cur.execute(SELECT_SQL, (user_id,))
    row = cur.fetchone()

    if row is None:
        today = today_iso()
        now_iso = dt.datetime.utcnow().isoformat()

        cur.execute(
            """
            INSERT OR IGNORE INTO users (
                user_id,
                free_used_today,
                last_reset_date,
                is_pro,
                free_photos_used_today,
                pro_until,
                traffic_source,

                last_activity_at,
                last_followup_at,
                followup_stage,

                last_topic,
                last_user_message,
                last_bot_message,
                last_followup_text,
                last_followup_type,
                last_followup_topic,
                last_limit_topic,

                last_limit_type,
                last_limit_at,
                last_paywall_text,
                last_paywall_at,

                last_tarot_meta,
                last_tarot_at,
                created_at,
                username,
                first_name,
                last_name,
                is_blocked
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?,
                ?, ?, ?, ?, ?
            )
            """,
            (
                user_id,
                0,
                today,
                0,
                0,
                None,
                None,  # traffic_source

                None,  # last_activity_at
                None,  # last_followup_at
                0,     # followup_stage

                None,  # last_topic
                None,  # last_user_message
                None,  # last_bot_message
                None,  # last_followup_text
                None,  # last_followup_type
                None,  # last_followup_topic
                None,  # last_limit_topic

                None,  # last_limit_type
                None,  # last_limit_at
                None,  # last_paywall_text
                None,  # last_paywall_at

                None,  # last_tarot_meta
                None,  # last_tarot_at
                now_iso,
                None,
                None,
                None,
                0,
            ),
        )
        conn.commit()

        _ensure_user_profile(user_id)

        cur.execute(SELECT_SQL, (user_id,))
        row = cur.fetchone()

        if row is None:
            return (user_id, 0, today, 0, 0, None, None, None, 0)

        return row

    _ensure_user_profile(user_id)
    return row


def update_user(
    user_id: int,
    used_text: int,
    last_date: str,
    is_pro: int,
    used_photos: int,
    pro_until: Optional[str],
    last_activity_at: Optional[str],
    last_followup_at: Optional[str],
    followup_stage: int,
) -> None:
    cur.execute(
        """
        UPDATE users
        SET free_used_today = ?,
            last_reset_date = ?,
            is_pro = ?,
            free_photos_used_today = ?,
            pro_until = ?,
            last_activity_at = ?,
            last_followup_at = ?,
            followup_stage = ?
        WHERE user_id = ?
        """,
        (
            used_text, last_date, is_pro, used_photos,
            pro_until, last_activity_at, last_followup_at, followup_stage,
            user_id,
        ),
    )
    conn.commit()


def _pro_active(pro_until: Optional[str]) -> bool:
    if not pro_until:
        return False
    try:
        dt_until = dt.datetime.fromisoformat(pro_until)
    except ValueError:
        return False
    return dt_until > dt.datetime.utcnow()


def set_traffic_source(user_id: int, source: str) -> None:
    get_user(user_id)
    cur.execute(
        """
        UPDATE users
        SET traffic_source = COALESCE(traffic_source, ?)
        WHERE user_id = ?
        """,
        (source, user_id),
    )
    conn.commit()


def update_user_identity(
    user_id: int,
    *,
    username: Optional[str],
    first_name: Optional[str],
    last_name: Optional[str],
) -> None:
    try:
        user_id = int(user_id)
    except Exception:
        return
    cur.execute(
        """
        UPDATE users
        SET username = ?,
            first_name = ?,
            last_name = ?
        WHERE user_id = ?
        """,
        (
            (username or "").strip() or None,
            (first_name or "").strip() or None,
            (last_name or "").strip() or None,
            user_id,
        ),
    )
    conn.commit()


def set_user_blocked(user_id: int, blocked: bool) -> None:
    try:
        user_id = int(user_id)
    except Exception:
        return
    cur.execute(
        """
        UPDATE users
        SET is_blocked = ?
        WHERE user_id = ?
        """,
        (1 if blocked else 0, user_id),
    )
    conn.commit()


def is_user_blocked(user_id: int) -> bool:
    try:
        user_id = int(user_id)
    except Exception:
        return False
    cur.execute("SELECT is_blocked FROM users WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    return bool(row and int(row[0] or 0) == 1)


def get_user_by_username(username: str):
    uname = (username or "").strip().lstrip("@").lower()
    if not uname:
        return None
    cur.execute(
        """
        SELECT user_id, username, first_name, last_name, created_at, last_activity_at, is_blocked
        FROM users
        WHERE lower(username) = ?
        """,
        (uname,),
    )
    return cur.fetchone()


def set_pro(user_id: int, days: int) -> None:
    (
        _uid, used_text, last_date, _is_pro, used_photos,
        pro_until, last_activity_at, last_followup_at, followup_stage
    ) = get_user(user_id)

    now = dt.datetime.utcnow()
    if _pro_active(pro_until):
        try:
            base = dt.datetime.fromisoformat(pro_until)
        except ValueError:
            base = now
    else:
        base = now

    new_until_dt = base + dt.timedelta(days=days)
    new_until = new_until_dt.isoformat()

    update_user(
        user_id,
        used_text,
        last_date,
        1,
        used_photos,
        new_until,
        last_activity_at,
        last_followup_at,
        followup_stage,
    )


def check_limit(user_id: int, chat_id: int, is_photo: bool = False) -> bool:
    """
    Лимит по сообщениям отключён. Контроль ведём только по раскладам.
    """
    return True


def touch_last_activity(user_id: int) -> None:
    (
        _uid, used_text, last_date, is_pro, used_photos,
        pro_until, _last_activity_at, last_followup_at, _followup_stage
    ) = get_user(user_id)

    now_iso = dt.datetime.utcnow().isoformat()
    update_user(
        user_id,
        used_text,
        last_date,
        is_pro,
        used_photos,
        pro_until,
        now_iso,
        last_followup_at,
        0,
    )


def mark_followup_sent(user_id: int) -> None:
    (
        _uid, used_text, last_date, is_pro, used_photos,
        pro_until, last_activity_at, _last_followup_at, followup_stage
    ) = get_user(user_id)

    now_iso = dt.datetime.utcnow().isoformat()
    update_user(
        user_id,
        used_text,
        last_date,
        is_pro,
        used_photos,
        pro_until,
        last_activity_at,
        now_iso,
        followup_stage + 1,
    )


def get_followup_state(user_id: int):
    row = get_user(user_id)
    return row[6], row[7], row[8]


def get_all_users_for_followup():
    cur.execute(
        """
        SELECT user_id,
               last_activity_at,
               last_followup_at,
               followup_stage
        FROM users
        WHERE last_activity_at IS NOT NULL
        """
    )
    return cur.fetchall()


# =========================================================
