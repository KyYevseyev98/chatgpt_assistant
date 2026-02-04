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


# INIT + MIGRATIONS
# =========================================================
def init_db() -> None:
    """
    Создаёт таблицы:
      - users (как было, для лимитов/платежей/фоллоуапов)
      - events (как было, для аналитики)
      - pro_payments (как было)
      - user_profiles (как было, для сегментов/счетчиков)
      - messages (НОВОЕ ТЗ: история сообщений)
      - user_profile (НОВОЕ ТЗ: профиль по user_id+chat_id)
    И выполняет безопасные миграции.
    """
    # --- users (legacy/compat) ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            free_used_today INTEGER DEFAULT 0,
            last_reset_date TEXT,
            is_pro INTEGER DEFAULT 0,
            free_photos_used_today INTEGER DEFAULT 0,
            pro_until TEXT,
            traffic_source TEXT,

            last_activity_at TEXT,
            last_followup_at TEXT,
            followup_stage INTEGER DEFAULT 0,

            last_topic TEXT,
            last_user_message TEXT,
            last_bot_message TEXT,
            last_followup_text TEXT,
            last_followup_type TEXT,
            last_followup_topic TEXT,
            last_limit_topic TEXT,

            last_limit_type TEXT,
            last_limit_at TEXT,
            last_paywall_text TEXT,
            last_paywall_at TEXT,

            last_tarot_meta TEXT,
            last_tarot_at TEXT
        )
        """
    )
    conn.commit()

    # миграции users
    cur.execute("PRAGMA table_info(users)")
    cols = [row[1] for row in cur.fetchall()]

    def _add_user_col(name: str, ddl: str) -> None:
        if name not in cols:
            cur.execute(f"ALTER TABLE users ADD COLUMN {ddl}")
            conn.commit()

    _add_user_col("free_photos_used_today", "free_photos_used_today INTEGER DEFAULT 0")
    _add_user_col("pro_until", "pro_until TEXT")
    _add_user_col("traffic_source", "traffic_source TEXT")

    _add_user_col("last_activity_at", "last_activity_at TEXT")
    _add_user_col("last_followup_at", "last_followup_at TEXT")
    _add_user_col("followup_stage", "followup_stage INTEGER DEFAULT 0")

    _add_user_col("last_topic", "last_topic TEXT")
    _add_user_col("last_user_message", "last_user_message TEXT")
    _add_user_col("last_bot_message", "last_bot_message TEXT")
    _add_user_col("last_followup_text", "last_followup_text TEXT")
    _add_user_col("last_followup_type", "last_followup_type TEXT")
    _add_user_col("last_followup_topic", "last_followup_topic TEXT")
    _add_user_col("last_limit_topic", "last_limit_topic TEXT")

    _add_user_col("last_limit_type", "last_limit_type TEXT")
    _add_user_col("last_limit_at", "last_limit_at TEXT")
    _add_user_col("last_paywall_text", "last_paywall_text TEXT")
    _add_user_col("last_paywall_at", "last_paywall_at TEXT")

    _add_user_col("last_tarot_meta", "last_tarot_meta TEXT")
    _add_user_col("last_tarot_at", "last_tarot_at TEXT")
    _add_user_col("created_at", "created_at TEXT")
    _add_user_col("username", "username TEXT")
    _add_user_col("first_name", "first_name TEXT")
    _add_user_col("last_name", "last_name TEXT")
    _add_user_col("is_blocked", "is_blocked INTEGER DEFAULT 0")

    # --- events (legacy/compat) ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            tokens INTEGER,
            is_pro INTEGER DEFAULT 0,
            meta TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.commit()

    # миграции events
    cur.execute("PRAGMA table_info(events)")
    event_cols = [row[1] for row in cur.fetchall()]

    def _add_event_col(name: str, ddl: str) -> None:
        if name not in event_cols:
            cur.execute(f"ALTER TABLE events ADD COLUMN {ddl}")
            conn.commit()

    _add_event_col("tokens", "tokens INTEGER")
    _add_event_col("is_pro", "is_pro INTEGER DEFAULT 0")
    _add_event_col("meta", "meta TEXT")
    _add_event_col("created_at", "created_at TEXT")

    # --- pro_payments (legacy/compat) ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS pro_payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            stars INTEGER NOT NULL,
            days INTEGER NOT NULL,
            traffic_source TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.commit()

    # --- user_profiles (legacy/compat, сегменты/счетчики) ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS user_profiles (
            user_id INTEGER PRIMARY KEY,

            segments TEXT,
            segments_json TEXT,

            topic_counts_json TEXT,
            total_messages INTEGER DEFAULT 0,
            total_photos INTEGER DEFAULT 0,
            total_voice INTEGER DEFAULT 0,
            pro_payments_count INTEGER DEFAULT 0,

            last_limit_type TEXT,
            last_lang TEXT,

            profile_updated_at TEXT,
            messages_since_profile_update INTEGER DEFAULT 0
        )
        """
    )
    conn.commit()

    cur.execute("PRAGMA table_info(user_profiles)")
    prof_cols = [row[1] for row in cur.fetchall()]

    def _add_prof_col(name: str, ddl: str) -> None:
        if name not in prof_cols:
            cur.execute(f"ALTER TABLE user_profiles ADD COLUMN {ddl}")
            conn.commit()

    _add_prof_col("segments", "segments TEXT")
    _add_prof_col("segments_json", "segments_json TEXT")
    _add_prof_col("topic_counts_json", "topic_counts_json TEXT")
    _add_prof_col("profile_updated_at", "profile_updated_at TEXT")
    _add_prof_col(
        "messages_since_profile_update",
        "messages_since_profile_update INTEGER DEFAULT 0",
    )

    # =========================================================
    # NEW (ТЗ): messages + user_profile (per chat)
    # =========================================================

    # messages: user_id, chat_id, role, text, ts
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            chat_id INTEGER NOT NULL,
            role TEXT NOT NULL,          -- "user" | "assistant" | "system"
            text TEXT NOT NULL,
            ts TEXT NOT NULL
        )
        """
    )
    conn.commit()

    # user_profile: user_id, chat_id, json_profile, updated_at
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS user_profile (
            user_id INTEGER NOT NULL,
            chat_id INTEGER NOT NULL,
            json_profile TEXT,
            updated_at TEXT,
            PRIMARY KEY (user_id, chat_id)
        )
        """
    )
    conn.commit()

    # tarot_history: хранение последних раскладов (ограничено по размеру)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS tarot_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            chat_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            question TEXT,
            spread_name TEXT,
            cards_meta TEXT,
            answer_excerpt TEXT
        )
        """
    )
    conn.commit()

    # support actions (admin balance adjustments)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS support_actions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            admin_id INTEGER,
            delta INTEGER NOT NULL,
            reason TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.commit()

    # api errors (miniapp/api diagnostics)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS api_errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            endpoint TEXT NOT NULL,
            status_code INTEGER NOT NULL,
            error_text TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


    _create_indexes()


def _create_indexes() -> None:
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_events_user_created ON events(user_id, created_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_events_type_created ON events(event_type, created_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_propay_user_created ON pro_payments(user_id, created_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_users_last_activity ON users(last_activity_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_users_last_followup ON users(last_followup_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_support_actions_user_created ON support_actions(user_id, created_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_api_errors_user_created ON api_errors(user_id, created_at)")

        # NEW
        cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_user_chat_ts ON messages(user_id, chat_id, ts)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_chat_ts ON messages(chat_id, ts)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_profile_updated ON user_profile(user_id, chat_id, updated_at)")
        conn.commit()
    except Exception:
        pass


# =========================================================
