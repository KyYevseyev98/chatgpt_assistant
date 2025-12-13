import sqlite3
import datetime as dt
import json
from typing import Tuple, Optional, Any, Dict, List

from config import (
    DB_PATH,
    FREE_TEXT_LIMIT_PER_DAY,
    FREE_PHOTO_LIMIT_PER_DAY,
    today_iso,
)

# --- формула дней игнора для follow-up ---
def required_ignored_days_for_stage(stage: int) -> int:
    n = stage + 1
    return 2 + (3 * (n - 1) * n) // 2


conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cur = conn.cursor()


def init_db() -> None:
    """
    Создаёт таблицы users, events, pro_payments, user_profiles
    и выполняет безопасные миграции.
    """
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
            last_limit_topic TEXT,

            last_limit_type TEXT,
            last_limit_at TEXT,
            last_paywall_text TEXT,
            last_paywall_at TEXT
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
    _add_user_col("last_limit_topic", "last_limit_topic TEXT")

    # NEW
    _add_user_col("last_limit_type", "last_limit_type TEXT")
    _add_user_col("last_limit_at", "last_limit_at TEXT")
    _add_user_col("last_paywall_text", "last_paywall_text TEXT")
    _add_user_col("last_paywall_at", "last_paywall_at TEXT")

    # --- events ---
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

    # --- pro_payments ---
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

    # --- user_profiles ---
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
    _add_prof_col("messages_since_profile_update", "messages_since_profile_update INTEGER DEFAULT 0")

    _create_indexes()


def _create_indexes() -> None:
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_events_user_created ON events(user_id, created_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_events_type_created ON events(event_type, created_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_propay_user_created ON pro_payments(user_id, created_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_users_last_activity ON users(last_activity_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_users_last_followup ON users(last_followup_at)")
        conn.commit()
    except Exception:
        pass


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
    cur.execute(
        """
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
        """,
        (user_id,),
    )
    row = cur.fetchone()
    if row is None:
        today = today_iso()
        cur.execute(
            """
            INSERT INTO users (
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
                last_limit_topic,
                last_limit_type,
                last_limit_at,
                last_paywall_text,
                last_paywall_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id, 0, today, 0, 0, None, None, None, None, 0,
                None, None, None, None, None,
                None, None, None, None
            ),
        )
        conn.commit()
        _ensure_user_profile(user_id)
        return (user_id, 0, today, 0, 0, None, None, None, 0)

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


def check_limit(user_id: int, is_photo: bool = False) -> bool:
    (
        _uid, used_text, last_date, is_pro, used_photos,
        pro_until, last_activity_at, last_followup_at, followup_stage
    ) = get_user(user_id)

    today = today_iso()

    # если PRO активна — без лимитов
    if _pro_active(pro_until):
        if not is_pro:
            update_user(
                user_id, used_text, last_date, 1, used_photos,
                pro_until, last_activity_at, last_followup_at, followup_stage
            )
        return True
    else:
        if is_pro or pro_until is not None:
            pro_until = None
            is_pro = 0
            update_user(
                user_id, used_text, last_date, is_pro, used_photos,
                pro_until, last_activity_at, last_followup_at, followup_stage
            )

    # новый день — сброс
    if last_date != today:
        used_text = 0
        used_photos = 0
        last_date = today

    if is_photo:
        if used_photos < FREE_PHOTO_LIMIT_PER_DAY:
            used_photos += 1
            update_user(
                user_id, used_text, last_date, is_pro, used_photos,
                pro_until, last_activity_at, last_followup_at, followup_stage
            )
            return True
        update_user(
            user_id, used_text, last_date, is_pro, used_photos,
            pro_until, last_activity_at, last_followup_at, followup_stage
        )
        return False

    # text/voice
    if used_text < FREE_TEXT_LIMIT_PER_DAY:
        used_text += 1
        update_user(
            user_id, used_text, last_date, is_pro, used_photos,
            pro_until, last_activity_at, last_followup_at, followup_stage
        )
        return True

    update_user(
        user_id, used_text, last_date, is_pro, used_photos,
        pro_until, last_activity_at, last_followup_at, followup_stage
    )
    return False


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


# -------------------- MEMORY --------------------
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
               last_followup_text, last_limit_topic,
               last_limit_type, last_limit_at,
               last_paywall_text, last_paywall_at
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
        "last_limit_topic": row[4],
        "last_limit_type": row[5],
        "last_limit_at": row[6],
        "last_paywall_text": row[7],
        "last_paywall_at": row[8],
    }


# -------------------- PROFILES --------------------
def _safe_load_json(s: Optional[str], fallback):
    if not s:
        return fallback
    try:
        return json.loads(s)
    except Exception:
        return fallback


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
) -> None:
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


def get_user_profile_snapshot(user_id: int) -> Dict[str, Any]:
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


# -------------------- EVENTS + PAYMENTS --------------------
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

    update_user_profile_on_event(
        user_id,
        event_type,
        lang=lang,
        segments=segments,
        segment_scores=segment_scores,
        topic=topic,
        last_limit_type=last_limit_type,
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

    update_user_profile_on_event(
        user_id,
        "payment",
        pro_payment_increment=1,
    )


def get_followup_personalization_snapshot(user_id: int) -> Dict[str, Any]:
    mem = get_user_memory_snapshot(user_id)
    prof = get_user_profile_snapshot(user_id)
    out = {}
    out.update(mem or {})
    out.update(prof or {})
    return out


# -------------------- SALES TRIGGERS --------------------
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

    # если тот же текст — не шлём
    if last_text and last_text == new_text.strip():
        return False

    # если отправляли paywall менее чем 2 минуты назад — не шлём повторно
    if last_at:
        try:
            dt_last = dt.datetime.fromisoformat(last_at)
            if (dt.datetime.utcnow() - dt_last).total_seconds() < 120:
                return False
        except Exception:
            pass

    return True