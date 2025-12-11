import sqlite3
import datetime as dt
from typing import Tuple, Optional

from config import (
    DB_PATH,
    FREE_TEXT_LIMIT_PER_DAY,
    FREE_PHOTO_LIMIT_PER_DAY,
    today_iso,
)

# --- формула дней игнора для follow-up ---
def required_ignored_days_for_stage(stage: int) -> int:
    """
    Сколько дней игнора нужно, чтобы отправить follow-up для данного stage.

    stage: 0 -> первый follow-up, 1 -> второй и т.д.

    Формула: дни игнора растут всё медленнее (интервал между рассылками растёт на +3 дня).
    """
    n = stage + 1
    # D(n) = 2 + 3 * (n-1)*n/2
    return 2 + (3 * (n - 1) * n) // 2


# --- глобальное соединение с БД ---
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cur = conn.cursor()


def init_db() -> None:
    """
    Создаёт таблицы users, events, pro_payments, user_profiles,
    если их нет, и выполняет простые миграции.
    """
    # --- таблица пользователей ---
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
            last_activity_at TEXT,           -- когда юзер последний раз что-то делал
            last_followup_at TEXT,           -- когда последний раз отправляли follow-up
            followup_stage INTEGER DEFAULT 0 -- какой по счёту follow-up уже был
        )
        """
    )
    conn.commit()

    # миграции по users (на случай старой схемы)
    cur.execute("PRAGMA table_info(users)")
    cols = [row[1] for row in cur.fetchall()]

    if "free_photos_used_today" not in cols:
        cur.execute(
            "ALTER TABLE users ADD COLUMN free_photos_used_today INTEGER DEFAULT 0"
        )
        conn.commit()

    if "pro_until" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN pro_until TEXT")
        conn.commit()

    if "traffic_source" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN traffic_source TEXT")
        conn.commit()

    if "last_activity_at" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN last_activity_at TEXT")
        conn.commit()

    if "last_followup_at" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN last_followup_at TEXT")
        conn.commit()

    if "followup_stage" not in cols:
        cur.execute(
            "ALTER TABLE users ADD COLUMN followup_stage INTEGER DEFAULT 0"
        )
        conn.commit()

    # --- таблица событий (логи запросов / оплат / и т.п.) ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,          -- 'text', 'voice', 'photo', 'payment', ...
            tokens INTEGER,                    -- зарезервировано под токены / длину
            is_pro INTEGER DEFAULT 0,          -- был ли PRO на момент события
            meta TEXT,                         -- доп. информация (JSON / payload / что угодно)
            created_at TEXT NOT NULL           -- UTC ISO-строка
        )
        """
    )
    conn.commit()

    # минимальные миграции для events
    cur.execute("PRAGMA table_info(events)")
    event_cols = [row[1] for row in cur.fetchall()]

    if "tokens" not in event_cols:
        cur.execute("ALTER TABLE events ADD COLUMN tokens INTEGER")
        conn.commit()
    if "is_pro" not in event_cols:
        cur.execute("ALTER TABLE events ADD COLUMN is_pro INTEGER DEFAULT 0")
        conn.commit()
    if "meta" not in event_cols:
        cur.execute("ALTER TABLE events ADD COLUMN meta TEXT")
        conn.commit()
    if "created_at" not in event_cols:
        cur.execute("ALTER TABLE events ADD COLUMN created_at TEXT")
        conn.commit()

    # --- таблица оплат PRO ---
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

    # --- таблица профилей пользователей (для сегментов / LTV) ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS user_profiles (
            user_id INTEGER PRIMARY KEY,
            segments TEXT,                 -- список тегов через запятую
            total_messages INTEGER DEFAULT 0,
            total_photos INTEGER DEFAULT 0,
            total_voice INTEGER DEFAULT 0,
            pro_payments_count INTEGER DEFAULT 0,
            last_limit_type TEXT,
            last_lang TEXT
        )
        """
    )
    conn.commit()


def _ensure_user_profile(user_id: int) -> None:
    """
    Гарантируем, что для user_id есть запись в user_profiles.
    """
    cur.execute(
        """
        INSERT OR IGNORE INTO user_profiles (
            user_id, segments, total_messages, total_photos,
            total_voice, pro_payments_count, last_limit_type, last_lang
        )
        VALUES (?, '', 0, 0, 0, 0, NULL, NULL)
        """,
        (user_id,),
    )
    conn.commit()


def get_user(
    user_id: int,
) -> Tuple[
    int,
    int,
    str,
    int,
    int,
    Optional[str],
    Optional[str],
    Optional[str],
    int,
]:
    """
    Возвращает:
    (
        user_id,
        used_text,
        last_date,
        is_pro_flag,
        used_photos,
        pro_until_iso,
        last_activity_at_iso,
        last_followup_at_iso,
        followup_stage
    )
    и создаёт пользователя, если его нет.
    """
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
                followup_stage
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (user_id, 0, today, 0, 0, None, None, None, None, 0),
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
            used_text,
            last_date,
            is_pro,
            used_photos,
            pro_until,
            last_activity_at,
            last_followup_at,
            followup_stage,
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
    """
    Сохраняем источник трафика для юзера.
    Если уже есть источник – не перезаписываем.
    """
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
    """
    Продлевает / выдаёт PRO на N дней.
    Если уже есть активная PRO – добавляем дни к текущей дате окончания.
    """
    (
        _uid,
        used_text,
        last_date,
        _is_pro,
        used_photos,
        pro_until,
        last_activity_at,
        last_followup_at,
        followup_stage,
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
    """
    True  – можно отвечать
    False – лимит превышен
    Учитывает PRO-подписку (если активна — лимитов нет).
    """
    (
        _uid,
        used_text,
        last_date,
        is_pro,
        used_photos,
        pro_until,
        last_activity_at,
        last_followup_at,
        followup_stage,
    ) = get_user(user_id)
    today = today_iso()

    # если PRO активна — сразу пропускаем без лимитов
    if _pro_active(pro_until):
        if not is_pro:
            update_user(
                user_id,
                used_text,
                last_date,
                1,
                used_photos,
                pro_until,
                last_activity_at,
                last_followup_at,
                followup_stage,
            )
        return True
    else:
        if is_pro or pro_until is not None:
            pro_until = None
            is_pro = 0
            update_user(
                user_id,
                used_text,
                last_date,
                is_pro,
                used_photos,
                pro_until,
                last_activity_at,
                last_followup_at,
                followup_stage,
            )

    # новый день – обнуляем счётчики
    if last_date != today:
        used_text = 0
        used_photos = 0
        last_date = today

    # считаем лимиты
    if is_photo:
        if used_photos < FREE_PHOTO_LIMIT_PER_DAY:
            used_photos += 1
            update_user(
                user_id,
                used_text,
                last_date,
                is_pro,
                used_photos,
                pro_until,
                last_activity_at,
                last_followup_at,
                followup_stage,
            )
            return True
        else:
            update_user(
                user_id,
                used_text,
                last_date,
                is_pro,
                used_photos,
                pro_until,
                last_activity_at,
                last_followup_at,
                followup_stage,
            )
            return False
    else:
        if used_text < FREE_TEXT_LIMIT_PER_DAY:
            used_text += 1
            update_user(
                user_id,
                used_text,
                last_date,
                is_pro,
                used_photos,
                pro_until,
                last_activity_at,
                last_followup_at,
                followup_stage,
            )
            return True
        else:
            update_user(
                user_id,
                used_text,
                last_date,
                is_pro,
                used_photos,
                pro_until,
                last_activity_at,
                last_followup_at,
                followup_stage,
            )
            return False


def touch_last_activity(user_id: int) -> None:
    """
    Обновляем время последней активности пользователя
    (любое его сообщение или /start) и сбрасываем стадию follow-up.
    """
    (
        _uid,
        used_text,
        last_date,
        is_pro,
        used_photos,
        pro_until,
        _last_activity_at,
        last_followup_at,
        _followup_stage,
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
    """
    Фиксируем, что отправили follow-up:
    - увеличиваем followup_stage
    - обновляем last_followup_at
    """
    (
        _uid,
        used_text,
        last_date,
        is_pro,
        used_photos,
        pro_until,
        last_activity_at,
        _last_followup_at,
        followup_stage,
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
    """
    Удобно доставать инфу для рассылок.
    Возвращает кортеж:
    (last_activity_at_iso, last_followup_at_iso, followup_stage)
    """
    row = get_user(user_id)
    return row[6], row[7], row[8]


def get_all_users_for_followup():
    """
    Возвращает пользователей, для которых ИМЕЕТ смысл
    запускать периодические follow-up'ы.
    """
    cur.execute(
        """
        SELECT user_id,
               last_activity_at,
               last_followup_at,
               followup_stage
        FROM users
        WHERE last_activity_at IS NOT NULL
          AND (last_followup_at IS NOT NULL OR followup_stage > 0)
        """
    )
    return cur.fetchall()


def update_user_profile_on_event(
    user_id: int,
    event_type: str,
    *,
    lang: Optional[str] = None,
    segments: Optional[list[str]] = None,
    pro_payment_increment: int = 0,
    last_limit_type: Optional[str] = None,
) -> None:
    """
    Обновляет профиль пользователя (user_profiles) при событии.
    """
    _ensure_user_profile(user_id)

    cur.execute(
        """
        SELECT segments,
               total_messages,
               total_photos,
               total_voice,
               pro_payments_count,
               last_limit_type,
               last_lang
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
        total_messages,
        total_photos,
        total_voice,
        pro_payments_count,
        last_limit_type_db,
        last_lang_db,
    ) = row

    total_messages = total_messages or 0
    total_photos = total_photos or 0
    total_voice = total_voice or 0
    pro_payments_count = pro_payments_count or 0

    if event_type == "text":
        total_messages += 1
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
            if s and s not in existing_segments:
                existing_segments.append(s)
    new_segments_str = ",".join(existing_segments)

    cur.execute(
        """
        UPDATE user_profiles
        SET segments = ?,
            total_messages = ?,
            total_photos = ?,
            total_voice = ?,
            pro_payments_count = ?,
            last_limit_type = ?,
            last_lang = ?
        WHERE user_id = ?
        """,
        (
            new_segments_str,
            total_messages,
            total_photos,
            total_voice,
            pro_payments_count,
            last_limit_type_db,
            last_lang_db,
            user_id,
        ),
    )
    conn.commit()


def get_user_profile_snapshot(user_id: int) -> dict:
    """
    Возвращает слепок профиля пользователя для GPT (для рассылок).
    """
    _ensure_user_profile(user_id)

    cur.execute(
        """
        SELECT segments,
               total_messages,
               total_photos,
               total_voice,
               pro_payments_count,
               last_limit_type,
               last_lang
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
        total_messages,
        total_photos,
        total_voice,
        pro_payments_count,
        last_limit_type,
        last_lang,
    ) = row

    # тянем источник трафика
    cur.execute(
        "SELECT traffic_source FROM users WHERE user_id = ?",
        (user_id,),
    )
    row2 = cur.fetchone()
    traffic_source = row2[0] if row2 else None

    segments_list = [s for s in (segments_str or "").split(",") if s.strip()]

    return {
        "segments": segments_list,
        "total_messages": total_messages or 0,
        "total_photos": total_photos or 0,
        "total_voice": total_voice or 0,
        "pro_payments_count": pro_payments_count or 0,
        "last_limit_type": last_limit_type,
        "last_lang": last_lang,
        "traffic_source": traffic_source,
    }


def log_event(
    user_id: int,
    event_type: str,
    *,
    tokens: Optional[int] = None,
    meta: Optional[str] = None,
    last_limit_type: Optional[str] = None,
) -> None:
    """
    Пишет запись в лог событий и, по возможности, обновляет профиль.
    """
    (
        _uid,
        _used_text,
        _last_date,
        _is_pro,
        _used_photos,
        pro_until,
        _last_activity_at,
        _last_followup_at,
        _followup_stage,
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

    # Апдейтим профиль (без языка/сегментов — они приходят отдельно)
    update_user_profile_on_event(
        user_id,
        event_type,
        last_limit_type=last_limit_type,
    )


def log_pro_payment(user_id: int, stars: int, days: int) -> None:
    """
    Логируем факт покупки PRO в таблицу pro_payments и обновляем профиль.
    """
    cur.execute(
        "SELECT traffic_source FROM users WHERE user_id = ?",
        (user_id,),
    )
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

    # фиксируем покупку в профиле
    update_user_profile_on_event(
        user_id,
        "payment",
        pro_payment_increment=1,
    )