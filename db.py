# db.py
import sqlite3
import datetime as dt
from typing import Tuple, Optional

from config import (
    DB_PATH,
    FREE_TEXT_LIMIT_PER_DAY,
    FREE_PHOTO_LIMIT_PER_DAY,
    today_iso,
)

# --- глобальное соединение с БД ---
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cur = conn.cursor()


def init_db() -> None:
    """
    Создаёт таблицу users, если её нет, и выполняет простые миграции.
    Вызывается один раз при старте бота.
    """
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            free_used_today INTEGER DEFAULT 0,
            last_reset_date TEXT,
            is_pro INTEGER DEFAULT 0,
            free_photos_used_today INTEGER DEFAULT 0,
            pro_until TEXT
        )
        """
    )
    conn.commit()

    # миграции на случай старой схемы
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


def get_user(user_id: int) -> Tuple[int, int, str, int, int, Optional[str]]:
    """
    Возвращает:
    (user_id, used_text, last_date, is_pro_flag, used_photos, pro_until_iso)
    и создаёт пользователя, если его нет.
    """
    cur.execute(
        """
        SELECT user_id,
               free_used_today,
               last_reset_date,
               is_pro,
               free_photos_used_today,
               pro_until
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
                pro_until
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (user_id, 0, today, 0, 0, None),
        )
        conn.commit()
        return (user_id, 0, today, 0, 0, None)
    return row


def update_user(
    user_id: int,
    used_text: int,
    last_date: str,
    is_pro: int,
    used_photos: int,
    pro_until: Optional[str],
) -> None:
    cur.execute(
        """
        UPDATE users
        SET free_used_today = ?,
            last_reset_date = ?,
            is_pro = ?,
            free_photos_used_today = ?,
            pro_until = ?
        WHERE user_id = ?
        """,
        (used_text, last_date, is_pro, used_photos, pro_until, user_id),
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


def set_pro(user_id: int, days: int) -> None:
    """
    Продлевает / выдаёт PRO на N дней.
    Если уже есть активная PRO – добавляем дни к текущей дате окончания.
    """
    user_id, used_text, last_date, is_pro, used_photos, pro_until = get_user(user_id)

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

    # включаем флаг is_pro = 1
    update_user(user_id, used_text, last_date, 1, used_photos, new_until)


def check_limit(user_id: int, is_photo: bool = False) -> bool:
    """
    True  – можно отвечать
    False – лимит превышен
    Учитывает PRO-подписку (если активна — лимитов нет).
    """
    user_id, used_text, last_date, is_pro, used_photos, pro_until = get_user(user_id)
    today = today_iso()

    # если PRO активна — сразу пропускаем без лимитов
    if _pro_active(pro_until):
        if not is_pro:
            # на всякий случай синхронизируем флаг
            update_user(user_id, used_text, last_date, 1, used_photos, pro_until)
        return True
    else:
        # если истекла — сбрасываем флаг и pro_until
        if is_pro or pro_until is not None:
            pro_until = None
            is_pro = 0
            update_user(user_id, used_text, last_date, is_pro, used_photos, pro_until)

    # новый день – обнуляем счётчики
    if last_date != today:
        used_text = 0
        used_photos = 0
        last_date = today

    # считаем лимиты
    if is_photo:
        if used_photos < FREE_PHOTO_LIMIT_PER_DAY:
            used_photos += 1
            update_user(user_id, used_text, last_date, is_pro, used_photos, pro_until)
            return True
        else:
            update_user(user_id, used_text, last_date, is_pro, used_photos, pro_until)
            return False
    else:
        if used_text < FREE_TEXT_LIMIT_PER_DAY:
            used_text += 1
            update_user(user_id, used_text, last_date, is_pro, used_photos, pro_until)
            return True
        else:
            update_user(user_id, used_text, last_date, is_pro, used_photos, pro_until)
            return False