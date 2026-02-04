# tarot/limits.py
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, date
from typing import Dict, Any


DATA_DIR = "data"
LIMITS_FILE = os.path.join(DATA_DIR, "tarot_limits.json")

FREE_LIFETIME = 300
DAILY_LIMIT = 2000


@dataclass
class UserLimits:
    free_used: int = 0
    last_day: str = ""     # YYYY-MM-DD
    daily_used: int = 0


def _today_str() -> str:
    return date.today().isoformat()


def _load_all() -> Dict[str, Any]:
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(LIMITS_FILE):
        return {}
    try:
        with open(LIMITS_FILE, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def _save_all(data: Dict[str, Any]) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp = LIMITS_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, LIMITS_FILE)


def get_user_limits(user_id: int) -> UserLimits:
    data = _load_all()
    raw = data.get(str(user_id), {})
    ul = UserLimits(
        free_used=int(raw.get("free_used", 0)),
        last_day=str(raw.get("last_day", "")),
        daily_used=int(raw.get("daily_used", 0)),
    )
    # сброс дневного лимита
    today = _today_str()
    if ul.last_day != today:
        ul.last_day = today
        ul.daily_used = 0
        data[str(user_id)] = ul.__dict__
        _save_all(data)
    return ul


def can_do_reading(user_id: int, has_subscription: bool) -> tuple[bool, str]:
    ul = get_user_limits(user_id)

    # суточный лимит всегда
    if ul.daily_used >= DAILY_LIMIT:
        return False, "Сейчас слишком большая нагрузка, расклады временно ограничены. Попробуй чуть позже 🙏"

    # если подписки нет — проверяем бесплатные 3 навсегда
    if not has_subscription and ul.free_used >= FREE_LIFETIME:
        return False, "У тебя уже использованы 3 бесплатных расклада. Чтобы продолжить — оформи подписку на месяц ⭐️"

    return True, "ok"


def mark_reading_used(user_id: int, has_subscription: bool) -> None:
    data = _load_all()
    ul = get_user_limits(user_id)

    ul.daily_used += 1
    if not has_subscription:
        ul.free_used += 1

    data[str(user_id)] = ul.__dict__
    _save_all(data)