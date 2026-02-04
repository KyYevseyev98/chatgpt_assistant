from __future__ import annotations

import datetime as dt
from typing import Any, Dict

from config import FREE_TAROT_FREE_COUNT, FREE_TEXT_FREE_COUNT
from .user_profile import get_user_profile_chat, patch_user_profile_chat


def _now_iso() -> str:
    return dt.datetime.utcnow().isoformat()


def _get_billing(user_id: int, chat_id: int) -> Dict[str, Any]:
    profile = get_user_profile_chat(user_id, chat_id) or {}
    return profile.get("billing") or {}


def _set_billing(user_id: int, chat_id: int, billing: Dict[str, Any]) -> None:
    patch_user_profile_chat(user_id, chat_id, patch={"billing": billing})


def ensure_billing_defaults(user_id: int, chat_id: int) -> None:
    b = _get_billing(user_id, chat_id)
    if b:
        return
    _set_billing(
        user_id,
        chat_id,
        {
            "text_used": 0,
            "tarot_free_used": 0,
            "tarot_credits": 0,
            "updated_at": _now_iso(),
        },
    )


def get_billing_snapshot(user_id: int, chat_id: int) -> Dict[str, Any]:
    b = _get_billing(user_id, chat_id) or {}
    return {
        "text_used": int(b.get("text_used") or 0),
        "text_free_left": max(0, int(FREE_TEXT_FREE_COUNT) - int(b.get("text_used") or 0)),
        "tarot_free_used": int(b.get("tarot_free_used") or 0),
        "tarot_free_left": max(0, int(FREE_TAROT_FREE_COUNT) - int(b.get("tarot_free_used") or 0)),
        "tarot_credits": int(b.get("tarot_credits") or 0),
        "updated_at": b.get("updated_at") or "",
    }


def can_consume_text(user_id: int, chat_id: int) -> bool:
    b = _get_billing(user_id, chat_id) or {}
    used = int(b.get("text_used") or 0)
    if used >= int(FREE_TEXT_FREE_COUNT):
        return False
    b["text_used"] = used + 1
    b["updated_at"] = _now_iso()
    _set_billing(user_id, chat_id, b)
    return True


def can_start_tarot(user_id: int, chat_id: int) -> bool:
    b = _get_billing(user_id, chat_id) or {}
    credits = int(b.get("tarot_credits") or 0)
    free_used = int(b.get("tarot_free_used") or 0)
    if credits > 0:
        return True
    return free_used < int(FREE_TAROT_FREE_COUNT)


def consume_tarot_credit(user_id: int, chat_id: int) -> None:
    b = _get_billing(user_id, chat_id) or {}
    credits = int(b.get("tarot_credits") or 0)
    free_used = int(b.get("tarot_free_used") or 0)
    if credits > 0:
        b["tarot_credits"] = credits - 1
    else:
        b["tarot_free_used"] = free_used + 1
    b["updated_at"] = _now_iso()
    _set_billing(user_id, chat_id, b)


def add_tarot_credits(user_id: int, chat_id: int, count: int) -> None:
    if count <= 0:
        return
    b = _get_billing(user_id, chat_id) or {}
    credits = int(b.get("tarot_credits") or 0)
    b["tarot_credits"] = credits + int(count)
    b["updated_at"] = _now_iso()
    _set_billing(user_id, chat_id, b)


def adjust_tarot_balance(user_id: int, chat_id: int, delta: int) -> Dict[str, int]:
    """
    Универсальная корректировка баланса:
    - delta > 0: добавляет кредиты
    - delta < 0: списывает кредиты, затем уменьшает free_left через увеличение free_used
    Возвращает snapshot с полями: credits, free_used.
    """
    b = _get_billing(user_id, chat_id) or {}
    credits = int(b.get("tarot_credits") or 0)
    free_used = int(b.get("tarot_free_used") or 0)

    if delta >= 0:
        credits += int(delta)
    else:
        to_spend = abs(int(delta))
        use_from_credits = min(credits, to_spend)
        credits -= use_from_credits
        to_spend -= use_from_credits
        if to_spend > 0:
            free_used = min(int(FREE_TAROT_FREE_COUNT), free_used + to_spend)

    b["tarot_credits"] = max(0, credits)
    b["tarot_free_used"] = max(0, free_used)
    b["updated_at"] = _now_iso()
    _set_billing(user_id, chat_id, b)
    return {"credits": b["tarot_credits"], "free_used": b["tarot_free_used"]}
