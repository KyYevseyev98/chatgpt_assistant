from __future__ import annotations

import datetime as dt
import json
import logging
from typing import Any, Dict, List, Optional

from config import (
    LONG_MEMORY_BLOCK_MAX_CHARS,
    LONG_MEMORY_MAX_EVENTS,
    LONG_MEMORY_MAX_ITEMS,
    LONG_MEMORY_MAX_SUMMARIES,
    LONG_MEMORY_SUMMARY_EVERY,
    LONG_MEMORY_SUMMARY_HISTORY,
)
from db_layer.messages import get_last_messages
from db_layer.user_profile import get_user_profile_chat, patch_user_profile_chat
from gpt_client import summarize_long_memory

logger = logging.getLogger(__name__)


def _log_exception(message: str) -> None:
    logger.debug(message, exc_info=True)


def _normalize_item(text: str) -> str:
    return " ".join((text or "").strip().split())


def _merge_items(existing: List[str], new_items: List[str], *, max_items: int) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in existing + new_items:
        norm = _normalize_item(item)
        if not norm:
            continue
        key = norm.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(norm)
        if len(out) >= max_items:
            break
    return out


def _ensure_list(v: Any) -> List[str]:
    if not v:
        return []
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    if isinstance(v, str):
        return [v.strip()] if v.strip() else []
    return []


def _now_iso() -> str:
    return dt.datetime.utcnow().isoformat()


def _get_long_memory_profile(user_id: int, chat_id: int) -> Dict[str, Any]:
    profile = get_user_profile_chat(user_id, chat_id) or {}
    return profile.get("long_memory") or {}


def _save_long_memory_profile(user_id: int, chat_id: int, mem: Dict[str, Any]) -> None:
    try:
        patch_user_profile_chat(user_id, chat_id, patch={"long_memory": mem})
    except Exception:
        _log_exception("long_memory save failed")


def build_long_memory_block(user_id: int, chat_id: int, *, lang: str = "ru") -> str:
    mem = _get_long_memory_profile(user_id, chat_id) or {}
    profile = mem.get("profile") or {}
    summaries = mem.get("summaries") or []
    events = mem.get("events") or []

    themes = _ensure_list(profile.get("themes"))
    goals = _ensure_list(profile.get("goals"))
    facts = _ensure_list(profile.get("facts"))
    boundaries = _ensure_list(profile.get("boundaries"))
    taboos = _ensure_list(profile.get("taboos"))
    prefs = _ensure_list(profile.get("preferences"))

    if not any([themes, goals, facts, boundaries, taboos, prefs, summaries, events]):
        return ""

    lines: List[str] = []
    if (lang or "ru").lower().startswith("uk"):
        lines.append("Довготривала памʼять про користувача (використовуй мʼяко, не вигадуй):")
        if themes:
            lines.append(f"- Теми: {', '.join(themes)}")
        if goals:
            lines.append(f"- Цілі: {', '.join(goals)}")
        if facts:
            lines.append(f"- Важливі факти: {', '.join(facts)}")
        if boundaries or taboos:
            bt = ", ".join(boundaries + taboos)
            lines.append(f"- Межі/табу: {bt}")
        if prefs:
            lines.append(f"- Переваги спілкування: {', '.join(prefs)}")
    else:
        lines.append("Долгосрочная память о пользователе (используй мягко, не выдумывай):")
        if themes:
            lines.append(f"- Темы: {', '.join(themes)}")
        if goals:
            lines.append(f"- Цели: {', '.join(goals)}")
        if facts:
            lines.append(f"- Важные факты: {', '.join(facts)}")
        if boundaries or taboos:
            bt = ", ".join(boundaries + taboos)
            lines.append(f"- Границы/табу: {bt}")
        if prefs:
            lines.append(f"- Предпочтения общения: {', '.join(prefs)}")

    if summaries:
        last_summary = summaries[-1]
        stext = (last_summary.get("text") or "").strip()
        if stext:
            lines.append(f"- Последняя заметка: {stext}")

    if events:
        last_events = events[-2:] if len(events) >= 2 else events
        ev_texts = [e.get("text") for e in last_events if e.get("text")]
        if ev_texts:
            lines.append(f"- Значимые события: {', '.join(ev_texts)}")

    block = "\n".join(lines).strip()
    if LONG_MEMORY_BLOCK_MAX_CHARS:
        block = block[: int(LONG_MEMORY_BLOCK_MAX_CHARS)]
    return block


async def maybe_update_long_memory(
    user_id: int,
    chat_id: int,
    *,
    lang: str = "ru",
    topic: Optional[str] = None,
) -> None:
    mem = _get_long_memory_profile(user_id, chat_id) or {}
    counter = int(mem.get("msg_counter") or 0) + 1
    mem["msg_counter"] = counter

    if counter < int(LONG_MEMORY_SUMMARY_EVERY):
        _save_long_memory_profile(user_id, chat_id, mem)
        return

    mem["msg_counter"] = 0
    history = get_last_messages(user_id, chat_id, limit=int(LONG_MEMORY_SUMMARY_HISTORY)) or []
    if not history:
        _save_long_memory_profile(user_id, chat_id, mem)
        return

    update = {}
    try:
        update = await summarize_long_memory(history=history, lang=lang, current_profile=mem.get("profile"))
    except Exception:
        _log_exception("long_memory summarize failed")
        update = {}

    if not update:
        _save_long_memory_profile(user_id, chat_id, mem)
        return

    mem_profile = mem.get("profile") or {}
    for key in ("themes", "goals", "facts", "boundaries", "taboos", "preferences"):
        existing = _ensure_list(mem_profile.get(key))
        new_items = _ensure_list(update.get(key))
        merged = _merge_items(existing, new_items, max_items=int(LONG_MEMORY_MAX_ITEMS))
        if merged:
            mem_profile[key] = merged

    mem["profile"] = mem_profile
    mem["updated_at"] = _now_iso()

    summary_text = (update.get("summary") or update.get("short_summary") or "").strip()
    if summary_text:
        summaries = mem.get("summaries") or []
        summaries.append({"at": _now_iso(), "topic": (topic or ""), "text": summary_text})
        mem["summaries"] = summaries[-int(LONG_MEMORY_MAX_SUMMARIES):]

    events = update.get("events") or update.get("significant_events") or []
    if isinstance(events, str):
        events = [events]
    if events:
        ev_list = mem.get("events") or []
        for e in events:
            et = _normalize_item(str(e))
            if et:
                ev_list.append({"at": _now_iso(), "text": et})
        mem["events"] = ev_list[-int(LONG_MEMORY_MAX_EVENTS):]

    _save_long_memory_profile(user_id, chat_id, mem)
