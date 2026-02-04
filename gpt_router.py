from __future__ import annotations

import json
import re
from typing import List, Dict, Optional


def safe_json_loads(s: str) -> Optional[dict]:
    if not s:
        return None
    s = s.strip()

    if s.startswith("```"):
        s = s.strip("`").strip()
        if s.lower().startswith("json"):
            s = s[4:].strip()

    if "{" in s and "}" in s:
        s = s[s.find("{"): s.rfind("}") + 1]

    try:
        return json.loads(s)
    except Exception:
        return None


def is_followup_like(text: str) -> bool:
    """Лёгкая эвристика: “подробнее/расшифруй/итог/2 карта” => продолжение, а не новый расклад."""
    t = (text or "").strip().lower()
    if not t:
        return False

    if len(t) <= 60 and any(x in t for x in ("подробнее", "поясни", "уточни", "расшифруй", "продолжи", "итог", "вывод")):
        return True
    if re.search(r"\bчто\s+значит\b", t):
        return True
    if re.search(r"\b(1|2|3|4|5|6|7)\s*(карта|карту)\b", t):
        return True
    return False


def history_tail(history: Optional[List[Dict[str, str]]], n: int = 12) -> List[Dict[str, str]]:
    if not history:
        return []
    return history[-n:]


def format_history_for_router(history: Optional[List[Dict[str, str]]], limit_chars: int = 1200) -> str:
    items = history_tail(history, n=10)
    lines = []
    for m in items:
        role = m.get("role", "")
        content = (m.get("content") or "").strip()
        if not content:
            continue
        lines.append(f"{role}: {content}")
    s = "\n".join(lines)
    if len(s) > limit_chars:
        s = s[-limit_chars:]
    return s
