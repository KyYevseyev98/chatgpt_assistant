from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional

from tarot.intent import classify_intent


Action = Literal["chat", "clarify", "reading", "followup"]
History = List[Dict[str, str]]


@dataclass
class RouteResult:
    """Normalized routing result used by handlers."""
    action: Action
    cards: int = 0
    spread_name: str = ""
    clarify_question: str = ""
    reason: str = ""


def normalize_route(data: Dict[str, Any]) -> RouteResult:
    """
    Нормализация результата GPT-router (оставляем совместимость).
    """
    action = str(data.get("action", "chat")).strip().lower()
    if action not in ("chat", "clarify", "reading"):
        action = "chat"

    try:
        cards = int(data.get("cards", 0) or 0)
    except Exception:
        cards = 0

    spread_name = str(data.get("spread_name", "")).strip()
    clarify_question = str(data.get("clarify_question", "")).strip()
    reason = str(data.get("reason", "")).strip()

    if action == "chat":
        return RouteResult(action="chat", reason=reason)

    if action == "clarify":
        if not clarify_question:
            clarify_question = "Уточни, пожалуйста: ты хочешь именно расклад Таро? Если да — про что?"
        return RouteResult(
            action="clarify",
            clarify_question=clarify_question,
            reason=reason,
        )

    # reading
    cards = max(1, min(cards if cards > 0 else 3, 7))
    if not spread_name:
        spread_name = f"{cards} карт"

    return RouteResult(
        action="reading",
        cards=cards,
        spread_name=spread_name,
        reason=reason,
    )


# ---------------- NEW SMART ROUTER ----------------

def decide_route(
    *,
    user_text: str,
    history: Optional[History],
    gpt_route_raw: Optional[Dict[str, Any]] = None,
) -> RouteResult:
    """
    Единая точка принятия решения.

    Приоритет:
    1. Follow-up (продолжение после расклада)
    2. Явный intent из текста
    3. GPT-router (если передан)
    4. Chat fallback
    """
    history = history or []

    intent = classify_intent(user_text, history=history)

    # 1) follow-up имеет абсолютный приоритет
    if intent.kind == "followup":
        return RouteResult(
            action="followup",
            reason=intent.reason,
        )

    # 2) clarify от intent
    if intent.kind == "clarify":
        return RouteResult(
            action="clarify",
            clarify_question=intent.clarify_question or "",
            reason=intent.reason,
        )

    # 3) явный reading из intent
    if intent.kind == "reading":
        # если GPT дал результат — используем его
        if gpt_route_raw:
            return normalize_route(gpt_route_raw)

        # fallback (редкий случай)
        return RouteResult(
            action="reading",
            cards=3,
            spread_name="Расклад",
            reason="intent_fallback",
        )

    # 4) intent=chat → но GPT может захотеть reading (например кнопки)
    if gpt_route_raw:
        normalized = normalize_route(gpt_route_raw)
        if normalized.action == "reading":
            return normalized

    # 5) обычный чат
    return RouteResult(
        action="chat",
        reason=intent.reason,
    )


# ---------------- CARDS PAYLOAD ----------------

def build_cards_payload(cards: List[Any]) -> List[Dict[str, Any]]:
    """Convert card objects into a serializable payload for GPT prompts."""
    out: List[Dict[str, Any]] = []
    for c in cards:
        if getattr(c, "meaning", None):
            m = c.meaning
            out.append({
                "key": c.key,
                "ru_name": m.ru_name,
                "keywords": m.keywords,
                "short": m.short,
                "shadow": m.shadow or "",
                "advice": m.advice or "",
                "file": c.filename,
            })
        else:
            out.append({
                "key": c.key,
                "ru_name": c.key,
                "keywords": "",
                "short": "Значение не найдено",
                "shadow": "",
                "advice": "",
                "file": c.filename,
            })
    return out
