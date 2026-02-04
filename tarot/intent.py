from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional


@dataclass(frozen=True)
class IntentResult:
    kind: str          # "chat" | "reading" | "followup" | "clarify"
    reason: str
    clarify_question: Optional[str] = None


# ЯВНЫЕ триггеры — только прямой запрос на расклад
EXPLICIT_TAROT_TRIGGERS = (
    "расклад", "расклад таро", "сделай расклад", "погадай", "гадание",
    "таро", "карты таро", "раскинь карты",
    "карта дня",
    "да/нет", "да или нет",
)

# бытовой / чат
SMALL_TALK = (
    "привет", "здравств", "как дела", "спасибо", "кто ты",
)

# расплывчатые фразы
VAGUE = (
    "что делать", "как быть", "не знаю", "подскажи", "помоги",
)

# фразы продолжения после расклада
FOLLOWUP_MARKERS = (
    "подробнее",
    "расскажи",
    "поясни",
    "а что значит",
    "и что дальше",
    "можешь уточнить",
    "продолжи",
)


def _last_assistant_was_tarot(history: List[dict]) -> bool:
    """
    Примитивный, но стабильный признак:
    если в последних ответах есть маркеры расклада.
    """
    if not history:
        return False

    for msg in reversed(history[-5:]):
        if msg.get("role") != "assistant":
            continue
        txt = (msg.get("content") or "").lower()
        if "карта" in txt or "расклад" in txt or "🃏" in txt:
            return True
    return False


def classify_intent(
    text: str,
    *,
    history: Optional[List[dict]] = None,
) -> IntentResult:
    t = (text or "").strip().lower()
    history = history or []

    if not t:
        return IntentResult("chat", "empty")

    # small talk → всегда chat
    if any(x in t for x in SMALL_TALK):
        return IntentResult("chat", "smalltalk")

    # follow-up после расклада
    if _last_assistant_was_tarot(history):
        if any(x in t for x in FOLLOWUP_MARKERS):
            return IntentResult("followup", "after_tarot_continuation")

    # расплывчатый запрос без явного таро
    if any(x in t for x in VAGUE) and "?" not in t and len(t) < 50:
        return IntentResult(
            "clarify",
            "vague",
            "Ты хочешь именно расклад Таро или просто совет? "
            "Если расклад — напиши тему и срок."
        )

    # явный запрос на таро
    if any(x in t for x in EXPLICIT_TAROT_TRIGGERS):
        return IntentResult("reading", "explicit_tarot_request")

    # всё остальное — обычный чат
    return IntentResult("chat", "default")