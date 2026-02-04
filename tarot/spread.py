from __future__ import annotations
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class SpreadPlan:
    cards: int
    name: str
    note: str
    need_clarify: bool = False
    clarify_question: Optional[str] = None


def choose_spread(
    *,
    route_action: str,
    cards: int,
    spread_name: str,
) -> SpreadPlan:
    """
    Spread НЕ принимает решений.
    Он лишь превращает решение роутера в исполняемый план.
    """

    # clarify
    if route_action == "clarify":
        return SpreadPlan(
            cards=0,
            name="clarify",
            note="router_requested_clarify",
            need_clarify=True,
            clarify_question="Уточни, пожалуйста, свой запрос.",
        )

    # follow-up → не тянем карты
    if route_action == "followup":
        return SpreadPlan(
            cards=0,
            name="followup",
            note="continue_existing_reading",
            need_clarify=False,
        )

    # chat → никакого расклада
    if route_action == "chat":
        return SpreadPlan(
            cards=0,
            name="chat",
            note="no_tarot",
            need_clarify=False,
        )

    # reading
    safe_cards = max(1, min(int(cards or 3), 7))
    safe_name = (spread_name or "").strip() or f"{safe_cards} карт"

    return SpreadPlan(
        cards=safe_cards,
        name=safe_name,
        note="router_decision",
        need_clarify=False,
    )