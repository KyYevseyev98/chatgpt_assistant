# tarot/deck.py
from __future__ import annotations

import os
import random
import re
from dataclasses import dataclass
from typing import List, Optional

from tarot.cards_data import get_meaning, CardMeaning


@dataclass(frozen=True)
class Card:
    key: str                  # e.g. MAJOR_00_FOOL, CUPS_07
    filename: str             # e.g. RWS1909_-_Cups_07.jpeg
    meaning: Optional[CardMeaning]


_MAJOR_RE = re.compile(r"RWS1909_-_(\d{2})_([A-Za-z_]+)\.jpe?g$", re.IGNORECASE)
_MINOR_RE = re.compile(r"RWS1909_-_(Cups|Wands|Swords|Pentacles)_(\d{2})\.jpe?g$", re.IGNORECASE)

_MAJOR_NAME_MAP = {
    "Fool": "FOOL",
    "Magician": "MAGICIAN",
    "High_Priestess": "HIGH_PRIESTESS",
    "Empress": "EMPRESS",
    "Emperor": "EMPEROR",
    "Hierophant": "HIEROPHANT",
    "Lovers": "LOVERS",
    "Chariot": "CHARIOT",
    "Strength": "STRENGTH",
    "Hermit": "HERMIT",
    "Wheel_of_Fortune": "WHEEL_OF_FORTUNE",
    "Justice": "JUSTICE",
    "Hanged_Man": "HANGED_MAN",
    "Death": "DEATH",
    "Temperance": "TEMPERANCE",
    "Devil": "DEVIL",
    "Tower": "TOWER",
    "Star": "STAR",
    "Moon": "MOON",
    "Sun": "SUN",
    "Judgement": "JUDGEMENT",
    "World": "WORLD",
}

_SUIT_MAP = {
    "Cups": "CUPS",
    "Wands": "WANDS",
    "Swords": "SWORDS",
    "Pentacles": "PENTACLES",
}


def _to_card_key(filename: str) -> Optional[str]:
    m = _MAJOR_RE.match(filename)
    if m:
        num = m.group(1)
        name = m.group(2)
        if name not in _MAJOR_NAME_MAP:
            return None
        return f"MAJOR_{num}_{_MAJOR_NAME_MAP[name]}"

    m = _MINOR_RE.match(filename)
    if m:
        suit = _SUIT_MAP[m.group(1)]
        rank = m.group(2)
        return f"{suit}_{rank}"

    return None


class Deck:
    def __init__(self, cards_dir: str):
        self.cards_dir = cards_dir
        self.filenames = self._load_filenames()
        if len(self.filenames) != 78:
            raise RuntimeError(f"Ожидал 78 карт, нашёл {len(self.filenames)} в {cards_dir}")

    def _load_filenames(self) -> List[str]:
        if not os.path.isdir(self.cards_dir):
            raise FileNotFoundError(f"Нет папки с картами: {self.cards_dir}")

        files = [
            f for f in os.listdir(self.cards_dir)
            if f.lower().endswith((".jpg", ".jpeg"))
        ]
        files.sort()
        return files

    def draw(self, n: int) -> List[Card]:
        if n <= 0:
            return []
        picks = random.sample(self.filenames, k=min(n, len(self.filenames)))
        out: List[Card] = []
        for fn in picks:
            key = _to_card_key(fn)
            out.append(Card(
                key=key or "UNKNOWN",
                filename=fn,
                meaning=get_meaning(key) if key else None,
            ))
        return out

    def abs_path(self, filename: str) -> str:
        return os.path.join(self.cards_dir, filename)


def get_default_deck() -> Deck:
    # project_root/assets/cards
    here = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(here)
    cards_dir = os.path.join(project_root, "assets", "cards")
    return Deck(cards_dir)


if __name__ == "__main__":
    deck = get_default_deck()
    cards = deck.draw(3)
    print(f"Всего карт: {len(deck.filenames)}")
    print("3 случайные карты:")
    for c in cards:
        nm = c.meaning.ru_name if c.meaning else c.key
        print(f"- {c.key} | {nm} | {c.filename}")