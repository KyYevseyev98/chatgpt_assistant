from __future__ import annotations

def required_ignored_days_for_stage(stage: int) -> int:
    """Формула дней игнора для follow-up."""
    n = stage + 1
    return 1 + (3 * (n - 1) * n) // 2
