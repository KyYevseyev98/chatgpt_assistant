from __future__ import annotations

import datetime as dt
from typing import Any, Dict, List, Optional

from .connection import conn


def _now_iso() -> str:
    return dt.datetime.utcnow().isoformat()


def log_support_action(
    user_id: int,
    *,
    admin_id: Optional[int],
    delta: int,
    reason: str,
) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO support_actions (user_id, admin_id, delta, reason, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (int(user_id), int(admin_id) if admin_id is not None else None, int(delta), reason or "", _now_iso()),
    )
    conn.commit()


def get_support_actions(user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT admin_id, delta, reason, created_at
        FROM support_actions
        WHERE user_id = ?
        ORDER BY id DESC
        LIMIT ?
        """,
        (int(user_id), int(limit)),
    )
    rows = cur.fetchall() or []
    return [
        {
            "admin_id": r[0],
            "delta": r[1],
            "reason": r[2],
            "created_at": r[3],
        }
        for r in rows
    ]


def log_api_error(
    *,
    user_id: Optional[int],
    endpoint: str,
    status_code: int,
    error_text: str,
) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO api_errors (user_id, endpoint, status_code, error_text, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            int(user_id) if user_id is not None else None,
            endpoint or "",
            int(status_code),
            (error_text or "")[:1000],
            _now_iso(),
        ),
    )
    conn.commit()


def get_api_errors(user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT endpoint, status_code, error_text, created_at
        FROM api_errors
        WHERE user_id = ?
        ORDER BY id DESC
        LIMIT ?
        """,
        (int(user_id), int(limit)),
    )
    rows = cur.fetchall() or []
    return [
        {
            "endpoint": r[0],
            "status_code": r[1],
            "error_text": r[2],
            "created_at": r[3],
        }
        for r in rows
    ]
