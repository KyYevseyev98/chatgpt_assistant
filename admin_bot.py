# admin_bot.py
import html
import json
import logging
import datetime as dt
from typing import Optional, Tuple, Dict, Any, List

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler,
    MessageHandler,
    filters,
)

from config import ADMIN_TG_TOKEN, ADMIN_IDS, STAR_USD_RATE, FREE_TAROT_FREE_COUNT
from db import (
    init_db,
    conn,
    get_tarot_limits_snapshot,
    get_last_tarot_history,
    get_support_actions,
    get_api_errors,
    adjust_tarot_balance,
    patch_user_profile_chat,
    set_user_blocked,
    is_user_blocked,
    get_user_by_username,
    log_support_action,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ============================================================
#  HELPERS
# ============================================================

def _utc_today() -> dt.date:
    return dt.datetime.utcnow().date()


def _is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


async def _ensure_admin(update: Update) -> bool:
    user = update.effective_user
    if not user:
        return False
    if not _is_admin(user.id):
        try:
            await update.effective_message.reply_text("⛔ Эта команда только для админов.")
        except Exception as e:
            logger.warning("Не удалось отправить отказ не-админу: %s", e)
        return False
    return True


def _period_info(period_key: str) -> Tuple[Optional[str], Optional[str], str, str]:
    """
    Возвращает:
      start_date (YYYY-MM-DD или None)
      end_date   (YYYY-MM-DD или None)
      label      (человеческое имя периода)
      range_text (строка периода)
    """
    today = _utc_today()

    if period_key == "today":
        start = end = today
        label = "Сегодня"
    elif period_key == "yesterday":
        end = today - dt.timedelta(days=1)
        start = end
        label = "Вчера"
    elif period_key == "7d":
        end = today
        start = today - dt.timedelta(days=6)
        label = "Последние 7 дней"
    elif period_key == "14d":
        end = today
        start = today - dt.timedelta(days=13)
        label = "Последние 14 дней"
    elif period_key == "28d":
        end = today
        start = today - dt.timedelta(days=27)
        label = "Последние 28 дней"
    else:
        return None, None, "За всё время", "всё время"

    start_s = start.isoformat()
    end_s = end.isoformat()
    range_text = start_s if start_s == end_s else f"{start_s} — {end_s}"
    return start_s, end_s, label, range_text


def _period_days(start_date: Optional[str], end_date: Optional[str], *, cur) -> int:
    """
    Возвращает количество дней в периоде.
    Для all-time (start/end None) берём диапазон по events.created_at.
    """
    if start_date and end_date:
        try:
            s = dt.datetime.fromisoformat(start_date)
            e = dt.datetime.fromisoformat(end_date)
            return max(1, (e - s).days + 1)
        except Exception:
            return 1

    cur.execute("SELECT MIN(created_at), MAX(created_at) FROM events")
    row = cur.fetchone() or (None, None)
    if not row[0] or not row[1]:
        return 1
    try:
        s = dt.datetime.fromisoformat(row[0])
        e = dt.datetime.fromisoformat(row[1])
        return max(1, (e - s).days + 1)
    except Exception:
        return 1


def _build_source_clause_users(alias: str, source: Optional[str]) -> Tuple[str, List[Any]]:
    """
    Фильтр по users.traffic_source
    """
    if not source or source == "all":
        return "", []
    if source == "organic":
        return f" AND ({alias}.traffic_source IS NULL OR {alias}.traffic_source = 'organic')", []
    return f" AND {alias}.traffic_source = ?", [source]


def _build_source_clause_pay(alias: str, source: Optional[str]) -> Tuple[str, List[Any]]:
    """
    Фильтр по pro_payments.traffic_source
    """
    if not source or source == "all":
        return "", []
    if source == "organic":
        return f" AND ({alias}.traffic_source IS NULL OR {alias}.traffic_source = 'organic')", []
    return f" AND {alias}.traffic_source = ?", [source]


def _build_date_clause(alias: str, start_date: Optional[str], end_date: Optional[str]) -> Tuple[str, List[Any]]:
    """
    Фильтр по дате в ISO created_at (YYYY-MM-DD...)
    """
    if not start_date or not end_date:
        return "", []
    return f" AND substr({alias}.created_at, 1, 10) BETWEEN ? AND ?", [start_date, end_date]


def _safe_div(num: float, den: float) -> float:
    return 0.0 if not den else num / den


def _fmt_pct(value: float) -> str:
    return f"{value * 100:.1f}%"


def _source_label(source: Optional[str]) -> str:
    if not source or source == "all":
        return "все"
    if source == "organic":
        return "organic (без тега)"
    return str(source)


def _fmt_stars_usd(stars: int) -> str:
    usd = float(stars) * float(STAR_USD_RATE)
    return f"{stars}⭐ (~${usd:.2f})"


def _h(text: Optional[str]) -> str:
    return html.escape(str(text)) if text is not None else ""


def _parse_topic_from_meta(meta: Optional[str]) -> Optional[str]:
    """
    meta пример: "topic:nutrition;batch_size:2"
    """
    if not meta:
        return None


def _fetch_user_row(user_id: int):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT user_id, username, first_name, last_name, created_at, last_activity_at, is_blocked
        FROM users
        WHERE user_id = ?
        """,
        (int(user_id),),
    )
    return cur.fetchone()


def _format_user_card(user_id: int) -> str:
    row = _fetch_user_row(user_id)
    if not row:
        return "Пользователь не найден."

    uid, username, first_name, last_name, created_at, last_activity_at, is_blocked_flag = row
    status = "blocked" if int(is_blocked_flag or 0) == 1 else "active"

    snap = get_tarot_limits_snapshot(user_id, user_id)
    balance = int(snap.get("tarot_free_lifetime_left") or 0) + int(snap.get("tarot_credits") or 0)

    lines = []
    lines.append("👤 <b>Профиль пользователя</b>")
    lines.append(f"• user_id: <b>{uid}</b>")
    lines.append(f"• telegram_user_id: <b>{uid}</b>")
    lines.append(f"• username: <b>@{_h(username)}</b>" if username else "• username: —")
    name = " ".join([x for x in [first_name, last_name] if x]) or "—"
    lines.append(f"• имя: <b>{_h(name)}</b>")
    lines.append(f"• дата регистрации: <b>{_h(created_at or '—')}</b>")
    lines.append(f"• активность: <b>{_h(last_activity_at or '—')}</b>")
    lines.append(f"• статус: <b>{_h(status)}</b>")
    lines.append(f"• баланс раскладов: <b>{balance}</b> (free={snap.get('tarot_free_lifetime_left')}, credits={snap.get('tarot_credits')})")
    lines.append("")

    # history: tarot
    tarot_hist = get_last_tarot_history(user_id, limit=5) or []
    lines.append("🃏 <b>Последние расклады</b>")
    if not tarot_hist:
        lines.append("• нет данных")
    else:
        for h in tarot_hist:
            lines.append(f"• {_h(h.get('created_at'))}: {_h(h.get('spread_name') or 'Расклад')}")
    lines.append("")

    # support actions
    actions = get_support_actions(user_id, limit=5) or []
    lines.append("🧾 <b>Баланс: изменения</b>")
    if not actions:
        lines.append("• нет данных")
    else:
        for a in actions:
            lines.append(f"• {_h(a.get('created_at'))} | {a.get('delta')} | {_h(a.get('reason'))}")
    lines.append("")

    # api errors
    errs = get_api_errors(user_id, limit=5) or []
    lines.append("🧯 <b>Ошибки API</b>")
    if not errs:
        lines.append("• нет данных")
    else:
        for e in errs:
            lines.append(f"• {_h(e.get('created_at'))} | {_h(e.get('endpoint'))} | {e.get('status_code')} | {_h(e.get('error_text'))}")

    return "\n".join(lines)


def _user_action_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("＋1", callback_data=f"user:add:1:{user_id}"),
                InlineKeyboardButton("＋3", callback_data=f"user:add:3:{user_id}"),
                InlineKeyboardButton("＋5", callback_data=f"user:add:5:{user_id}"),
            ],
            [
                InlineKeyboardButton("－1", callback_data=f"user:sub:1:{user_id}"),
                InlineKeyboardButton("－3", callback_data=f"user:sub:3:{user_id}"),
                InlineKeyboardButton("－5", callback_data=f"user:sub:5:{user_id}"),
            ],
            [
                InlineKeyboardButton("Сбросить сессию", callback_data=f"user:reset:{user_id}"),
                InlineKeyboardButton("Обновить", callback_data=f"user:refresh:{user_id}"),
            ],
            [
                InlineKeyboardButton("Заблокировать", callback_data=f"user:block:{user_id}"),
                InlineKeyboardButton("Разблокировать", callback_data=f"user:unblock:{user_id}"),
            ],
        ]
    )
    if "topic:" not in meta:
        return None
    try:
        after = meta.split("topic:", 1)[1]
        topic = after.split(";", 1)[0].strip()
        return topic or None
    except Exception:
        return None


def _safe_load_json(s: Optional[str], fallback):
    if not s:
        return fallback
    try:
        return json.loads(s)
    except Exception:
        return fallback


def _parse_kv_meta(meta: Optional[str]) -> Dict[str, str]:
    """
    meta format: "k1:v1;k2:v2"
    """
    out: Dict[str, str] = {}
    if not meta:
        return out
    parts = [p for p in meta.split(";") if ":" in p]
    for p in parts:
        k, v = p.split(":", 1)
        out[k.strip()] = v.strip()
    return out


# ============================================================
#  CORE: STATS
# ============================================================

def _compute_stats(period_key: str, source: Optional[str] = None) -> Dict[str, Any]:
    start_date, end_date, period_label, range_text = _period_info(period_key)
    cur = conn.cursor()
    days_in_period = _period_days(start_date, end_date, cur=cur)

    src = source or "all"

    # --- total users in base (filtered by users.traffic_source) ---
    u_clause, u_params = _build_source_clause_users("u", src)
    cur.execute(
        f"SELECT COUNT(*) FROM users u WHERE 1=1 {u_clause}",
        u_params,
    )
    total_users_base = cur.fetchone()[0] or 0

    # --- follow-up state ---
    # сколько юзеров вообще получили хотя бы 1 follow-up
    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM users u
        WHERE 1=1 {u_clause}
          AND (u.followup_stage > 0 OR u.last_followup_at IS NOT NULL)
        """,
        u_params,
    )
    users_with_followups = cur.fetchone()[0] or 0

    # --- followups sent today (по last_followup_at) ---
    today_s = _utc_today().isoformat()
    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM users u
        WHERE 1=1 {u_clause}
          AND u.last_followup_at IS NOT NULL
          AND substr(u.last_followup_at, 1, 10) = ?
        """,
        u_params + [today_s],
    )
    followups_today = cur.fetchone()[0] or 0

    # --- events by type in period ---
    eu_clause, eu_params = _build_source_clause_users("u", src)
    e_clause, e_params = _build_date_clause("e", start_date, end_date)
    params_events = eu_params + e_params

    cur.execute(
        f"""
        SELECT e.event_type, COUNT(*)
        FROM events e
        LEFT JOIN users u ON u.user_id = e.user_id
        WHERE 1=1 {eu_clause} {e_clause}
        GROUP BY e.event_type
        """,
        params_events,
    )
    rows = cur.fetchall()
    events_by_type: Dict[str, int] = {t: c for (t, c) in rows}

    # ✅ start events are "start:<source>" now
    # считаем все start:* за период
    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM events e
        LEFT JOIN users u ON u.user_id = e.user_id
        WHERE 1=1 {eu_clause} {e_clause}
          AND e.event_type LIKE 'start:%'
        """,
        params_events,
    )
    start_cnt = cur.fetchone()[0] or 0

    text_cnt = events_by_type.get("text", 0)
    voice_cnt = events_by_type.get("voice", 0)
    photo_cnt = events_by_type.get("photo", 0)
    messages_total = text_cnt + voice_cnt + photo_cnt
    messages_per_day = _safe_div(messages_total, days_in_period)

    # --- active users in period (sent at least 1 msg) ---
    cur.execute(
        f"""
        SELECT COUNT(DISTINCT e.user_id)
        FROM events e
        LEFT JOIN users u ON u.user_id = e.user_id
        WHERE 1=1 {eu_clause} {e_clause}
          AND e.event_type IN ('text','voice','photo')
        """,
        params_events,
    )
    active_users = cur.fetchone()[0] or 0

    # --- tarot readings in period ---
    cur.execute(
        f"""
        SELECT COUNT(*), COUNT(DISTINCT e.user_id)
        FROM events e
        LEFT JOIN users u ON u.user_id = e.user_id
        WHERE 1=1 {eu_clause} {e_clause}
          AND e.event_type = 'tarot'
        """,
        params_events,
    )
    row_tarot = cur.fetchone()
    tarot_cnt = row_tarot[0] or 0
    tarot_users = row_tarot[1] or 0
    tarot_per_user = _safe_div(tarot_cnt, tarot_users or 1)
    tarot_per_active = _safe_div(tarot_cnt, active_users or 1)
    tarot_per_day = _safe_div(tarot_cnt, days_in_period)

    tarot_cards_sum = 0
    tarot_cards_n = 0
    cur.execute(
        f"""
        SELECT e.meta
        FROM events e
        LEFT JOIN users u ON u.user_id = e.user_id
        WHERE 1=1 {eu_clause} {e_clause}
          AND e.event_type = 'tarot'
        """,
        params_events,
    )
    for (meta,) in cur.fetchall() or []:
        kv = _parse_kv_meta(meta)
        if "cards" in kv:
            try:
                tarot_cards_sum += int(kv.get("cards") or 0)
                tarot_cards_n += 1
            except Exception:
                pass
    avg_tarot_cards = _safe_div(tarot_cards_sum, tarot_cards_n or 1)

    # --- paywall shown ---
    cur.execute(
        f"""
        SELECT COUNT(*), COUNT(DISTINCT e.user_id)
        FROM events e
        LEFT JOIN users u ON u.user_id = e.user_id
        WHERE 1=1 {eu_clause} {e_clause}
          AND e.event_type = 'tarot_paywall'
        """,
        params_events,
    )
    row_pw = cur.fetchone()
    paywall_cnt = row_pw[0] or 0
    paywall_users = row_pw[1] or 0

    # --- purchases (tarot packs) ---
    cur.execute(
        f"""
        SELECT COUNT(*), COUNT(DISTINCT e.user_id)
        FROM events e
        LEFT JOIN users u ON u.user_id = e.user_id
        WHERE 1=1 {eu_clause} {e_clause}
          AND e.event_type = 'tarot_purchase'
        """,
        params_events,
    )
    row_buy = cur.fetchone()
    purchase_cnt = row_buy[0] or 0
    purchase_users = row_buy[1] or 0

    stars_sum = 0
    spreads_sum = 0
    pack_counts: Dict[str, int] = {}
    cur.execute(
        f"""
        SELECT e.meta
        FROM events e
        LEFT JOIN users u ON u.user_id = e.user_id
        WHERE 1=1 {eu_clause} {e_clause}
          AND e.event_type = 'tarot_purchase'
        """,
        params_events,
    )
    for (meta,) in cur.fetchall() or []:
        kv = _parse_kv_meta(meta)
        try:
            stars_sum += int(kv.get("stars") or 0)
            spreads_sum += int(kv.get("spreads") or 0)
            pack = kv.get("pack") or ""
            if pack:
                pack_counts[pack] = pack_counts.get(pack, 0) + 1
        except Exception:
            pass
    avg_purchase_stars = _safe_div(stars_sum, purchase_cnt or 1)
    purchases_per_user = _safe_div(purchase_cnt, purchase_users or 1)
    avg_stars_per_payer = _safe_div(stars_sum, purchase_users or 1)

    # --- referrals ---
    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM events e
        LEFT JOIN users u ON u.user_id = e.user_id
        WHERE 1=1 {eu_clause} {e_clause}
          AND e.event_type = 'ref_start'
        """,
        params_events,
    )
    ref_start_cnt = cur.fetchone()[0] or 0

    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM events e
        LEFT JOIN users u ON u.user_id = e.user_id
        WHERE 1=1 {eu_clause} {e_clause}
          AND e.event_type = 'ref_reward'
        """,
        params_events,
    )
    ref_reward_cnt = cur.fetchone()[0] or 0

    cur.execute(
        f"""
        SELECT e.meta
        FROM events e
        LEFT JOIN users u ON u.user_id = e.user_id
        WHERE 1=1 {eu_clause} {e_clause}
          AND e.event_type = 'ref_start'
        """,
        params_events,
    )
    ref_inviter_counts: Dict[str, int] = {}
    for (meta,) in cur.fetchall() or []:
        kv = _parse_kv_meta(meta)
        inviter = kv.get("inviter") or ""
        if inviter:
            ref_inviter_counts[inviter] = ref_inviter_counts.get(inviter, 0) + 1
    ref_inviters = len(ref_inviter_counts)
    ref_avg_per_inviter = _safe_div(ref_start_cnt, ref_inviters or 1)

    # --- limit events in period (text/photo/voice limit) ---
    cur.execute(
        f"""
        SELECT COUNT(*), COUNT(DISTINCT e.user_id)
        FROM events e
        LEFT JOIN users u ON u.user_id = e.user_id
        WHERE 1=1 {eu_clause} {e_clause}
          AND e.event_type LIKE '%limit%'
        """,
        params_events,
    )
    limit_row = cur.fetchone()
    limit_events_cnt = limit_row[0] or 0
    limit_users_cnt = limit_row[1] or 0

    # --- returned after ignore (>2 days) ---
    returned_after_ignore = 0
    if start_date and end_date:
        cur.execute(
            f"""
            SELECT e.user_id, MIN(e.created_at) AS first_in_period
            FROM events e
            LEFT JOIN users u ON u.user_id = e.user_id
            WHERE 1=1 {eu_clause} {e_clause}
              AND e.event_type IN ('text','voice','photo')
            GROUP BY e.user_id
            """,
            params_events,
        )
        rows_first = cur.fetchall()

        for user_id, first_ts in rows_first:
            if not first_ts:
                continue
            try:
                first_dt = dt.datetime.fromisoformat(first_ts)
            except Exception:
                continue

            cur.execute(
                """
                SELECT MAX(created_at)
                FROM events
                WHERE user_id = ?
                  AND event_type IN ('text','voice','photo')
                  AND created_at < ?
                """,
                (user_id, first_ts),
            )
            prev_ts = cur.fetchone()[0]
            if not prev_ts:
                continue
            try:
                prev_dt = dt.datetime.fromisoformat(prev_ts)
            except Exception:
                continue

            delta_days = (first_dt - prev_dt).total_seconds() / 86400.0
            if delta_days >= 2.0:
                returned_after_ignore += 1

    # --- avg messages per active user ---
    avg_msgs_per_user = _safe_div(messages_total, active_users)

    # --- TOP topics (from events.meta topic:...) ---
    top_topics: List[Tuple[str, int]] = []
    if start_date and end_date:
        cur.execute(
            f"""
            SELECT e.meta, COUNT(*)
            FROM events e
            LEFT JOIN users u ON u.user_id = e.user_id
            WHERE 1=1 {eu_clause} {e_clause}
              AND e.event_type = 'text'
              AND e.meta LIKE 'topic:%'
            GROUP BY e.meta
            """,
            params_events,
        )
        rows_meta = cur.fetchall()
        topic_map: Dict[str, int] = {}
        for meta, cnt in rows_meta:
            t = _parse_topic_from_meta(meta)
            if not t:
                continue
            topic_map[t] = topic_map.get(t, 0) + int(cnt or 0)
        top_topics = sorted(topic_map.items(), key=lambda x: x[1], reverse=True)[:5]

    # --- TOP limit topics (from users.last_limit_topic) ---
    cur.execute(
        f"""
        SELECT u.last_limit_topic, COUNT(*)
        FROM users u
        WHERE 1=1 {u_clause}
          AND u.last_limit_topic IS NOT NULL
          AND u.last_limit_topic != ''
        GROUP BY u.last_limit_topic
        ORDER BY COUNT(*) DESC
        LIMIT 5
        """,
        u_params,
    )
    top_limit_topics = [(r[0], r[1]) for r in cur.fetchall() if r and r[0]]

    # --- billing aggregate (credits + free left) ---
    total_credits = 0
    total_free_left = 0
    users_with_credits = 0
    users_with_free = 0
    users_with_billing = 0
    cur.execute(
        f"""
        SELECT up.json_profile
        FROM user_profile up
        LEFT JOIN users u ON u.user_id = up.user_id
        WHERE 1=1 {u_clause}
        """,
        u_params,
    )
    for (json_profile,) in cur.fetchall() or []:
        prof = _safe_load_json(json_profile, {}) or {}
        billing = prof.get("billing") or {}
        credits = int(billing.get("tarot_credits") or 0)
        free_used = int(billing.get("tarot_free_used") or 0)
        free_left = max(0, int(FREE_TAROT_FREE_COUNT) - free_used)
        total_credits += credits
        total_free_left += free_left
        if credits > 0:
            users_with_credits += 1
        if free_left > 0:
            users_with_free += 1
        users_with_billing += 1

    # --- churn funnel by tarot counts (within period) ---
    cur.execute(
        f"""
        SELECT e.user_id, COUNT(*) AS cnt
        FROM events e
        LEFT JOIN users u ON u.user_id = e.user_id
        WHERE 1=1 {eu_clause} {e_clause}
          AND e.event_type = 'tarot_reading'
        GROUP BY e.user_id
        """,
        params_events,
    )
    tarot_counts_by_user: Dict[int, int] = {int(uid): int(cnt) for uid, cnt in cur.fetchall() or []}
    cur.execute(
        f"""
        SELECT DISTINCT e.user_id
        FROM events e
        LEFT JOIN users u ON u.user_id = e.user_id
        WHERE 1=1 {eu_clause} {e_clause}
          AND e.event_type LIKE 'start:%'
        """,
        params_events,
    )
    start_users = [int(r[0]) for r in cur.fetchall() or []]
    start_users_set = set(start_users)
    start_cnt_users = len(start_users_set) if start_users_set else 0

    tarot0 = 0
    tarot1 = 0
    tarot2 = 0
    tarot3 = 0
    tarot3plus = 0
    for uid in start_users_set:
        c = tarot_counts_by_user.get(uid, 0)
        if c <= 0:
            tarot0 += 1
        elif c == 1:
            tarot1 += 1
        elif c == 2:
            tarot2 += 1
        elif c == 3:
            tarot3 += 1
        else:
            tarot3plus += 1

    pct_tarot0 = _safe_div(tarot0, start_cnt_users or 1)
    pct_tarot1 = _safe_div(tarot1, start_cnt_users or 1)
    pct_tarot2 = _safe_div(tarot2, start_cnt_users or 1)
    pct_tarot3 = _safe_div(tarot3, start_cnt_users or 1)
    pct_tarot3plus = _safe_div(tarot3plus, start_cnt_users or 1)

    # among users with >=3 tarot readings in period: referral usage and purchases
    tarot3_users = {uid for uid, cnt in tarot_counts_by_user.items() if cnt >= 3}
    if tarot3_users:
        placeholders = ",".join(["?"] * len(tarot3_users))
        cur.execute(
            f"""
            SELECT COUNT(DISTINCT e.user_id)
            FROM events e
            WHERE e.event_type IN ('ref_reward','ref_start')
              AND e.user_id IN ({placeholders})
            """,
            list(tarot3_users),
        )
        tarot3_ref_users = cur.fetchone()[0] or 0
        cur.execute(
            f"""
            SELECT COUNT(DISTINCT e.user_id)
            FROM events e
            WHERE e.event_type = 'tarot_purchase'
              AND e.user_id IN ({placeholders})
            """,
            list(tarot3_users),
        )
        tarot3_buy_users = cur.fetchone()[0] or 0
    else:
        tarot3_ref_users = 0
        tarot3_buy_users = 0
    pct_tarot3_ref = _safe_div(tarot3_ref_users, len(tarot3_users) or 1)
    pct_tarot3_buy = _safe_div(tarot3_buy_users, len(tarot3_users) or 1)

    # ====================================================
    # CONVERSIONS
    # ====================================================
    base_users = total_users_base if total_users_base > 0 else 1

    pct_active_users = _safe_div(active_users, base_users)
    pct_start_to_first_msg = _safe_div(active_users, start_cnt or 1)
    pct_first_to_limit = _safe_div(limit_users_cnt, active_users or 1)
    pct_active_to_tarot = _safe_div(tarot_users, active_users or 1)
    pct_tarot_to_paywall = _safe_div(paywall_users, tarot_users or 1)
    pct_paywall_to_purchase = _safe_div(purchase_users, paywall_users or 1)
    pct_ref_reward = _safe_div(ref_reward_cnt, ref_start_cnt or 1)

    pct_text_of_msgs = _safe_div(text_cnt, messages_total or 1)
    pct_voice_of_msgs = _safe_div(voice_cnt, messages_total or 1)
    pct_photo_of_msgs = _safe_div(photo_cnt, messages_total or 1)

    return {
        "period_key": period_key,
        "period_label": period_label,
        "range_text": range_text,
        "source": src,

        "total_users_base": total_users_base,

        "start_cnt": start_cnt,
        "active_users": active_users,
        "returned_after_ignore": returned_after_ignore,

        "messages_total": messages_total,
        "avg_msgs_per_user": avg_msgs_per_user,
        "text_cnt": text_cnt,
        "voice_cnt": voice_cnt,
        "photo_cnt": photo_cnt,
        "messages_per_day": messages_per_day,

        "tarot_cnt": tarot_cnt,
        "tarot_users": tarot_users,
        "avg_tarot_cards": avg_tarot_cards,
        "tarot_per_user": tarot_per_user,
        "tarot_per_active": tarot_per_active,
        "tarot_per_day": tarot_per_day,

        "paywall_cnt": paywall_cnt,
        "paywall_users": paywall_users,

        "purchase_cnt": purchase_cnt,
        "purchase_users": purchase_users,
        "stars_sum": stars_sum,
        "spreads_sum": spreads_sum,
        "avg_purchase_stars": avg_purchase_stars,
        "purchases_per_user": purchases_per_user,
        "avg_stars_per_payer": avg_stars_per_payer,
        "pack_counts": pack_counts,

        "ref_start_cnt": ref_start_cnt,
        "ref_reward_cnt": ref_reward_cnt,
        "ref_inviters": ref_inviters,
        "ref_avg_per_inviter": ref_avg_per_inviter,

        "limit_events_cnt": limit_events_cnt,
        "limit_users_cnt": limit_users_cnt,

        "users_with_followups": users_with_followups,
        "followups_today": followups_today,

        "top_topics": top_topics,
        "top_limit_topics": top_limit_topics,

        "total_credits": total_credits,
        "total_free_left": total_free_left,
        "users_with_credits": users_with_credits,
        "users_with_free": users_with_free,
        "users_with_billing": users_with_billing,

        "start_cnt_users": start_cnt_users,
        "tarot0": tarot0,
        "tarot1": tarot1,
        "tarot2": tarot2,
        "tarot3": tarot3,
        "tarot3plus": tarot3plus,
        "pct_tarot0": pct_tarot0,
        "pct_tarot1": pct_tarot1,
        "pct_tarot2": pct_tarot2,
        "pct_tarot3": pct_tarot3,
        "pct_tarot3plus": pct_tarot3plus,
        "pct_tarot3_ref": pct_tarot3_ref,
        "pct_tarot3_buy": pct_tarot3_buy,

        # pct
        "pct_active_users": pct_active_users,
        "pct_start_to_first_msg": pct_start_to_first_msg,
        "pct_first_to_limit": pct_first_to_limit,
        "pct_active_to_tarot": pct_active_to_tarot,
        "pct_tarot_to_paywall": pct_tarot_to_paywall,
        "pct_paywall_to_purchase": pct_paywall_to_purchase,
        "pct_ref_reward": pct_ref_reward,
        "pct_text_of_msgs": pct_text_of_msgs,
        "pct_voice_of_msgs": pct_voice_of_msgs,
        "pct_photo_of_msgs": pct_photo_of_msgs,
    }


def _build_stats_keyboard(period_key: str, source: Optional[str]) -> InlineKeyboardMarkup:
    src = source or "all"
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Сегодня", callback_data=f"stats:today:{src}"),
                InlineKeyboardButton("Вчера", callback_data=f"stats:yesterday:{src}"),
            ],
            [
                InlineKeyboardButton("7 дней", callback_data=f"stats:7d:{src}"),
                InlineKeyboardButton("14 дней", callback_data=f"stats:14d:{src}"),
                InlineKeyboardButton("28 дней", callback_data=f"stats:28d:{src}"),
            ],
            [InlineKeyboardButton("За всё время", callback_data=f"stats:all:{src}")],
        ]
    )


def _format_stats_text(stats: Dict[str, Any]) -> str:
    period_label = stats["period_label"]
    range_text = stats["range_text"]
    source = stats["source"]

    total_users_base = stats["total_users_base"]
    start_cnt = stats["start_cnt"]
    active_users = stats["active_users"]
    returned_after_ignore = stats["returned_after_ignore"]

    messages_total = stats["messages_total"]
    avg_msgs_per_user = stats["avg_msgs_per_user"]
    text_cnt = stats["text_cnt"]
    voice_cnt = stats["voice_cnt"]
    photo_cnt = stats["photo_cnt"]
    messages_per_day = stats["messages_per_day"]

    tarot_cnt = stats["tarot_cnt"]
    tarot_users = stats["tarot_users"]
    avg_tarot_cards = stats["avg_tarot_cards"]
    tarot_per_user = stats["tarot_per_user"]
    tarot_per_active = stats["tarot_per_active"]
    tarot_per_day = stats["tarot_per_day"]

    paywall_cnt = stats["paywall_cnt"]
    paywall_users = stats["paywall_users"]

    purchase_cnt = stats["purchase_cnt"]
    purchase_users = stats["purchase_users"]
    stars_sum = stats["stars_sum"]
    spreads_sum = stats["spreads_sum"]
    avg_purchase_stars = stats["avg_purchase_stars"]
    purchases_per_user = stats["purchases_per_user"]
    avg_stars_per_payer = stats["avg_stars_per_payer"]
    pack_counts = stats.get("pack_counts") or {}

    ref_start_cnt = stats["ref_start_cnt"]
    ref_reward_cnt = stats["ref_reward_cnt"]
    ref_inviters = stats["ref_inviters"]
    ref_avg_per_inviter = stats["ref_avg_per_inviter"]

    limit_events_cnt = stats["limit_events_cnt"]
    limit_users_cnt = stats["limit_users_cnt"]

    users_with_followups = stats["users_with_followups"]
    followups_today = stats["followups_today"]

    total_credits = stats["total_credits"]
    total_free_left = stats["total_free_left"]
    users_with_credits = stats["users_with_credits"]
    users_with_free = stats["users_with_free"]
    users_with_billing = stats["users_with_billing"]

    top_topics = stats.get("top_topics") or []
    top_limit_topics = stats.get("top_limit_topics") or []

    pct_active_users = _fmt_pct(stats["pct_active_users"])
    pct_start_to_first_msg = _fmt_pct(stats["pct_start_to_first_msg"])
    pct_first_to_limit = _fmt_pct(stats["pct_first_to_limit"])
    pct_active_to_tarot = _fmt_pct(stats["pct_active_to_tarot"])
    pct_tarot_to_paywall = _fmt_pct(stats["pct_tarot_to_paywall"])
    pct_paywall_to_purchase = _fmt_pct(stats["pct_paywall_to_purchase"])
    pct_ref_reward = _fmt_pct(stats["pct_ref_reward"])

    pct_text_of_msgs = _fmt_pct(stats["pct_text_of_msgs"])
    pct_voice_of_msgs = _fmt_pct(stats["pct_voice_of_msgs"])
    pct_photo_of_msgs = _fmt_pct(stats["pct_photo_of_msgs"])

    start_cnt_users = stats["start_cnt_users"]
    tarot0 = stats["tarot0"]
    tarot1 = stats["tarot1"]
    tarot2 = stats["tarot2"]
    tarot3 = stats["tarot3"]
    tarot3plus = stats["tarot3plus"]
    pct_tarot0 = _fmt_pct(stats["pct_tarot0"])
    pct_tarot1 = _fmt_pct(stats["pct_tarot1"])
    pct_tarot2 = _fmt_pct(stats["pct_tarot2"])
    pct_tarot3 = _fmt_pct(stats["pct_tarot3"])
    pct_tarot3plus = _fmt_pct(stats["pct_tarot3plus"])
    pct_tarot3_ref = _fmt_pct(stats["pct_tarot3_ref"])
    pct_tarot3_buy = _fmt_pct(stats["pct_tarot3_buy"])

    lines: List[str] = []
    lines.append(f"📊 <b>Статистика — {period_label}</b>")
    lines.append(f"🗓 Период: {range_text}")
    lines.append(f"🔗 Источник: <b>{_h(_source_label(source))}</b>")
    lines.append("")

    lines.append("👥 <b>Пользователи</b>")
    lines.append(f"• Всего в базе: <b>{total_users_base}</b>")
    lines.append(f"• Новые старты за период: <b>{start_cnt}</b>")
    lines.append(f"• Активные (сообщения): <b>{active_users}</b> ({pct_active_users} от базы)")
    lines.append(f"• Вернулись после игнора >2 дней: <b>{returned_after_ignore}</b>")
    lines.append("")

    lines.append("💬 <b>Сообщения</b>")
    lines.append(f"• Всего: <b>{messages_total}</b> (в день: {messages_per_day:.1f})")
    lines.append(f"• Среднее на активного: <b>{avg_msgs_per_user:.1f}</b>")
    lines.append(f"• Текст: <b>{text_cnt}</b> ({pct_text_of_msgs})")
    lines.append(f"• Голос: <b>{voice_cnt}</b> ({pct_voice_of_msgs})")
    lines.append(f"• Фото: <b>{photo_cnt}</b> ({pct_photo_of_msgs})")
    lines.append("")

    lines.append("🃏 <b>Таро</b>")
    lines.append(f"• Раскладов: <b>{tarot_cnt}</b> (в день: {tarot_per_day:.1f})")
    lines.append(f"• Уникальных пользователей: <b>{tarot_users}</b> ({pct_active_to_tarot} от активных)")
    lines.append(f"• Среднее число карт: <b>{avg_tarot_cards:.1f}</b>")
    lines.append(f"• Раскладов на юзера: <b>{tarot_per_user:.2f}</b>")
    lines.append(f"• Раскладов на активного: <b>{tarot_per_active:.2f}</b>")
    lines.append(f"• Пейволлов: <b>{paywall_cnt}</b> / юзеров: <b>{paywall_users}</b> ({pct_tarot_to_paywall} от таро)")
    lines.append("")

    lines.append("💰 <b>Покупки раскладов</b>")
    lines.append(f"• Покупок: <b>{purchase_cnt}</b> / юзеров: <b>{purchase_users}</b>")
    lines.append(f"• Куплено раскладов: <b>{spreads_sum}</b>")
    lines.append(f"• Сумма оплат: <b>{_fmt_stars_usd(stars_sum)}</b>")
    lines.append(f"• Средний чек: <b>{avg_purchase_stars:.1f}⭐</b>")
    lines.append(f"• Покупок на юзера: <b>{purchases_per_user:.2f}</b>")
    lines.append(f"• Средняя оплата на платящего: <b>{avg_stars_per_payer:.1f}⭐</b>")
    lines.append(f"• Конверсия paywall → покупка: <b>{pct_paywall_to_purchase}</b>")
    lines.append("")

    if pack_counts:
        lines.append("📦 <b>Пакеты (частота)</b>")
        for k, v in sorted(pack_counts.items(), key=lambda x: x[1], reverse=True):
            lines.append(f"• {k} раскладов: {v}")
        lines.append("")

    lines.append("🎯 <b>Рефералы</b>")
    lines.append(f"• Переходы по реф. ссылке: <b>{ref_start_cnt}</b>")
    lines.append(f"• Наград выдано: <b>{ref_reward_cnt}</b> ({pct_ref_reward} от реф. стартов)")
    lines.append(f"• Пользователей, кто привёл рефералов: <b>{ref_inviters}</b>")
    lines.append(f"• Среднее рефералов на пригласившего: <b>{ref_avg_per_inviter:.2f}</b>")
    lines.append("")

    lines.append("💳 <b>Баланс (снимок)</b>")
    lines.append(f"• Всего кредитов на балансе: <b>{total_credits}</b>")
    lines.append(f"• Бесплатных раскладов осталось: <b>{total_free_left}</b>")
    lines.append(f"• Юзеров с кредитами: <b>{users_with_credits}</b>")
    lines.append(f"• Юзеров с бесплатными: <b>{users_with_free}</b>")
    lines.append(f"• Юзеров с billing-профилем: <b>{users_with_billing}</b>")
    lines.append("")

    lines.append("🚦 <b>Ограничения</b>")
    lines.append(f"• Срабатываний лимитов: <b>{limit_events_cnt}</b>")
    lines.append(f"• Юзеров, уперлись в лимит: <b>{limit_users_cnt}</b> ({pct_first_to_limit} от активных)")
    lines.append("")

    lines.append("📨 <b>Рассылки / фоллоуапы</b>")
    lines.append(f"• Пользователей с follow-up: <b>{users_with_followups}</b>")
    lines.append(f"• Отправлено сегодня: <b>{followups_today}</b>")
    lines.append("")

    lines.append("🧩 <b>Воронка</b>")
    lines.append(f"1️⃣ start → msg: <b>{pct_start_to_first_msg}</b>")
    lines.append(f"2️⃣ msg → limit: <b>{pct_first_to_limit}</b>")
    lines.append(f"3️⃣ msg → tarot: <b>{pct_active_to_tarot}</b>")
    lines.append(f"4️⃣ tarot → paywall: <b>{pct_tarot_to_paywall}</b>")
    lines.append(f"5️⃣ paywall → purchase: <b>{pct_paywall_to_purchase}</b>")
    lines.append("")

    lines.append("🧮 <b>Отвалы по раскладам (среди стартов периода)</b>")
    lines.append(f"• 0 раскладов: <b>{tarot0}</b> ({pct_tarot0})")
    lines.append(f"• 1 расклад: <b>{tarot1}</b> ({pct_tarot1})")
    lines.append(f"• 2 расклада: <b>{tarot2}</b> ({pct_tarot2})")
    lines.append(f"• 3 расклада: <b>{tarot3}</b> ({pct_tarot3})")
    lines.append(f"• 4+ расклада: <b>{tarot3plus}</b> ({pct_tarot3plus})")
    lines.append(f"• Из 3+ раскладов: рефералы <b>{pct_tarot3_ref}</b>, покупки <b>{pct_tarot3_buy}</b>")
    lines.append("")

    if top_topics:
        lines.append("🔥 <b>Топ тем (по text.meta)</b>")
        for t, c in top_topics:
            lines.append(f"• {_h(t)}: {c}")
        lines.append("")

    if top_limit_topics:
        lines.append("⛔ <b>Топ тем, где упираются в лимит (last_limit_topic)</b>")
        for t, c in top_limit_topics:
            lines.append(f"• {_h(t)}: {c}")
        lines.append("")

    return "\n".join(lines)


# ============================================================
#  HANDLERS
# ============================================================

async def admin_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _ensure_admin(update):
        return

    text = (
        "👋 <b>Админ-панель Astra</b>\n\n"
        "Команды:\n"
        "• /stats — статистика (кнопки периодов)\n"
        "• /offers — список источников (/start?src=...)\n"
        "• /user &lt;id|@username&gt; — профиль пользователя\n"
    )
    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Статистика (сегодня)", callback_data="stats:today:all")],
            [InlineKeyboardButton("Источники", callback_data="offer_stats:all:today")],
        ]
    )
    await update.message.reply_text(text, parse_mode="HTML", reply_markup=keyboard)


async def debug_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _ensure_admin(update):
        return
    msg = update.message
    if not msg:
        return
    chat_id = msg.chat_id
    chat_type = msg.chat.type if msg.chat else "unknown"
    title = msg.chat.title if msg.chat else ""
    text = (
        f"chat_id: <code>{chat_id}</code>\n"
        f"type: <code>{chat_type}</code>\n"
        f"title: <code>{_h(title)}</code>"
    )
    await msg.reply_text(text, parse_mode="HTML")


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /stats [period] [source]
    Пример: /stats 7d ads_tt
    """
    if not await _ensure_admin(update):
        return

    args = context.args or []
    period_key = args[0] if args else "today"
    source = args[1] if len(args) >= 2 else "all"

    valid_periods = {"today", "yesterday", "7d", "14d", "28d", "all"}
    if period_key not in valid_periods:
        period_key = "today"

    stats = _compute_stats(period_key, source)
    text = _format_stats_text(stats)
    keyboard = _build_stats_keyboard(period_key, source)

    await update.message.reply_text(text, reply_markup=keyboard, parse_mode="HTML")


async def stats_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return

    if not _is_admin(query.from_user.id):
        await query.answer("Команда только для админов", show_alert=True)
        return

    try:
        _, period_key, source = query.data.split(":", maxsplit=2)
    except Exception:
        await query.answer("Некорректные данные", show_alert=True)
        return

    stats = _compute_stats(period_key, source)
    text = _format_stats_text(stats)
    keyboard = _build_stats_keyboard(period_key, source)

    await query.edit_message_text(text, reply_markup=keyboard, parse_mode="HTML")
    await query.answer()


async def user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _ensure_admin(update):
        return
    args = context.args or []
    if not args:
        await update.message.reply_text("Использование: /user <id|@username>")
        return

    query = args[0].strip()
    user_id = None
    if query.isdigit():
        user_id = int(query)
    elif query.startswith("@"):
        row = get_user_by_username(query)
        user_id = int(row[0]) if row else None
    else:
        row = get_user_by_username(query)
        user_id = int(row[0]) if row else None

    if not user_id:
        await update.message.reply_text("Пользователь не найден.")
        return

    text = _format_user_card(user_id)
    keyboard = _user_action_keyboard(user_id)
    await update.message.reply_text(text, reply_markup=keyboard, parse_mode="HTML")


async def forwarded_user_lookup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _ensure_admin(update):
        return
    msg = update.effective_message
    if not msg:
        return

    fwd_from = getattr(msg, "forward_from", None)
    fwd_origin = getattr(msg, "forward_origin", None)
    user_id = None
    username = None

    if fwd_from:
        user_id = getattr(fwd_from, "id", None)
        username = getattr(fwd_from, "username", None)
    elif fwd_origin and getattr(fwd_origin, "type", "") == "user":
        user = getattr(fwd_origin, "sender_user", None)
        if user:
            user_id = getattr(user, "id", None)
            username = getattr(user, "username", None)

    if not user_id and username:
        row = get_user_by_username(username)
        user_id = int(row[0]) if row else None

    if not user_id:
        await msg.reply_text("Не удалось определить пользователя из пересланного сообщения.")
        return

    text = _format_user_card(int(user_id))
    keyboard = _user_action_keyboard(int(user_id))
    await msg.reply_text(text, reply_markup=keyboard, parse_mode="HTML")


async def user_action_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    if not _is_admin(query.from_user.id):
        await query.answer("Команда только для админов", show_alert=True)
        return

    try:
        _, action, value, user_id_raw = query.data.split(":", maxsplit=3)
    except Exception:
        await query.answer("Некорректные данные", show_alert=True)
        return

    try:
        user_id = int(user_id_raw)
    except Exception:
        await query.answer("Некорректный user_id", show_alert=True)
        return

    admin_id = query.from_user.id
    if action in {"add", "sub"}:
        try:
            delta = int(value)
            if action == "sub":
                delta = -abs(delta)
            else:
                delta = abs(delta)
            adjust_tarot_balance(user_id, user_id, delta)
            log_support_action(user_id, admin_id=admin_id, delta=delta, reason=f"admin_{action}_{abs(int(value))}")
            await query.answer("Баланс обновлён", show_alert=False)
        except Exception as e:
            logger.exception("Failed to adjust balance: %s", e)
            await query.answer("Ошибка изменения баланса", show_alert=True)
    elif action == "reset":
        try:
            cur = conn.cursor()
            cur.execute("SELECT chat_id FROM user_profile WHERE user_id = ?", (user_id,))
            rows = cur.fetchall() or []
            for (chat_id,) in rows:
                patch_user_profile_chat(
                    user_id,
                    int(chat_id),
                    patch={},
                    delete_keys=["pending_tarot", "pre_dialog"],
                )
            await query.answer("Сессия сброшена", show_alert=False)
        except Exception as e:
            logger.exception("Failed to reset session: %s", e)
            await query.answer("Ошибка сброса сессии", show_alert=True)
    elif action == "block":
        try:
            set_user_blocked(user_id, True)
            await query.answer("Пользователь заблокирован", show_alert=False)
        except Exception:
            await query.answer("Ошибка блокировки", show_alert=True)
    elif action == "unblock":
        try:
            set_user_blocked(user_id, False)
            await query.answer("Пользователь разблокирован", show_alert=False)
        except Exception:
            await query.answer("Ошибка разблокировки", show_alert=True)
    elif action == "refresh":
        await query.answer()
    else:
        await query.answer("Неизвестное действие", show_alert=True)
        return

    # refresh card
    text = _format_user_card(user_id)
    keyboard = _user_action_keyboard(user_id)
    try:
        await query.edit_message_text(text, reply_markup=keyboard, parse_mode="HTML")
    except Exception:
        pass


async def offers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /offers — список источников traffic_source
    """
    if not await _ensure_admin(update):
        return

    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            COALESCE(traffic_source, 'organic') AS src,
            COUNT(*) AS users_cnt
        FROM users
        GROUP BY src
        ORDER BY users_cnt DESC
        LIMIT 50
        """
    )
    rows = cur.fetchall()

    if not rows:
        await update.message.reply_text("Пока нет источников трафика.")
        return

    lines = ["🔗 <b>Источники трафика</b>", ""]
    keyboard_rows: List[List[InlineKeyboardButton]] = []

    for src, users_cnt in rows:
        lines.append(f"• <b>{_h(src)}</b>: {users_cnt}")
        keyboard_rows.append(
            [InlineKeyboardButton(f"{src} ({users_cnt})", callback_data=f"offer_stats:{src}:today")]
        )

    await update.message.reply_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(keyboard_rows),
        parse_mode="HTML",
    )


async def offer_stats_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    callback_data = "offer_stats:<src>:<period>"
    """
    query = update.callback_query
    if not query:
        return

    if not _is_admin(query.from_user.id):
        await query.answer("Команда только для админов", show_alert=True)
        return

    try:
        _, src, period_key = query.data.split(":", maxsplit=2)
    except Exception:
        await query.answer("Некорректные данные", show_alert=True)
        return

    stats = _compute_stats(period_key, src)
    text = _format_stats_text(stats)
    keyboard = _build_stats_keyboard(period_key, src)

    await context.bot.send_message(
        chat_id=query.message.chat_id,
        text=text,
        reply_markup=keyboard,
        parse_mode="HTML",
    )
    await query.answer("Открываю статистику")


# ============================================================
#  MAIN
# ============================================================

def main():
    init_db()

    if not ADMIN_TG_TOKEN:
        raise RuntimeError("ADMIN_TG_TOKEN не задан. Укажи его в .env")

    if not ADMIN_IDS:
        logger.warning("ADMIN_IDS пуст — админ-бот никого не пустит в команды.")

    app = ApplicationBuilder().token(ADMIN_TG_TOKEN).build()

    app.add_handler(CommandHandler("start", admin_start))
    app.add_handler(CommandHandler("debug_chat_id", debug_chat_id))
    app.add_handler(CommandHandler("stats", stats_command))
    app.add_handler(CommandHandler("offers", offers_command))
    app.add_handler(CommandHandler("user", user_command))

    app.add_handler(CallbackQueryHandler(stats_callback, pattern=r"^stats:"))
    app.add_handler(CallbackQueryHandler(offer_stats_callback, pattern=r"^offer_stats:"))
    app.add_handler(CallbackQueryHandler(user_action_callback, pattern=r"^user:"))
    app.add_handler(MessageHandler(filters.FORWARDED, forwarded_user_lookup))

    logger.info("Admin bot started")
    app.run_polling()


if __name__ == "__main__":
    main()
