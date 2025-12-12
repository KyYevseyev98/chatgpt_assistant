# admin_bot.py
import logging
import datetime as dt
from typing import Optional, Tuple, Dict, Any, List

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler,
)

from config import ADMIN_TG_TOKEN, ADMIN_IDS, STAR_USD_RATE
from db import init_db, conn

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


def _parse_topic_from_meta(meta: Optional[str]) -> Optional[str]:
    """
    meta пример: "topic:nutrition;batch_size:2"
    """
    if not meta:
        return None
    if "topic:" not in meta:
        return None
    try:
        after = meta.split("topic:", 1)[1]
        topic = after.split(";", 1)[0].strip()
        return topic or None
    except Exception:
        return None


# ============================================================
#  CORE: STATS
# ============================================================

def _compute_stats(period_key: str, source: Optional[str] = None) -> Dict[str, Any]:
    start_date, end_date, period_label, range_text = _period_info(period_key)
    cur = conn.cursor()

    src = source or "all"

    # --- total users in base (filtered by users.traffic_source) ---
    u_clause, u_params = _build_source_clause_users("u", src)
    cur.execute(
        f"SELECT COUNT(*) FROM users u WHERE 1=1 {u_clause}",
        u_params,
    )
    total_users_base = cur.fetchone()[0] or 0

    # --- active PRO right now (pro_until > now UTC) ---
    now_iso = dt.datetime.utcnow().isoformat()
    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM users u
        WHERE 1=1 {u_clause}
          AND u.pro_until IS NOT NULL
          AND u.pro_until > ?
        """,
        u_params + [now_iso],
    )
    pro_active_now = cur.fetchone()[0] or 0

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

    # --- PRO payments in period (filter by pro_payments.traffic_source) ---
    p_clause, p_params = _build_source_clause_pay("p", src)
    pd_clause, pd_params = _build_date_clause("p", start_date, end_date)
    params_pay = p_params + pd_params

    cur.execute(
        f"""
        SELECT
            COUNT(*)                 AS pay_count,
            COUNT(DISTINCT user_id)  AS pay_users,
            COALESCE(SUM(stars), 0)  AS total_stars,
            COALESCE(SUM(days), 0)   AS total_days,
            COALESCE(AVG(stars), 0)  AS avg_stars
        FROM pro_payments p
        WHERE 1=1 {p_clause} {pd_clause}
        """,
        params_pay,
    )
    row_pay = cur.fetchone()
    pay_count_period = row_pay[0] or 0
    pay_users_period = row_pay[1] or 0
    total_stars_period = row_pay[2] or 0
    total_days_period = row_pay[3] or 0
    avg_payment_stars = float(row_pay[4] or 0.0)

    # --- all-time paying users for this source ---
    cur.execute(
        f"""
        SELECT COUNT(DISTINCT user_id)
        FROM pro_payments p
        WHERE 1=1 {p_clause}
        """,
        p_params,
    )
    pay_users_all = cur.fetchone()[0] or 0

    # --- repeat payers all-time for this source ---
    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT user_id
            FROM pro_payments p
            WHERE 1=1 {p_clause}
            GROUP BY user_id
            HAVING COUNT(*) > 1
        ) t
        """,
        p_params,
    )
    repeat_payers_all = cur.fetchone()[0] or 0

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

    # ====================================================
    # CONVERSIONS
    # ====================================================
    base_users = total_users_base if total_users_base > 0 else 1

    pct_active_users = _safe_div(active_users, base_users)
    pct_with_subscription = _safe_div(pay_users_all, base_users)
    pct_repeat_payers = _safe_div(repeat_payers_all, pay_users_all or 1)

    pct_start_to_first_msg = _safe_div(active_users, start_cnt or 1)
    pct_start_to_pay = _safe_div(pay_users_period, start_cnt or 1)
    pct_first_to_limit = _safe_div(limit_users_cnt, active_users or 1)
    pct_limit_to_pay = _safe_div(pay_users_period, limit_users_cnt or 1)

    pct_text_of_msgs = _safe_div(text_cnt, messages_total or 1)
    pct_voice_of_msgs = _safe_div(voice_cnt, messages_total or 1)
    pct_photo_of_msgs = _safe_div(photo_cnt, messages_total or 1)

    return {
        "period_key": period_key,
        "period_label": period_label,
        "range_text": range_text,
        "source": src,

        "total_users_base": total_users_base,
        "pro_active_now": pro_active_now,

        "start_cnt": start_cnt,
        "active_users": active_users,
        "returned_after_ignore": returned_after_ignore,

        "pay_users_all": pay_users_all,
        "repeat_payers_all": repeat_payers_all,

        "pay_count_period": pay_count_period,
        "pay_users_period": pay_users_period,
        "total_stars_period": total_stars_period,
        "total_days_period": total_days_period,
        "avg_payment_stars": avg_payment_stars,

        "messages_total": messages_total,
        "avg_msgs_per_user": avg_msgs_per_user,
        "text_cnt": text_cnt,
        "voice_cnt": voice_cnt,
        "photo_cnt": photo_cnt,

        "limit_events_cnt": limit_events_cnt,
        "limit_users_cnt": limit_users_cnt,

        "users_with_followups": users_with_followups,
        "followups_today": followups_today,

        "top_topics": top_topics,
        "top_limit_topics": top_limit_topics,

        # pct
        "pct_active_users": pct_active_users,
        "pct_with_subscription": pct_with_subscription,
        "pct_start_to_first_msg": pct_start_to_first_msg,
        "pct_start_to_pay": pct_start_to_pay,
        "pct_first_to_limit": pct_first_to_limit,
        "pct_limit_to_pay": pct_limit_to_pay,
        "pct_repeat_payers": pct_repeat_payers,
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
    # header
    period_label = stats["period_label"]
    range_text = stats["range_text"]
    source = stats["source"]

    total_users_base = stats["total_users_base"]
    pro_active_now = stats["pro_active_now"]

    start_cnt = stats["start_cnt"]
    active_users = stats["active_users"]
    returned_after_ignore = stats["returned_after_ignore"]

    pay_users_all = stats["pay_users_all"]
    repeat_payers_all = stats["repeat_payers_all"]

    pay_count_period = stats["pay_count_period"]
    pay_users_period = stats["pay_users_period"]
    total_stars_period = stats["total_stars_period"]
    total_days_period = stats["total_days_period"]
    avg_payment_stars = stats["avg_payment_stars"]

    messages_total = stats["messages_total"]
    avg_msgs_per_user = stats["avg_msgs_per_user"]
    text_cnt = stats["text_cnt"]
    voice_cnt = stats["voice_cnt"]
    photo_cnt = stats["photo_cnt"]

    limit_events_cnt = stats["limit_events_cnt"]
    limit_users_cnt = stats["limit_users_cnt"]

    users_with_followups = stats["users_with_followups"]
    followups_today = stats["followups_today"]

    top_topics = stats.get("top_topics") or []
    top_limit_topics = stats.get("top_limit_topics") or []

    # pct
    pct_active_users = _fmt_pct(stats["pct_active_users"])
    pct_with_subscription = _fmt_pct(stats["pct_with_subscription"])
    pct_start_to_first_msg = _fmt_pct(stats["pct_start_to_first_msg"])
    pct_start_to_pay = _fmt_pct(stats["pct_start_to_pay"])
    pct_first_to_limit = _fmt_pct(stats["pct_first_to_limit"])
    pct_limit_to_pay = _fmt_pct(stats["pct_limit_to_pay"])
    pct_repeat_payers = _fmt_pct(stats["pct_repeat_payers"])

    pct_text_of_msgs = _fmt_pct(stats["pct_text_of_msgs"])
    pct_voice_of_msgs = _fmt_pct(stats["pct_voice_of_msgs"])
    pct_photo_of_msgs = _fmt_pct(stats["pct_photo_of_msgs"])

    lines: List[str] = []
    lines.append(f"📊 <b>Статистика — {period_label}</b>")
    lines.append(f"🗓 Период: {range_text}")
    lines.append(f"🔗 Источник: <b>{_source_label(source)}</b>")
    lines.append("")

    lines.append("👥 <b>Пользователи</b>")
    lines.append(f"• Всего в базе: <b>{total_users_base}</b>")
    lines.append(f"• PRO активны сейчас: <b>{pro_active_now}</b>")
    lines.append(f"• Стартов за период: <b>{start_cnt}</b>")
    lines.append(f"• Активные (сообщения): <b>{active_users}</b> ({pct_active_users} от базы)")
    lines.append(f"• Вернулись после игнора >2 дней: <b>{returned_after_ignore}</b>")
    lines.append("")

    lines.append("💰 <b>Монетизация</b>")
    lines.append(f"• Платящих юзеров (всё время): <b>{pay_users_all}</b> ({pct_with_subscription} от базы)")
    lines.append(f"• Повторные плательщики (всё время): <b>{repeat_payers_all}</b> ({pct_repeat_payers} от платящих)")
    lines.append(f"• Оплат за период: <b>{pay_count_period}</b>")
    lines.append(f"• Платящих юзеров за период: <b>{pay_users_period}</b>")
    lines.append(f"• Получено за период: <b>{_fmt_stars_usd(total_stars_period)}</b>")
    lines.append(f"• Дней PRO начислено: <b>{total_days_period}</b>")
    lines.append(f"• Средняя оплата: <b>{avg_payment_stars:.1f}⭐</b>")
    lines.append("")

    lines.append("✉️ <b>Сообщения</b>")
    lines.append(f"• Всего сообщений: <b>{messages_total}</b>")
    lines.append(f"• Среднее на активного: <b>{avg_msgs_per_user:.2f}</b>")
    lines.append(f"• Текст: <b>{text_cnt}</b> ({pct_text_of_msgs})")
    lines.append(f"• Голос: <b>{voice_cnt}</b> ({pct_voice_of_msgs})")
    lines.append(f"• Фото: <b>{photo_cnt}</b> ({pct_photo_of_msgs})")
    lines.append("")

    lines.append("🚧 <b>Лимиты</b>")
    lines.append(f"• Событий лимита: <b>{limit_events_cnt}</b>")
    lines.append(f"• Юзеров упёрлись: <b>{limit_users_cnt}</b>")
    lines.append("")

    lines.append("📨 <b>Follow-up</b>")
    lines.append(f"• Юзеров, кому слали follow-up хоть раз: <b>{users_with_followups}</b>")
    lines.append(f"• Follow-up сегодня: <b>{followups_today}</b>")
    lines.append("")

    lines.append("🧩 <b>Воронка</b>")
    lines.append(f"1️⃣ start → msg: <b>{pct_start_to_first_msg}</b>")
    lines.append(f"2️⃣ start → pay: <b>{pct_start_to_pay}</b>")
    lines.append(f"3️⃣ msg → limit: <b>{pct_first_to_limit}</b>")
    lines.append(f"4️⃣ limit → pay: <b>{pct_limit_to_pay}</b>")
    lines.append("")

    if top_topics:
        lines.append("🔥 <b>Топ тем (по text.meta)</b>")
        for t, c in top_topics:
            lines.append(f"• {t}: {c}")
        lines.append("")

    if top_limit_topics:
        lines.append("⛔ <b>Топ тем, где упираются в лимит (last_limit_topic)</b>")
        for t, c in top_limit_topics:
            lines.append(f"• {t}: {c}")
        lines.append("")

    return "\n".join(lines)


# ============================================================
#  HANDLERS
# ============================================================

async def admin_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _ensure_admin(update):
        return

    text = (
        "👋 <b>Админ-панель Foxy</b>\n\n"
        "Команды:\n"
        "• /stats — статистика (кнопки периодов)\n"
        "• /offers — список источников (/start?src=...)\n"
    )
    await update.message.reply_text(text, parse_mode="HTML")


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
        lines.append(f"• <b>{src}</b>: {users_cnt}")
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
    app.add_handler(CommandHandler("stats", stats_command))
    app.add_handler(CommandHandler("offers", offers_command))

    app.add_handler(CallbackQueryHandler(stats_callback, pattern=r"^stats:"))
    app.add_handler(CallbackQueryHandler(offer_stats_callback, pattern=r"^offer_stats:"))

    logger.info("Admin bot started")
    app.run_polling()


if __name__ == "__main__":
    main()