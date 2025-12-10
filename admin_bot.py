# admin_bot.py
import logging
import datetime as dt
from typing import Optional, Tuple, Dict, Any, List

from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler,
)

from config import ADMIN_TG_TOKEN, ADMIN_IDS
from db import init_db, conn

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ============================================================
#  ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================

def _utc_today() -> dt.date:
    """Текущая дата в UTC."""
    return dt.datetime.utcnow().date()


def _is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


async def _ensure_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Проверяем, что юзер — админ. Если нет — шлём отказ и возвращаем False."""
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
      period_label (человеческое название)
      range_text  (строка для вывода)
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
        # "all"
        return None, None, "За всё время", "всё время"

    start_s = start.isoformat()
    end_s = end.isoformat()
    range_text = start_s if start_s == end_s else f"{start_s} — {end_s}"
    return start_s, end_s, label, range_text


def _build_source_clause(
    alias: str,
    source: Optional[str],
) -> Tuple[str, List[Any]]:
    """
    Формирует фрагмент WHERE по источнику трафика.
    alias — псевдоним таблицы (обычно u или p).
    """
    if not source or source == "all":
        return "", []

    if source == "organic":
        clause = f" AND ({alias}.traffic_source IS NULL OR {alias}.traffic_source = 'organic')"
        return clause, []

    clause = f" AND {alias}.traffic_source = ?"
    return clause, [source]


def _build_date_clause(
    alias: str,
    start_date: Optional[str],
    end_date: Optional[str],
) -> Tuple[str, List[Any]]:
    """
    Фрагмент WHERE по датам (по дате в ISO-строке created_at).
    """
    if not start_date or not end_date:
        return "", []

    clause = f" AND substr({alias}.created_at, 1, 10) BETWEEN ? AND ?"
    return clause, [start_date, end_date]


def _safe_div(num: float, den: float) -> float:
    if not den:
        return 0.0
    return num / den


def _fmt_pct(value: float) -> str:
    return f"{value * 100:.1f}%"


def _source_label(source: Optional[str]) -> str:
    if not source or source == "all":
        return "все"
    if source == "organic":
        return "organic (без тега)"
    return source


# ============================================================
#  СЕРДЦЕ: РАСЧЁТ СТАТИСТИКИ
# ============================================================

def _compute_stats(
    period_key: str,
    source: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Основная функция, которая дергает БД и считает все нужные цифры
    для заданного периода и источника.
    """
    start_date, end_date, period_label, range_text = _period_info(period_key)
    cur = conn.cursor()

    # --- пользователи в базе (по source, без привязки к периодам) ---
    if not source or source == "all":
        cur.execute("SELECT COUNT(*) FROM users")
    elif source == "organic":
        cur.execute(
            """
            SELECT COUNT(*) FROM users
            WHERE traffic_source IS NULL OR traffic_source = 'organic'
            """
        )
    else:
        cur.execute(
            "SELECT COUNT(*) FROM users WHERE traffic_source = ?",
            (source,),
        )
    total_users_base = cur.fetchone()[0] or 0

    # --- события по типам за период ---
    src_clause_e, src_params_e = _build_source_clause("u", source)
    date_clause_e, date_params_e = _build_date_clause("e", start_date, end_date)
    params_events = src_params_e + date_params_e

    cur.execute(
        f"""
        SELECT e.event_type, COUNT(*)
        FROM events e
        LEFT JOIN users u ON u.user_id = e.user_id
        WHERE 1=1 {src_clause_e} {date_clause_e}
        GROUP BY e.event_type
        """
        ,
        params_events,
    )
    rows_events = cur.fetchall()
    events_by_type: Dict[str, int] = {t: c for (t, c) in rows_events}

    text_cnt = events_by_type.get("text", 0)
    voice_cnt = events_by_type.get("voice", 0)
    photo_cnt = events_by_type.get("photo", 0)
    start_cnt = events_by_type.get("start", 0)

    # все события "лимита" (любые event_type, где есть "limit")
    cur.execute(
        f"""
        SELECT COUNT(*), COUNT(DISTINCT e.user_id)
        FROM events e
        LEFT JOIN users u ON u.user_id = e.user_id
        WHERE e.event_type LIKE '%limit%'
          {src_clause_e} {date_clause_e}
        """
        ,
        params_events,
    )
    limit_row = cur.fetchone()
    limit_events_cnt = limit_row[0] or 0
    limit_users_cnt = limit_row[1] or 0

    messages_total = text_cnt + voice_cnt + photo_cnt

    # --- активные пользователи (писали сообщения) за период ---
    cur.execute(
        f"""
        SELECT COUNT(DISTINCT e.user_id)
        FROM events e
        LEFT JOIN users u ON u.user_id = e.user_id
        WHERE e.event_type IN ('text','voice','photo')
          {src_clause_e} {date_clause_e}
        """
        ,
        params_events,
    )
    active_users = cur.fetchone()[0] or 0

    # --- пользователи, написавшие хотя бы одно сообщение (то же, что active) ---
    first_msg_users = active_users

    # --- пользователи, которые вернулись после игнора (>2 дней) ---
    returned_after_ignore = 0
    if start_date and end_date:
        # Берём по каждому юзеру первую активность (сообщение) в период
        cur.execute(
            f"""
            SELECT e.user_id, MIN(e.created_at) AS first_in_period
            FROM events e
            LEFT JOIN users u ON u.user_id = e.user_id
            WHERE e.event_type IN ('text','voice','photo')
              {src_clause_e} {date_clause_e}
            GROUP BY e.user_id
            """
            ,
            params_events,
        )
        rows_first = cur.fetchall()
        for user_id, first_ts in rows_first:
            if not first_ts:
                continue
            first_dt = dt.datetime.fromisoformat(first_ts)

            # Ищем последнюю активность ДО этого сообщения
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
            prev_dt = dt.datetime.fromisoformat(prev_ts)
            delta_days = (first_dt - prev_dt).total_seconds() / 86400.0
            if delta_days >= 2.0:
                returned_after_ignore += 1

    # --- сообщения на юзера ---
    avg_msgs_per_user = _safe_div(messages_total, active_users)

    # --- /start за период (по событиям) ---
    # уже есть start_cnt

    # --- PRO-оплаты за период ---
    src_clause_p, src_params_p = _build_source_clause("p", source)
    date_clause_p, date_params_p = _build_date_clause("p", start_date, end_date)
    params_pay = src_params_p + date_params_p

    cur.execute(
        f"""
        SELECT 
            COUNT(*)              AS pay_count,
            COUNT(DISTINCT user_id) AS pay_users,
            COALESCE(SUM(stars), 0) AS total_stars,
            COALESCE(SUM(days), 0)  AS total_days,
            COALESCE(AVG(stars), 0) AS avg_stars
        FROM pro_payments p
        WHERE 1=1 {src_clause_p} {date_clause_p}
        """
        ,
        params_pay,
    )
    row_pay = cur.fetchone()
    pay_count_period = row_pay[0] or 0
    pay_users_period = row_pay[1] or 0
    total_stars_period = row_pay[2] or 0
    total_days_period = row_pay[3] or 0
    avg_payment_stars = float(row_pay[4] or 0.0)

    # --- все платящие юзеры (за всё время, для этого source) ---
    cur.execute(
        f"""
        SELECT COUNT(DISTINCT user_id)
        FROM pro_payments p
        WHERE 1=1 {src_clause_p}
        """
        ,
        src_params_p,
    )
    pay_users_all = cur.fetchone()[0] or 0

    # --- юзеры, оплатившие >1 раза (за всё время) ---
    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT user_id
            FROM pro_payments p
            WHERE 1=1 {src_clause_p}
            GROUP BY user_id
            HAVING COUNT(*) > 1
        ) t
        """
        ,
        src_params_p,
    )
    repeat_payers_all = cur.fetchone()[0] or 0

    # ====================================================
    #  РАСЧЁТ ПРОЦЕНТОВ
    # ====================================================

    # консистентная база пользователей для процента "от всех"
    base_users = total_users_base if total_users_base > 0 else 1

    pct_active_users = _safe_div(active_users, base_users)
    pct_with_subscription = _safe_div(pay_users_all, base_users)

    pct_start_to_first_msg = _safe_div(first_msg_users, start_cnt or 1)
    pct_start_to_pay = _safe_div(pay_users_period, start_cnt or 1)
    pct_first_to_limit = _safe_div(limit_users_cnt, first_msg_users or 1)
    pct_limit_to_pay = _safe_div(pay_users_period, limit_users_cnt or 1)
    pct_repeat_payers = _safe_div(repeat_payers_all, pay_users_all or 1)

    pct_text_of_msgs = _safe_div(text_cnt, messages_total or 1)
    pct_voice_of_msgs = _safe_div(voice_cnt, messages_total or 1)
    pct_photo_of_msgs = _safe_div(photo_cnt, messages_total or 1)

    stats = {
        "period_key": period_key,
        "period_label": period_label,
        "range_text": range_text,
        "source": source or "all",

        "total_users_base": total_users_base,
        "start_cnt": start_cnt,
        "active_users": active_users,
        "pay_users_all": pay_users_all,
        "returned_after_ignore": returned_after_ignore,

        "pay_count_period": pay_count_period,
        "pay_users_period": pay_users_period,
        "total_stars_period": total_stars_period,
        "total_days_period": total_days_period,
        "avg_payment_stars": avg_payment_stars,
        "repeat_payers_all": repeat_payers_all,

        "messages_total": messages_total,
        "avg_msgs_per_user": avg_msgs_per_user,
        "text_cnt": text_cnt,
        "voice_cnt": voice_cnt,
        "photo_cnt": photo_cnt,

        "limit_events_cnt": limit_events_cnt,
        "limit_users_cnt": limit_users_cnt,

        # проценты
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
    return stats


def _build_stats_keyboard(period_key: str, source: Optional[str]) -> InlineKeyboardMarkup:
    src = source or "all"
    buttons_row1 = [
        InlineKeyboardButton("Сегодня", callback_data=f"stats:today:{src}"),
        InlineKeyboardButton("Вчера", callback_data=f"stats:yesterday:{src}"),
    ]
    buttons_row2 = [
        InlineKeyboardButton("7 дней", callback_data=f"stats:7d:{src}"),
        InlineKeyboardButton("14 дней", callback_data=f"stats:14d:{src}"),
        InlineKeyboardButton("28 дней", callback_data=f"stats:28d:{src}"),
    ]
    buttons_row3 = [
        InlineKeyboardButton("За всё время", callback_data=f"stats:all:{src}"),
    ]
    return InlineKeyboardMarkup([buttons_row1, buttons_row2, buttons_row3])


def _format_stats_text(stats: Dict[str, Any]) -> str:
    """
    Собираем красивый текст по словарю статистики.
    """
    period_label = stats["period_label"]
    range_text = stats["range_text"]
    source = stats["source"]

    total_users_base = stats["total_users_base"]
    start_cnt = stats["start_cnt"]
    active_users = stats["active_users"]
    pay_users_all = stats["pay_users_all"]
    returned_after_ignore = stats["returned_after_ignore"]

    pay_count_period = stats["pay_count_period"]
    pay_users_period = stats["pay_users_period"]
    total_stars_period = stats["total_stars_period"]
    total_days_period = stats["total_days_period"]
    avg_payment_stars = stats["avg_payment_stars"]
    repeat_payers_all = stats["repeat_payers_all"]

    messages_total = stats["messages_total"]
    avg_msgs_per_user = stats["avg_msgs_per_user"]
    text_cnt = stats["text_cnt"]
    voice_cnt = stats["voice_cnt"]
    photo_cnt = stats["photo_cnt"]

    limit_events_cnt = stats["limit_events_cnt"]
    limit_users_cnt = stats["limit_users_cnt"]

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
    lines.append(f"Период: {range_text}")
    lines.append(f"Источник: {_source_label(source)}")
    lines.append("")

    # --- пользователи: сначала число, потом описание ---
    lines.append("👥 <b>Пользователи</b>")
    lines.append(f"• {total_users_base} — всего юзеров в базе")
    lines.append(f"• {start_cnt} — Старт за период")
    lines.append(f"• {active_users} — активные (писали сообщения)")
    lines.append(f"• {pay_users_all} — с оплатой PRO (за всё время)")
    lines.append(f"• {returned_after_ignore} — вернулись после игнора (>2 дней)")
    lines.append("")

    # --- монетизация ---
    lines.append("💰 <b>Монетизация</b>")
    lines.append(f"• {pay_count_period} — кол-во оплат за период")
    lines.append(f"• {pay_users_period} — платящих юзеров за период")
    lines.append(f"• {total_stars_period} — звёзд получено за период")
    lines.append(f"• {total_days_period} — дней PRO начислено за период")
    lines.append(f"• {avg_payment_stars:.2f} ⭐ — средняя оплата (звёзды)")
    lines.append(f"• {repeat_payers_all} — платили >1 раза (за всё время)")
    lines.append("")

    # --- сообщения ---
    lines.append("✉️ <b>Сообщения</b>")
    lines.append(f"• {messages_total} — всего сообщений за период")
    lines.append(
        f"• {avg_msgs_per_user:.2f} — в среднем сообщений на юзера с сообщениями"
    )
    lines.append(
        f"• Текст: {text_cnt} ({pct_text_of_msgs}), "
        f"Voice: {voice_cnt} ({pct_voice_of_msgs}), "
        f"Фото: {photo_cnt} ({pct_photo_of_msgs})"
    )
    lines.append("")

    # --- воронка числа ---
    lines.append("🧩 <b>Воронка (числа)</b>")
    lines.append(f"1️⃣ Старт: {start_cnt}")
    lines.append(f"2️⃣ Первое сообщение: {stats['active_users']}")
    lines.append(f"3️⃣ Уперлись в лимит: {limit_users_cnt} (событий лимита: {limit_events_cnt})")
    lines.append(f"4️⃣ Оплатили PRO (за период): {pay_users_period}")
    lines.append("")

    # --- воронка проценты ---
    lines.append("📈 <b>Проценты и конверсии</b>")
    lines.append(f"• {pct_active_users} — активных юзеров от всех юзеров")
    lines.append(f"• {pct_with_subscription} — с подпиской (оплатой) от всех юзеров")
    lines.append(
        f"• {pct_start_to_first_msg} — от старта к первому сообщению (за период)"
    )
    lines.append(f"• {pct_start_to_pay} — от старта к оплате (за период)")
    lines.append(
        f"• {pct_first_to_limit} — от первого сообщения к лимиту (за период)"
    )
    lines.append(
        f"• {pct_limit_to_pay} — от пользователей с лимитом к оплате (за период)"
    )
    lines.append(
        f"• {pct_repeat_payers} — платили >1 раза от всех платящих (за всё время)"
    )
    lines.append(
        f"• {pct_text_of_msgs} — текстовых сообщений от всех сообщений"
    )
    lines.append(
        f"• {pct_voice_of_msgs} — голосовых от всех сообщений"
    )
    lines.append(
        f"• {pct_photo_of_msgs} — фото от всех сообщений"
    )

    return "\n".join(lines)


# ============================================================
#  ХЭНДЛЕРЫ ДЛЯ СТАТИСТИКИ
# ============================================================

async def admin_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _ensure_admin(update, context):
        return

    text = (
        "👋 Привет, админ!\n\n"
        "Этот бот показывает аналитику по основному Foxy-боту.\n\n"
        "Основные команды:\n"
        "• /stats — общая статистика с кнопками периодов\n"
        "• /offers — список тегов /start?src=...\n\n"
        "По кнопкам внизу можно переключать периоды.\n"
    )
    await update.message.reply_text(text)


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /stats [period] [source]
    Пример: /stats 7d ads_tt
    Если ничего не указано — сегодня, все источники.
    """
    if not await _ensure_admin(update, context):
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
    """
    Обработка нажатий кнопок периодов: callback_data = "stats:<period>:<source>"
    """
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


# ============================================================
#  СПИСОК ОФФЕРОВ / ИСТОЧНИКОВ
# ============================================================

async def offers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /offers — список всех источников (traffic_source), по нажатию — статистика
    только по этому источнику.
    """
    if not await _ensure_admin(update, context):
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
        await update.message.reply_text("Пока нет ни одного источника трафика.")
        return

    lines = ["🔗 <b>Офферы / источники</b>", ""]
    keyboard_rows: List[List[InlineKeyboardButton]] = []

    for src, users_cnt in rows:
        lines.append(f"• {src}: {users_cnt} юзеров")
        btn_text = f"{src} ({users_cnt})"
        keyboard_rows.append(
            [
                InlineKeyboardButton(
                    btn_text,
                    callback_data=f"offer_stats:{src}:today",
                )
            ]
        )

    text = "\n".join(lines)
    keyboard = InlineKeyboardMarkup(keyboard_rows)

    await update.message.reply_text(text, reply_markup=keyboard, parse_mode="HTML")


async def offer_stats_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Нажатие на конкретный оффер: callback_data = "offer_stats:<src>:<period>"
    На первый клик период всегда today, дальше можно менять кнопками.
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

    # Отдельное сообщение, чтобы не портить список офферов
    await context.bot.send_message(
        chat_id=query.message.chat_id,
        text=text,
        reply_markup=keyboard,
        parse_mode="HTML",
    )
    await query.answer("Открываю статистику по офферу")


# ============================================================
#  MAIN
# ============================================================

def main():
    # инициализируем БД (если что-то не создано)
    init_db()

    if not ADMIN_TG_TOKEN:
        raise RuntimeError(
            "ADMIN_TG_TOKEN не задан. Укажи его в .env (ADMIN_TG_TOKEN=...)"
        )

    if not ADMIN_IDS:
        logger.warning("ADMIN_IDS пуст — админ-бот никого не пустит в команды.")

    app = ApplicationBuilder().token(ADMIN_TG_TOKEN).build()

    # команды
    app.add_handler(CommandHandler("start", admin_start))
    app.add_handler(CommandHandler("stats", stats_command))
    app.add_handler(CommandHandler("offers", offers_command))

    # колбэки
    app.add_handler(CallbackQueryHandler(stats_callback, pattern=r"^stats:"))
    app.add_handler(CallbackQueryHandler(offer_stats_callback, pattern=r"^offer_stats:"))

    logger.info("Admin bot started")
    app.run_polling()


if __name__ == "__main__":
    main()