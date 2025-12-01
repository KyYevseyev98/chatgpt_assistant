# admin_bot.py
import logging
import datetime as dt

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)

from config import ADMIN_TG_TOKEN, ADMIN_IDS
from db import init_db, conn

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def _today_prefix() -> str:
    """Строка вида '2025-11-30%' для фильтра по сегодняшнему дню (UTC)."""
    return dt.date.today().isoformat() + "%"


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


# ---------- /start ----------

async def admin_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _ensure_admin(update, context):
        return

    text = (
        "👋 Привет, админ!\n\n"
        "Этот бот показывает аналитику по основному ChatGPT-боту.\n\n"
        "Доступные команды:\n"
        "/today — статистика за сегодня\n"
        "/sources — разрез по источникам трафика за сегодня\n"
    )
    await update.message.reply_text(text)


# ---------- /today ----------

async def stats_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _ensure_admin(update, context):
        return

    cur = conn.cursor()
    today_like = _today_prefix()

    # 1) события за сегодня по типам
    cur.execute(
        """
        SELECT event_type, COUNT(*) 
        FROM events
        WHERE created_at LIKE ?
        GROUP BY event_type
        """,
        (today_like,),
    )
    rows_events = cur.fetchall()
    events_summary = {row[0]: row[1] for row in rows_events}

    total_events = sum(events_summary.values())

    # 2) активные пользователи за сегодня (любые события)
    cur.execute(
        """
        SELECT COUNT(DISTINCT user_id)
        FROM events
        WHERE created_at LIKE ?
        """,
        (today_like,),
    )
    active_users_today = cur.fetchone()[0] or 0

    # 3) новые пользователи сегодня — по /start
    cur.execute(
        """
        SELECT COUNT(DISTINCT user_id)
        FROM events
        WHERE event_type = 'start'
          AND created_at LIKE ?
        """,
        (today_like,),
    )
    new_users_today = cur.fetchone()[0] or 0

    # 4) PRO-оплаты за сегодня
    cur.execute(
        """
        SELECT 
            COUNT(*) as pay_count,
            COALESCE(SUM(stars), 0) as total_stars,
            COALESCE(SUM(days), 0) as total_days
        FROM pro_payments
        WHERE created_at LIKE ?
        """,
        (today_like,),
    )
    pay_row = cur.fetchone()
    pay_count = pay_row[0] or 0
    total_stars = pay_row[1] or 0
    total_days = pay_row[2] or 0

    # красиво собираем текст
    lines = []
    lines.append("📊 Статистика за сегодня (UTC):\n")

    lines.append(f"👥 Активных пользователей: {active_users_today}")
    lines.append(f"🆕 Новых пользователей (/start): {new_users_today}")
    lines.append("")
    lines.append("✉️ События по типам:")
    if events_summary:
        for etype, cnt in events_summary.items():
            emoji = {
                "start": "🚀",
                "text": "💬",
                "voice": "🎤",
                "photo": "🖼️",
            }.get(etype, "•")
            lines.append(f"  {emoji} {etype}: {cnt}")
    else:
        lines.append("  Пока нет событий за сегодня.")

    lines.append("")
    lines.append("💰 PRO-оплаты за сегодня:")
    lines.append(f"  Кол-во оплат: {pay_count}")
    lines.append(f"  Суммарно звёзд: {total_stars}")
    lines.append(f"  Суммарно дней PRO: {total_days}")

    text = "\n".join(lines)
    await update.message.reply_text(text)


# ---------- /sources ----------

async def stats_sources(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _ensure_admin(update, context):
        return

    cur = conn.cursor()
    today_like = _today_prefix()

    # 1) активность по источникам (events + users)
    cur.execute(
        """
        SELECT 
            COALESCE(u.traffic_source, '(none)') AS src,
            COUNT(DISTINCT e.user_id) AS users_cnt,
            COUNT(*) AS events_cnt
        FROM events e
        LEFT JOIN users u ON u.user_id = e.user_id
        WHERE e.created_at LIKE ?
        GROUP BY src
        ORDER BY events_cnt DESC
        LIMIT 20
        """,
        (today_like,),
    )
    rows_activity = cur.fetchall()

    # 2) оплаты по источникам
    cur.execute(
        """
        SELECT 
            COALESCE(traffic_source, '(none)') AS src,
            COUNT(*) AS pay_cnt,
            COALESCE(SUM(stars), 0) AS total_stars,
            COALESCE(SUM(days), 0) AS total_days
        FROM pro_payments
        WHERE created_at LIKE ?
        GROUP BY src
        ORDER BY pay_cnt DESC
        LIMIT 20
        """,
        (today_like,),
    )
    rows_payments = cur.fetchall()

    lines = []
    lines.append("📈 Источники трафика за сегодня (UTC):\n")

    lines.append("🔹 Активность по источникам:")
    if rows_activity:
        for src, users_cnt, events_cnt in rows_activity:
            lines.append(f"  • {src}: users={users_cnt}, events={events_cnt}")
    else:
        lines.append("  Нет активности за сегодня.")

    lines.append("")
    lines.append("💳 Оплаты по источникам:")
    if rows_payments:
        for src, pay_cnt, total_stars, total_days in rows_payments:
            lines.append(
                f"  • {src}: payments={pay_cnt}, stars={total_stars}, days={total_days}"
            )
    else:
        lines.append("  Оплат за сегодня пока нет.")

    text = "\n".join(lines)
    await update.message.reply_text(text)


def main():
    # на всякий случай
    init_db()

    if not ADMIN_TG_TOKEN:
        raise RuntimeError(
            "ADMIN_TG_TOKEN не задан. Укажи его в .env (ADMIN_TG_TOKEN=...)"
        )

    if not ADMIN_IDS:
        logger.warning("ADMIN_IDS пуст — админ-бот никого не пустит в команды.")

    app = ApplicationBuilder().token(ADMIN_TG_TOKEN).build()

    app.add_handler(CommandHandler("start", admin_start))
    app.add_handler(CommandHandler("today", stats_today))
    app.add_handler(CommandHandler("sources", stats_sources))

    logger.info("Admin bot started")
    app.run_polling()


if __name__ == "__main__":
    main()