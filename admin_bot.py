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


def _today_str_utc() -> str:
    """
    Дата 'YYYY-MM-DD' в UTC.
    Мы created_at пишем через datetime.utcnow().isoformat(),
    поэтому ориентируемся на UTC.
    """
    return dt.datetime.utcnow().date().isoformat()


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
        "/today — статистика за сегодня (UTC)\n"
        "/sources — разрез по источникам трафика за сегодня\n"
    )
    await update.message.reply_text(text)


# ---------- /today ----------

async def stats_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _ensure_admin(update, context):
        return

    cur = conn.cursor()
    today = _today_str_utc()  # 'YYYY-MM-DD'

    # ==== общие цифры по базе ====
    # всего пользователей в БД
    cur.execute("SELECT COUNT(*) FROM users")
    total_users = cur.fetchone()[0] or 0

    # всего событий в логе
    cur.execute("SELECT COUNT(*) FROM events")
    total_events_all = cur.fetchone()[0] or 0

    # ==== за сегодня ====
    # события за сегодня по типам
    cur.execute(
        """
        SELECT event_type, COUNT(*)
        FROM events
        WHERE substr(created_at, 1, 10) = ?
        GROUP BY event_type
        """,
        (today,),
    )
    rows_events = cur.fetchall()
    events_summary = {row[0]: row[1] for row in rows_events}

    total_events_today = sum(events_summary.values())

    # отдельные типы
    text_today = events_summary.get("text", 0)
    voice_today = events_summary.get("voice", 0)
    photo_today = events_summary.get("photo", 0)
    start_today = events_summary.get("start", 0)

    # считаем «сообщения» как text + voice + photo
    messages_today = text_today + voice_today + photo_today

    # активные пользователи за сегодня
    cur.execute(
        """
        SELECT COUNT(DISTINCT user_id)
        FROM events
        WHERE substr(created_at, 1, 10) = ?
        """,
        (today,),
    )
    active_users_today = cur.fetchone()[0] or 0

    # новые пользователи сегодня — по /start
    cur.execute(
        """
        SELECT COUNT(DISTINCT user_id)
        FROM events
        WHERE event_type = 'start'
          AND substr(created_at, 1, 10) = ?
        """,
        (today,),
    )
    new_users_today = cur.fetchone()[0] or 0

    # PRO-оплаты за сегодня
    cur.execute(
        """
        SELECT 
            COUNT(*) as pay_count,
            COALESCE(SUM(stars), 0) as total_stars,
            COALESCE(SUM(days), 0) as total_days
        FROM pro_payments
        WHERE substr(created_at, 1, 10) = ?
        """,
        (today,),
    )
    pay_row = cur.fetchone()
    pay_count = pay_row[0] or 0
    total_stars = pay_row[1] or 0
    total_days = pay_row[2] or 0

    # красиво собираем текст
    lines = []
    lines.append("📊 Статистика за сегодня (UTC):\n")

    # блок по пользователям
    lines.append(f"👥 Всего пользователей в базе: {total_users}")
    lines.append(f"👥 Активных сегодня: {active_users_today}")
    lines.append(f"🆕 Новых сегодня (/start): {new_users_today}")
    lines.append("")

    # блок по событиям
    lines.append("✉️ События за сегодня:")
    lines.append(f"  • Всего событий: {total_events_today}")
    lines.append(f"  • Сообщений (text+voice+photo): {messages_today}")
    lines.append(f"    - text: {text_today}")
    lines.append(f"    - voice: {voice_today}")
    lines.append(f"    - photo: {photo_today}")
    lines.append(f"  • /start: {start_today}")
    lines.append("")
    lines.append("💰 PRO-оплаты за сегодня:")
    lines.append(f"  • Кол-во оплат: {pay_count}")
    lines.append(f"  • Суммарно звёзд: {total_stars}")
    lines.append(f"  • Суммарно дней PRO: {total_days}")
    lines.append("")
    lines.append("📚 Общие логи:")
    lines.append(f"  • Всего событий в events: {total_events_all}")

    text = "\n".join(lines)
    await update.message.reply_text(text)


# ---------- /sources ----------

async def stats_sources(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await _ensure_admin(update, context):
        return

    cur = conn.cursor()
    today = _today_str_utc()

    # активность по источникам (events + users)
    cur.execute(
        """
        SELECT 
            COALESCE(u.traffic_source, 'organic') AS src,
            COUNT(DISTINCT e.user_id) AS users_cnt,
            COUNT(*) AS events_cnt
        FROM events e
        LEFT JOIN users u ON u.user_id = e.user_id
        WHERE substr(e.created_at, 1, 10) = ?
        GROUP BY src
        ORDER BY events_cnt DESC
        LIMIT 20
        """,
        (today,),
    )
    rows_activity = cur.fetchall()

    # оплаты по источникам
    cur.execute(
        """
        SELECT 
            COALESCE(traffic_source, 'organic') AS src,
            COUNT(*) AS pay_cnt,
            COALESCE(SUM(stars), 0) AS total_stars,
            COALESCE(SUM(days), 0) AS total_days
        FROM pro_payments
        WHERE substr(created_at, 1, 10) = ?
        GROUP BY src
        ORDER BY pay_cnt DESC
        LIMIT 20
        """,
        (today,),
    )
    rows_payments = cur.fetchall()

    lines = []
    lines.append("📈 Источники трафика за сегодня (UTC):\n")

    lines.append("🔹 Активность по источникам:")
    if rows_activity:
        for src, users_cnt, events_cnt in rows_activity:
            lines.append(f"  • {src}: активных юзеров={users_cnt}, событий={events_cnt}")
    else:
        lines.append("  Нет активности за сегодня.")

    lines.append("")
    lines.append("💳 Оплаты по источникам:")
    if rows_payments:
        for src, pay_cnt, total_stars, total_days in rows_payments:
            lines.append(
                f"  • {src}: оплат={pay_cnt}, звёзд={total_stars}, дней PRO={total_days}"
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