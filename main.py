# main.py

import logging
import datetime as dt

from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    PreCheckoutQueryHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
from telegram.request import HTTPXRequest
from telegram import MenuButtonWebApp, WebAppInfo

from config import TG_TOKEN

# handlers (твои основные)
from handlers import (
    start,
    reset_dialog,
    handle_message,
    handle_photo,
    handle_voice,
    pro_command,
    pro_button,
    precheckout_callback,
    successful_payment_callback,
    handle_webapp_data,
)


from db import (
    init_db,
    get_all_users_for_followup,
    required_ignored_days_for_stage,
    mark_followup_sent,
)

from jobs import send_ignore_followup

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


async def periodic_followups_job(context: ContextTypes.DEFAULT_TYPE):
    """
    Периодический обход пользователей и follow-up, если они игнорят достаточно долго.
    """
    now = dt.datetime.utcnow()
    users = get_all_users_for_followup()

    for user_id, last_activity_at, last_followup_at, followup_stage in users:
        if not last_activity_at:
            continue

        try:
            last_activity_dt = dt.datetime.fromisoformat(last_activity_at)
        except Exception:
            continue

        ignored_days = (now - last_activity_dt).days
        required_days = required_ignored_days_for_stage(followup_stage)

        if ignored_days < required_days:
            continue

        # не спамим: если уже сегодня слали — пропускаем
        if last_followup_at:
            try:
                last_f_dt = dt.datetime.fromisoformat(last_followup_at)
                if (now - last_f_dt).days == 0:
                    continue
            except Exception:
                pass

        lang = "ru"  # потом можно тянуть из user_profiles

        try:
            await send_ignore_followup(context, user_id, lang, followup_stage)
        except Exception as e:
            logger.warning("Не удалось отправить follow-up пользователю %s: %s", user_id, e)


def main():
    init_db()

    from telegram.ext import Defaults
    from telegram.constants import ParseMode

    request = HTTPXRequest(
        read_timeout=30,
        write_timeout=30,
        connect_timeout=30,
        pool_timeout=30,
    )

    async def _post_init(app):
        from config import WEBAPP_URL
        if WEBAPP_URL:
            try:
                await app.bot.set_chat_menu_button(
                    menu_button=MenuButtonWebApp(text="Кабинет", web_app=WebAppInfo(url=WEBAPP_URL))
                )
            except Exception as e:
                logger.warning("Failed to set menu button: %s", e)

    app = (
        ApplicationBuilder()
       .token(TG_TOKEN)
       .request(request)
       .defaults(Defaults(parse_mode=ParseMode.HTML))
       .post_init(_post_init)
       .build()
    )

    # -------------------- COMMANDS --------------------
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("reset", reset_dialog))
    app.add_handler(CommandHandler("pro", pro_command))

    # -------------------- CALLBACK BUTTONS --------------------
    # ✅ кнопки покупки раскладов + реферал
    app.add_handler(CallbackQueryHandler(pro_button, pattern=r"^(buy_tarot_|ref_)"))

    # -------------------- MESSAGES --------------------
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(MessageHandler(filters.StatusUpdate.WEB_APP_DATA, handle_webapp_data))

    # фото как PHOTO
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))

    # фото как DOCUMENT (когда отправляют "как файл")
    app.add_handler(MessageHandler(filters.Document.IMAGE, handle_photo))

    # voice
    app.add_handler(MessageHandler(filters.VOICE, handle_voice))

    # -------------------- PAYMENTS --------------------
    app.add_handler(PreCheckoutQueryHandler(precheckout_callback))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment_callback))

    # -------------------- ERROR HANDLER --------------------
    async def _on_error(update, context):
        logger.exception("Update error: %s", context.error)

    app.add_error_handler(_on_error)

    # -------------------- PERIODIC FOLLOWUPS --------------------
    if app.job_queue:
        app.job_queue.run_repeating(
            periodic_followups_job,
            interval=60 * 60,
            first=60 * 10,
        )

    logger.info("ChatGPT Assistant запущен")
    # более устойчивые сетевые настройки + повторные попытки инициализации
    app.run_polling(bootstrap_retries=10)


if __name__ == "__main__":
    main()
