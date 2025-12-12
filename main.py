# main.py
import logging

from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    PreCheckoutQueryHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes
)

from config import TG_TOKEN
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
    topics_command,      # <-- добавили
    topic_button,  
)

import datetime as dt
from jobs import followup_after_limit_job

from db import (
    init_db,
    get_all_users_for_followup,
    required_ignored_days_for_stage,
    mark_followup_sent,
)
from gpt_client import generate_followup_text

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def main():
    # инициализируем базу (создаст таблицы, если их нет)
    init_db()

    app = ApplicationBuilder().token(TG_TOKEN).build()

    # команды
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("reset", reset_dialog))
    app.add_handler(CommandHandler("pro", pro_command))
   #app.add_handler(CommandHandler("topics", topics_command))  # <-- новая команда

    # обычные сообщения и фото
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    app.add_handler(MessageHandler(filters.VOICE, handle_voice))

    # кнопки PRO
    app.add_handler(CallbackQueryHandler(pro_button, pattern=r"^buy_pro_"))

    # кнопки тем
    app.add_handler(CallbackQueryHandler(topic_button, pattern=r"^(topic_|topics_close)"))
    

    # платежи
    app.add_handler(PreCheckoutQueryHandler(precheckout_callback))
    app.add_handler(
        MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment_callback)
    )
        # периодические follow-up'ы: проверка раз в час
    app.job_queue.run_repeating(
    periodic_followups_job,
    interval=60 * 60,   # раз в час
    first=60 * 10,      # первую проверку через 10 минут после старта бота
    )

    app.job_queue.run_repeating(
    followup_after_limit_job,
    interval=60 * 60 * 6,  # раз в 6 часов
    first=60 * 20,
)
    logger.info("ChatGPT Assistant запущен")
    app.run_polling()
    
async def periodic_followups_job(context: ContextTypes.DEFAULT_TYPE):
    """
    Периодический обход всех пользователей и отправка follow-up,
    если они игнорят достаточно долго.
    Схема дней игнора задаётся required_ignored_days_for_stage(stage).
    """
    now = dt.datetime.utcnow()
    users = get_all_users_for_followup()

    for user_id, last_activity_at, last_followup_at, followup_stage in users:
        if not last_activity_at:
            # если юзер вообще ни разу ничего не делал — пропускаем
            continue

        try:
            last_activity_dt = dt.datetime.fromisoformat(last_activity_at)
        except Exception:
            continue

        ignored_days = (now - last_activity_dt).days

        # сколько дней нужно игнора для текущей ступени
        required_days = required_ignored_days_for_stage(followup_stage)
        if ignored_days < required_days:
            continue

        # чтобы не заспамить: если уже сегодня слали — пропускаем
        if last_followup_at:
            try:
                last_f_dt = dt.datetime.fromisoformat(last_followup_at)
                if (now - last_f_dt).days == 0:
                    continue
            except Exception:
                pass

        lang = "ru"  # позже можно читать язык юзера из БД

        try:
            text = await generate_followup_text(
                lang=lang,
                ignored_days=ignored_days,
                stage=followup_stage,
                last_user_message=None,
                last_bot_message=None,
                last_followup_text=None,
            )
            await context.bot.send_message(chat_id=user_id, text=text)
            mark_followup_sent(user_id)
        except Exception as e:
            print(f"Не удалось отправить follow-up пользователю {user_id}: {e}")



if __name__ == "__main__":
    main()