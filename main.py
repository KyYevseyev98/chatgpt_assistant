# main.py
import logging

from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    PreCheckoutQueryHandler,
    CallbackQueryHandler,
    filters,
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
)
from db import init_db  # <-- ДОБАВИЛИ


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

    # обычные сообщения и фото
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    app.add_handler(MessageHandler(filters.VOICE, handle_voice))

    # кнопки PRO
    app.add_handler(CallbackQueryHandler(pro_button, pattern=r"^buy_pro_"))

    # платежи
    app.add_handler(PreCheckoutQueryHandler(precheckout_callback))
    app.add_handler(
        MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment_callback)
    )

    logger.info("ChatGPT Assistant запущен")
    app.run_polling()


if __name__ == "__main__":
    main()