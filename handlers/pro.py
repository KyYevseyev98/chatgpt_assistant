import logging
from telegram import Update, LabeledPrice
from telegram.ext import ContextTypes
from telegram.constants import ParseMode

from config import (
    PRO_STARS_7_DAYS,
    PRO_STARS_30_DAYS,
    PRO_DAYS_7,
    PRO_DAYS_30,
)
from localization import (
    get_lang,
    pro_success_text,
    pro_error_text,
)
from db import set_pro, log_pro_payment

logger = logging.getLogger(__name__)


# ---------- КНОПКИ PRO ----------
def _pro_keyboard(lang: str):
    from telegram import InlineKeyboardMarkup, InlineKeyboardButton

    if lang.startswith("uk"):
        text_7 = f"⭐ PRO на 7 днів — {PRO_STARS_7_DAYS}⭐"
        text_30 = f"🔥 PRO на 30 днів — {PRO_STARS_30_DAYS}⭐"
    elif lang.startswith("en"):
        text_7 = f"⭐ PRO for 7 days — {PRO_STARS_7_DAYS}⭐"
        text_30 = f"🔥 PRO for 30 days — {PRO_STARS_30_DAYS}⭐"
    else:
        text_7 = f"⭐ PRO на 7 дней — {PRO_STARS_7_DAYS}⭐"
        text_30 = f"🔥 PRO на 30 дней — {PRO_STARS_30_DAYS}⭐"

    keyboard = [
        [InlineKeyboardButton(text_7, callback_data="buy_pro_7")],
        [InlineKeyboardButton(text_30, callback_data="buy_pro_30")],
    ]
    return InlineKeyboardMarkup(keyboard)


# ---------- CALLBACK: НАЖАТИЕ КНОПОК ----------
async def pro_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return

    await query.answer()
    user = query.from_user
    lang = get_lang(user)

    if query.data == "buy_pro_7":
        stars = PRO_STARS_7_DAYS
        days = PRO_DAYS_7
        title = "PRO 7 days"
    elif query.data == "buy_pro_30":
        stars = PRO_STARS_30_DAYS
        days = PRO_DAYS_30
        title = "PRO 30 days"
    else:
        return

    prices = [LabeledPrice(label=title, amount=stars)]

    try:
        await query.message.reply_invoice(
            title=title,
            description=title,
            payload=f"pro_{days}",
            provider_token="",  # ОБЯЗАТЕЛЬНО пусто для Telegram Stars
            currency="XTR",
            prices=prices,
        )
    except Exception as e:
        logger.exception("Ошибка при создании инвойса PRO: %s", e)
        await query.message.reply_text(pro_error_text(lang))


# ---------- УСПЕШНАЯ ОПЛАТА ----------
async def successful_payment_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or not msg.successful_payment:
        return

    user = msg.from_user
    lang = get_lang(user)

    payload = msg.successful_payment.invoice_payload
    stars = msg.successful_payment.total_amount

    if payload == f"pro_{PRO_DAYS_7}":
        days = PRO_DAYS_7
    elif payload == f"pro_{PRO_DAYS_30}":
        days = PRO_DAYS_30
    else:
        logger.warning("Неизвестный payload оплаты: %s", payload)
        return

    try:
        # 1️⃣ Активируем PRO
        set_pro(user.id, days)

        # 2️⃣ Логируем оплату
        log_pro_payment(user.id, stars=stars, days=days)

        # 3️⃣ Уведомляем пользователя
        await msg.reply_text(
            pro_success_text(lang, days),
            parse_mode=ParseMode.HTML,
        )

    except Exception as e:
        logger.exception("Ошибка при обработке успешной оплаты PRO: %s", e)
        await msg.reply_text(pro_error_text(lang))