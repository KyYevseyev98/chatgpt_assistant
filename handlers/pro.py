import logging

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    Update,
)
from telegram.ext import ContextTypes

from config import (
    PRO_WEEK_STARS,
    PRO_MONTH_STARS,
    PRO_QUARTER_STARS,
)
from db import set_pro, log_pro_payment, log_event
from localization import (
    get_lang,
    pro_offer_text,
    pro_success_text,
)

logger = logging.getLogger(__name__)


def _pro_keyboard(lang: str) -> InlineKeyboardMarkup:
    """
    Кнопки тарифов PRO.
    """
    if lang.startswith("uk"):
        week_label = f"7 днів — ⭐{PRO_WEEK_STARS}"
        month_label = f"30 днів — ⭐{PRO_MONTH_STARS}"
        quarter_label = f"90 днів — ⭐{PRO_QUARTER_STARS}"
    elif lang.startswith("en"):
        week_label = f"7 days — ⭐{PRO_WEEK_STARS}"
        month_label = f"30 days — ⭐{PRO_MONTH_STARS}"
        quarter_label = f"90 days — ⭐{PRO_QUARTER_STARS}"
    else:
        week_label = f"7 дней — ⭐{PRO_WEEK_STARS}"
        month_label = f"30 дней — ⭐{PRO_MONTH_STARS}"
        quarter_label = f"90 дней — ⭐{PRO_QUARTER_STARS}"

    keyboard = [
        [InlineKeyboardButton(week_label, callback_data="buy_pro_week")],
        [InlineKeyboardButton(month_label, callback_data="buy_pro_month")],
        [InlineKeyboardButton(quarter_label, callback_data="buy_pro_quarter")],
    ]
    return InlineKeyboardMarkup(keyboard)


async def pro_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /pro — показать описание подписки и кнопки.
    """
    user = update.effective_user
    lang = get_lang(user)

    text = pro_offer_text(lang)
    await update.message.reply_text(text, reply_markup=_pro_keyboard(lang))


async def pro_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обработка нажатия кнопок 'buy_pro_*' — отправка инвойса со звёздами.
    """
    query = update.callback_query
    await query.answer()
    user = query.from_user
    lang = get_lang(user)

    data = query.data

    if data == "buy_pro_week":
        stars = PRO_WEEK_STARS
        days = 7
        payload = "pro_week"
        title = "ChatGPT PRO — 7 days"
    elif data == "buy_pro_month":
        stars = PRO_MONTH_STARS
        days = 30
        payload = "pro_month"
        title = "ChatGPT PRO — 30 days"
    elif data == "buy_pro_quarter":
        stars = PRO_QUARTER_STARS
        days = 90
        payload = "pro_quarter"
        title = "ChatGPT PRO — 90 days"
    else:
        return

    if lang.startswith("uk"):
        description = f"PRO-доступ до ChatGPT у Telegram на {days} днів без лімітів."
    elif lang.startswith("en"):
        description = f"PRO access to ChatGPT in Telegram for {days} days with no limits."
    else:
        description = f"PRO-доступ к ChatGPT в Telegram на {days} дней без лимитов."

    prices = [LabeledPrice(label=title, amount=stars)]  # amount = кол-во звёзд

    await query.message.reply_invoice(
        title=title,
        description=description,
        payload=payload,
        provider_token="",  # для Telegram Stars можно оставить пустым
        currency="XTR",
        prices=prices,
    )


async def precheckout_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обязательный хендлер для подтверждения pre-checkout.
    """
    query = update.pre_checkout_query
    await query.answer(ok=True)


# handlers/pro.py

import logging

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    Update,
)
from telegram.ext import ContextTypes

from config import (
    PRO_WEEK_STARS,
    PRO_MONTH_STARS,
    PRO_QUARTER_STARS,
)
from db import set_pro, log_pro_payment   # <= тут ДОБАВИЛИ log_pro_payment
from localization import (
    get_lang,
    pro_offer_text,
    pro_success_text,
)

logger = logging.getLogger(__name__)

# ... остальной код файла не трогаем ...


async def successful_payment_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
):
    """
    Когда оплата прошла — включаем PRO + логируем платеж.
    """
    user = update.effective_user
    lang = get_lang(user)
    payment = update.message.successful_payment
    payload = payment.invoice_payload

    if payload == "pro_week":
        days = 7
        stars = PRO_WEEK_STARS
    elif payload == "pro_month":
        days = 30
        stars = PRO_MONTH_STARS
    elif payload == "pro_quarter":
        days = 90
        stars = PRO_QUARTER_STARS
    else:
        # на всякий случай дефолт
        days = 30
        stars = PRO_MONTH_STARS

    # включаем PRO
    set_pro(user.id, days)

    # логируем оплату в pro_payments
    try:
        log_pro_payment(user.id, stars=stars, days=days)
    except Exception as e:
        logger.warning("Не удалось залогировать оплату PRO: %s", e)

    await update.message.reply_text(pro_success_text(lang, days))