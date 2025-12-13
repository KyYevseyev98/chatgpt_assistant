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
from db import set_pro, log_pro_payment
from localization import (
    get_lang,
    pro_offer_text,
    pro_success_text,
)

logger = logging.getLogger(__name__)


def _pro_keyboard(lang: str) -> InlineKeyboardMarkup:
    if lang.startswith("uk"):
        labels = [
            f"7 днів — ⭐{PRO_WEEK_STARS}",
            f"30 днів — ⭐{PRO_MONTH_STARS}",
            f"90 днів — ⭐{PRO_QUARTER_STARS}",
        ]
    elif lang.startswith("en"):
        labels = [
            f"7 days — ⭐{PRO_WEEK_STARS}",
            f"30 days — ⭐{PRO_MONTH_STARS}",
            f"90 days — ⭐{PRO_QUARTER_STARS}",
        ]
    else:
        labels = [
            f"7 дней — ⭐{PRO_WEEK_STARS}",
            f"30 дней — ⭐{PRO_MONTH_STARS}",
            f"90 дней — ⭐{PRO_QUARTER_STARS}",
        ]

    return InlineKeyboardMarkup([
        [InlineKeyboardButton(labels[0], callback_data="buy_pro_week")],
        [InlineKeyboardButton(labels[1], callback_data="buy_pro_month")],
        [InlineKeyboardButton(labels[2], callback_data="buy_pro_quarter")],
    ])


async def pro_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    lang = get_lang(user)

    await update.message.reply_text(
        pro_offer_text(lang),
        reply_markup=_pro_keyboard(lang),
    )


async def pro_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user = query.from_user
    lang = get_lang(user)

    if query.data == "buy_pro_week":
        days, stars, payload = 7, PRO_WEEK_STARS, "pro_week"
        title = "ChatGPT PRO — 7 days"
    elif query.data == "buy_pro_month":
        days, stars, payload = 30, PRO_MONTH_STARS, "pro_month"
        title = "ChatGPT PRO — 30 days"
    elif query.data == "buy_pro_quarter":
        days, stars, payload = 90, PRO_QUARTER_STARS, "pro_quarter"
        title = "ChatGPT PRO — 90 days"
    else:
        return

    description = (
        f"PRO access to ChatGPT in Telegram for {days} days."
        if lang.startswith("en")
        else f"PRO-доступ к ChatGPT на {days} дней."
    )

    prices = [LabeledPrice(label=title, amount=stars)]

    await query.message.reply_invoice(
        title=title,
        description=description,
        payload=payload,
        provider_token="",
        currency="XTR",
        prices=prices,
    )


async def precheckout_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.pre_checkout_query.answer(ok=True)


async def successful_payment_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    lang = get_lang(user)

    payload = update.message.successful_payment.invoice_payload

    if payload == "pro_week":
        days, stars = 7, PRO_WEEK_STARS
    elif payload == "pro_month":
        days, stars = 30, PRO_MONTH_STARS
    elif payload == "pro_quarter":
        days, stars = 90, PRO_QUARTER_STARS
    else:
        return

    set_pro(user.id, days)
    log_pro_payment(user.id, stars=stars, days=days)

    await update.message.reply_text(
        pro_success_text(lang, days)
    )