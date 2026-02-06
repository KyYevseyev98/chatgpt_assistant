import logging
import json

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    Update,
)
from telegram.ext import ContextTypes

from db import add_tarot_credits, log_event
from config import REFERRAL_REWARD_SPREADS, TAROT_PACKS
from localization import (
    get_lang,
    pro_offer_text,
    pro_success_text,
)
from handlers.common import reply_and_mirror

logger = logging.getLogger(__name__)


def _pro_keyboard(lang: str) -> InlineKeyboardMarkup:
    buy_label = "🃏 Купить расклады"
    ref_label = "🤝 Пригласить подругу"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(buy_label, callback_data="buy_tarot_open")],
        [InlineKeyboardButton(ref_label, callback_data="ref_invite")],
    ])


def _pro_text(lang: str) -> str:
    return pro_offer_text(lang)


def _packs_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for p in TAROT_PACKS:
        label = f"{p['spreads']} раскладов — ⭐{p['stars']}"
        rows.append([InlineKeyboardButton(label, callback_data=f"buy_tarot_pack_{p['key']}")])
    rows.append([InlineKeyboardButton("◀️ Назад", callback_data="buy_tarot_back")])
    return InlineKeyboardMarkup(rows)


def _referral_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="ref_back")]])


async def pro_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    lang = get_lang(user)

    await reply_and_mirror(
        update.message,
        pro_offer_text(lang),
        reply_markup=_pro_keyboard(lang),
        parse_mode="HTML",
    )


async def pro_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user = query.from_user
    lang = get_lang(user)

    if query.data == "buy_tarot_open":
        await query.message.edit_reply_markup(reply_markup=_packs_keyboard())
        return

    if query.data == "buy_tarot_back":
        await query.message.edit_reply_markup(reply_markup=_pro_keyboard(lang))
        return

    if query.data == "ref_invite":
        # store original text to restore on "back"
        try:
            orig_text = (query.message.text or "").strip()
            if orig_text:
                key = f"paywall_text_{query.message.message_id}"
                context.user_data[key] = orig_text
        except Exception:
            pass

        bot_username = context.bot_data.get("bot_username")
        if not bot_username:
            try:
                me = await context.bot.get_me()
                bot_username = (me.username or "").strip()
                if bot_username:
                    context.bot_data["bot_username"] = bot_username
            except Exception:
                bot_username = ""

        if not bot_username:
            text = (
                "Сейчас не удалось получить имя бота для ссылки.\n"
                "Попробуй ещё раз через пару секунд."
            )
            await query.message.edit_text(text, reply_markup=_referral_keyboard())
            return

        link = f"https://t.me/{bot_username}?start=ref_{user.id}"

        text = (
            "🤝 <b>Пригласи подругу по этой ссылке — и получишь бонусные расклады.</b>\n"
            f"✨ <b>Бонус:</b> +{REFERRAL_REWARD_SPREADS} расклада на баланс.\n\n"
            "⚠️ <b>Важно:</b> бонус начисляется только если она не просто запустит бота, "
            "а сделает хотя бы один расклад.\n\n"
            "<blockquote><b>Ваша личная реферальная ссылка:</b>\n"
            f"{link}</blockquote>"
        )
        await query.message.edit_text(text, reply_markup=_referral_keyboard(), parse_mode="HTML")
        return

    if query.data == "ref_back":
        key = f"paywall_text_{query.message.message_id}"
        restored = context.user_data.get(key) or _pro_text(lang)
        await query.message.edit_text(restored, reply_markup=_pro_keyboard(lang), parse_mode="HTML")
        return

    if not query.data.startswith("buy_tarot_pack_"):
        return

    pack_key = query.data.replace("buy_tarot_pack_", "").strip()
    pack = next((p for p in TAROT_PACKS if p["key"] == pack_key), None)
    if not pack:
        return

    spreads = int(pack["spreads"])
    stars = int(pack["stars"])
    payload = f"tarot_pack_{pack_key}"
    title = f"{spreads} раскладов"
    description = "Покупка пакета раскладов."
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
    if not payload.startswith("tarot_pack_"):
        return

    pack_key = payload.replace("tarot_pack_", "").strip()
    pack = next((p for p in TAROT_PACKS if p["key"] == pack_key), None)
    if not pack:
        return

    spreads = int(pack["spreads"])
    try:
        add_tarot_credits(user.id, update.effective_chat.id, spreads)
    except Exception:
        logger.exception("Failed to add tarot credits")
    try:
        log_event(user.id, "tarot_purchase", meta=f"pack:{pack_key};spreads:{spreads};stars:{pack['stars']}")
    except Exception:
        logger.exception("Failed to log tarot purchase")

    await reply_and_mirror(
        update.message,
        pro_success_text(lang),
        parse_mode="HTML",
    )


async def handle_webapp_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or not msg.web_app_data:
        return

    user = update.effective_user
    data = msg.web_app_data.data or ""
    try:
        payload = json.loads(data)
    except Exception:
        payload = {}

    action = payload.get("action")
    if action != "buy_pack":
        return

    pack_key = str(payload.get("pack") or "").strip()
    pack = next((p for p in TAROT_PACKS if p["key"] == pack_key), None)
    if not pack:
        await reply_and_mirror(msg, "Не удалось определить пакет. Попробуй ещё раз.")
        return

    spreads = int(pack["spreads"])
    stars = int(pack["stars"])
    payload_id = f"tarot_pack_{pack_key}"
    title = f"{spreads} раскладов"
    description = "Покупка пакета раскладов."
    prices = [LabeledPrice(label=title, amount=stars)]

    await context.bot.send_invoice(
        chat_id=msg.chat_id,
        title=title,
        description=description,
        payload=payload_id,
        provider_token="",
        currency="XTR",
        prices=prices,
    )
