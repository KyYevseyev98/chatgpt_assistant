import logging
from typing import List, Dict

from telegram import Update
from telegram.ext import ContextTypes
from telegram.constants import ChatAction

from config import MAX_HISTORY_MESSAGES
from db import check_limit
from localization import (
    get_lang,
    start_text,
    reset_text,
    forbidden_reply,
    text_limit_reached,
)
from gpt_client import (
    is_forbidden_topic,
    ask_gpt,
)

from .common import send_smart_answer
from .pro import _pro_keyboard

logger = logging.getLogger(__name__)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    lang = get_lang(user)

    context.chat_data["history"] = []

    await update.message.reply_text(start_text(lang))


async def reset_dialog(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    lang = get_lang(user)

    context.chat_data["history"] = []

    await update.message.reply_text(reset_text(lang))


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or not msg.text:
        return

    text = msg.text.strip()
    user = update.effective_user
    user_id = user.id
    lang = get_lang(user)

    # запоминаем последний текст пользователя (можно использовать для фото)
    context.chat_data["last_user_text"] = text

    # защищённые темы
    if is_forbidden_topic(text):
        await msg.reply_text(forbidden_reply(lang))
        return

    # лимит текстов
    if not check_limit(user_id, is_photo=False):
        await msg.reply_text(
            text_limit_reached(lang),
            reply_markup=_pro_keyboard(lang),
        )
        return

    history: List[Dict[str, str]] = context.chat_data.get("history", [])
    history.append({"role": "user", "content": text})

    # один раз показываем "печатает..."
    try:
        await context.bot.send_chat_action(
            chat_id=msg.chat_id,
            action=ChatAction.TYPING,
        )
    except Exception as e:
        logger.warning("Не удалось отправить typing (text): %s", e)

    # запрос к GPT
    try:
        answer = await ask_gpt(history, lang)
    except Exception as e:
        logger.exception("Ошибка при запросе к OpenAI: %s", e)
        if lang.startswith("uk"):
            answer = "Сталася помилка. Спробуй пізніше."
        elif lang.startswith("en"):
            answer = "An error occurred. Try again later."
        else:
            answer = "Произошла ошибка. Попробуйте позже."

    history.append({"role": "assistant", "content": answer})

    if len(history) > MAX_HISTORY_MESSAGES:
        history = history[-MAX_HISTORY_MESSAGES:]

    context.chat_data["history"] = history

    await send_smart_answer(msg, answer)