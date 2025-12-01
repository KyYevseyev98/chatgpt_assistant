import logging
from typing import List, Dict

from telegram import Update
from telegram.ext import ContextTypes
from telegram.constants import ChatAction

from config import MAX_HISTORY_MESSAGES
from db import check_limit, log_event, set_traffic_source
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


# handlers/text.py

import logging
from typing import List, Dict

from telegram import Update
from telegram.ext import ContextTypes
from telegram.constants import ChatAction

from config import MAX_HISTORY_MESSAGES
from db import check_limit, log_event, set_traffic_source   # <= ВАЖНО здесь log_event и set_traffic_source
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

    # --- читаем source из /start <source> ---
    args = context.args
    if args:
        source = args[0]      # например "ads_tt"
    else:
        source = "organic"

    # --- сохраняем источник трафика (один раз) ---
    try:
        set_traffic_source(user.id, source)
    except Exception as e:
        logger.warning("Не удалось сохранить traffic_source: %s", e)

    # --- логируем событие /start ---
    try:
        log_event(user.id, f"start:{source}")
    except Exception as e:
        logger.warning("Не удалось залогировать событие start: %s", e)

    await update.message.reply_text(start_text(lang))


async def reset_dialog(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    lang = get_lang(user)

    context.chat_data["history"] = []

    await update.message.reply_text(reset_text(lang))

    # логируем сброс диалога
    try:
        log_event(user.id, "reset")
    except Exception as e:
        logger.warning("Не удалось залогировать событие reset: %s", e)


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
        try:
            log_event(user_id, "forbidden_text")
        except Exception as e:
            logger.warning("Не удалось залогировать forbidden_text: %s", e)
        return

    # лимит текстов
    if not check_limit(user_id, is_photo=False):
        await msg.reply_text(
            text_limit_reached(lang),
            reply_markup=_pro_keyboard(lang),
        )
        try:
            log_event(user_id, "text_limit_reached")
        except Exception as e:
            logger.warning("Не удалось залогировать text_limit_reached: %s", e)
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

    # логируем успешный текстовый запрос
    try:
        # можно в tokens временно писать длину текста
        log_event(user_id, "text", tokens=len(text))
    except Exception as e:
        logger.warning("Не удалось залогировать text-событие: %s", e)

    await send_smart_answer(msg, answer)