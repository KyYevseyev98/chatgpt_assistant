# handlers/text.py
import logging
from typing import List, Dict

from telegram import Update
from telegram.ext import ContextTypes
from telegram.constants import ChatAction

from config import MAX_HISTORY_MESSAGES
from db import (
    check_limit,
    log_event,
    set_traffic_source,
    touch_last_activity,
)
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
from jobs import schedule_first_followup

from .common import send_smart_answer
from .pro import _pro_keyboard
from .topics import get_current_topic

logger = logging.getLogger(__name__)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /start:
    - считаем как активность (обновляем last_activity)
    - если юзер совсем новый — ставим одноразовый follow-up через 30 сек
    - логируем источник трафика
    - сбрасываем историю
    - отправляем приветственный текст
    """
    user = update.effective_user
    lang = get_lang(user)

    # /start тоже считаем активностью
    touch_last_activity(user.id)

    # ставим первый follow-up для совсем нового пользователя
    try:
        schedule_first_followup(context.application, user.id, lang)
    except Exception as e:
        logger.warning("Не удалось запланировать first_followup: %s", e)

    # общая история и истории по темам
    context.chat_data["history"] = []
    context.chat_data["history_by_topic"] = {}

    # --- читаем source из /start <source> ---
    args = context.args
    if args:
        source = args[0]  # например "ads_tt"
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

    # просто приветственный текст, без клавиатуры тем
    await update.message.reply_text(start_text(lang))


async def reset_dialog(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Сброс диалога (команда /reset).
    """
    user = update.effective_user
    lang = get_lang(user)

    context.chat_data["history"] = []
    context.chat_data["history_by_topic"] = {}

    await update.message.reply_text(reset_text(lang))

    # логируем сброс диалога
    try:
        log_event(user.id, "reset")
    except Exception as e:
        logger.warning("Не удалось залогировать событие reset: %s", e)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обработка обычного текстового сообщения.
    Учитываем:
      - запрещённые темы
      - лимиты (free / PRO)
      - историю по текущей теме (вкладке)
    """
    msg = update.message
    if not msg or not msg.text:
        return

    text = msg.text.strip()
    user = update.effective_user
    user_id = user.id
    lang = get_lang(user)

    # любая активность пользователя — обновляем last_activity
    touch_last_activity(user_id)

    # запоминаем последний текст пользователя (может пригодиться для фото)
    context.chat_data["last_user_text"] = text

    # --- защита по запрещённым темам ---
    if is_forbidden_topic(text):
        await msg.reply_text(forbidden_reply(lang))
        try:
            log_event(user_id, "forbidden_text")
        except Exception as e:
            logger.warning("Не удалось залогировать forbidden_text: %s", e)
        return

    # --- проверка лимита текстов ---
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

    # --- какая сейчас тема (вкладка) ---
    topic = get_current_topic(context)  # "chat", "travel", "fitness", "content"

    history_by_topic: Dict[str, List[Dict[str, str]]] = context.chat_data.get(
        "history_by_topic", {}
    )
    history = history_by_topic.get(topic, [])

    # добавляем сообщение пользователя в историю этой темы
    history.append({"role": "user", "content": text})
    if len(history) > MAX_HISTORY_MESSAGES:
        history = history[-MAX_HISTORY_MESSAGES:]

    # сохраняем историю обратно
    history_by_topic[topic] = history
    context.chat_data["history_by_topic"] = history_by_topic
    # для обратной совместимости — общая история = история текущей темы
    context.chat_data["history"] = history

    # --- "печатает..." ---
    try:
        await context.bot.send_chat_action(
            chat_id=msg.chat_id,
            action=ChatAction.TYPING,
        )
    except Exception as e:
        logger.warning("Не удалось отправить typing (text): %s", e)

    # --- запрос к GPT ---
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

    # добавляем ответ ассистента в историю темы
    history.append({"role": "assistant", "content": answer})
    if len(history) > MAX_HISTORY_MESSAGES:
        history = history[-MAX_HISTORY_MESSAGES:]

    history_by_topic[topic] = history
    context.chat_data["history_by_topic"] = history_by_topic
    context.chat_data["history"] = history  # на всякий случай оставляем общий last-history

    # логируем успешный текстовый запрос
    try:
        # в tokens можно временно писать длину текста, а в meta — тему
        log_event(user_id, "text", tokens=len(text), meta=f"topic:{topic}")
    except Exception as e:
        logger.warning("Не удалось залогировать text-событие: %s", e)

    # отправляем ответ БЕЗ клавиатуры тем
    await send_smart_answer(msg, answer)