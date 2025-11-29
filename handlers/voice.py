import logging
import asyncio
from typing import List, Dict
from contextlib import suppress

from telegram import Update
from telegram.ext import ContextTypes

from config import MAX_HISTORY_MESSAGES
from db import check_limit
from localization import (
    get_lang,
    text_limit_reached,
)
from gpt_client import (
    ask_gpt,
    transcribe_voice,
)

from .common import send_smart_answer, send_typing_action
from .pro import _pro_keyboard

logger = logging.getLogger(__name__)


async def handle_voice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or not msg.voice:
        return

    user = update.effective_user
    user_id = user.id
    lang = get_lang(user)

    # лимит считаем как текстовый запрос
    if not check_limit(user_id, is_photo=False):
        await msg.reply_text(
            text_limit_reached(lang),
            reply_markup=_pro_keyboard(lang),
        )
        return

    # фоновый typing на всё время обработки
    stop_event = asyncio.Event()
    typing_task = asyncio.create_task(
        send_typing_action(context.bot, msg.chat_id, stop_event)
    )

    try:
        # скачиваем голосовое
        voice = msg.voice
        file = await voice.get_file()
        bio = await file.download_as_bytearray()
        voice_bytes = bytes(bio)

        # 1) расшифровка голосового -> текст
        try:
            transcribed_text = await transcribe_voice(voice_bytes)
        except Exception as e:
            logger.exception("Ошибка при расшифровке голосового: %s", e)
            if lang.startswith("uk"):
                transcribed_text = "Не вдалося розпізнати голосове повідомлення."
            elif lang.startswith("en"):
                transcribed_text = "I couldn’t transcribe this voice message."
            else:
                transcribed_text = "Не удалось распознать голосовое сообщение."

        # сохраняем как последний текст пользователя (для фото / контекста)
        context.chat_data["last_user_text"] = transcribed_text

        history: List[Dict[str, str]] = context.chat_data.get("history", [])
        history.append({"role": "user", "content": transcribed_text})

        # 2) запрос к GPT
        try:
            answer = await ask_gpt(history, lang)
        except Exception as e:
            logger.exception("Ошибка при запросе к OpenAI (voice->text): %s", e)
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
    finally:
        stop_event.set()
        with suppress(Exception):
            await typing_task

    # отвечаем ТЕКСТОМ + отдельные блоки кода (без стриминга)
    await send_smart_answer(msg, answer)