import logging
import asyncio
from typing import List, Dict
from contextlib import suppress

from telegram import Update
from telegram.ext import ContextTypes

from config import MAX_HISTORY_MESSAGES
from localization import get_lang, text_limit_reached, pro_offer_text
from gpt_client import ask_gpt, transcribe_voice, generate_limit_paywall_text
from db import (
    check_limit,
    log_event,
    touch_last_activity,
    set_last_limit_info,
    get_followup_personalization_snapshot,
    set_last_paywall_text,
    should_send_limit_paywall,
)
from .common import send_smart_answer, send_typing_action
from .pro import _pro_keyboard
from .topics import get_current_topic

logger = logging.getLogger(__name__)


async def handle_voice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or not msg.voice:
        return

    user = update.effective_user
    user_id = user.id
    lang = get_lang(user)

    touch_last_activity(user.id)
    topic = get_current_topic(context)

    if not check_limit(user_id, is_photo=False):
        try:
            set_last_limit_info(user_id, topic=topic, limit_type="voice")
        except Exception:
            pass

        paywall = ""
        try:
            prof = get_followup_personalization_snapshot(user_id)
            paywall = await generate_limit_paywall_text(
                lang=lang,
                limit_type="voice",
                topic=topic,
                last_user_message="(voice message)",
                user_profile=prof,
            )
        except Exception:
            paywall = ""

        if not paywall:
            paywall = text_limit_reached(lang)

        try:
            if should_send_limit_paywall(user_id, paywall):
                set_last_paywall_text(user_id, paywall)
        except Exception:
            pass

        final_text = paywall.strip() + "\n\n" + pro_offer_text(lang)

        await msg.reply_text(
            final_text,
            reply_markup=_pro_keyboard(lang),
        )
        try:
            log_event(user_id, "voice_limit_reached")
        except Exception:
            pass
        return

    stop_event = asyncio.Event()
    typing_task = asyncio.create_task(
        send_typing_action(context.bot, msg.chat_id, stop_event)
    )

    try:
        voice = msg.voice
        file = await voice.get_file()
        bio = await file.download_as_bytearray()
        voice_bytes = bytes(bio)

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

        # сохраняем как последний текст пользователя
        context.chat_data["last_user_text"] = transcribed_text

        history_by_topic: Dict[str, List[Dict[str, str]]] = context.chat_data.get("history_by_topic", {})
        history = history_by_topic.get(topic, [])

        history.append({"role": "user", "content": transcribed_text})
        if len(history) > MAX_HISTORY_MESSAGES:
            history = history[-MAX_HISTORY_MESSAGES:]
        history_by_topic[topic] = history
        context.chat_data["history_by_topic"] = history_by_topic

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
        history_by_topic[topic] = history
        context.chat_data["history_by_topic"] = history_by_topic

        try:
            log_event(user_id, "voice", tokens=len(transcribed_text) if transcribed_text else None)
        except Exception:
            pass

    finally:
        stop_event.set()
        with suppress(Exception):
            await typing_task

    await send_smart_answer(msg, answer)