# handlers/text.py
import logging
import asyncio
from typing import List, Dict, Any

from telegram import Update, Message
from telegram.ext import ContextTypes
from telegram.constants import ChatAction

from config import MAX_HISTORY_MESSAGES
from db import (
    check_limit,
    log_event,
    set_traffic_source,
    touch_last_activity,
    set_last_context,
    set_last_limit_topic,
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

BATCH_DELAY_SEC = 0.4


def _build_user_text_with_reply(msg: Message, lang: str) -> str:
    base_text = (msg.text or "").strip()
    reply = msg.reply_to_message

    if not reply:
        return base_text

    replied_text = (reply.text or reply.caption or "").strip()
    if replied_text:
        replied_text = replied_text[:500]

    if not replied_text:
        return base_text

    if lang.startswith("uk"):
        header_other = "Повідомлення, на яке я відповідаю:"
        header_me = "Мій коментар або запитання:"
    elif lang.startswith("en"):
        header_other = "Message I'm replying to:"
        header_me = "My reply or question:"
    else:
        header_other = "Сообщение, на которое я отвечаю:"
        header_me = "Мой комментарий или вопрос:"

    combined = (
        f"{header_other}\n\"{replied_text}\"\n\n"
        f"{header_me}\n{base_text}"
    )
    return combined.strip()


async def _flush_text_batch(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    user_id: int,
) -> None:
    try:
        await asyncio.sleep(BATCH_DELAY_SEC)
    except Exception:
        return

    chat_data = context.chat_data
    batch: List[Dict[str, Any]] = chat_data.get("pending_batch") or []
    if not batch:
        chat_data["batch_task"] = None
        return

    chat_data["pending_batch"] = []
    chat_data["batch_task"] = None

    last_item = batch[-1]
    topic = last_item["topic"]
    lang = last_item["lang"]
    last_msg: Message = last_item["msg"]

    combined_text = "\n\n".join(item["text"] for item in batch)
    total_raw_len = sum(len(item["raw_text"]) for item in batch)
    batch_size = len(batch)

    history_by_topic: Dict[str, List[Dict[str, str]]] = chat_data.get("history_by_topic", {})
    history = history_by_topic.get(topic, [])

    history.append({"role": "user", "content": combined_text})
    if len(history) > MAX_HISTORY_MESSAGES:
        history = history[-MAX_HISTORY_MESSAGES:]

    history_by_topic[topic] = history
    chat_data["history_by_topic"] = history_by_topic
    chat_data["history"] = history

    try:
        await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)
    except Exception as e:
        logger.warning("Не удалось отправить typing (batch): %s", e)

    try:
        answer = await ask_gpt(history, lang)
    except Exception as e:
        logger.exception("Ошибка при запросе к OpenAI (batch): %s", e)
        if lang.startswith("uk"):
            answer = "Сталася помилка. Спробуй пізніше."
        elif lang.startswith("en"):
            answer = "An error occurred. Try again later."
        else:
            answer = "Произошла ошибка. Попробуйте позже."

        # --- мягкий апселл (НЕ всегда) ---
    from db import should_soft_upsell
    from gpt_client import generate_soft_upsell_text

    if should_soft_upsell(user_id):
        try:
            upsell = await generate_soft_upsell_text(lang, topic)
            answer = answer.strip() + "\n\n" + upsell
        except Exception:
            pass

    # добавляем ответ ассистента в историю
    history.append({"role": "assistant", "content": answer})
    if len(history) > MAX_HISTORY_MESSAGES:
        history = history[-MAX_HISTORY_MESSAGES:]
    history_by_topic[topic] = history
    chat_data["history_by_topic"] = history_by_topic
    chat_data["history"] = history

    # ✅ сохраняем память для персонализации follow-up
    try:
        set_last_context(
            user_id,
            topic=topic,
            last_user_message=combined_text,
            last_bot_message=answer,
        )
    except Exception as e:
        logger.warning("Не удалось сохранить last_context: %s", e)

    # ✅ логируем событие text (topic/lang — в профиль)
    try:
        log_event(
            user_id,
            "text",
            tokens=total_raw_len,
            meta=f"topic:{topic};batch_size:{batch_size}",
            lang=lang,
            topic=topic,
        )
    except Exception as e:
        logger.warning("Не удалось залогировать text-событие (batch): %s", e)

    await send_smart_answer(last_msg, answer)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    lang = get_lang(user)

    touch_last_activity(user.id)

    try:
        schedule_first_followup(context.application, user.id, lang)
    except Exception as e:
        logger.warning("Не удалось запланировать first_followup: %s", e)

    context.chat_data["history"] = []
    context.chat_data["history_by_topic"] = {}
    context.chat_data["pending_batch"] = []
    context.chat_data["batch_task"] = None

    args = context.args
    source = args[0] if args else "organic"

    try:
        set_traffic_source(user.id, source)
    except Exception as e:
        logger.warning("Не удалось сохранить traffic_source: %s", e)

    try:
        log_event(user.id, f"start:{source}", lang=lang)
    except Exception as e:
        logger.warning("Не удалось залогировать событие start: %s", e)

    await update.message.reply_text(start_text(lang))


async def reset_dialog(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    lang = get_lang(user)

    context.chat_data["history"] = []
    context.chat_data["history_by_topic"] = {}
    context.chat_data["pending_batch"] = []
    context.chat_data["batch_task"] = None

    await update.message.reply_text(reset_text(lang))

    try:
        log_event(user.id, "reset", lang=lang)
    except Exception as e:
        logger.warning("Не удалось залогировать событие reset: %s", e)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or not msg.text:
        return

    raw_text = msg.text.strip()
    user = update.effective_user
    user_id = user.id
    lang = get_lang(user)

    touch_last_activity(user_id)

    context.chat_data["last_user_text"] = raw_text

    if is_forbidden_topic(raw_text):
        await msg.reply_text(forbidden_reply(lang))
        try:
            log_event(user_id, "forbidden_text", lang=lang)
        except Exception as e:
            logger.warning("Не удалось залогировать forbidden_text: %s", e)
        return

    topic = get_current_topic(context)

    # лимит
    if not check_limit(user_id, is_photo=False):
        try:
            set_last_limit_topic(user_id, topic)
        except Exception:
            pass

        await msg.reply_text(
            text_limit_reached(lang),
            reply_markup=_pro_keyboard(lang),
        )

        try:
            log_event(
                user_id,
                "text_limit_reached",
                lang=lang,
                topic=topic,
                last_limit_type="text",
            )
        except Exception:
            pass

        return

    enriched_text = _build_user_text_with_reply(msg, lang)

    chat_data = context.chat_data
    batch: List[Dict[str, Any]] = chat_data.get("pending_batch") or []

    batch.append(
        {
            "text": enriched_text,
            "raw_text": raw_text,
            "topic": topic,
            "lang": lang,
            "msg": msg,
        }
    )
    chat_data["pending_batch"] = batch

    batch_task = chat_data.get("batch_task")
    if batch_task is None or batch_task.done():
        task = context.application.create_task(
            _flush_text_batch(context, msg.chat_id, user_id)
        )
        chat_data["batch_task"] = task