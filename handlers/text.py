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

# сколько ждать, чтобы собрать несколько сообщений в один батч (секунды)
BATCH_DELAY_SEC = 0.4


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

def _build_user_text_with_reply(msg: Message, lang: str) -> str:
    """
    Если пользователь отвечает на какое-то сообщение (reply_to_message),
    мы включаем текст того сообщения в запрос, чтобы GPT понимал контекст.

    Формат примерно:
    'Сообщение, на которое я отвечаю: "..." \n\n Мой комментарий или вопрос: ...'
    """
    base_text = (msg.text or "").strip()
    reply = msg.reply_to_message

    if not reply:
        return base_text

    replied_text = (reply.text or reply.caption or "").strip()
    # На всякий случай ограничим длину, чтобы не таскать километровые простыни
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
    """
    Ждёт небольшую паузу, потом забирает все накопленные сообщения
    из context.chat_data['pending_batch'] и одним батчем отправляет в GPT.
    """
    try:
        await asyncio.sleep(BATCH_DELAY_SEC)
    except Exception:
        return

    chat_data = context.chat_data
    batch: List[Dict[str, Any]] = chat_data.get("pending_batch") or []
    if not batch:
        # уже всё сбросили
        chat_data["batch_task"] = None
        return

    # забираем и очищаем батч
    chat_data["pending_batch"] = []
    chat_data["batch_task"] = None

    # берём последнюю запись как "основную" для темы/языка/сообщения
    last_item = batch[-1]
    topic = last_item["topic"]
    lang = last_item["lang"]
    last_msg: Message = last_item["msg"]

    # склеиваем тексты
    combined_text = "\n\n".join(item["text"] for item in batch)
    total_raw_len = sum(len(item["raw_text"]) for item in batch)
    batch_size = len(batch)

    # достаём историю по теме
    history_by_topic: Dict[str, List[Dict[str, str]]] = chat_data.get(
        "history_by_topic", {}
    )
    history = history_by_topic.get(topic, [])

    # добавляем единое "user"-сообщение с комбинированным текстом
    history.append({"role": "user", "content": combined_text})
    if len(history) > MAX_HISTORY_MESSAGES:
        history = history[-MAX_HISTORY_MESSAGES:]

    history_by_topic[topic] = history
    chat_data["history_by_topic"] = history_by_topic
    chat_data["history"] = history  # общая история = текущая тема

    # "печатает..."
    try:
        await context.bot.send_chat_action(
            chat_id=chat_id,
            action=ChatAction.TYPING,
        )
    except Exception as e:
        logger.warning("Не удалось отправить typing (batch): %s", e)

    # запрос к GPT
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

    # добавляем ответ ассистента в историю
    history.append({"role": "assistant", "content": answer})
    if len(history) > MAX_HISTORY_MESSAGES:
        history = history[-MAX_HISTORY_MESSAGES:]

    history_by_topic[topic] = history
    chat_data["history_by_topic"] = history_by_topic
    chat_data["history"] = history

    # логируем одно событие text с инфой о батче
    try:
        log_event(
            user_id,
            "text",
            tokens=total_raw_len,
            meta=f"topic:{topic};batch_size:{batch_size}",
        )
    except Exception as e:
        logger.warning("Не удалось залогировать text-событие (batch): %s", e)

    # отправляем ответ, реплаемся на последнее сообщение из батча
    await send_smart_answer(last_msg, answer)


# ---------------------------------------------------------------------------
# /start и /reset — без изменений по сути
# ---------------------------------------------------------------------------

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
    context.chat_data["pending_batch"] = []
    context.chat_data["batch_task"] = None

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
    context.chat_data["pending_batch"] = []
    context.chat_data["batch_task"] = None

    await update.message.reply_text(reset_text(lang))

    # логируем сброс диалога
    try:
        log_event(user.id, "reset")
    except Exception as e:
        logger.warning("Не удалось залогировать событие reset: %s", e)


# ---------------------------------------------------------------------------
# Обработка текстовых сообщений с батчингом и учётом reply_to_message
# ---------------------------------------------------------------------------

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обработка обычного текстового сообщения.

    Новое:
      - несколько сообщений подряд (форварды, куски текста) за ~0.7 сек
        склеиваются в один запрос к GPT;
      - если пользователь отвечает на какое-то сообщение (reply),
        исходное сообщение подмешивается в текст, чтобы GPT видел контекст.
    """
    msg = update.message
    if not msg or not msg.text:
        return

    raw_text = msg.text.strip()
    user = update.effective_user
    user_id = user.id
    lang = get_lang(user)

    # любая активность пользователя — обновляем last_activity
    touch_last_activity(user_id)

    # сохраняем последний сырой текст (может пригодиться для фото)
    context.chat_data["last_user_text"] = raw_text

    # --- защита по запрещённым темам (по сырому тексту) ---
    if is_forbidden_topic(raw_text):
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

    # формируем текст с учётом reply_to_message
    enriched_text = _build_user_text_with_reply(msg, lang)

    # кладём сообщение в батч
    chat_data = context.chat_data
    batch: List[Dict[str, Any]] = chat_data.get("pending_batch") or []

    batch.append(
        {
            "text": enriched_text,   # то, что пойдёт в GPT
            "raw_text": raw_text,    # сырой текст для статистики
            "topic": topic,
            "lang": lang,
            "msg": msg,              # последнее сообщение, к нему будем реплаиться
        }
    )
    chat_data["pending_batch"] = batch

    # если задачи на флеш ещё нет — запускаем
    batch_task = chat_data.get("batch_task")
    if batch_task is None or batch_task.done():
        task = context.application.create_task(
            _flush_text_batch(context, msg.chat_id, user_id)
        )
        chat_data["batch_task"] = task