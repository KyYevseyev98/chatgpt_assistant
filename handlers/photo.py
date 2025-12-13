import logging
import asyncio
from typing import Dict, List, Set
from contextlib import suppress

from telegram import Update
from telegram.ext import ContextTypes

from config import MAX_HISTORY_MESSAGES
from db import (
    check_limit,
    log_event,
    touch_last_activity,
    set_last_limit_info,
    get_followup_personalization_snapshot,
    set_last_paywall_text,
    should_send_limit_paywall,
)
from localization import (
    get_lang,
    photo_limit_reached,
    photo_placeholder_text,
    multi_photo_not_allowed,
    pro_offer_text,
)
from gpt_client import ask_gpt_with_image, generate_limit_paywall_text
from .common import send_smart_answer, send_typing_action, get_media_lock
from .pro import _pro_keyboard
from .topics import get_current_topic

logger = logging.getLogger(__name__)


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return

    # ✅ принимаем и обычное PHOTO, и картинку как DOCUMENT (image/*)
    is_photo = bool(msg.photo)
    is_doc_image = bool(msg.document and (msg.document.mime_type or "").startswith("image/"))

    if not is_photo and not is_doc_image:
        return

    user = update.effective_user
    user_id = user.id
    lang = get_lang(user)

    touch_last_activity(user.id)

    # защита от альбома (для PHOTO)
    if is_photo and msg.media_group_id is not None:
        media_group_id = msg.media_group_id
        handled_groups: Set[str] = context.chat_data.get("handled_media_groups", set())
        if media_group_id in handled_groups:
            return
        handled_groups.add(media_group_id)
        context.chat_data["handled_media_groups"] = handled_groups

        await msg.reply_text(multi_photo_not_allowed(lang))
        try:
            log_event(user_id, "multi_photo_not_allowed")
        except Exception as e:
            logger.warning("Не удалось залогировать multi_photo_not_allowed: %s", e)
        return

    topic = get_current_topic(context)

    # лимит фото
    if not check_limit(user_id, is_photo=True):
        try:
            set_last_limit_info(user_id, topic=topic, limit_type="photo")
        except Exception:
            pass

        paywall = ""
        try:
            prof = get_followup_personalization_snapshot(user_id)
            paywall = await generate_limit_paywall_text(
                lang=lang,
                limit_type="photo",
                topic=topic,
                last_user_message=(msg.caption or context.chat_data.get("last_user_text") or ""),
                user_profile=prof,
            )
        except Exception:
            paywall = ""

        if not paywall:
            paywall = photo_limit_reached(lang)

        # антидубль
        try:
            if should_send_limit_paywall(user_id, paywall):
                set_last_paywall_text(user_id, paywall)
            else:
                return
        except Exception:
            pass

        final_text = paywall.strip() + "\n\n" + pro_offer_text(lang)

        await msg.reply_text(
            final_text,
            reply_markup=_pro_keyboard(lang),
        )
        try:
            log_event(user_id, "photo_limit_reached")
        except Exception as e:
            logger.warning("Не удалось залогировать photo_limit_reached: %s", e)
        return

    # IMPORTANT: синхронизация медиа, чтобы текст не обгонял фото
    media_lock = get_media_lock(context)

    async with media_lock:
        stop_event = asyncio.Event()
        typing_task = asyncio.create_task(send_typing_action(context.bot, msg.chat_id, stop_event))

        try:
            # скачиваем фото (PHOTO или DOCUMENT image/*)
            try:
                if is_photo:
                    photo = msg.photo[-1]
                    file = await photo.get_file()
                else:
                    file = await msg.document.get_file()

                bio = await file.download_as_bytearray()
                image_bytes = bytes(bio)
            except Exception as e:
                logger.exception("Ошибка при скачивании фото: %s", e)
                await msg.reply_text(photo_placeholder_text(lang))
                try:
                    log_event(user_id, "photo_download_error", meta=str(e))
                except Exception:
                    pass
                return

            user_question = (
                (msg.caption or "").strip()
                or context.chat_data.get("last_user_text")
                or ("Опиши это изображение." if not lang.startswith("uk") else "Опиши це зображення.")
            )

            history_by_topic: Dict[str, List[Dict[str, str]]] = context.chat_data.get("history_by_topic", {})
            history = history_by_topic.get(topic, [])

            # ДО запроса фиксируем, что пришло фото (чтобы контекст не терялся)
            history.append({"role": "user", "content": f"[PHOTO] {user_question}"})
            if len(history) > MAX_HISTORY_MESSAGES:
                history = history[-MAX_HISTORY_MESSAGES:]

            try:
                answer = await ask_gpt_with_image(
                    history=history,
                    lang=lang,
                    image_bytes=image_bytes,
                    user_question=user_question,
                )
            except Exception as e:
                logger.exception("Ошибка при запросе к OpenAI (image): %s", e)
                answer = photo_placeholder_text(lang)
                try:
                    log_event(user_id, "photo_gpt_error", meta=str(e))
                except Exception:
                    pass

            history.append({"role": "assistant", "content": answer})
            if len(history) > MAX_HISTORY_MESSAGES:
                history = history[-MAX_HISTORY_MESSAGES:]

            history_by_topic[topic] = history
            context.chat_data["history_by_topic"] = history_by_topic

            # ✅ подсказка текстовому хендлеру: "медиа только что было"
            context.chat_data["last_media_ts"] = asyncio.get_event_loop().time()
            context.chat_data["last_image_ok"] = True

            try:
                log_event(user_id, "photo")
            except Exception:
                pass

        finally:
            stop_event.set()
            with suppress(Exception):
                await typing_task

    await send_smart_answer(msg, answer)