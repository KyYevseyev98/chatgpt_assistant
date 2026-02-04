# handlers/photo.py

import asyncio
import logging
from typing import Dict, List, Optional, Set, Tuple
from contextlib import suppress

from telegram import Update, Message
from telegram.constants import ChatAction
from telegram.ext import ContextTypes

from config import MAX_HISTORY_MESSAGES, UNLIMITED_USERNAMES
from db import (
    check_limit,
    log_event,
    touch_last_activity,
    set_last_limit_info,
    add_message,
    get_last_messages,
    get_followup_personalization_snapshot,
    set_last_paywall_text,
    should_send_limit_paywall,
    get_tarot_limits_snapshot,
    ensure_billing_defaults,
    update_user_identity,
    is_user_blocked,
)
from localization import (
    get_lang,
    photo_limit_reached,
    photo_placeholder_text,
    multi_photo_not_allowed,
)
from gpt_client import ask_gpt_with_image, generate_limit_paywall_text, generate_limit_paywall_text_via_chat
from long_memory import build_long_memory_block, maybe_update_long_memory
from .common import send_smart_answer, send_typing_action, get_media_lock, trim_history_for_model
from .pro import _pro_keyboard
from .topics import get_current_topic

logger = logging.getLogger(__name__)


def _log_exception(message: str) -> None:
    logger.debug(message, exc_info=True)


async def _send_tarot_paywall(
    msg: Message,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    user_id: int,
    topic: str,
    last_user_message: str,
    lang: str,
) -> None:
    try:
        await context.bot.send_chat_action(chat_id=msg.chat_id, action=ChatAction.TYPING)
    except Exception:
        _log_exception("paywall typing failed")

    paywall = ""
    try:
        prof = get_followup_personalization_snapshot(user_id)
        history = get_last_messages(user_id, msg.chat_id, limit=MAX_HISTORY_MESSAGES) or []
        paywall = await generate_limit_paywall_text(
            lang=lang,
            limit_type="tarot",
            topic=topic,
            last_user_message=last_user_message,
            user_profile=prof,
            history=history,
        )
    except Exception as e:
        logger.warning("paywall generate failed: %s", e, exc_info=True)
        paywall = ""

    if not paywall:
        try:
            paywall = await generate_limit_paywall_text_via_chat(history=history, lang=lang)
        except Exception:
            paywall = ""

    if not paywall:
        paywall = (
            "Похоже, сейчас бесплатная часть уже закончилась.\n\n"
            "Если хочешь, я могу продолжить и сделать глубокий расклад с учётом контекста. "
            "Пакеты раскладов остаются на балансе — можно использовать их в удобное время.\n\n"
            "Готова предложить варианты, чтобы мы шли дальше спокойно и по делу."
        )
    else:
        logger.info("PAYWALL generated len=%s", len(paywall))

    try:
        log_event(user_id, "tarot_paywall", meta="channel:photo", lang=lang, topic=topic)
    except Exception:
        _log_exception("paywall log_event failed")

    await msg.reply_text(paywall.strip(), reply_markup=_pro_keyboard(lang))
    try:
        from handlers.text import _safe_patch_user_profile_chat, _set_tarot_session_mode
        _safe_patch_user_profile_chat(user_id, msg.chat_id, delete_keys=["pending_tarot", "pre_dialog"])
        _set_tarot_session_mode(context, enabled=False)
    except Exception:
        _log_exception("paywall state reset failed")
    try:
        set_last_paywall_text(user_id, paywall)
    except Exception:
        _log_exception("set_last_paywall_text failed")


def _safe_log_event(*args, **kwargs) -> None:
    try:
        log_event(*args, **kwargs)
    except Exception:
        _log_exception("photo: log_event failed")


def _safe_add_messages(user_id: int, chat_id: int, user_text: str, assistant_text: str) -> None:
    try:
        add_message(user_id, chat_id, "user", user_text)
        add_message(user_id, chat_id, "assistant", assistant_text)
    except Exception:
        _log_exception("photo: add_message failed")


# =========================================================
# EXTRACTORS (text/caption + forward/reply context)
# =========================================================

def _safe_msg_text(m: Optional[Message]) -> str:
    if not m:
        return ""
    return ((m.text or m.caption or "") or "").strip()


def _describe_forward_source(m: Message) -> str:
    """
    Короткая подпись источника форварда (без лишней персональной инфы).
    PTB v20+: forward_origin может быть.
    """
    try:
        origin = getattr(m, "forward_origin", None)
        if origin:
            # ForwardOriginChat / ForwardOriginUser / ForwardOriginChannel / ForwardOriginHiddenUser
            otype = origin.__class__.__name__
            # берём только имя/тайтл, если доступно
            name = ""
            if hasattr(origin, "sender_user") and origin.sender_user:
                name = origin.sender_user.first_name or ""
            elif hasattr(origin, "sender_chat") and origin.sender_chat:
                name = origin.sender_chat.title or ""
            elif hasattr(origin, "sender_name") and origin.sender_name:
                name = origin.sender_name or ""
            name = (name or "").strip()
            if name:
                return f"{otype}:{name}"
            return f"{otype}"
    except Exception:
        _log_exception("suppressed exception")

    # legacy fields
    try:
        if getattr(m, "forward_from_chat", None):
            return f"chat:{m.forward_from_chat.title or 'unknown'}"
        if getattr(m, "forward_from", None):
            return f"user:{m.forward_from.first_name or 'unknown'}"
        if getattr(m, "forward_sender_name", None):
            return f"hidden:{m.forward_sender_name}"
    except Exception:
        _log_exception("suppressed exception")

    return "unknown"


def extract_message_text_with_sources_for_image(
    msg: Message,
    *,
    lang: str,
    fallback_last_text: str = "",
) -> Tuple[str, str]:
    """
    Возвращает:
      (clean_text, sources_block)

    clean_text — то, что пойдёт как user_question (главный запрос).
    sources_block — строки вида:
        FORWARDED(from=...): "..."
        REPLY_TO: "..."
    Эти строки добавляем в контекст/историю модели, чтобы она понимала "это".
    """
    # основной текст: caption (для фото) или fallback_last_text
    base = ((msg.caption or "") or "").strip()

    # если caption пустой — используем fallback (последний текст из чата)
    if not base and fallback_last_text:
        base = (fallback_last_text or "").strip()

    # если всё ещё пусто — дефолтный вопрос
    if not base:
        if lang.startswith("uk"):
            base = "Опиши це зображення."
        elif lang.startswith("en"):
            base = "Describe this image."
        else:
            base = "Опиши это изображение."

    # единый экстрактор (ТЗ): reply/forward + USER
    from .common import extract_message_text as _extract
    data = _extract(msg, override_main_text=base)
    parts = data.get("parts") or {}
    # отдельным блоком сохраняем только источники, без USER, чтобы не дублировать вопрос
    sources: List[str] = []
    if parts.get("reply_to"):
        sources.append(f'REPLY_TO: "{str(parts.get("reply_to"))[:700]}"')
    if parts.get("forwarded"):
        sources.append(f'FORWARDED: "{str(parts.get("forwarded"))[:700]}"')
    sources_block = "\n".join(sources).strip()

    # clean_text (главный запрос) — кратко, но оставляем как есть
    clean_text = base.strip()
    if len(clean_text) > 700:
        clean_text = clean_text[:700]

    return clean_text, sources_block


# =========================================================
# HANDLER
# =========================================================

async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle incoming photo/document-image messages."""
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
    username = (getattr(user, "username", "") or "").lower().strip()

    touch_last_activity(user.id)
    try:
        update_user_identity(
            user_id,
            username=getattr(user, "username", None),
            first_name=getattr(user, "first_name", None),
            last_name=getattr(user, "last_name", None),
        )
    except Exception:
        _log_exception("update_user_identity failed")

    if is_user_blocked(user_id):
        await msg.reply_text("Доступ ограничен. Напишите в поддержку.")
        return

    # защита от альбома (для PHOTO)
    if is_photo and msg.media_group_id is not None:
        media_group_id = msg.media_group_id
        handled_groups: Set[str] = context.chat_data.get("handled_media_groups", set())
        if media_group_id in handled_groups:
            return
        handled_groups.add(media_group_id)
        context.chat_data["handled_media_groups"] = handled_groups

        await msg.reply_text(multi_photo_not_allowed(lang))
        _safe_log_event(user_id, "multi_photo_not_allowed")
        return

    topic = get_current_topic(context)

    # ✅ единая логика: caption + forward/reply контекст (для tarot routing)
    fallback_last_text = str(context.chat_data.get("last_user_text") or "").strip()
    user_question, sources_block = extract_message_text_with_sources_for_image(
        msg,
        lang=lang,
        fallback_last_text=fallback_last_text,
    )
    from .common import extract_message_text as _extract
    data = _extract(msg, override_main_text=user_question)
    parts = data.get("parts") or {}
    clean_text = (parts.get("main") or "").strip()
    if not clean_text:
        clean_text = (parts.get("forwarded") or "").strip() or (parts.get("reply_to") or "").strip()
    extracted = (data.get("clean_text") or "").strip()

    # unified tarot routing (shared across text/voice/photo)
    # глобальный стоп: если расклады закончились, отвечаем paywall на любое сообщение
    if username not in UNLIMITED_USERNAMES:
        try:
            ensure_billing_defaults(user_id, msg.chat_id)
        except Exception:
            _log_exception("ensure_billing_defaults failed")
        snap = get_tarot_limits_snapshot(user_id, msg.chat_id)
        logger.info(
            "PAYWALL check user_id=%s chat_id=%s free_left=%s credits=%s",
            user_id,
            msg.chat_id,
            snap.get("tarot_free_lifetime_left"),
            snap.get("tarot_credits"),
        )
        if int(snap.get("tarot_free_lifetime_left") or 0) <= 0 and int(snap.get("tarot_credits") or 0) <= 0:
            await _send_tarot_paywall(
                msg,
                context,
                user_id=user_id,
                topic=topic,
                last_user_message=(msg.caption or context.chat_data.get("last_user_text") or ""),
                lang=lang,
            )
            return

    try:
        from handlers.text import _handle_tarot_routing
        handled = await _handle_tarot_routing(
            update,
            context,
            user_id=user_id,
            lang=lang,
            clean_text=clean_text,
            extracted=extracted,
            topic=topic,
        )
        if handled:
            return
    except Exception:
        _log_exception("photo: tarot routing failed")

    # лимит фото
    if username not in UNLIMITED_USERNAMES and not check_limit(user_id, msg.chat_id, is_photo=True):
        try:
            set_last_limit_info(user_id, topic=topic, limit_type="photo")
        except Exception:
            _log_exception("suppressed exception")

        paywall = ""
        try:
            prof = get_followup_personalization_snapshot(user_id)
            history = get_last_messages(user_id, msg.chat_id, limit=MAX_HISTORY_MESSAGES) or []
            paywall = await generate_limit_paywall_text(
                lang=lang,
                limit_type="text",
                topic=topic,
                last_user_message=(msg.caption or context.chat_data.get("last_user_text") or ""),
                user_profile=prof,
                history=history,
            )
        except Exception:
            paywall = ""

        # антидубль
        try:
            if should_send_limit_paywall(user_id, paywall):
                set_last_paywall_text(user_id, paywall)
            else:
                return
        except Exception:
            _log_exception("suppressed exception")

        if not paywall:
            return
        final_text = paywall.strip()

        await msg.reply_text(
            final_text,
            reply_markup=_pro_keyboard(lang),
        )
        _safe_log_event(user_id, "photo_limit_reached")
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
                _safe_log_event(user_id, "photo_download_error", meta=str(e))
                return

            # сохраняем как последний "текст пользователя" (для последующих фото без caption)
            context.chat_data["last_user_text"] = user_question

            # ТЗ: история — в SQLite messages (user_id+chat_id)
            history = get_last_messages(user_id, msg.chat_id, limit=MAX_HISTORY_MESSAGES) or []

            # ✅ в контекст модели уходит “чистый текст + источник”
            hist_user = f"[PHOTO]\nUSER: {user_question}"
            if sources_block:
                hist_user += "\n" + sources_block
            history_for_model = trim_history_for_model(list(history) + [{"role": "user", "content": hist_user}])
            memory_block = build_long_memory_block(user_id, msg.chat_id, lang=lang)
            if memory_block:
                history_for_model = [{"role": "system", "content": memory_block}] + history_for_model

            try:
                answer = await ask_gpt_with_image(
                    history=history_for_model,
                    lang=lang,
                    image_bytes=image_bytes,
                    user_question=user_question,
                )
            except Exception as e:
                logger.exception("Ошибка при запросе к OpenAI (image): %s", e)
                answer = photo_placeholder_text(lang)
                _safe_log_event(user_id, "photo_gpt_error", meta=str(e))

            # сохраняем в БД (ТЗ)
            _safe_add_messages(user_id, msg.chat_id, hist_user, answer)

            # ✅ подсказка текстовому хендлеру: "медиа только что было"
            context.chat_data["last_media_ts"] = asyncio.get_event_loop().time()
            context.chat_data["last_image_ok"] = True

            # meta полезно для дебага: есть ли sources_block
            meta = "ctx:yes" if sources_block else "ctx:no"
            _safe_log_event(user_id, "photo", meta=meta)

        finally:
            stop_event.set()
            with suppress(Exception):
                await typing_task

    await send_smart_answer(msg, answer)
    try:
        asyncio.create_task(maybe_update_long_memory(user_id, msg.chat_id, lang=lang, topic="photo"))
    except Exception:
        _log_exception("photo: long memory update scheduling failed")
