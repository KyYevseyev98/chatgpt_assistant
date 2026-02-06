# handlers/voice.py

import asyncio
import logging
from contextlib import suppress
from typing import Dict, List

from telegram import Update, Message
from telegram.ext import ContextTypes
from telegram.constants import ChatAction

from config import MAX_HISTORY_MESSAGES, UNLIMITED_USERNAMES
from localization import get_lang, text_limit_reached
from gpt_client import ask_gpt, transcribe_voice, generate_limit_paywall_text, generate_limit_paywall_text_via_chat
from db import (
    check_limit,
    log_event,
    touch_last_activity,
    set_last_limit_info,
    get_followup_personalization_snapshot,
    set_last_paywall_text,
    should_send_limit_paywall,
    get_tarot_limits_snapshot,
    ensure_billing_defaults,
    # ✅ если у тебя уже добавлено в db.py — будет использовано
    set_last_context,
    add_message,
    get_last_messages,
    update_user_identity,
    is_user_blocked,
)
from .common import send_smart_answer, reply_and_mirror, send_typing_action, get_media_lock, trim_history_for_model, build_profile_system_block
from admin_forum import mirror_user_message
from .pro import _pro_keyboard
from .topics import get_current_topic
from long_memory import build_long_memory_block, maybe_update_long_memory

logger = logging.getLogger(__name__)

History = List[Dict[str, str]]


def _log_exception(message: str) -> None:
    """Log suppressed exceptions at debug level."""
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
        log_event(user_id, "tarot_paywall", meta="channel:voice", lang=lang, topic=topic)
    except Exception:
        _log_exception("paywall log_event failed")

    await reply_and_mirror(msg, paywall.strip(), reply_markup=_pro_keyboard(lang))
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


def _fallback_error_text(lang: str) -> str:
    """Localized fallback error response for voice handler."""
    if lang.startswith("uk"):
        return "Сталася помилка. Спробуй пізніше."
    if lang.startswith("en"):
        return "An error occurred. Try again later."
    return "Произошла ошибка. Попробуйте позже."


def _extract_reply_text(msg: Message, limit: int = 700) -> str:
    """
    Достаём текст из reply_to_message (текст/подпись). Нужен для кейса:
    'а вот это что значит?' в ответе на сообщение.
    """
    rep = msg.reply_to_message
    if not rep:
        return ""
    txt = (rep.text or rep.caption or "").strip()
    if not txt:
        return ""
    return txt[:limit]


def _extract_forward_info(msg: Message, limit: int = 700) -> str:
    """
    Достаём контент/метаданные пересылки.
    Для voice обычно текста нет, но важно хотя бы отметить источник и,
    если вдруг есть caption/text (бывает при пересланном сообщении с текстом),
    — захватить его.
    """
    # PTB v20+: forward_origin; старые поля тоже оставим на всякий
    origin = getattr(msg, "forward_origin", None)
    fwd_from = getattr(msg, "forward_from", None)
    fwd_name = getattr(msg, "forward_sender_name", None)

    # Текст у голосового обычно отсутствует, но вдруг это не чистый voice
    fwd_text = (getattr(msg, "text", None) or getattr(msg, "caption", None) or "").strip()
    if fwd_text:
        return fwd_text[:limit]

    # Если есть явный origin — оставим короткую подпись "кто переслал"
    if origin:
        try:
            otype = getattr(origin, "type", None)
            if otype == "user":
                u = getattr(origin, "sender_user", None)
                if u:
                    name = " ".join([x for x in [u.first_name, u.last_name] if x]) or (u.username or "user")
                    return f"(переслано от: {name})"
            if otype == "channel":
                ch = getattr(origin, "chat", None)
                if ch and getattr(ch, "title", None):
                    return f"(переслано из канала: {ch.title})"
            if otype == "chat":
                ch = getattr(origin, "sender_chat", None)
                if ch and getattr(ch, "title", None):
                    return f"(переслано из чата: {ch.title})"
            if otype:
                return f"(переслано: {otype})"
        except Exception:
            _log_exception("extract_forward_info: forward_origin parse failed")

    if fwd_from:
        try:
            name = " ".join([x for x in [fwd_from.first_name, fwd_from.last_name] if x]) or (fwd_from.username or "user")
            return f"(переслано от: {name})"
        except Exception:
            _log_exception("extract_forward_info: forward_from parse failed")
            return "(переслано)"

    if fwd_name:
        return f"(переслано от: {str(fwd_name)[:60]})"

    return ""


def _build_user_text_with_sources(msg: Message, transcribed_text: str) -> str:
    """
    Формируем "чистый текст + источник":
      FORWARDED: ...
      REPLY_TO: ...
      USER: ...
    """
    parts: List[str] = []

    fwd = _extract_forward_info(msg)
    if fwd:
        parts.append(f"FORWARDED: {fwd}")

    rep = _extract_reply_text(msg)
    if rep:
        parts.append(f"REPLY_TO: {rep}")

    base = (transcribed_text or "").strip()
    if base:
        parts.append(f"USER: {base}")
    else:
        parts.append("USER: (voice message)")

    return "\n".join(parts).strip()


async def handle_voice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle incoming voice messages and generate a chat response."""
    msg = update.message
    if not msg or not msg.voice:
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
        await reply_and_mirror(msg, "Доступ ограничен. Напишите в поддержку.")
        return
    topic = get_current_topic(context)

    # ----------------- MEDIA LOCK -----------------
    media_lock = get_media_lock(context)
    answer = None  # чтобы не было UnboundLocalError

    async with media_lock:
        stop_event = asyncio.Event()
        typing_task = asyncio.create_task(send_typing_action(context.bot, msg.chat_id, stop_event))

        try:
            # 1) download voice bytes
            voice = msg.voice
            file = await voice.get_file()
            bio = await file.download_as_bytearray()
            voice_bytes = bytes(bio)

            # 2) transcribe
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

            # 3) единый экстрактор (ТЗ): voice transcript + reply/forward
            from .common import extract_message_text as _extract
            data = _extract(msg, override_main_text=transcribed_text)
            enriched_text = (data.get("clean_text") or "").strip() or (transcribed_text or "").strip()
            parts = data.get("parts") or {}
            clean_text = (parts.get("main") or "").strip()
            if not clean_text:
                clean_text = (parts.get("forwarded") or "").strip() or (parts.get("reply_to") or "").strip()

            # сохраняем как последний текст пользователя (для подписи к фото и т.п.)
            context.chat_data["last_user_text"] = (transcribed_text or "").strip()

            try:
                await mirror_user_message(context.bot, msg, enriched_text or transcribed_text)
            except Exception:
                _log_exception("admin_forum mirror user failed")

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
                        last_user_message="(voice message)",
                        lang=lang,
                    )
                    return

            # unified tarot routing (shared across text/voice/photo)
            try:
                from handlers.text import _handle_tarot_routing
                handled = await _handle_tarot_routing(
                    update,
                    context,
                    user_id=user_id,
                    lang=lang,
                    clean_text=clean_text,
                    extracted=enriched_text,
                    topic=topic,
                )
                if handled:
                    return
            except Exception:
                _log_exception("voice: tarot routing failed")

            # ----------------- LIMITS (voice chat) -----------------
            if username not in UNLIMITED_USERNAMES and not check_limit(user_id, msg.chat_id, is_photo=False):
                try:
                    set_last_limit_info(user_id, topic=topic, limit_type="voice")
                except Exception:
                    _log_exception("voice: set_last_limit_info failed")

                paywall = ""
                try:
                    prof = get_followup_personalization_snapshot(user_id)
                    history = get_last_messages(user_id, msg.chat_id, limit=MAX_HISTORY_MESSAGES) or []
                    paywall = await generate_limit_paywall_text(
                        lang=lang,
                        limit_type="text",
                        topic=topic,
                        last_user_message="(voice message)",
                        user_profile=prof,
                        history=history,
                    )
                except Exception:
                    paywall = ""

                try:
                    if should_send_limit_paywall(user_id, paywall):
                        set_last_paywall_text(user_id, paywall)
                    else:
                        return
                except Exception:
                    _log_exception("voice: should_send_limit_paywall failed")

                if not paywall:
                    return
                final_text = paywall.strip()

                await reply_and_mirror(
                    final_text,
                    reply_markup=_pro_keyboard(lang),
                )
                try:
                    log_event(user_id, "voice_limit_reached")
                except Exception:
                    _log_exception("voice: log_event failed")
                return

            # 4) история — в SQLite messages (user_id+chat_id)
            history: History = get_last_messages(user_id, msg.chat_id, limit=MAX_HISTORY_MESSAGES) or []
            hist_user = f"[VOICE]\n{enriched_text}"
            history_for_model = trim_history_for_model(list(history) + [{"role": "user", "content": hist_user}])
            memory_block = build_long_memory_block(user_id, msg.chat_id, lang=lang)
            if memory_block:
                history_for_model = [{"role": "system", "content": memory_block}] + history_for_model
            try:
                prof = get_user_profile_chat(user_id, msg.chat_id) or {}
                prof_block = build_profile_system_block(prof)
                if prof_block:
                    history_for_model = [prof_block] + history_for_model
            except Exception:
                _log_exception("profile block failed")

            # 5) ask gpt
            try:
                answer = await ask_gpt(history_for_model, lang)
            except Exception as e:
                logger.exception("Ошибка при запросе к OpenAI (voice->text): %s", e)
                answer = _fallback_error_text(lang)

            # сохраняем в БД (ТЗ)
            try:
                add_message(user_id, msg.chat_id, "user", hist_user)
                add_message(user_id, msg.chat_id, "assistant", answer)
            except Exception:
                _log_exception("voice: add_message failed")

            # 6) memory snapshot for follow-ups / personalization
            try:
                set_last_context(
                    user_id,
                    topic=topic,
                    last_user_message=enriched_text,
                    last_bot_message=answer,
                )
            except Exception:
                _log_exception("voice: set_last_context failed")

            # 7) log
            try:
                log_event(user_id, "voice", tokens=len(enriched_text) if enriched_text else None, lang=lang, topic=topic)
            except Exception:
                _log_exception("voice: log_event failed")

        finally:
            stop_event.set()
            with suppress(Exception):
                await typing_task

    if answer is None:
        # на всякий случай
        answer = _fallback_error_text(lang)

    await send_smart_answer(msg, answer)
    try:
        asyncio.create_task(maybe_update_long_memory(user_id, msg.chat_id, lang=lang, topic=topic))
    except Exception:
        _log_exception("voice: long memory update scheduling failed")
