import asyncio
import logging
import os
import random
import tempfile
import time
import datetime as dt
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional

from PIL import Image
from telegram import InputFile, Message
from telegram.constants import ChatAction
from telegram.error import BadRequest
from telegram.ext import ContextTypes

from config import MAX_HISTORY_MESSAGES, MAX_TAROT_HISTORY_PER_USER, TAROT_SESSION_TTL_SEC, MAX_HISTORY_CHARS, UNLIMITED_USERNAMES
from tarot.spread_image import render_spread
from tarot.deck import get_default_deck
from tarot.router import build_cards_payload, RouteResult

from db import (
    add_message,
    check_tarot_limits,
    get_last_messages,
    get_user_profile_chat,
    patch_user_profile_chat,
    get_followup_personalization_snapshot,
    log_event,
    log_tarot_reading,
    set_last_context,
    set_last_limit_info,
    set_last_paywall_text,
    should_send_limit_paywall,
    add_tarot_history,
    get_last_tarot_history,
    consume_tarot_credit,
    add_tarot_credits,
)
from gpt_client import generate_limit_paywall_text, tarot_intro_post, tarot_reading_answer
from jobs import schedule_limit_followup
from handlers.pro import _pro_keyboard
from config import REFERRAL_REWARD_SPREADS
from long_memory import build_long_memory_block, maybe_update_long_memory
from handlers.common import send_smart_answer, reply_and_mirror, build_profile_system_block

logger = logging.getLogger(__name__)


def _log_exception(message: str) -> None:
    """Log suppressed exceptions at debug level."""
    logger.debug(message, exc_info=True)

# ---- paths (жёстко от файла, чтобы не зависеть от cwd) ----
BASE_DIR = Path(__file__).resolve().parents[1]
ASSETS_DIR = BASE_DIR / "assets"
TABLE_PATH = ASSETS_DIR / "table" / "table.jpg"
TMP_DIR = ASSETS_DIR / "tmp"
TMP_DIR.mkdir(parents=True, exist_ok=True)

SHUFFLE_VIDEO_PATH = ASSETS_DIR / "shuffle" / "shuffle.mp4"
SHUFFLE_SECONDS = 4.0


# ---------------- JPEG helpers ----------------

def _to_telegram_jpeg_bytes(src_path: str, *, max_side: int = 1280, quality: int = 85) -> BytesIO:
    """Read an image, resize if needed, and return Telegram-friendly JPEG bytes."""
    img = Image.open(src_path)
    img.load()
    img = img.convert("RGB")

    if max(img.size) > max_side:
        img.thumbnail((max_side, max_side), Image.LANCZOS)

    bio = BytesIO()
    bio.name = "spread.jpg"
    img.save(bio, format="JPEG", quality=quality, optimize=False, progressive=False, subsampling=2)
    bio.seek(0)
    return bio


def _repack_for_telegram(src_path: str) -> str:
    """Repack a JPEG to improve Telegram compatibility; returns new path."""
    img = Image.open(src_path)
    img.load()
    img = img.convert("RGB")

    max_side = 1280
    if max(img.size) > max_side:
        img.thumbnail((max_side, max_side), Image.LANCZOS)

    fd, out_path = tempfile.mkstemp(prefix="tg_safe_", suffix=".jpg")
    os.close(fd)
    img.save(out_path, "JPEG", quality=85, optimize=False, progressive=False, subsampling=2)
    return out_path


def _positions_for(n_cards: int) -> List[str]:
    """Return spread position labels depending on card count."""
    if n_cards <= 1:
        return ["Главный тон / совет"]
    if n_cards == 2:
        return ["Суть вопроса", "Что важно учесть"]
    if n_cards == 3:
        return ["Ситуация сейчас", "Что влияет/мешает", "Ближайший шаг"]
    if n_cards == 4:
        return ["Суть ситуации", "Скрытый фактор", "Ресурс", "Что делать дальше"]
    if n_cards == 5:
        return ["Ядро ситуации", "Внешние факторы", "Ресурс", "Риск", "Вероятный вектор"]
    if n_cards == 6:
        return [
            "Суть ситуации",
            "Что из прошлого влияет",
            "Скрытый фактор",
            "Ресурс",
            "Риск/препятствие",
            "Ближайший результат",
        ]
    if n_cards >= 7:
        return [
            "Суть ситуации",
            "Что влияет сейчас",
            "Что мешает",
            "Ресурс",
            "На что опереться",
            "Ближайший поворот",
            "Итоговый вектор",
        ]
    return [f"Позиция {i}" for i in range(1, n_cards + 1)]


def _choose_cards_count(question_text: str, spread_name: str) -> int:
    """Choose number of cards based on request type and light heuristics."""
    t = (question_text or "").strip().lower()
    name = (spread_name or "").strip().lower()

    # Hard overrides for single-card formats
    if "карта дня" in t or "карточка дня" in t or "карта дня" in name:
        return 1
    if "да/нет" in t or "да или нет" in t or "да нет" in t or "да/нет" in name:
        return 1
    if any(k in t for k in ("одной картой", "одну карту", "кратко", "быстро")):
        return 1

    # Soft hint: user mentions explicit card count
    if "1 карта" in t or "одна карта" in t:
        base = 1
    elif "2 карты" in t or "две карты" in t:
        base = 2
    elif "3 карты" in t or "три карты" in t:
        base = 3
    elif "4 карты" in t or "четыре карты" in t:
        base = 4
    elif "5 карт" in t or "пять карт" in t:
        base = 5
    elif "6 карт" in t or "шесть карт" in t:
        base = 6
    elif "7 карт" in t or "семь карт" in t:
        base = 7
    else:
        base = 0

    # Theme-based baselines (держим 1–3 по умолчанию)
    if base == 0 and (any(k in t for k in ("отношен", "любов", "пара", "бывш")) or "отношения" in name):
        base = 3
    elif base == 0 and (any(k in t for k in ("деньг", "работ", "карьер", "бизнес", "доход")) or "деньги" in name):
        base = 3
    elif base == 0 and any(k in t for k in ("будуще", "перспектив", "дальше", "что будет")):
        base = 4
    elif base == 0:
        # Length-based baseline (1–3 чаще, 4–5 реже)
        length = len(t)
        if length < 50:
            base = 1
        elif length < 90:
            base = 2
        elif length < 160:
            base = 3
        else:
            base = 4

    # Slightly increase for multi-question/complex requests
    if t.count("?") >= 2 or (" и " in t and len(t) > 80):
        base = min(7, base + 1)

    # Light randomization with strong bias toward 1–3 cards
    if base <= 2:
        options = [1, 1, 2, 2, 3]
    elif base == 3:
        options = [2, 3, 3, 3]
    elif base == 4:
        options = [3, 4, 4, 5]
    elif base == 5:
        options = [4, 5]
    elif base == 6:
        options = [5, 6]
    else:
        options = [6, 7]
    return random.choice(options)


def _cards_caption(cards: List[Any], positions: List[str]) -> str:
    """Build HTML caption with card names and their semantic roles."""
    lines = ["🃏 <b>Карты и роли:</b>"]
    for i, c in enumerate(cards, start=1):
        name = c.meaning.ru_name if getattr(c, "meaning", None) else getattr(c, "key", "Карта")
        pos = positions[i - 1] if i - 1 < len(positions) else f"Позиция {i}"
        lines.append(f"{i}) <b>{name}</b> — {pos}")
    return "\n".join(lines)



async def _send_shuffle_then_delete(context: ContextTypes.DEFAULT_TYPE, chat_id: int, *, seconds: float = 4.0) -> None:
    """Показывает короткую анимацию 'перемешивания' и удаляет сообщение.

    Telegram иногда капризничает с send_animation для mp4 в зависимости от кодека/метаданных.
    Поэтому делаем fallback: animation -> video -> document.
    """
    path = Path(SHUFFLE_VIDEO_PATH)
    if not path.is_file():
        logger.warning("Shuffle video not found: %s", path)
        return

    sent = None
    # 1) try animation
    try:
        with open(path, "rb") as f:
            inp = InputFile(f, filename=path.name)
            sent = await context.bot.send_animation(
                chat_id=chat_id,
                animation=inp,
                supports_streaming=True,
            )
    except Exception as e:
        logger.warning("send_animation failed: %s", e)

    # 2) fallback to video
    if not sent:
        try:
            with open(path, "rb") as f:
                inp = InputFile(f, filename=path.name)
                sent = await context.bot.send_video(
                    chat_id=chat_id,
                    video=inp,
                    supports_streaming=True,
                )
        except Exception as e:
            logger.warning("send_video failed: %s", e)

    # 3) last resort: document
    if not sent:
        try:
            with open(path, "rb") as f:
                inp = InputFile(f, filename=path.name)
                sent = await context.bot.send_document(
                    chat_id=chat_id,
                    document=inp,
                )
        except Exception as e:
            logger.warning("send_document failed: %s", e)
            return

    try:
        await asyncio.sleep(max(0.5, float(seconds)))
    except Exception:
        return

    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=sent.message_id)
    except Exception as e:
        logger.debug("Could not delete shuffle message: %s", e)


def _cleanup_tmp_dir(tmp_dir: Path, *, max_files: int = 200, max_age_hours: int = 24) -> None:
    """Remove old temp images to avoid disk growth."""
    try:
        files = [p for p in tmp_dir.glob("*.jpg") if p.is_file()]
        # удаляем по возрасту
        cutoff = (asyncio.get_event_loop().time() if asyncio.get_event_loop().is_running() else None)
    except Exception:
        files = []

    try:
        import time
        now = time.time()
        cutoff_ts = now - max_age_hours * 3600
        for p in files:
            try:
                if p.stat().st_mtime < cutoff_ts:
                    p.unlink(missing_ok=True)
            except Exception:
                _log_exception("suppressed exception")

        # ограничение по кол-ву
        files = [p for p in tmp_dir.glob("*.jpg") if p.is_file()]
        files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        for p in files[max_files:]:
            try:
                p.unlink(missing_ok=True)
            except Exception:
                _log_exception("suppressed exception")
    except Exception:
        _log_exception("suppressed exception")


def _build_history_hint(user_id: int, chat_id: int, *, lang: str = "ru") -> str:
    """Компактный контекст: последние сообщения + краткая память по раскладам."""
    parts: List[str] = []

    try:
        mem_block = build_long_memory_block(user_id, chat_id, lang=lang)
        if mem_block:
            parts.append(mem_block)
    except Exception:
        _log_exception("suppressed exception")

    try:
        hist: List[Dict[str, str]] = get_last_messages(user_id, chat_id, limit=MAX_HISTORY_MESSAGES) or []
        if hist:
            # 50 сообщений максимум, но обрежем каждое
            lines = []
            for m in hist[-MAX_HISTORY_MESSAGES:]:
                role = m.get("role")
                content = (m.get("content") or "").strip()
                if not content:
                    continue
                content = content.replace("\n", " ")
                if len(content) > 220:
                    content = content[:220] + "…"
                lines.append(f"{role}: {content}")
            if lines:
                parts.append("Диалог (сжатый контекст):\n" + "\n".join(lines))
    except Exception:
        _log_exception("suppressed exception")

    try:
        tarot_hist = get_last_tarot_history(user_id, chat_id, limit=min(10, MAX_TAROT_HISTORY_PER_USER)) or []
        if tarot_hist:
            tlines = []
            for r in tarot_hist:
                q = (r.get("question") or "").strip().replace("\n", " ")
                sp = (r.get("spread_name") or "").strip()
                ex = (r.get("answer_excerpt") or "").strip().replace("\n", " ")
                if len(q) > 160:
                    q = q[:160] + "…"
                if len(ex) > 220:
                    ex = ex[:220] + "…"
                tlines.append(f"- {sp}: {q} | {ex}")
            parts.append("Память по прошлым раскладам (кратко):\n" + "\n".join(tlines))
    except Exception:
        _log_exception("suppressed exception")

    limit = min(3000, int(MAX_HISTORY_CHARS or 3000))
    return "\n\n".join(parts)[:limit]


async def run_tarot_reading_full(
    msg: Message,
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    question_text: str,
    route: RouteResult,
    *,
    lang: str = "ru",
) -> None:
    """ЕДИНАЯ функция расклада: пост-ответ -> анимация -> фото доски -> список карт -> трактовка.

    Никаких альтернативных путей, которые могут пропустить этапы.
    """

    # mark tarot session active for continuity UX
    context.chat_data["tarot_mode"] = True
    context.chat_data["tarot_mode_until"] = time.time() + float(TAROT_SESSION_TTL_SEC)

    # лимиты
    username = (getattr(msg.from_user, "username", "") or "").lower().strip()
    can_do = True
    reason_text = ""
    if username not in UNLIMITED_USERNAMES:
        can_do, reason_text = check_tarot_limits(user_id, msg.chat_id)
    if not can_do:
        try:
            set_last_limit_info(user_id, topic="tarot", limit_type="tarot")
        except Exception:
            _log_exception("suppressed exception")

        paywall = ""
        try:
            prof = get_followup_personalization_snapshot(user_id)
            history = get_last_messages(user_id, msg.chat_id, limit=MAX_HISTORY_MESSAGES) or []
            paywall = await generate_limit_paywall_text(
                lang=lang,
                limit_type="tarot",
                topic="tarot",
                last_user_message=question_text,
                user_profile=prof,
                history=history,
                context_hint=history_hint,
            )
        except Exception:
            paywall = ""

        try:
            if paywall and not should_send_limit_paywall(user_id, paywall):
                return
        except Exception:
            _log_exception("suppressed exception")

        final_text = (paywall or "Чтобы продолжить, можно купить расклады.").strip()
        try:
            log_event(user_id, "tarot_paywall", meta="channel:tarot_flow", lang=lang, topic="tarot")
        except Exception:
            _log_exception("paywall log_event failed")
        await reply_and_mirror(msg, final_text, reply_markup=_pro_keyboard(lang))
        try:
            if paywall:
                set_last_paywall_text(user_id, paywall)
        except Exception:
            _log_exception("suppressed exception")
        try:
            schedule_limit_followup(context.application, user_id, lang)
        except Exception:
            _log_exception("suppressed exception")
        return

    # deck
    try:
        deck = get_default_deck()
    except Exception as e:
        logger.exception("Deck init failed: %s", e)
        await reply_and_mirror(msg, "Не могу загрузить колоду (assets/cards). Проверь, что папка и 78 файлов карт на месте.")
        return

    spread_name = (getattr(route, "spread_name", "") or "").strip()
    n_cards = _choose_cards_count(question_text, spread_name)
    if not spread_name:
        spread_name = f"{n_cards} карт"

    # --- 0) живой пост-ответ (Arcana-стиль) ---
    history_hint = _build_history_hint(user_id, msg.chat_id, lang=lang)

    try:
        await context.bot.send_chat_action(chat_id=msg.chat_id, action=ChatAction.TYPING)
    except Exception:
        _log_exception("suppressed exception")

    intro = None
    try:
        history = get_last_messages(user_id, msg.chat_id, limit=MAX_HISTORY_MESSAGES) or []
        try:
            prof = get_user_profile_chat(user_id, msg.chat_id) or {}
            prof_block = build_profile_system_block(prof)
            if prof_block:
                history = [prof_block] + history
        except Exception:
            _log_exception("profile block failed")
        intro = await tarot_intro_post(
            lang=lang,
            user_question=question_text,
            spread_name=spread_name,
            n_cards=n_cards,
            history_hint=history_hint,
            history=history,
        )
    except Exception:
        intro = None

    # ТЗ: пост-ответ должен быть ВСЕГДА. Если GPT не дал интро, используем безопасный шаблон.
    if not intro:
        intro = (
            f"🔮 <b>{spread_name}</b>\n"
            f"Вопрос: { (question_text or '').strip()[:700] }\n\n"
            "Сейчас перемешаю колоду и покажу расклад."
        )

    try:
        await reply_and_mirror(msg, intro, parse_mode="HTML")
    except Exception:
        try:
            await reply_and_mirror(msg, intro)
        except Exception:
            _log_exception("suppressed exception")

    # --- 1) тянем карты ---
    cards = deck.draw(n_cards)
    if not cards:
        await reply_and_mirror(msg, "Не удалось вытянуть карты. Проверь колоду (assets/cards).")
        return

    positions = _positions_for(len(cards))

    # --- 2) рендер доски ---
    card_paths = [deck.abs_path(c.filename) for c in cards]
    out_path = str(TMP_DIR / f"spread_{user_id}_{msg.message_id}.jpg")

    try:
        render_spread(str(TABLE_PATH), card_paths, out_path)
    except Exception:
        logger.exception("spread render failed; fallback")
        try:
            imgs = []
            for cp in card_paths:
                try:
                    im = Image.open(cp).convert("RGB")
                    imgs.append(im)
                except Exception:
                    _log_exception("suppressed exception")
            if not imgs:
                Image.new("RGB", (1024, 1024), (15, 15, 18)).save(out_path, "JPEG", quality=92)
            else:
                W, H = 1024, 1024
                canvas = Image.new("RGB", (W, H), (15, 15, 18))
                n = len(imgs)
                cols = 3 if n >= 3 else n
                rows = (n + cols - 1) // cols
                pad = 18
                slot_w = (W - pad * (cols + 1)) // cols
                slot_h = (H - pad * (rows + 1)) // rows
                for i, im in enumerate(imgs[:7]):
                    r = i // cols
                    c = i % cols
                    x0 = pad + c * (slot_w + pad)
                    y0 = pad + r * (slot_h + pad)
                    im2 = im.copy()
                    im2.thumbnail((slot_w, slot_h))
                    canvas.paste(im2, (x0 + (slot_w - im2.width)//2, y0 + (slot_h - im2.height)//2))
                canvas.save(out_path, "JPEG", quality=92)
        except Exception:
            out_path = ""

    # --- 3) shuffle (перед показом карт) ---
    try:
        await _send_shuffle_then_delete(context, msg.chat_id, seconds=SHUFFLE_SECONDS)
    except Exception:
        _log_exception("suppressed exception")

    # --- 4) показываем карту-доску (всегда пытаемся) ---
    sent_spread_ok = False
    safe_path = None

    if out_path and os.path.exists(out_path):
        try:
            await context.bot.send_chat_action(chat_id=msg.chat_id, action=ChatAction.UPLOAD_PHOTO)
        except Exception:
            _log_exception("suppressed exception")

        try:
            bio = _to_telegram_jpeg_bytes(out_path)
            await context.bot.send_photo(chat_id=msg.chat_id, photo=InputFile(bio))
            sent_spread_ok = True
        except BadRequest as e:
            if "Image_process_failed" in str(e):
                try:
                    safe_path = _repack_for_telegram(out_path)
                    await context.bot.send_photo(chat_id=msg.chat_id, photo=InputFile(safe_path))
                    sent_spread_ok = True
                except Exception:
                    _log_exception("suppressed exception")
            else:
                logger.exception("send_photo BadRequest")
        except Exception:
            logger.exception("send_photo failed")

    # --- 5) ВСЕГДА отправляем текст с картами/позициями (железно) ---
    try:
        await context.bot.send_message(chat_id=msg.chat_id, text=_cards_caption(cards, positions), parse_mode="HTML")
    except Exception:
        # fallback без HTML
        try:
            plain = ["Выпали карты:"]
            for i, c in enumerate(cards, start=1):
                name = c.meaning.ru_name if getattr(c, "meaning", None) else getattr(c, "key", "Карта")
                pos = positions[i - 1] if i - 1 < len(positions) else f"Позиция {i}"
                plain.append(f"{i}) {name} — {pos}")
            await context.bot.send_message(chat_id=msg.chat_id, text="\n".join(plain))
        except Exception:
            _log_exception("suppressed exception")

    # --- 6) GPT трактовка (Arcana-стиль только тут) ---
    cards_payload = build_cards_payload(cards)

    try:
        await context.bot.send_chat_action(chat_id=msg.chat_id, action=ChatAction.TYPING)
    except Exception:
        _log_exception("suppressed exception")

    history = get_last_messages(user_id, msg.chat_id, limit=MAX_HISTORY_MESSAGES) or []
    try:
        prof = get_user_profile_chat(user_id, msg.chat_id) or {}
        prof_block = build_profile_system_block(prof)
        if prof_block:
            history = [prof_block] + history
    except Exception:
        _log_exception("profile block failed")
    answer = await tarot_reading_answer(
        lang=lang,
        user_question=question_text,
        spread_name=spread_name,
        cards_payload=cards_payload,
        history_hint=history_hint,
        history=history,
    )

    await send_smart_answer(msg, answer)

    try:
        consume_tarot_credit(user_id, msg.chat_id)
    except Exception:
        _log_exception("suppressed exception")

    # referral reward: if this user came by referral and made 1st reading
    try:
        prof = get_user_profile_chat(user_id, msg.chat_id) or {}
        ref = prof.get("referral") or {}
        inviter_id = ref.get("inviter_id")
        credited = bool(ref.get("credited"))
        if inviter_id and not credited and int(inviter_id) != int(user_id):
            add_tarot_credits(int(inviter_id), int(inviter_id), int(REFERRAL_REWARD_SPREADS))
            try:
                log_event(int(inviter_id), "ref_reward", meta=f"spreads:{REFERRAL_REWARD_SPREADS};ref_user:{user_id}")
            except Exception:
                _log_exception("referral reward log_event failed")
            ref["credited"] = True
            ref["credited_at"] = dt.datetime.utcnow().isoformat()
            patch_user_profile_chat(user_id, msg.chat_id, patch={"referral": ref})
            try:
                await context.bot.send_message(
                    chat_id=int(inviter_id),
                    text=f"Поздравляем! Вам зачислено {REFERRAL_REWARD_SPREADS} расклада(ов) за приглашённого друга.",
                )
                # сбрасываем возможные ожидания, чтобы "супер" не запускал расклад
                try:
                    from handlers.text import _safe_patch_user_profile_chat, _set_tarot_session_mode
                    _safe_patch_user_profile_chat(int(inviter_id), int(inviter_id), delete_keys=["pending_tarot", "pre_dialog"])
                    _set_tarot_session_mode(context, enabled=False)
                except Exception:
                    _log_exception("referral state reset failed")
            except Exception:
                _log_exception("referral notify failed")
    except Exception:
        _log_exception("suppressed exception")

    # --- 7) сохраняем историю: messages + tarot_history (обрезки) ---
    try:
        add_message(user_id, msg.chat_id, "user", question_text)
        add_message(user_id, msg.chat_id, "assistant", answer)
    except Exception:
        _log_exception("suppressed exception")

    try:
        cards_meta = [{"key": c.key, "name": (c.meaning.ru_name if c.meaning else c.key), "file": c.filename} for c in cards]
        excerpt = (answer or "").strip()
        if len(excerpt) > 800:
            excerpt = excerpt[:800] + "…"
        add_tarot_history(user_id, msg.chat_id, question_text, spread_name, cards_meta, excerpt)
    except Exception:
        _log_exception("suppressed exception")

    try:
        cards_meta = [{"key": c.key, "name": (c.meaning.ru_name if c.meaning else c.key), "file": c.filename} for c in cards]
        log_tarot_reading(user_id, question=question_text, spread_name=spread_name, cards_meta=cards_meta, lang=lang)
    except Exception:
        _log_exception("suppressed exception")

    try:
        set_last_context(user_id, topic="tarot", last_user_message=question_text, last_bot_message=answer)
    except Exception:
        _log_exception("suppressed exception")

    try:
        log_event(user_id, "tarot", lang=lang, topic="tarot", meta=f"cards:{len(cards)};spread:{spread_name}")
    except Exception:
        _log_exception("suppressed exception")

    try:
        asyncio.create_task(maybe_update_long_memory(user_id, msg.chat_id, lang=lang, topic="tarot"))
    except Exception:
        _log_exception("long memory update scheduling failed")

    # --- 8) уборка мусора ---
    try:
        if safe_path and os.path.exists(safe_path):
            os.remove(safe_path)
    except Exception:
        _log_exception("suppressed exception")

    try:
        if out_path and os.path.exists(out_path):
            os.remove(out_path)
    except Exception:
        _log_exception("suppressed exception")

    try:
        _cleanup_tmp_dir(TMP_DIR)
    except Exception:
        _log_exception("suppressed exception")
