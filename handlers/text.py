# handlers/text.py
import asyncio
import datetime as dt
import logging
import datetime as dt
import os
import re
import tempfile
import time
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from PIL import Image
from telegram import (
    Update,
    Message,
    InputFile,
)
from telegram.ext import ContextTypes
from telegram.constants import ChatAction
from telegram.error import BadRequest

from tarot.deck import get_default_deck
from tarot.router import RouteResult, build_cards_payload, normalize_route
from tarot.spread_image import render_spread

from config import MAX_HISTORY_MESSAGES, TAROT_SESSION_TTL_SEC, PRE_DIALOG_TTL_SEC, PRE_DIALOG_MAX_QUESTIONS
from db import (
    check_limit,
    log_event,
    set_traffic_source,
    touch_last_activity,
    set_last_context,
    set_last_limit_info,
    get_followup_personalization_snapshot,
    get_user_memory_snapshot,
    set_last_followup_meta,
    set_last_followup_context,
    touch_last_followup_at,
    set_last_paywall_text,
    should_send_limit_paywall,
    # TAROT
    check_tarot_limits,
    get_tarot_limits_snapshot,
    log_tarot_reading,
    add_message,
    get_last_messages,
    update_user_profile_chat_if_new_facts,
    get_user_profile_chat,
    patch_user_profile_chat,
    update_user_identity,
    is_user_blocked,
)
from db_layer.billing import ensure_billing_defaults
from localization import (
    get_lang,
    reset_text,
    forbidden_reply,
    text_limit_reached,
)
from gpt_client import (
    is_forbidden_topic,
    ask_gpt,
    classify_tarot_intent,
    generate_clarify_question,
    generate_limit_paywall_text,
    generate_limit_paywall_text_via_chat,
    # TAROT router + answer
    route_tarot_action,
    tarot_reading_answer,
)
from jobs import schedule_limit_followup

from .common import send_smart_answer, reply_and_mirror, wait_for_media_if_needed, trim_history_for_model, build_profile_system_block
from admin_forum import mirror_user_message
from .pro import _pro_keyboard
from .tarot_flow import run_tarot_reading_full
from .topics import get_current_topic
from long_memory import build_long_memory_block, maybe_update_long_memory
from config import UNLIMITED_USERNAMES


logger = logging.getLogger(__name__)


def _log_exception(message: str) -> None:
    logger.debug(message, exc_info=True)


def _safe_get_last_messages(user_id: int, chat_id: int, *, limit: int) -> List[Dict[str, str]]:
    try:
        return get_last_messages(user_id, chat_id, limit=limit) or []
    except Exception:
        _log_exception("get_last_messages failed")
        return []


def _safe_add_user_and_assistant_messages(
    user_id: int,
    chat_id: int,
    user_text: str,
    assistant_text: str,
) -> None:
    try:
        add_message(user_id, chat_id, "user", user_text)
        add_message(user_id, chat_id, "assistant", assistant_text)
    except Exception:
        _log_exception("add_message failed")


def _safe_set_last_context(
    user_id: int,
    *,
    topic: Optional[str],
    last_user_message: Optional[str],
    last_bot_message: Optional[str],
) -> None:
    try:
        set_last_context(
            user_id,
            topic=topic,
            last_user_message=last_user_message,
            last_bot_message=last_bot_message,
        )
    except Exception:
        _log_exception("set_last_context failed")


def _safe_log_event(*args: Any, **kwargs: Any) -> None:
    try:
        log_event(*args, **kwargs)
    except Exception:
        _log_exception("log_event failed")


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
        history = _safe_get_last_messages(user_id, msg.chat_id, limit=MAX_HISTORY_MESSAGES)
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
        logger.warning("PAYWALL empty user_id=%s chat_id=%s", user_id, msg.chat_id)
        paywall = (
            "Похоже, сейчас бесплатная часть уже закончилась.\n\n"
            "Если хочешь, я могу продолжить и сделать глубокий расклад с учётом контекста. "
            "Пакеты раскладов остаются на балансе — можно использовать их в удобное время.\n\n"
            "Готова предложить варианты, чтобы мы шли дальше спокойно и по делу."
        )
    else:
        logger.info("PAYWALL generated len=%s", len(paywall))

    try:
        _safe_log_event(user_id, "tarot_paywall", meta="channel:text", lang=lang, topic=topic)
    except Exception:
        _log_exception("paywall log_event failed")

    await reply_and_mirror(msg, paywall.strip(), reply_markup=_pro_keyboard(lang))
    try:
        _safe_patch_user_profile_chat(user_id, msg.chat_id, delete_keys=["pending_tarot", "pre_dialog"])
        _set_tarot_session_mode(context, enabled=False)
    except Exception:
        _log_exception("paywall state reset failed")
    try:
        set_last_paywall_text(user_id, paywall)
    except Exception:
        _log_exception("set_last_paywall_text failed")


def _safe_patch_user_profile_chat(
    user_id: int,
    chat_id: int,
    *,
    patch: Optional[Dict[str, Any]] = None,
    delete_keys: Optional[List[str]] = None,
) -> None:
    try:
        patch_user_profile_chat(user_id, chat_id, patch=patch, delete_keys=delete_keys)
    except Exception:
        _log_exception("patch_user_profile_chat failed")


async def handle_tarot_flow(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    route: RouteResult,
    question_text: str,
    *,
    lang: str = "ru",
) -> None:
    """Единая точка входа в tarot flow.

    Нужна, чтобы продолжение после уточняющего вопроса ВСЕГДА шло через
    run_tarot_reading_full (пост-ответ -> анимация -> фото -> трактовка).
    """
    msg = update.effective_message
    user = update.effective_user
    if not msg or not user:
        return

    # after clarification we consider the tarot flow explicitly requested
    context.user_data["astra_mode_armed"] = False

    await run_tarot_reading_full(msg, context, user.id, question_text, route, lang=lang)

BATCH_DELAY_SEC = 0.4

# ---- paths (жёстко от файла, чтобы не зависеть от cwd) ----
BASE_DIR = Path(__file__).resolve().parents[1]  # project root (рядом с assets/)
ASSETS_DIR = BASE_DIR / "assets"
TABLE_PATH = ASSETS_DIR / "table" / "table.jpg"
TMP_DIR = Path(tempfile.gettempdir()) / "astra_tmp"
TMP_DIR.mkdir(parents=True, exist_ok=True)
TMP_TTL_SEC = 6 * 60 * 60  # 6 hours
TMP_MAX_FILES = 300  # hard cap to avoid disk bloat

def _cleanup_tmp_files(dir_path: Path, *, ttl_sec: int = TMP_TTL_SEC, max_files: int = TMP_MAX_FILES) -> None:
    """Delete old tarot render files in tmp dir to prevent disk growth."""
    try:
        if not dir_path or not dir_path.exists():
            return
        now = time.time()
        # 1) remove old files
        for p in dir_path.glob('*'):
            try:
                if not p.is_file():
                    continue
                age = now - float(p.stat().st_mtime)
                if age > ttl_sec:
                    p.unlink(missing_ok=True)
            except Exception:
                _log_exception("suppressed exception")
        # 2) hard cap by oldest
        files = [p for p in dir_path.glob('*') if p.is_file()]
        if len(files) > max_files:
            files.sort(key=lambda x: x.stat().st_mtime)
            for p in files[: max(0, len(files) - max_files)]:
                try:
                    p.unlink(missing_ok=True)
                except Exception:
                    _log_exception("suppressed exception")
    except Exception:
        _log_exception("suppressed exception")

SHUFFLE_VIDEO_PATH = ASSETS_DIR / "shuffle" / "shuffle.mp4"
SHUFFLE_SECONDS = 4.0


# ---------------- MESSAGE EXTRACTION (единый текст + источники) ----------------

_FILLER_WORDS = {
    "ну", "слушай", "слухай", "ась", "ээ", "эм", "мм",
    "короче", "типа", "значит", "вообще", "в общем",
    "пожалуйста", "плиз", "как бы",
}


def _normalize_for_intent(text: str) -> str:
    """
    Normalize text for intent detection:
    - lowercase
    - remove filler words
    - collapse repeats and punctuation noise
    """
    t = (text or "").lower()
    t = re.sub(r"[^a-zа-яё0-9]+", " ", t, flags=re.IGNORECASE)
    t = re.sub(r"\s+", " ", t).strip()
    if not t:
        return ""
    tokens = [tok for tok in t.split() if tok and tok not in _FILLER_WORDS]
    if not tokens:
        return ""
    # collapse consecutive repeats
    out = [tokens[0]]
    for tok in tokens[1:]:
        if tok != out[-1]:
            out.append(tok)
    return " ".join(out).strip()


FOLLOWUP_TRIGGERS = (
    "подробнее", "расскажи подробнее", "поясни", "поясни подробнее", "углуби",
    "расшифруй", "расшифруй это", "продолжай", "продолжи",
    "что это значит", "что значит", "а это", "а вот это", "и что дальше",
)

# Явные триггеры на новый расклад (ТЗ: анти-галлюцинации интента)
TAROT_TRIGGERS = (
    "сделай расклад", "хочу расклад", "расклад", "сделай таро", "хочу таро", "таро",
    "по таро", "по картам", "по картам таро", "узнай у карт", "узнать у карт", "узнай по картам",
    "вытяни карту", "вытащи карту", "тяни карту", "достань карту",
    "что говорят карты", "что скажут карты", "карты скажут", "карты таро",
    "погадай", "погадай мне", "гадание", "карта дня", "карточка дня",
    "да/нет", "да нет", "да или нет", "ответ да или нет", "сделай да/нет", "сделай да или нет",
    "на любовь", "на отношения", "на неделю", "на месяц",
    "кто обо мне думает", "кто обо мне думает?",
)

TAROT_TRIGGERS_NORM = tuple(
    sorted({t for t in (_normalize_for_intent(x) for x in TAROT_TRIGGERS) if t})
)
def _strip_fake_shuffle(text: str) -> str:
    if not text:
        return text
    text = re.sub(r"^\s*\(.*вытягиваю.*карты.*\)\s*$", "", text, flags=re.IGNORECASE | re.MULTILINE)
    text = re.sub(r"^\s*\(.*тасую.*колоду.*\)\s*$", "", text, flags=re.IGNORECASE | re.MULTILINE)
    text = re.sub(r"\(\s*.*?(вытягиваю.*?карты|тасую.*?колоду).*?\s*\)", "", text, flags=re.IGNORECASE)
    return text.strip()


def _has_explicit_tarot_trigger(text: str) -> bool:
    t = _normalize_for_intent(text)
    if not t:
        return False
    # явный отказ от расклада — не считаем триггером
    if _exit_tarot_mode_requested(t):
        return False
    if t.startswith("не ") or " не " in t:
        return False
    # быстрый отсев бытовых коротышей
    if len(t) < 3:
        return False
    return any(k in t for k in TAROT_TRIGGERS_NORM)


def _extract_requested_cards(text: str) -> Optional[int]:
    """Extract requested number of cards from user text (1..7)."""
    t = _normalize_for_intent(text)
    if not t:
        return None

    # ranges like "1-2", "1–2", "1/2"
    m = re.search(r"\b([1-7])\s*[-/–]\s*([1-7])\b", t)
    if m:
        a = int(m.group(1))
        b = int(m.group(2))
        return max(a, b)

    # explicit digits
    m = re.search(r"\b([1-7])\b", t)
    if m:
        return int(m.group(1))

    # word forms
    word_map = {
        "одна": 1, "одной": 1, "один": 1, "перв": 1,
        "две": 2, "двух": 2, "втор": 2, "пара": 2,
        "три": 3, "трех": 3,
        "четыре": 4, "четырех": 4,
        "пять": 5, "пяти": 5,
        "шесть": 6, "шести": 6,
        "семь": 7, "семи": 7,
    }
    for w, n in word_map.items():
        if re.search(rf"\b{w}\b", t):
            return n

    return None


def _is_yes_no_question(text: str) -> bool:
    t = _normalize_for_intent(text)
    if not t:
        return False
    return any(
        k in t
        for k in (
            "да или нет",
            "да/нет",
            "да нет",
            "ответ да или нет",
            "да ли",
            "правда ли",
            "верно ли",
            "верно ли что",
            "это правда",
            "это так",
        )
    )


def _infer_cards_count(text: str, *, has_context: bool) -> int:
    """Heuristic for cards count: 1-3 for simple, 5-7 for complex."""
    t = _normalize_for_intent(text)
    if not t:
        return 3
    if _is_yes_no_question(t):
        return 1
    length = len((text or "").strip())
    tokens = len(t.split())
    complex_markers = ("почему", "что делать", "как быть", "перспектива", "разбор", "глубже", "сложно", "комплекс")
    if any(k in t for k in complex_markers) and (tokens >= 12 or has_context):
        return 5
    if tokens <= 8 and length <= 70:
        return 2
    return 3


def _looks_like_tech_question(text: str) -> bool:
    t = _normalize_for_intent(text)
    if not t:
        return False
    tech_markers = (
        "код", "ошибк", "баг", "исключен", "трейсбек", "traceback", "stack trace",
        "проект", "репозитор", "git", "коммит", "commit", "pr", "pull request", "issue",
        "python", "javascript", "js", "ts", "java", "c#", "c++", "golang", "go ",
        "api", "endpoint", "http", "json", "yaml", "sql", "db", "database", "таблиц", "схем",
        "лог", "логи", "stack", "debug", "фикс", "build", "deploy", "docker", "k8s",
        "конфиг", "config", "env", "переменн", "пакет", "pip", "npm", "requirements",
        "virtualenv", "venv", "framework", "library", "sdk", "localhost", "порт",
    )
    return any(k in t for k in tech_markers)


def bot_decides_need_spread(text: str) -> bool:
    """Return True only when user explicitly asks for tarot and it's not a tech question."""
    # По умолчанию НЕ делаем расклад; делаем только по явному триггеру и не по тех. теме.
    if not _has_explicit_tarot_trigger(text):
        return False
    if _looks_like_tech_question(text):
        return False
    return True


def _choose_trigger_text(clean_text: str, extracted: str) -> str:
    if clean_text and len(clean_text.strip()) >= 3:
        return clean_text
    return extracted


def _exit_tarot_mode_requested(text: str) -> bool:
    t = _normalize_for_intent(text)
    if not t:
        return False
    exit_phrases = (
        "поговорим без карт",
        "просто поговорим",
        "без карт",
        "без расклада",
        "без таро",
        "обычный чат",
        "давай без карт",
        "не надо расклад",
        "не нужно расклад",
        "не надо карты",
        "не делай расклад",
        "не делай таро",
        "не хочу расклад",
        "не хочу таро",
        "не гадай",
    )
    return any(_normalize_for_intent(p) in t for p in exit_phrases)


def _looks_like_tarot_invite(text: str) -> bool:
    t = _normalize_for_intent(text)
    if not t:
        return False
    invite_phrases = (
        "хочешь, сделаю расклад",
        "хочешь сделаю расклад",
        "хочешь — сделаю расклад",
        "хочешь, сделаю таро",
        "давай сделаем расклад",
        "могу сделать расклад",
        "могу сделать таро-расклад",
        "предлагаю расклад",
        "сделать расклад?",
        "хочешь расклад",
    )
    return any(_normalize_for_intent(p) in t for p in invite_phrases)


def _is_confirmation_text(text: str) -> bool:
    t = _normalize_for_intent(text)
    if not t:
        return False
    if _exit_tarot_mode_requested(t):
        return False
    if t.startswith("не ") or " не " in t:
        return False
    confirmations = (
        "да", "давай", "ок", "okay", "хочу", "поехали", "сделай", "делай", "конечно",
    )
    return any(_normalize_for_intent(p) == t or _normalize_for_intent(p) in t for p in confirmations)


def _extract_invite_topic(text: str) -> Optional[str]:
    t = _normalize_for_intent(text)
    if any(k in t for k in ("любов", "отношен", "чувств", "бывш")):
        return "love"
    if any(k in t for k in ("деньг", "работ", "карьер", "бизнес", "доход", "финанс")):
        return "money"
    if any(k in t for k in ("будуще", "недел", "месяц", "день", "завтра", "путь", "перспектив")):
        return "future"
    return None


def _invite_topic_to_spread_name(topic: Optional[str]) -> str:
    if topic == "love":
        return "Отношения"
    if topic == "money":
        return "Деньги/работа"
    if topic == "future":
        return "Совет/будущее"
    return "Расклад"


def _has_tarot_consent(text: str) -> bool:
    t = _normalize_for_intent(text)
    if not t:
        return False
    consent_phrases = (
        "да", "давай", "ок", "okay", "хочу", "поехали", "сделай", "делай", "конечно",
        "согласен", "согласна", "готов", "готова",
        "делай расклад", "сделай расклад",
    )
    return any(_normalize_for_intent(p) == t or _normalize_for_intent(p) in t for p in consent_phrases)


def _build_pre_dialog_summary(state: Dict[str, Any]) -> str:
    theme = (state.get("theme") or "").strip()
    horizon = (state.get("horizon") or "").strip()
    context = (state.get("context") or "").strip()
    goal = (state.get("goal") or "").strip()
    parts = []
    if theme:
        parts.append(f"Тема: {theme}")
    if horizon:
        parts.append(f"Горизонт: {horizon}")
    if context:
        parts.append(f"Контекст: {context}")
    if goal:
        parts.append(f"Цель: {goal}")
    return "\n".join(parts).strip()


def _get_recent_followup_invite(user_id: int) -> Dict[str, Any]:
    try:
        snap = get_user_memory_snapshot(user_id) or {}
        f_type = (snap.get("last_followup_type") or "").strip()
        f_topic = (snap.get("last_followup_topic") or "").strip()
        f_at = snap.get("last_followup_at") or ""
        if f_type != "tarot_invite" or not f_at:
            return {}
        try:
            last_dt = dt.datetime.fromisoformat(f_at)
            age = (dt.datetime.utcnow() - last_dt).total_seconds()
            if age <= float(TAROT_SESSION_TTL_SEC):
                return {"type": f_type, "topic": f_topic, "age_sec": age}
        except Exception:
            _log_exception("followup invite time parse failed")
            return {}
    except Exception:
        _log_exception("followup invite snapshot failed")
    return {}


def _get_pre_dialog_state(user_id: int, chat_id: int) -> Dict[str, Any]:
    try:
        profile = get_user_profile_chat(user_id, chat_id) or {}
        return profile.get("pre_dialog") or {}
    except Exception:
        _log_exception("pre_dialog read failed")
        return {}


def _set_pre_dialog_state(user_id: int, chat_id: int, state: Dict[str, Any]) -> None:
    try:
        patch_user_profile_chat(
            user_id,
            chat_id,
            patch={"pre_dialog": state},
        )
    except Exception:
        _log_exception("pre_dialog write failed")


def _clear_pre_dialog_state(user_id: int, chat_id: int) -> None:
    try:
        patch_user_profile_chat(user_id, chat_id, delete_keys=["pre_dialog"])
    except Exception:
        _log_exception("pre_dialog clear failed")


def _is_pre_dialog_active(state: Dict[str, Any]) -> bool:
    if not state:
        return False
    try:
        until = float(state.get("expires_at") or 0)
        return time.time() <= until
    except Exception:
        return False


def _is_pre_dialog_expired(state: Dict[str, Any]) -> bool:
    if not state:
        return False
    try:
        until = float(state.get("expires_at") or 0)
        return time.time() > until
    except Exception:
        return False


def _has_enough_context(text: str) -> bool:
    # crude heuristic: at least ~2 sentences or 25+ meaningful tokens
    t = _normalize_for_intent(text)
    if not t:
        return False
    if len(t.split()) >= 12:
        return True
    if len((text or "").strip()) >= 80:
        return True
    if (text or "").count(".") + (text or "").count("!") + (text or "").count("?") >= 2:
        return True
    return False


def _extract_theme(text: str) -> str:
    t = _normalize_for_intent(text)
    if any(k in t for k in ("отношен", "любов", "чувств", "бывш", "пара", "парень", "девуш", "нрав", "свидан", "кофе", "влюб")):
        return "отношения"
    if any(k in t for k in ("деньг", "работ", "карьер", "бизнес", "доход", "финанс")):
        return "финансы/работа"
    if any(k in t for k in ("выбор", "решен", "сомнен", "дилем")):
        return "выбор"
    if any(k in t for k in ("состояни", "тревог", "устал", "выгор", "настроен")):
        return "состояние"
    return "другое"


def _extract_horizon(text: str) -> str:
    t = _normalize_for_intent(text)
    if any(k in t for k in ("сегодня", "сейчас", "завтра", "ближайш")):
        return "сегодня/ближайшие дни"
    if any(k in t for k in ("недел", "месяц")):
        return "неделя/месяц"
    if any(k in t for k in ("3 мес", "три мес", "квартал")):
        return "3 месяца"
    if any(k in t for k in ("год", "полгода")):
        return "год"
    return ""


def _extract_age(text: str) -> Optional[int]:
    t = _normalize_for_intent(text)
    if not t:
        return None
    m = re.search(r"\b(\d{1,2})\s*(лет|года|год)\b", t)
    if not m:
        return None
    age = int(m.group(1))
    if 16 <= age <= 50:
        return age
    return None


def _extract_gender(text: str) -> Optional[str]:
    t = _normalize_for_intent(text)
    if any(k in t for k in ("я парень", "я мужчина", "мужчина")):
        return "male"
    if any(k in t for k in ("я девушка", "я женщина", "женщина")):
        return "female"
    return None


_COMMON_FEMALE_NAMES = {
    # RU/UA common
    "анна", "аня", "анастасия", "настя", "наталья", "наташа", "екатерина", "катя",
    "мария", "маша", "александра", "саша", "ольга", "елена", "лена", "ирина",
    "юлия", "юля", "светлана", "света", "ксения", "полина", "алёна", "алена",
    "дарья", "даша", "виктория", "вика", "елизавета", "лиза", "карина",
    "марина", "татьяна", "таня", "любовь", "любаша", "нина", "валентина",
    "валя", "лилия", "лиля", "людмила", "люда", "алиса", "анжелика", "анжела",
    "вера", "вероника", "галина", "гала", "диана", "евгения", "женя", "жанна",
    "зоя", "инна", "ирина", "кира", "кристина", "лариса", "лидия", "мадина",
    "маргарита", "рита", "милана", "марианна", "надежда", "надя", "оксана",
    "ольвия", "ольвия", "оливия", "пелагея", "полина", "рада", "регина",
    "ромина", "сабина", "снежана", "софия", "софья", "соня", "таисия", "тася",
    "тамара", "ульяна", "фаина", "яна", "алина", "валерия", "лерa", "диана",
    "евдокия", "евгения", "елина", "жанна", "инга", "камилла", "каролина",
    "клара", "клара", "марта", "нелли", "оксана", "павлина", "рада", "рина",
    "серафима", "стефания", "стеша", "теодора", "фёдора", "эвелина",
    # UA variants
    "наталя", "катерина", "олена", "олесья", "олесся", "оксана", "ксенія", "ксенія",
    "софія", "юлія", "валерія", "дарина", "владислава", "влада", "злата",
    "любов", "люба", "мар'яна", "марьяна", "марина", "мирослава", "мира",
}

_COMMON_MALE_NAMES = {
    # RU/UA common
    "иван", "ваня", "александр", "саша", "дмитрий", "дима", "сергей", "серёжа", "сережа",
    "андрей", "павел", "паша", "михаил", "миша", "никита", "артём", "артем",
    "кирилл", "илья", "максим", "денис", "игорь", "владимир", "вова", "влад",
    "виктор", "евгений", "женя", "алексей", "лёша", "леша", "анатолий", "анатолій",
    "борис", "валентин", "валера", "валерий", "виталий", "витя", "григорий",
    "глеб", "данил", "даниил", "иван", "евгений", "егор", "зенон", "захар",
    "илья", "константин", "костя", "леонид", "лев", "матвей", "михаил",
    "николай", "коля", "олег", "пётр", "петр", "роман", "ростислав", "руслан",
    "савелий", "семён", "семен", "степан", "стас", "станислав", "тимур",
    "фёдор", "федор", "юрий", "юра", "ярослав",
    # UA variants
    "андрій", "олександр", "сергій", "дмитро", "михайло", "петро", "юрій",
    "олег", "богдан", "василь", "василій", "иван", "іван", "тарас", "остап",
    "євген", "єгор", "максим", "микола", "миколай", "гриць", "григорій",
    "ілля", "іван", "львів", "роман", "степан", "станіслав", "тимофій",
}

_COMMON_FEMALE_NAMES_LAT = {
    "anna", "anastasia", "natalia", "natasha", "ekaterina", "katya", "maria", "masha",
    "alexandra", "sasha", "olga", "elena", "irina", "julia", "yulia", "svetlana",
    "ksenia", "polina", "alena", "alyona", "daria", "dasha", "victoria", "vika",
    "elizabeth", "liza", "karina", "marina", "tatyana", "vera", "veronica",
    "kseniya", "sofia", "sonya", "yana", "alina", "valeria", "oksana",
}

_COMMON_MALE_NAMES_LAT = {
    "ivan", "alexander", "alexandr", "sasha", "dmitry", "dima", "sergey", "sergei",
    "andrey", "andrei", "pavel", "pasha", "mikhail", "misha", "nikita", "artem",
    "artyom", "kirill", "ilya", "ilyas", "maxim", "denis", "igor", "vladimir",
    "victor", "evgeny", "evgenii", "alexey", "aleksey", "anatoly", "boris",
    "vitaly", "gleb", "danil", "daniil", "egor", "konstantin", "kostya",
    "leonid", "lev", "matvey", "nikolai", "oleg", "petr", "roman", "ruslan",
    "stanislav", "timur", "fedor", "yuri", "yuriy", "yaroslav",
}

_LAT_TO_CYR = {
    # male
    "kirill": "Кирилл",
    "sergey": "Сергей",
    "sergei": "Сергей",
    "alexey": "Алексей",
    "aleksey": "Алексей",
    "alexei": "Алексей",
    "alexander": "Александр",
    "alexandr": "Александр",
    "dmitry": "Дмитрий",
    "dmitriy": "Дмитрий",
    "andrey": "Андрей",
    "andrei": "Андрей",
    "nikita": "Никита",
    "maxim": "Максим",
    "ivan": "Иван",
    "pavel": "Павел",
    "mikhail": "Михаил",
    "yuri": "Юрий",
    "yuriy": "Юрий",
    "roman": "Роман",
    "ruslan": "Руслан",
    "igor": "Игорь",
    "fedor": "Фёдор",
    "petr": "Пётр",
    "oleg": "Олег",
    # female
    "natalia": "Наталья",
    "natasha": "Наташа",
    "anastasia": "Анастасия",
    "maria": "Мария",
    "elena": "Елена",
    "ekaterina": "Екатерина",
    "katya": "Катя",
    "sofia": "София",
    "sonya": "Соня",
    "polina": "Полина",
    "olga": "Ольга",
    "irina": "Ирина",
    "yulia": "Юлия",
    "julia": "Юлия",
    "svetlana": "Светлана",
    "ksenia": "Ксения",
    "kseniya": "Ксения",
    "oksana": "Оксана",
    "alina": "Алина",
    "victoria": "Виктория",
    "vika": "Вика",
    "daria": "Дарья",
    "dasha": "Даша",
}


def _collapse_repeats(s: str) -> str:
    if not s:
        return s
    out = [s[0]]
    for ch in s[1:]:
        if ch != out[-1]:
            out.append(ch)
    return "".join(out)


def _normalize_name_from_account(raw: str) -> Optional[str]:
    if not raw:
        return None
    t = raw.strip().lower()
    # remove non-letters
    t = re.sub(r"[^a-zа-яёіїєґ']", "", t)
    if not (3 <= len(t) <= 16):
        return None
    if any(x in t for x in ("bot", "admin", "support", "shop", "store", "official")):
        return None
    # exact latin -> cyrillic mapping first
    if t in _LAT_TO_CYR:
        return _LAT_TO_CYR[t]
    # exact match before collapsing repeats
    if t in _COMMON_FEMALE_NAMES or t in _COMMON_MALE_NAMES:
        return t.capitalize()
    if t in _COMMON_FEMALE_NAMES_LAT or t in _COMMON_MALE_NAMES_LAT:
        return t.capitalize()
    # then try collapsed repeats for noisy nicknames
    t = _collapse_repeats(t)
    # diminutive to base (наташка -> наташа)
    if t.endswith("шка"):
        candidate = t[:-3] + "ша"
        if candidate in _COMMON_FEMALE_NAMES:
            t = candidate
    if t in _LAT_TO_CYR:
        return _LAT_TO_CYR[t]
    if t in _COMMON_FEMALE_NAMES or t in _COMMON_MALE_NAMES:
        return t.capitalize()
    if t in _COMMON_FEMALE_NAMES_LAT or t in _COMMON_MALE_NAMES_LAT:
        return t.capitalize()
    return None


def _infer_gender_from_name(name: str) -> Optional[str]:
    n = (name or "").strip().lower()
    if not n:
        return None
    if n in _COMMON_FEMALE_NAMES:
        return "female"
    if n in _COMMON_MALE_NAMES:
        return "male"
    if n in _COMMON_FEMALE_NAMES_LAT:
        return "female"
    if n in _COMMON_MALE_NAMES_LAT:
        return "male"
    return None


def _profile_missing(profile: Dict[str, Any]) -> List[str]:
    missing = []
    if not (profile.get("name") or "").strip():
        missing.append("имя")
    if not profile.get("age"):
        missing.append("возраст")
    return missing


def _profile_hint(profile: Dict[str, Any]) -> str:
    parts = []
    if profile.get("name"):
        parts.append(f"имя={profile.get('name')}")
    if profile.get("age"):
        parts.append(f"возраст={profile.get('age')}")
    if profile.get("gender"):
        parts.append(f"пол={profile.get('gender')}")
    return ", ".join(parts)


def _profile_prompt_for_chat(profile: Dict[str, Any]) -> str:
    missing = _profile_missing(profile)
    if not missing:
        return ""
    asked_at = profile.get("profile_ask_at")
    try:
        if asked_at:
            last_dt = dt.datetime.fromisoformat(asked_at)
            if (dt.datetime.utcnow() - last_dt).total_seconds() < 24 * 3600:
                return ""
    except Exception:
        pass
    # ask only one missing item at a time, gently
    target = missing[0]
    if target == "имя":
        return "Если уместно, мягко спроси, как к нему можно обращаться."
    if target == "возраст":
        return "Если уместно, мягко спроси, сколько лет, без давления."
    return ""


def _next_pre_dialog_question(state: Dict[str, Any], user_text: str) -> str:
    theme = state.get("theme") or _extract_theme(user_text)
    horizon = state.get("horizon") or _extract_horizon(user_text)

    if not theme or theme == "другое":
        return "Хочу понять тебя точнее, чтобы расклад был честным и полезным. О чём это в целом: отношения, работа/деньги, выбор, состояние — или другое?"
    if not horizon:
        return "На какой горизонт хочешь посмотреть: сегодня, ближайшие дни, неделя/месяц, 3 месяца, год?"
    if not state.get("goal"):
        return "Что именно ты хочешь узнать? (например: получится ли, перспективы, что делать дальше)"
    if theme == "отношения" and not state.get("context"):
        return "О ком именно речь и что между вами происходит сейчас? (кто этот человек, как вы связаны, что случилось)"
    if not state.get("context"):
        return "Что происходит сейчас в этой ситуации? Можно 2–5 предложений — этого достаточно."
    return "Сформулируй один конкретный вопрос, на который хочешь получить ответ через карты."


def _update_pre_dialog_state(state: Dict[str, Any], user_text: str) -> Dict[str, Any]:
    new_theme = _extract_theme(user_text)
    new_horizon = _extract_horizon(user_text)
    theme = state.get("theme") or new_theme
    if theme == "другое" and new_theme and new_theme != "другое":
        theme = new_theme
    horizon = state.get("horizon") or new_horizon
    context = state.get("context") or (user_text if _has_enough_context(user_text) else "")
    goal = state.get("goal") or (
        user_text
        if any(
            k in _normalize_for_intent(user_text)
            for k in (
                "хочу",
                "нужно",
                "понять",
                "узнать",
                "что делать",
                "как быть",
                "получится",
                "смогу",
                "сможет",
                "успех",
                "запустить",
                "запуск",
                "старт",
            )
        ) or _is_yes_no_question(user_text)
        else ""
    )
    requested_cards = state.get("requested_cards") or _extract_requested_cards(user_text)

    questions = int(state.get("questions", 0) or 0) + 1
    return {
        "theme": theme,
        "horizon": horizon,
        "context": context,
        "goal": goal,
        "questions": questions,
        "requested_cards": requested_cards,
        "expires_at": time.time() + float(PRE_DIALOG_TTL_SEC),
    }


def _pre_dialog_is_ready(state: Dict[str, Any]) -> bool:
    if not state:
        return False
    if not state.get("theme"):
        return False
    if not state.get("horizon"):
        return False
    if not state.get("goal"):
        return False
    return True


def _set_tarot_session_mode(context: ContextTypes.DEFAULT_TYPE, *, enabled: bool) -> None:
    if enabled:
        context.chat_data["tarot_mode"] = True
        context.chat_data["tarot_mode_until"] = time.time() + float(TAROT_SESSION_TTL_SEC)
    else:
        context.chat_data["tarot_mode"] = False
        context.chat_data["tarot_mode_until"] = 0


def _is_tarot_session_active(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> bool:
    enabled = bool(context.chat_data.get("tarot_mode"))
    until = float(context.chat_data.get("tarot_mode_until") or 0)
    now = time.time()
    if enabled and now <= until:
        return True
    if enabled and now > until:
        _set_tarot_session_mode(context, enabled=False)

    # fallback across restarts: check recent tarot activity from DB
    try:
        snap = get_user_memory_snapshot(user_id) or {}
        last_tarot_at = snap.get("last_tarot_at") or ""
        if last_tarot_at:
            last_dt = dt.datetime.fromisoformat(last_tarot_at)
            if (dt.datetime.utcnow() - last_dt).total_seconds() <= float(TAROT_SESSION_TTL_SEC):
                _set_tarot_session_mode(context, enabled=True)
                return True
    except Exception:
        _log_exception("tarot session fallback check failed")
    return False

def _is_followup_text(t: str) -> bool:
    t = (t or "").strip().lower()
    if not t:
        return False
    if len(t) <= 2:
        return False
    return any(x in t for x in FOLLOWUP_TRIGGERS)


def _is_answer_to_tarot_questions(user_text: str, last_bot_message: str) -> bool:
    """
    Heuristic: user replies to end-of-reading questions.
    We treat short/confirming replies to a bot message that contained questions.
    """
    u = (user_text or "").strip().lower()
    if not u:
        return False
    lb = (last_bot_message or "").strip()
    if "?" not in lb:
        return False
    if len(u) <= 120:
        return True
    if u.startswith(("да", "нет", "не знаю", "не уверен", "думаю", "скорее", "наверное")):
        return True
    return False


def _route_override_from_trigger(text: str, *, mode: str = "") -> RouteResult:
    """
    Жёсткий оверрайд: если пользователь ЯВНО попросил таро, но GPT-роутер ошибся и вернул chat,
    мы всё равно запускаем tarot flow, чтобы НИКОГДА не было "расклада" без карт/интро.

    Важно: этот оверрайд включаем только для route.action == "chat" (а не для clarify),
    чтобы не гадать на совсем непонятный запрос.
    """
    t = (text or "").lower()

    # 1) да/нет => 1 карта
    if any(k in t for k in ("да/нет", "да нет", "да или нет", "ответ да", "сделай да/нет", "сделай да или нет")):
        return RouteResult(action="reading", cards=1, spread_name="Да/Нет", clarify_question="", reason="override_yes_no")

    # 2) карта дня
    if any(k in t for k in ("карта дня", "карточка дня")):
        return RouteResult(action="reading", cards=1, spread_name="Карта дня", clarify_question="", reason="override_card_day")

    # 3) режим из кнопок (если был armed)
    m = (mode or "").lower().strip()
    if m in ("love", "money", "self", "future"):
        spread_map = {"love": "Отношения", "money": "Деньги/работа", "self": "Самопознание", "future": "Совет/будущее"}
        return RouteResult(action="reading", cards=5, spread_name=spread_map.get(m, "Расклад"), clarify_question="", reason="override_mode_hint")

    # 4) по словам — грубо, но стабильно
    if any(k in t for k in ("любов", "отношен", "верн", "чувств", "бывш", "пара", "роман")):
        return RouteResult(action="reading", cards=5, spread_name="Отношения", clarify_question="", reason="override_keywords_love")

    if any(k in t for k in ("деньг", "работ", "карьер", "оффер", "зарплат", "бизнес", "клиент", "доход")):
        return RouteResult(action="reading", cards=5, spread_name="Деньги/работа", clarify_question="", reason="override_keywords_money")

    if any(k in t for k in ("само", "внутр", "я ", "меня", "мне", "ценност", "смысл", "страх")):
        return RouteResult(action="reading", cards=5, spread_name="Самопознание", clarify_question="", reason="override_keywords_self")

    # дефолт: 3 карты
    return RouteResult(action="reading", cards=3, spread_name="Расклад", clarify_question="", reason="override_default")

def _forward_meta(msg: Message) -> str:
    """
    Достаём безопасную "шапку" для forwarded.
    У PTB forward_origin может быть разным (user/chat/channel/hidden_user).
    Мы не делаем жёсткой типизации — просто пытаемся вытащить что получится.
    """
    try:
        # PTB 20+: msg.forward_origin может быть объектом с разными полями
        fo = getattr(msg, "forward_origin", None)
        if fo:
            # пробуем самые частые варианты
            sender_user = getattr(fo, "sender_user", None)
            sender_user_name = ""
            if sender_user:
                sender_user_name = " ".join(
                    [x for x in [getattr(sender_user, "first_name", None), getattr(sender_user, "last_name", None)] if x]
                ).strip()

            sender_chat = getattr(fo, "sender_chat", None)
            sender_chat_title = getattr(sender_chat, "title", None) if sender_chat else None

            hidden_user_name = getattr(fo, "sender_user_name", None) or getattr(fo, "sender_name", None)

            if sender_user_name:
                return f"from: {sender_user_name}"
            if sender_chat_title:
                return f"from: {sender_chat_title}"
            if hidden_user_name:
                return f"from: {hidden_user_name}"
    except Exception:
        _log_exception("suppressed exception")

    # fallback на старые поля
    try:
        if getattr(msg, "forward_sender_name", None):
            return f"from: {msg.forward_sender_name}"
        ff = getattr(msg, "forward_from", None)
        if ff:
            nm = " ".join([x for x in [getattr(ff, "first_name", None), getattr(ff, "last_name", None)] if x]).strip()
            if nm:
                return f"from: {nm}"
        fc = getattr(msg, "forward_from_chat", None)
        if fc and getattr(fc, "title", None):
            return f"from: {fc.title}"
    except Exception:
        _log_exception("suppressed exception")

    return "from: unknown"


def extract_message_text(msg: Message, lang: str = "ru") -> Tuple[str, str]:
    """Совместимость: в этом модуле местами ожидают (clean, combined).

    Единый источник истины — handlers.common.extract_message_text().
    """
    from .common import extract_message_text as _extract

    data = _extract(msg)
    combined = (data.get("clean_text") or "").strip()
    parts = data.get("parts") or {}

    main = (parts.get("main") or "").strip()
    clean = main
    if not clean:
        # fallback: если main пустой — берём forwarded/reply как "clean"
        clean = (parts.get("forwarded") or "").strip() or (parts.get("reply_to") or "").strip()

    return clean, combined


# ---------------- JPEG helpers ----------------

def _to_telegram_jpeg_bytes(src_path: str, *, max_side: int = 1280, quality: int = 85) -> BytesIO:
    """
    Загружает src_path, приводит к супер-совместимому JPEG (baseline RGB),
    уменьшает если надо, возвращает BytesIO (готово для send_photo).
    """
    img = Image.open(src_path)
    img.load()
    img = img.convert("RGB")

    if max(img.size) > max_side:
        img.thumbnail((max_side, max_side), Image.LANCZOS)

    bio = BytesIO()
    bio.name = "spread.jpg"

    img.save(
        bio,
        format="JPEG",
        quality=quality,
        optimize=False,
        progressive=False,
        subsampling=2,
    )
    bio.seek(0)

    if bio.getbuffer().nbytes < 10_000:
        raise ValueError(f"Rendered image too small: {bio.getbuffer().nbytes} bytes")

    return bio


def _repack_for_telegram(src_path: str) -> str:
    """
    Делает максимально совместимый JPEG для Telegram:
    - RGB
    - baseline (progressive=False)
    - ограничение размера до 1280px по большей стороне
    Возвращает путь к новому файлу (в tmp).
    """
    img = Image.open(src_path)
    img.load()
    img = img.convert("RGB")

    MAX_SIDE = 1280
    if max(img.size) > MAX_SIDE:
        img.thumbnail((MAX_SIDE, MAX_SIDE), Image.LANCZOS)

    fd, out_path = tempfile.mkstemp(prefix="tg_safe_", suffix=".jpg")
    os.close(fd)

    img.save(
        out_path,
        "JPEG",
        quality=85,
        optimize=False,
        progressive=False,
        subsampling=2,
    )
    return out_path


# ---------------- START UI (кнопки) ----------------


# =========================
# MAIN MENU + PRESET QUESTIONS
# =========================


def start_text_tarot() -> str:
    return (
        "Привет! Я <b>Астра</b> ✨\n"
        "Я делаю расклады Таро по колоде Rider–Waite <b>с картинками карт</b> и даю "
        "<b>объёмные</b>, понятные трактовки без мистической воды.\n\n"
        "Просто напиши свой запрос — я помогу уточнить и разобраться."
    )


async def _flush_text_batch(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    user_id: int,
) -> None:
    try:
        await asyncio.sleep(BATCH_DELAY_SEC)
    except Exception:
        return

    await wait_for_media_if_needed(context)

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

    # ТЗ: история — в SQLite messages (user_id+chat_id), а не в оперативке.
    history = _safe_get_last_messages(user_id, chat_id, limit=MAX_HISTORY_MESSAGES)
    history_for_model = trim_history_for_model(list(history) + [{"role": "user", "content": combined_text}])
    memory_block = build_long_memory_block(user_id, chat_id, lang=lang)
    if memory_block:
        history_for_model = [{"role": "system", "content": memory_block}] + history_for_model
    # lightweight profile prompt (ask name/age gently if missing)
    try:
        prof = get_user_profile_chat(user_id, chat_id) or {}
        prof_block = build_profile_system_block(prof)
        if prof_block:
            history_for_model = [prof_block] + history_for_model
        profile_prompt = _profile_prompt_for_chat(prof)
        if profile_prompt:
            history_for_model = [{"role": "system", "content": profile_prompt}] + history_for_model
            patch_user_profile_chat(
                user_id,
                chat_id,
                patch={"profile_ask_at": dt.datetime.utcnow().isoformat()},
            )
    except Exception:
        _log_exception("profile prompt failed")

    try:
        # anti-loop: if assistant repeats same leading question pattern, force variety
        try:
            last_assistant = [
                (m.get("content") or "").strip().lower()
                for m in history[-6:]
                if (m.get("role") or "") == "assistant"
            ]
            repeated = [t for t in last_assistant if t.startswith("что для тебя важнее")]
            if len(repeated) >= 2:
                history_for_model = (
                    [{"role": "system", "content": "Не повторяй одинаковую формулировку вопроса. Сформулируй вопрос иначе или попроси конкретику."}]
                    + history_for_model
                )
        except Exception:
            _log_exception("anti-loop check failed")

        await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)
    except Exception:
        _log_exception("suppressed exception")

    try:
        answer = await ask_gpt(history_for_model, lang)
        answer = _strip_fake_shuffle(answer)
    except Exception:
        answer = "Произошла ошибка. Попробуйте позже."

    # сохраняем в БД (ТЗ)
    _safe_add_user_and_assistant_messages(user_id, chat_id, combined_text, answer)
    _safe_set_last_context(
        user_id,
        topic=topic,
        last_user_message=combined_text,
        last_bot_message=answer,
    )
    _safe_log_event(
        user_id,
        "text",
        tokens=total_raw_len,
        meta=f"topic:{topic};batch_size:{batch_size}",
        lang=lang,
        topic=topic,
    )

    # если бот предложил расклад — фиксируем invite, чтобы "давай" от пользователя запустило таро
    try:
        if _looks_like_tarot_invite(answer):
            inv_topic = _extract_invite_topic(combined_text) or ""
            set_last_followup_meta(user_id, followup_type="tarot_invite", followup_topic=inv_topic)
            # snapshot: current user request + assistant question context
            set_last_followup_context(
                user_id,
                question=combined_text,
                context=answer,
            )
            touch_last_followup_at(user_id)
    except Exception:
        _log_exception("set last_followup_meta failed")

    # Не включаем tarot_session_mode от мягкого предложения в обычном чате.
    await send_smart_answer(last_msg, answer)
    try:
        asyncio.create_task(maybe_update_long_memory(user_id, chat_id, lang=lang, topic=topic))
    except Exception:
        _log_exception("long memory update scheduling failed")


# ---------------- TAROT helpers ----------------

def _build_cards_plain(cards) -> str:
    """Простой список карт без HTML — на случай если Telegram режет форматирование."""
    parts = ["Выпали карты:"]
    for i, c in enumerate(cards or [], start=1):
        try:
            nm = (c.meaning.ru_name if getattr(c, "meaning", None) else getattr(c, "key", "Карта"))
        except Exception:
            nm = "Карта"
        parts.append(f"{i}) {nm}")
    return "\n".join(parts)



def _build_cards_caption(cards) -> str:
    lines = ["🃏 <b>Выпавшие карты:</b>"]
    for i, c in enumerate(cards, start=1):
        nm = c.meaning.ru_name if getattr(c, "meaning", None) else c.key
        lines.append(f"{i}) {nm}")
    return "\n".join(lines)


def _build_intro_post(route, user_question: str, n_cards: int, user_name: str = "") -> str:
    """
    ТЗ: не шаблонно, по-дружески, без «Пользователь просит...».
    Должно объяснять, что сейчас будет, и сколько карт.
    """
    title = (getattr(route, "spread_name", "") or "").strip() or "Расклад"
    q = (user_question or "").strip()

    name = (user_name or "").strip()
    if name:
        hi = f"{name}, "
    else:
        hi = ""

    # лёгкая вариативность без отдельного вызова GPT
    openers = [
        f"{hi}я поняла. Давай спокойно посмотрим по картам — без лишней мистики.",
        f"{hi}окей, давай разложим ситуацию по полочкам через карты.",
        f"{hi}слышу тебя. Сейчас сделаю расклад и дам понятный план, что делать дальше.",
        f"{hi}давай проясним это через карты — коротко, но в точку.",
    ]

    # что именно проверяем (очень коротко)
    if n_cards <= 1:
        structure = "Возьму <b>1 карту</b> — чтобы поймать главный тон/совет на сейчас."
    elif n_cards == 3:
        structure = "Возьму <b>3 карты</b>: что сейчас, что мешает/скрыто, и куда ведёт ближайший шаг."
    elif n_cards == 5:
        structure = "Возьму <b>5 карт</b>: ты, внешние факторы, ресурс, риск и самый вероятный вектор."
    else:
        structure = f"Возьму <b>{n_cards} карт</b> — чтобы увидеть картину шире и не промазать в деталях."

    # маленькая эмпатия по вопросу (если есть)
    empath = ""
    if q:
        short_q = (q[:160] + "…") if len(q) > 160 else q
        empath = f"Запрос слышу: «{short_q}»\n"

    opener = openers[abs(hash(q or title)) % len(openers)]

    return (
        f"📝 <b>{title}</b>\n"
        f"{empath}"
        f"{opener}\n"
        f"{structure}\n\n"
        "Сейчас перемешаю колоду и покажу, что выпало. 👇"
    )



# --- shuffle media ---
from telegram import InputFile  # noqa
from telegram.ext import ContextTypes  # noqa

async def _send_shuffle_then_delete(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    *,
    seconds: float = 4.0,
) -> None:
    path: Path = SHUFFLE_VIDEO_PATH
    if not isinstance(path, Path):
        path = Path(str(path))

    if not path.is_file():
        logger.warning("shuffle animation not found: %s", path)
        return

    ext = path.suffix.lower()
    sent_msg = None

    try:
        with open(path, "rb") as f:
            if ext == ".mp4":
                inp = InputFile(f, filename="shuffle.mp4")
                try:
                    sent_msg = await context.bot.send_animation(
                        chat_id=chat_id,
                        animation=inp,
                        supports_streaming=True,
                    )
                except Exception:
                    logger.exception("send_animation(mp4) failed, trying send_video")
                    f.seek(0)
                    inp2 = InputFile(f, filename="shuffle.mp4")
                    sent_msg = await context.bot.send_video(
                        chat_id=chat_id,
                        video=inp2,
                        supports_streaming=True,
                    )
            else:
                inp = InputFile(f)
                try:
                    sent_msg = await context.bot.send_animation(chat_id=chat_id, animation=inp)
                except Exception:
                    logger.exception("send_animation failed, trying send_document")
                    f.seek(0)
                    inp2 = InputFile(f, filename=path.name or "shuffle.bin")
                    sent_msg = await context.bot.send_document(chat_id=chat_id, document=inp2)
    except Exception:
        logger.exception("failed to send shuffle media")
        return

    if not sent_msg:
        return

    try:
        await asyncio.sleep(max(0.5, float(seconds)))
    except Exception:
        return

    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=sent_msg.message_id)
    except Exception:
        _log_exception("suppressed exception")


def _cards_payload_from_last_tarot_meta(last_tarot_meta: Any) -> List[Dict[str, Any]]:
    """
    last_tarot_meta хранится строкой JSON в users.last_tarot_meta
    или уже dict (если кто-то передал так).
    Строим cards_payload для tarot_reading_answer БЕЗ нового вытягивания.
    """
    meta_obj: Dict[str, Any] = {}

    try:
        if isinstance(last_tarot_meta, str) and last_tarot_meta.strip():
            import json
            meta_obj = json.loads(last_tarot_meta) or {}
        elif isinstance(last_tarot_meta, dict):
            meta_obj = last_tarot_meta
    except Exception:
        meta_obj = {}

    cards_meta = meta_obj.get("cards") or []
    if not isinstance(cards_meta, list) or not cards_meta:
        return []

    # пробуем восстановить значения из колоды по key
    payload: List[Dict[str, Any]] = []
    deck = None
    try:
        deck = get_default_deck()
    except Exception:
        deck = None

    deck_cards = []
    try:
        deck_cards = list(getattr(deck, "cards", []) or [])
    except Exception:
        deck_cards = []

    def _find_card_by_key(k: str):
        for c in deck_cards:
            if getattr(c, "key", None) == k:
                return c
        return None

    for cm in cards_meta[:10]:
        k = str(cm.get("key") or "").strip()
        nm = str(cm.get("name") or "").strip()
        fl = str(cm.get("file") or "").strip()

        cobj = _find_card_by_key(k) if (deck and k) else None
        if cobj and getattr(cobj, "meaning", None):
            m = cobj.meaning
            payload.append({
                "key": getattr(cobj, "key", k or nm or "card"),
                "ru_name": m.ru_name,
                "keywords": m.keywords,
                "short": m.short,
                "shadow": m.shadow or "",
                "advice": m.advice or "",
                "file": getattr(cobj, "filename", fl),
            })
        else:
            payload.append({
                "key": k or (nm or "card"),
                "ru_name": nm or k or "Карта",
                "keywords": "",
                "short": "Продолжаем трактовку по текущему раскладу.",
                "shadow": "",
                "advice": "",
                "file": fl or "",
            })

    return payload


async def _handle_tarot_followup(
    msg: Message,
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    user_text: str,
) -> bool:
    """
    Follow-up после расклада:
    - если последний контекст/мета по таро есть
    - и фраза выглядит как "подробнее/что значит" и т.п.
    Тогда НЕ тянем карты заново, а расширяем трактовку текущих.
    Возвращает True если обработали.
    """
    snap = None
    try:
        snap = get_followup_personalization_snapshot(user_id) or {}
    except Exception:
        snap = {}

    last_topic = (snap.get("last_topic") or "").strip().lower()
    last_tarot_meta = snap.get("last_tarot_meta")
    last_bot_message = (snap.get("last_bot_message") or "")
    if last_topic != "tarot" or not last_tarot_meta:
        return False

    if _has_explicit_tarot_trigger(user_text):
        return False

    if not _is_followup_text(user_text) and not _is_answer_to_tarot_questions(user_text, last_bot_message):
        return False

    cards_payload = _cards_payload_from_last_tarot_meta(last_tarot_meta)
    if not cards_payload:
        return False

    # spread name попробуем из meta
    spread_name = "Текущий расклад"
    try:
        if isinstance(last_tarot_meta, str) and last_tarot_meta.strip():
            import json
            mo = json.loads(last_tarot_meta) or {}
            spread_name = (mo.get("spread") or spread_name)[:48]
        elif isinstance(last_tarot_meta, dict):
            spread_name = (last_tarot_meta.get("spread") or spread_name)[:48]
    except Exception:
        _log_exception("suppressed exception")


    # ✅ Фоллоу-ап: показываем карты снова (текущий расклад), чтобы не было ответа "в пустоту"
    try:
        await reply_and_mirror(
            msg,
            f"🔁 <b>Продолжаю расшифровку</b> расклада «{spread_name}». Карты те же — вот они 👇",
            parse_mode="HTML",
        )
    except Exception:
        try:
            await reply_and_mirror(msg, f"Продолжаю расшифровку расклада «{spread_name}». Карты те же — вот они:")
        except Exception:
            _log_exception("suppressed exception")

    # 1) сперва пробуем отправить картинку расклада (из файлов карт)
    sent_cards_ok = False
    try:
        deck = get_default_deck()
        card_files = [str(c.get("file") or "").strip() for c in (cards_payload or []) if isinstance(c, dict)]
        card_paths = [deck.abs_path(cf) for cf in card_files if cf]
        if card_paths:
            out_path = str(TMP_DIR / f"followup_spread_{user_id}_{msg.message_id}.jpg")
            try:
                render_spread(str(TABLE_PATH), card_paths, out_path)
            except Exception:
                out_path = ""

            if out_path and os.path.exists(out_path):
                try:
                    bio = _to_telegram_jpeg_bytes(out_path, max_side=1280, quality=85)
                except Exception:
                    bio = None

                if bio is not None:
                    caption = "🃏 Карты: " + ", ".join([c.get("ru_name") or c.get("key") or "Карта" for c in cards_payload if isinstance(c, dict)])
                    try:
                        await context.bot.send_photo(chat_id=msg.chat_id, photo=InputFile(bio), caption=caption)
                        sent_cards_ok = True
                    except Exception:
                        sent_cards_ok = False
    except Exception:
        sent_cards_ok = False

    # 2) железный фолбэк: всегда хотя бы текстом
    if not sent_cards_ok:
        try:
            names = [c.get("ru_name") or c.get("key") or "Карта" for c in cards_payload if isinstance(c, dict)]
            await context.bot.send_message(chat_id=msg.chat_id, text="🃏 Карты: " + ", ".join(names))
        except Exception:
            _log_exception("suppressed exception")

    # персонализация/контекст
    personalization = ""
    try:
        parts = []
        lu = (snap.get("last_user_message") or "")[:250]
        lb = (snap.get("last_bot_message") or "")[:250]
        if lu:
            parts.append(f"Последнее сообщение пользователя: {lu}")
        if lb:
            parts.append(f"Последний ответ бота: {lb}")
        personalization = "\n".join(parts)[:900]
    except Exception:
        personalization = ""

    try:
        await context.bot.send_chat_action(chat_id=msg.chat_id, action=ChatAction.TYPING)
    except Exception:
        _log_exception("suppressed exception")

    history = _safe_get_last_messages(user_id, msg.chat_id, limit=MAX_HISTORY_MESSAGES)
    try:
        prof = get_user_profile_chat(user_id, msg.chat_id) or {}
        prof_block = build_profile_system_block(prof)
        if prof_block:
            history = [prof_block] + history
    except Exception:
        _log_exception("profile block failed")
    answer = await tarot_reading_answer(
        lang="ru",
        user_question=f"FOLLOW-UP: {user_text}",
        spread_name=spread_name,
        cards_payload=cards_payload,
        history_hint=personalization,
        history=history,
    )

    answer = _strip_fake_shuffle(answer)

    await send_smart_answer(msg, answer)

    # ТЗ: история в messages
    _safe_add_user_and_assistant_messages(user_id, msg.chat_id, user_text, answer)
    _safe_set_last_context(user_id, topic="tarot", last_user_message=user_text, last_bot_message=answer)
    _safe_log_event(user_id, "tarot_followup", lang="ru", topic="tarot", meta="followup_expand_current")

    return True


# ---------------- TAROT main flow ----------------

async def _handle_tarot_reading(
    msg: Message,
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    raw_text: str,
    route,
) -> None:
    logger.warning(
        "TAROT FINAL cards=%s spread=%s question=%s",
        getattr(route, "cards", None),
        getattr(route, "spread_name", None),
        raw_text[:80],
    )

    # лимиты
    can_do, reason_text = check_tarot_limits(user_id, msg.chat_id)
    if not can_do:
        try:
            set_last_limit_info(user_id, topic="tarot", limit_type="tarot")
        except Exception:
            _log_exception("suppressed exception")

        paywall = ""
        try:
            prof = get_followup_personalization_snapshot(user_id)
            history = _safe_get_last_messages(user_id, msg.chat_id, limit=MAX_HISTORY_MESSAGES)
            paywall = await generate_limit_paywall_text(
                lang="ru",
                limit_type="tarot",
                topic="tarot",
                last_user_message=raw_text,
                user_profile=prof,
                history=history,
            )
        except Exception:
            paywall = ""

        try:
            if paywall and not should_send_limit_paywall(user_id, paywall):
                return
        except Exception:
            _log_exception("suppressed exception")

        final_text = (paywall or "Чтобы продолжить, можно купить расклады.").strip()
        await reply_and_mirror(msg, final_text, reply_markup=_pro_keyboard("ru"))

        try:
            if paywall:
                set_last_paywall_text(user_id, paywall)
        except Exception:
            _log_exception("suppressed exception")

        try:
            schedule_limit_followup(context.application, user_id, "ru")
        except Exception:
            _log_exception("suppressed exception")

        _safe_log_event(
            user_id,
            "tarot_limit_reached",
            lang="ru",
            topic="tarot",
            last_limit_type="tarot",
            meta=(reason_text or "")[:200],
        )
        return

    # cleanup tmp renders

    _cleanup_tmp_files(TMP_DIR)


    # deck init
    try:
        deck = get_default_deck()
    except Exception as e:
        logger.exception("Deck init failed: %s", e)
        await reply_and_mirror(msg, "Не могу загрузить колоду (assets/cards). Проверь, что папка и 78 файлов карт на месте.")
        return

    # сколько карт (ДОЛЖНО прийти от GPT-роутера)
    n_cards = int(getattr(route, "cards", 0) or 0)
    if n_cards < 1:
        # защита от кривого JSON: но в норме сюда не попадём
        n_cards = 3
    if n_cards > 7:
        n_cards = 7

    # 0) «пост-ответ»
    try:
        await context.bot.send_chat_action(chat_id=msg.chat_id, action=ChatAction.TYPING)
    except Exception:
        _log_exception("suppressed exception")

    user_name = ""
    try:
        p = get_user_profile_chat(user_id, msg.chat_id) or {}
        user_name = (p.get("name") or "").strip()
    except Exception:
        user_name = ""

    intro = _build_intro_post(route, raw_text, n_cards, user_name=user_name)
    try:
        await reply_and_mirror(msg, intro, parse_mode="HTML")
    except Exception:
        try:
            await reply_and_mirror(msg, intro.replace("<b>", "").replace("</b>", ""))
        except Exception:
            _log_exception("suppressed exception")

    # 1) тянем карты
    cards = deck.draw(n_cards)
    logger.warning("TAROT DRAWN n=%s keys=%s", n_cards, [c.key for c in cards])
    if not cards:
        await reply_and_mirror(msg, "Не удалось вытянуть карты. Проверь колоду (assets/cards).")
        return

    # 2) рендер расклада заранее
    card_paths = [deck.abs_path(c.filename) for c in cards]
    out_path = str(TMP_DIR / f"spread_{user_id}_{msg.message_id}.jpg")
    
    try:
        render_spread(str(TABLE_PATH), card_paths, out_path)
    except Exception:
        # Фолбэк: даже если стол/рендер сломался — всё равно формируем картинку с картами.
        logger.exception("spread render failed; fallback to simple renderer")
        try:
            from PIL import Image

            imgs = []
            for cp in card_paths:
                try:
                    im = Image.open(cp).convert("RGB")
                    imgs.append(im)
                except Exception:
                    _log_exception("suppressed exception")

            # если совсем нечего — пробуем хотя бы 1х1 пустышку
            if not imgs:
                Image.new("RGB", (1024, 1024), (15, 15, 18)).save(out_path, "JPEG", quality=92)
            else:
                W = 1024
                H = 1024
                canvas = Image.new("RGB", (W, H), (15, 15, 18))
                # простая сетка 1..7
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
            # последняя линия обороны: пусть хотя бы продолжит без картинки
            pass

    # готовим байты под Telegram
    try:
        bio = _to_telegram_jpeg_bytes(out_path, max_side=1280, quality=85)
    except Exception:
        logger.exception("spread bytes prepare failed")
        bio = None

    # 3) shuffle
    try:
        await _send_shuffle_then_delete(context, msg.chat_id, seconds=SHUFFLE_SECONDS)
    except Exception:
        _log_exception("suppressed exception")

    # 4) отправляем фото расклада
    try:
        await context.bot.send_chat_action(chat_id=msg.chat_id, action=ChatAction.UPLOAD_PHOTO)
    except Exception:
        _log_exception("suppressed exception")

    sent_spread_ok = False
    if bio is not None:
        try:
            await context.bot.send_photo(chat_id=msg.chat_id, photo=InputFile(bio), caption=_build_cards_caption(cards), parse_mode="HTML")
            sent_spread_ok = True
        except BadRequest as e:
            if "Image_process_failed" in str(e):
                safe_path = _repack_for_telegram(out_path)
                try:
                    await context.bot.send_photo(chat_id=msg.chat_id, photo=InputFile(safe_path), caption=_build_cards_caption(cards), parse_mode="HTML")
                    sent_spread_ok = True
                finally:
                    try:
                        if os.path.exists(safe_path):
                            os.remove(safe_path)
                    except Exception:
                        _log_exception("suppressed exception")
            else:
                logger.exception("send_photo BadRequest")
        except Exception:
            logger.exception("send_photo failed")

    if not sent_spread_ok:
    # ЖЕЛЕЗНЫЙ фолбэк: гарантируем, что пользователь увидит карты текстом
        try:
            await context.bot.send_message(chat_id=msg.chat_id, text=_build_cards_caption(cards), parse_mode="HTML")
        except Exception:
            await context.bot.send_message(chat_id=msg.chat_id, text=_build_cards_plain(cards))
            

    # cleanup rendered spread file
    try:
        if out_path and os.path.exists(out_path):
            os.remove(out_path)
    except Exception:
        _log_exception("suppressed exception")

    # 5) payload для GPT
    cards_payload = build_cards_payload(cards)

    # 6) персонализация
    personalization = ""
    try:
        snap = get_followup_personalization_snapshot(user_id) or {}
        profile = {}
        try:
            profile = get_user_profile_chat(user_id, msg.chat_id) or {}
        except Exception:
            profile = {}

        parts = []
        # профиль (чтобы бот был «своим»)
        if profile:
            nm = (profile.get("name") or "").strip()
            if nm:
                parts.append(f"Имя пользователя: {nm}")
            prefs = profile.get("prefs") or profile.get("preferences") or ""
            if isinstance(prefs, str) and prefs.strip():
                parts.append(f"Предпочтения: {prefs.strip()[:200]}")

        lu = (snap.get("last_user_message") or "")[:250]
        lb = (snap.get("last_bot_message") or "")[:250]
        if lu:
            parts.append(f"Последнее сообщение пользователя: {lu}")
        if lb:
            parts.append(f"Последний ответ бота: {lb}")
        last_tarot = snap.get("last_tarot_meta")
        if last_tarot:
            parts.append(f"Последний расклад (meta): {str(last_tarot)[:350]}")
        personalization = "\n".join(parts)[:900]
    except Exception:
        personalization = ""

    # 7) анализ
    try:
        await context.bot.send_chat_action(chat_id=msg.chat_id, action=ChatAction.TYPING)
    except Exception:
        _log_exception("suppressed exception")

    history = _safe_get_last_messages(user_id, msg.chat_id, limit=MAX_HISTORY_MESSAGES)
    answer = await tarot_reading_answer(
        lang="ru",
        user_question=raw_text,
        spread_name=getattr(route, "spread_name", "") or f"{n_cards} карт",
        cards_payload=cards_payload,
        history_hint=personalization,
        history=history,
    )

    answer = _strip_fake_shuffle(answer)

    await send_smart_answer(msg, answer)

    # 8) лог
    try:
        cards_meta = [
            {"key": c.key, "name": (c.meaning.ru_name if c.meaning else c.key), "file": c.filename}
            for c in cards
        ]
        log_tarot_reading(
            user_id,
            question=raw_text,
            spread_name=getattr(route, "spread_name", "") or f"{n_cards} карт",
            cards_meta=cards_meta,
            lang="ru",
        )
    except Exception:
        _log_exception("suppressed exception")

    _safe_set_last_context(user_id, topic="tarot", last_user_message=raw_text, last_bot_message=answer)


# ---------------- HANDLERS ----------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    lang = get_lang(user)

    touch_last_activity(user.id)
    try:
        update_user_identity(
            user.id,
            username=getattr(user, "username", None),
            first_name=getattr(user, "first_name", None),
            last_name=getattr(user, "last_name", None),
        )
    except Exception:
        _log_exception("update_user_identity failed")

    context.chat_data["history"] = []
    context.chat_data["history_by_topic"] = {}
    context.chat_data["pending_batch"] = []
    context.chat_data["batch_task"] = None

    args = context.args
    source = args[0] if args else "organic"
    # referral tracking: /start ref_<inviter_id>
    try:
        if source.startswith("ref_"):
            inviter_id_raw = source.replace("ref_", "").strip()
            inviter_id = int(inviter_id_raw)
            if inviter_id != user.id:
                prof = get_user_profile_chat(user.id, update.effective_chat.id) or {}
                if not prof.get("referral"):
                    patch_user_profile_chat(
                        user.id,
                        update.effective_chat.id,
                        patch={
                            "referral": {
                                "inviter_id": inviter_id,
                                "credited": False,
                                "started_at": dt.datetime.utcnow().isoformat(),
                            }
                        },
                    )
                _safe_log_event(user.id, "ref_start", meta=f"inviter:{inviter_id}", lang=lang, topic="start")
    except Exception:
        _log_exception("referral start parse failed")
    try:
        # persist name/gender on /start
        first_name = (getattr(user, "first_name", "") or "").strip()
        username = (getattr(user, "username", "") or "").strip()
        name_candidate = _normalize_name_from_account(first_name) or _normalize_name_from_account(username)
        patch_profile = {"lang": (lang or "").strip()}
        if name_candidate:
            patch_profile["name"] = name_candidate
            gender_from_name = _infer_gender_from_name(name_candidate)
            if gender_from_name:
                patch_profile["gender"] = gender_from_name
        update_user_profile_chat_if_new_facts(user.id, update.effective_chat.id, patch_profile)
    except Exception:
        _log_exception("start profile enrich failed")

    try:
        set_traffic_source(user.id, source)
    except Exception:
        _log_exception("suppressed exception")
    _safe_log_event(user.id, f"start:{source}", meta=f"source:{source}", lang=lang, topic="start")

    await reply_and_mirror(
        update.message,
        start_text_tarot(),
        parse_mode="HTML",
    )


async def reset_dialog(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    lang = get_lang(user)

    context.chat_data["history"] = []
    context.chat_data["history_by_topic"] = {}
    context.chat_data["pending_batch"] = []
    context.chat_data["batch_task"] = None
    _set_tarot_session_mode(context, enabled=False)

    await reply_and_mirror(update.message, reset_text(lang))

    _safe_log_event(user.id, "reset", lang=lang)


async def _handle_tarot_routing(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    user_id: int,
    lang: str,
    clean_text: str,
    extracted: str,
    topic: str,
) -> bool:
    msg = update.message
    if not msg:
        return False

    if _exit_tarot_mode_requested(clean_text or extracted):
        _set_tarot_session_mode(context, enabled=False)
        _clear_pre_dialog_state(user_id, msg.chat_id)

    # recent follow-up invite: start one-shot clarification on confirmation
    invite = _get_recent_followup_invite(user_id)
    if invite:
        topic = _extract_invite_topic(clean_text or extracted) or invite.get("topic")
        if _exit_tarot_mode_requested(clean_text or extracted):
            return False
        if _is_confirmation_text(clean_text or extracted) or topic:
            try:
                set_last_followup_meta(user_id, followup_type="handled", followup_topic=invite.get("topic") or "")
            except Exception:
                _log_exception("followup invite mark handled failed")

            # use stored invite snapshot if available
            snap = get_followup_personalization_snapshot(user_id) or {}
            invite_q = (snap.get("last_user_message") or "").strip()
            invite_ctx = (snap.get("last_bot_message") or "").strip()
            question_text = invite_q or (clean_text or extracted)
            if invite_ctx:
                question_text = f"{question_text}\n\nКонтекст: {invite_ctx}"

            history_for_router = _safe_get_last_messages(user_id, msg.chat_id, limit=MAX_HISTORY_MESSAGES)
            try:
                route_raw = await route_tarot_action(
                    question_text,
                    lang="ru",
                    history_hint="",
                    history=history_for_router,
                )
            except Exception:
                route_raw = {"action": "reading", "cards": 0, "spread_name": "", "clarify_question": "", "reason": "router_error"}
            route = normalize_route(route_raw)
            if getattr(route, "action", "") == "chat":
                inferred_cards = _infer_cards_count(question_text, has_context=True)
                route = RouteResult(
                    action="reading",
                    cards=inferred_cards,
                    spread_name="Расклад",
                    clarify_question="",
                    reason="force_after_invite",
                )
            _set_tarot_session_mode(context, enabled=True)
            await run_tarot_reading_full(msg, context, user_id, question_text, route)
            return True

    # ✅ Если ранее в рамках расклада мы задали уточняющий вопрос, то следующую
    # реплику трактуем как ответ на уточнение и продолжаем расклад, а не
    # уходим в обычный "chat".
    try:
        profile_chat = get_user_profile_chat(user_id, msg.chat_id) or {}
        pending_tarot = profile_chat.get("pending_tarot") or {}
        if pending_tarot.get("status") == "awaiting_clarification":
            # Снимаем ожидание, чтобы не зациклиться
            _safe_patch_user_profile_chat(user_id, msg.chat_id, delete_keys=["pending_tarot"])

            # В этом месте router_text ещё может не быть инициализирован,
            # поэтому используем уже подготовленный extracted (текст пользователя
            # + reply/forward контекст, если был).
            clarification_text = extracted
            base = (pending_tarot.get("original_text") or "").strip()
            if base:
                combined = f"{base}\n\nУточнение пользователя: {clarification_text}"
            else:
                combined = f"Уточнение пользователя: {clarification_text}"

            forced_route = RouteResult(
                action="reading",
                cards=int(pending_tarot.get("cards") or 3),
                spread_name=(pending_tarot.get("spread_name") or "Расклад"),
                clarify_question="",
                reason="continue_after_clarification",
            )

            _set_tarot_session_mode(context, enabled=True)
            await handle_tarot_flow(update, context, forced_route, combined)
            return True
    except Exception:
        # если что-то пошло не так — просто продолжаем обычный флоу
        _log_exception("suppressed exception")

    # Stage 1: LLM classifier for tarot decision
    trigger_text = _choose_trigger_text(clean_text, extracted)
    if _looks_like_tech_question(trigger_text):
        return False

    history_for_classifier = _safe_get_last_messages(user_id, msg.chat_id, limit=MAX_HISTORY_MESSAGES)
    cls = {}
    try:
        cls = await classify_tarot_intent(
            context=history_for_classifier,
            user_message=trigger_text,
            lang=lang,
        )
    except Exception:
        _log_exception("tarot intent classify failed")
        cls = {}

    should_do = bool(cls.get("should_do_tarot"))
    intent_type = str(cls.get("intent_type") or "normal_chat").strip()
    confidence = float(cls.get("confidence") or 0.0)
    proposed_q = str(cls.get("proposed_question") or "").strip()
    details = cls.get("extracted_details") or {}

    if intent_type == "clarification_needed" and proposed_q:
        await send_smart_answer(msg, proposed_q)
        # важно: сохраняем диалог, иначе модель "не помнит" что уже спрашивала
        user_text_for_db = extracted or clean_text or trigger_text
        _safe_add_user_and_assistant_messages(user_id, msg.chat_id, user_text_for_db, proposed_q)
        _safe_set_last_context(
            user_id,
            topic=topic,
            last_user_message=user_text_for_db,
            last_bot_message=proposed_q,
        )
        return True

    if should_do and confidence >= 0.92:
        # Build enriched question from extracted details
        question_text = trigger_text
        try:
            q = (details.get("question") or "").strip()
            ctx = (details.get("context") or "").strip()
            spread = (details.get("spread_type") or "").strip()
            extra_parts = []
            if q:
                extra_parts.append(f"Вопрос: {q}")
            if ctx:
                extra_parts.append(f"Контекст: {ctx}")
            if spread:
                extra_parts.append(f"Тип расклада: {spread}")
            if extra_parts:
                question_text = f"{trigger_text}\n\n" + "\n".join(extra_parts)
        except Exception:
            pass

        # route tarot (stage 2)
        try:
            route_raw = await route_tarot_action(
                question_text,
                lang="ru",
                history_hint="",
                history=history_for_classifier,
            )
        except Exception:
            route_raw = {"action": "reading", "cards": 0, "spread_name": "", "clarify_question": "", "reason": "router_error"}

        route = normalize_route(route_raw)
        if getattr(route, "action", "") == "chat":
            # force tarot if classifier confirmed
            inferred_cards = _infer_cards_count(question_text, has_context=True)
            route = RouteResult(
                action="reading",
                cards=inferred_cards,
                spread_name="Расклад",
                clarify_question="",
                reason="force_after_classifier",
            )

        _set_tarot_session_mode(context, enabled=True)
        await run_tarot_reading_full(msg, context, user_id, question_text, route)
        return True

    # ✅ FOLLOW-UP после расклада: "подробнее" => расширяем текущие карты, без нового расклада
    try:
        handled = await _handle_tarot_followup(msg, context, user_id, clean_text or extracted)
        if handled:
            _set_tarot_session_mode(context, enabled=True)
            return True
    except Exception:
        # если что-то пошло не так — просто продолжаем обычный флоу
        _log_exception("suppressed exception")

    # Stage 1 classifier already handled tarot routing above.
    return False


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return

    user = update.effective_user
    user_id = user.id
    lang = get_lang(user)
    username = (getattr(user, "username", "") or "").lower().strip()

    touch_last_activity(user_id)
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

    # --- дедуп апдейтов (иногда PTB/сеть дублирует) ---
    try:
        upd_id = int(getattr(update, "update_id", 0) or 0)
        seen = context.chat_data.get("seen_update_ids") or set()
        if upd_id and upd_id in seen:
            return
        if upd_id:
            seen.add(upd_id)
            # не даём сету разрастаться
            if len(seen) > 300:
                # оставляем приблизительно последние 200
                seen = set(list(seen)[-200:])
            context.chat_data["seen_update_ids"] = seen
    except Exception:
        _log_exception("suppressed exception")

    # ✅ ЕДИНЫЙ сбор текста (forward/reply/user)
    clean_text, extracted = extract_message_text(msg, lang=lang)
    logger.info("MSG text received user_id=%s chat_id=%s text=%r", user_id, msg.chat_id, (clean_text or extracted)[:120])

    # если вообще нет текста — выходим (не ломаем)
    if not extracted.strip():
        return

    try:
        await mirror_user_message(context.bot, msg, extracted)
    except Exception:
        _log_exception("admin_forum mirror user failed")

    # last_user_text = "чистый" (для подписи к фото и т.п.)
    if clean_text:
        context.chat_data["last_user_text"] = clean_text

    # ✅ анти forbidden: проверяем по чистому пользовательскому тексту,
    # но если его нет — по extracted
    check_text = clean_text or extracted
    if _exit_tarot_mode_requested(check_text):
        _set_tarot_session_mode(context, enabled=False)
    if is_forbidden_topic(check_text):
        await reply_and_mirror(msg, forbidden_reply(lang))
        _safe_log_event(user_id, "forbidden_text", lang=lang)
        return

    topic = get_current_topic(context)
    await wait_for_media_if_needed(context)

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
            logger.info("PAYWALL trigger user_id=%s chat_id=%s", user_id, msg.chat_id)
            await _send_tarot_paywall(
                msg,
                context,
                user_id=user_id,
                topic=topic,
                last_user_message=check_text,
                lang=lang,
            )
            return

    # --- профиль (user_id + chat_id) обновляем только если появились новые факты ---
    try:
        first_name = (getattr(user, "first_name", "") or "").strip()
        username = (getattr(user, "username", "") or "").strip()
        name_candidate = _normalize_name_from_account(first_name) or _normalize_name_from_account(username)
        patch_profile = {"lang": (lang or "").strip()}
        if name_candidate:
            patch_profile["name"] = name_candidate
            gender_from_name = _infer_gender_from_name(name_candidate)
            if gender_from_name:
                patch_profile["gender"] = gender_from_name
        update_user_profile_chat_if_new_facts(user_id, msg.chat_id, patch_profile)
    except Exception:
        _log_exception("suppressed exception")

    # capture age/gender from user text if present
    try:
        age = _extract_age(clean_text or extracted)
        gender = _extract_gender(clean_text or extracted)
        patch = {}
        if age:
            patch["age"] = age
        if gender:
            patch["gender"] = gender
        if patch:
            update_user_profile_chat_if_new_facts(user_id, msg.chat_id, patch)
    except Exception:
        _log_exception("profile enrich failed")

    # unified tarot routing (shared across text/voice/photo)
    if await _handle_tarot_routing(
        update,
        context,
        user_id=user_id,
        lang=lang,
        clean_text=clean_text,
        extracted=extracted,
        topic=topic,
    ):
        return

    router_text = extracted

    # chat (обычный режим)
    if username not in UNLIMITED_USERNAMES and not check_limit(user_id, msg.chat_id, is_photo=False):
        try:
            set_last_limit_info(user_id, topic=topic, limit_type="text")
        except Exception:
            _log_exception("suppressed exception")

        paywall = ""
        try:
            prof = get_followup_personalization_snapshot(user_id)
            history = _safe_get_last_messages(user_id, msg.chat_id, limit=MAX_HISTORY_MESSAGES)
            paywall = await generate_limit_paywall_text(
                lang=lang,
                limit_type="text",
                topic=topic,
                last_user_message=router_text,
                user_profile=prof,
                history=history,
            )
        except Exception:
            paywall = ""

        try:
            if not should_send_limit_paywall(user_id, paywall):
                return
        except Exception:
            _log_exception("suppressed exception")

        if not paywall:
            return
        final_text = paywall.strip()

        await reply_and_mirror(msg, final_text, reply_markup=_pro_keyboard(lang))
        try:
            set_last_paywall_text(user_id, paywall)
        except Exception:
            _log_exception("suppressed exception")

        try:
            schedule_limit_followup(context.application, user_id, lang)
        except Exception:
            _log_exception("suppressed exception")

        _safe_log_event(user_id, "text_limit_reached", lang=lang, topic=topic, last_limit_type="text")
        return

    # ✅ в batch отправляем extracted (чистый текст + источник), raw_text = clean_text (для подсчёта)
    enriched_text = router_text

    chat_data = context.chat_data
    batch: List[Dict[str, Any]] = chat_data.get("pending_batch") or []
    batch.append(
        {
            "text": enriched_text,
            "raw_text": clean_text or enriched_text,
            "topic": topic,
            "lang": lang,
            "msg": msg,
        }
    )
    chat_data["pending_batch"] = batch

    batch_task = chat_data.get("batch_task")
    if batch_task is None or batch_task.done():
        task = context.application.create_task(_flush_text_batch(context, msg.chat_id, user_id))
        chat_data["batch_task"] = task
