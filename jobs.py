# jobs.py
from typing import Any, Dict, List, Tuple
from datetime import datetime
import random
import json

from telegram.ext import Application, ContextTypes

from db import (
    get_user,
    mark_followup_sent,
    set_last_followup_text,
    set_last_followup_meta,
    get_followup_personalization_snapshot,
    get_user_memory_snapshot,
    log_event,
)
from gpt_client import generate_followup_text
from localization import start_text


# ---------------------------------------------------------
# Follow-up pools (ignore users)
# ---------------------------------------------------------
_INVITE_TOPICS = {
    "love": [
        "Кто сейчас думает о тебе",
        "Есть ли тайная влюблённость",
        "Любовный прогноз на ближайшие дни",
        "Кто скоро намекнёт тебе на чувства",
        "Кто из прошлого всё ещё вспоминает тебя",
        "Что он/она сейчас чувствует к тебе",
        "Будет ли неожиданный контакт или сообщение",
    ],
    "future": [
        "На правильном ли ты пути сейчас",
        "Чем тебя удивит завтрашний день",
        "Как пройдёт эта неделя",
        "Что тебя скоро порадует",
        "Какое приятное событие уже рядом",
        "На что стоит обратить внимание в ближайшие дни",
    ],
    "money": [
        "Что будет с финансами в этом месяце",
        "Ждёт ли тебя денежный шанс",
        "Что важно не упустить в работе или проекте",
        "Где сейчас твоя точка роста",
    ],
}

_INVITE_TEMPLATES = [
    "Есть ощущение, что тебе может откликнуться тема:\n"
    "«{topic}».\n"
    "Если хочешь — мягко посмотрю это через карты.\n"
    "Я рядом.",
    "Иногда полезно подсветить важное через расклад.\n"
    "Могу глянуть по картам тему «{topic}».\n"
    "Без спешки — как тебе комфортно.\n"
    "Я рядом 🃏",
    "Если хочется немного ясности,\n"
    "могу посмотреть тему «{topic}» через карты.\n"
    "Спокойно, мягко, без давления.\n"
    "Я рядом.",
    "Есть тема, которая часто волнует:\n"
    "«{topic}».\n"
    "Если захочешь — сделаю расклад и разберём вместе.\n"
    "Я рядом ✨",
]

_CARE_MESSAGES = [
    "Просто решила напомнить, что я рядом 🙂 Если захочешь поговорить — пиши.",
    "Как ты сейчас? Иногда полезно просто выговориться, даже без запроса.",
    "Если захочешь поговорить — я на связи.",
]

_MICRO_VALUE_MESSAGES = [
    "Маленькая мысль: если перегруз — не обязательно решать всё сразу. Один честный шаг уже меняет направление.",
    "Напоминание: даже маленькие шаги сегодня — это большие результаты завтра. Если захочешь поговорить — пиши.",
]


def _pick_invite_topic(last_topic: str) -> Tuple[str, str]:
    topics = list(_INVITE_TOPICS.keys())
    if last_topic in topics and len(topics) > 1:
        topics.remove(last_topic)
    chosen = random.choice(topics)
    topic_text = random.choice(_INVITE_TOPICS[chosen])
    return chosen, topic_text


def _build_ignore_followup(user_id: int, stage: int) -> Tuple[str, str, str]:
    """
    Returns (text, followup_type, followup_topic)
    followup_type: tarot_invite | care | micro
    followup_topic: love | future | money | ""
    """
    mem = get_user_memory_snapshot(user_id) or {}
    last_text = (mem.get("last_followup_text") or "").strip()
    last_type = (mem.get("last_followup_type") or "").strip()
    last_topic = (mem.get("last_followup_topic") or "").strip()

    roll = random.random()
    if roll < 0.8:
        f_type = "tarot_invite"
        topic_key, topic_text = _pick_invite_topic(last_topic)
        template = random.choice(_INVITE_TEMPLATES)
        text = template.format(topic=topic_text)
        # avoid repeating exact text
        if text == last_text:
            template = random.choice([t for t in _INVITE_TEMPLATES if t != template] or _INVITE_TEMPLATES)
            text = template.format(topic=topic_text)
        return text, f_type, topic_key

    if roll < 0.95:
        f_type = "care"
        text = random.choice([t for t in _CARE_MESSAGES if t != last_text] or _CARE_MESSAGES)
        return text, f_type, ""

    f_type = "micro"
    text = random.choice([t for t in _MICRO_VALUE_MESSAGES if t != last_text] or _MICRO_VALUE_MESSAGES)
    return text, f_type, ""


async def send_ignore_followup(context: ContextTypes.DEFAULT_TYPE, user_id: int, lang: str, stage: int) -> None:
    text, f_type, f_topic = _build_ignore_followup(user_id, stage)
    await context.bot.send_message(chat_id=user_id, text=text)
    try:
        set_last_followup_text(user_id, text)
        set_last_followup_meta(user_id, followup_type=f_type, followup_topic=f_topic)
    except Exception:
        pass
    try:
        log_event(
            user_id,
            "followup_sent",
            meta=json.dumps({"type": f_type, "topic": f_topic, "stage": stage}, ensure_ascii=False),
            topic="followup",
        )
    except Exception:
        pass
    mark_followup_sent(user_id)


def schedule_first_followup(app: Application, user_id: int, lang: str) -> None:
    """
    Одноразовый follow-up через 30 сек для совсем новых:
    у которых ещё НЕ было follow-up и stage=0.
    """
    (
        _uid, _used_text, _last_date, _is_pro, _used_photos,
        _pro_until, last_activity_at, last_followup_at, followup_stage
    ) = get_user(user_id)

    if last_followup_at is not None or followup_stage > 0:
        return
    if app.job_queue is None:
        return

    app.job_queue.run_once(
        first_followup_job,
        when=30,
        name=f"first_followup_{user_id}",
        data={
            "user_id": user_id,
            "lang": lang,
            "activity_snapshot": last_activity_at,
        },
    )


async def first_followup_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    data: Dict[str, Any] = context.job.data or {}
    user_id = data.get("user_id")
    lang = data.get("lang", "ru")
    activity_snapshot = data.get("activity_snapshot")

    if not user_id:
        return

    (
        _uid, _used_text, _last_date, _is_pro, _used_photos,
        _pro_until, last_activity_at, last_followup_at, followup_stage
    ) = get_user(user_id)

    # если юзер уже что-то написал после /start — не шлём
    if last_activity_at != activity_snapshot:
        return

    # если уже отправляли что-то — не шлём
    if last_followup_at is not None or followup_stage > 0:
        return

    user_profile = get_followup_personalization_snapshot(user_id)
    greeting = start_text(lang)

    text = await generate_followup_text(
        lang=lang,
        ignored_days=0,
        stage=0,
        last_user_message=None,
        last_bot_message=greeting,
        last_followup_text=None,
        user_profile=user_profile,
    )

    await context.bot.send_message(chat_id=user_id, text=text)

    try:
        set_last_followup_text(user_id, text)
    except Exception:
        pass

    mark_followup_sent(user_id)


def schedule_limit_followup(app: Application, user_id: int, lang: str) -> None:
    """
    Follow-up после упора в лимит: через 25 минут, один раз.
    """
    if app.job_queue is None:
        return

    app.job_queue.run_once(
        limit_followup_job,
        when=25 * 60,
        name=f"limit_followup_{user_id}",
        data={"user_id": user_id, "lang": lang},
    )


async def limit_followup_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    data: Dict[str, Any] = context.job.data or {}
    user_id = data.get("user_id")
    lang = data.get("lang", "ru")
    if not user_id:
        return

    mem = get_user_memory_snapshot(user_id)

    # должен быть факт лимита + факт paywall
    if not mem.get("last_limit_type") or not mem.get("last_paywall_at"):
        return

    # лимит должен быть "свежим" (например, за последние 6 часов)
    last_limit_at = mem.get("last_limit_at")
    if last_limit_at:
        try:
            dt_limit = datetime.fromisoformat(last_limit_at)
            if (datetime.utcnow() - dt_limit).total_seconds() > 6 * 3600:
                return
        except Exception:
            pass

    # антиспам: если юзер активничал недавно — не шлём
    try:
        (
            _uid, _used_text, _last_date, _is_pro, _used_photos,
            _pro_until, last_activity_at, _last_followup_at, _followup_stage
        ) = get_user(user_id)

        if last_activity_at:
            dt_act = datetime.fromisoformat(last_activity_at)
            if (datetime.utcnow() - dt_act).total_seconds() < 10 * 60:
                return
    except Exception:
        pass

    user_profile = get_followup_personalization_snapshot(user_id)

    text = await generate_followup_text(
        lang=lang,
        ignored_days=0,
        stage=99,
        last_user_message=mem.get("last_user_message"),
        last_bot_message=mem.get("last_bot_message"),
        last_followup_text=mem.get("last_followup_text"),
        user_profile=user_profile,
    )

    await context.bot.send_message(chat_id=user_id, text=text)

    try:
        set_last_followup_text(user_id, text)
    except Exception:
        pass

    mark_followup_sent(user_id)
