from typing import Any, Dict
from datetime import datetime, timedelta

from telegram.ext import Application, ContextTypes

from db import (
    get_user,
    mark_followup_sent,
    set_last_followup_text,
    get_followup_personalization_snapshot,
    get_user_memory_snapshot,
)
from gpt_client import generate_followup_text
from localization import start_text


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

    if last_activity_at != activity_snapshot:
        return
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
    # если нет факта лимита — не отправляем
    if not mem.get("last_limit_topic") or not mem.get("last_limit_type"):
        return

    user_profile = get_followup_personalization_snapshot(user_id)

    # если человек уже активничал — не спамим
    last_activity_at = mem.get("last_activity_at")
    if last_activity_at:
        try:
            dt_act = datetime.fromisoformat(last_activity_at)
            if (datetime.utcnow() - dt_act).total_seconds() < 10 * 60:
                return
        except Exception:
            pass

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