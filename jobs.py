# jobs.py
from typing import Any, Dict

from telegram.ext import Application, ContextTypes

from db import (
    get_user,
    mark_followup_sent,
    set_last_followup_text,
    get_followup_personalization_snapshot,
)
from gpt_client import generate_followup_text
from localization import start_text


def schedule_first_followup(app: Application, user_id: int, lang: str) -> None:
    """
    Ставит одноразовый follow-up через 30 сек только для совсем новых,
    у которых ещё НЕ было follow-up и stage=0.
    """
    (
        _uid,
        _used_text,
        _last_date,
        _is_pro,
        _used_photos,
        _pro_until,
        last_activity_at,
        last_followup_at,
        followup_stage,
    ) = get_user(user_id)

    # если уже что-то отправляли — не надо
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
        _uid,
        _used_text,
        _last_date,
        _is_pro,
        _used_photos,
        _pro_until,
        last_activity_at,
        last_followup_at,
        followup_stage,
    ) = get_user(user_id)

    # если юзер уже что-то написал после /start — не шлём
    if last_activity_at != activity_snapshot:
        return

    # если уже отправили что-то — не шлём
    if last_followup_at is not None or followup_stage > 0:
        return

    # слепок профиля/памяти (может быть пустым — ок)
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

    # сохраняем текст фоллоу-апа, чтобы следующий был другой
    try:
        set_last_followup_text(user_id, text)
    except Exception:
        pass

    mark_followup_sent(user_id)

async def followup_after_limit_job(context: ContextTypes.DEFAULT_TYPE):
    from db import get_all_users_for_followup, should_followup_after_limit
    from gpt_client import generate_followup_text

    users = get_all_users_for_followup()

    for user_id, *_ in users:
        if not should_followup_after_limit(user_id):
            continue

        try:
            text = await generate_followup_text(
                lang="ru",
                ignored_days=1,
                stage=99,  # спец-follow-up
                last_user_message=None,
                last_bot_message=None,
                last_followup_text=None,
            )
            await context.bot.send_message(chat_id=user_id, text=text)
            mark_followup_sent(user_id)
        except Exception:
            continue