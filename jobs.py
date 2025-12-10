import datetime as dt
from typing import Any, Dict

from telegram.ext import Application, ContextTypes

from db import get_user, mark_followup_sent
from gpt_client import generate_followup_text
from localization import start_text


def schedule_first_followup(app: Application, user_id: int, lang: str) -> None:
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

    if last_followup_at is not None or followup_stage > 0:
        return

    if app.job_queue is None:
        return  # перестраховка

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

    if last_activity_at != activity_snapshot:
        return

    if last_followup_at is not None or followup_stage > 0:
        return

    # генерим текст (stage=0, ignored_days=0)
    greeting = start_text(lang)  # то самое первое приветственное сообщение

    text = await generate_followup_text(
        lang=lang,
        ignored_days=0,
        stage=0,
        last_user_message=None,
        last_bot_message=greeting,       # <-- передаём приветствие сюда
        last_followup_text=None,
    )

    await context.bot.send_message(chat_id=user_id, text=text)
    mark_followup_sent(user_id)