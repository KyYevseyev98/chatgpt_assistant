import json
import os
import time
import logging
from typing import Optional

from config import ADMIN_FORUM_CHAT_ID

logger = logging.getLogger(__name__)

_default_threads_path = os.path.join(os.path.dirname(__file__), "admin_forum_threads.json")
THREADS_FILE = os.getenv("ADMIN_FORUM_THREADS_FILE", _default_threads_path)
MAX_TG_LEN = 3500


def _load_threads() -> dict:
    try:
        with open(THREADS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception:
        logger.exception("admin_forum: failed to load threads file")
        return {}


def _save_threads(data: dict) -> None:
    try:
        tmp = f"{THREADS_FILE}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp, THREADS_FILE)
    except Exception:
        logger.exception("admin_forum: failed to save threads file")


async def _ensure_thread(bot, user_id: int, username: str = "", first_name: str = "", last_name: str = "") -> Optional[int]:
    if not ADMIN_FORUM_CHAT_ID:
        logger.warning("admin_forum: ADMIN_FORUM_CHAT_ID not set")
        return None

    data = _load_threads()
    key = str(user_id)
    thread_id = None
    if key in data:
        try:
            thread_id = int(data[key].get("thread_id") or 0) or None
        except Exception:
            thread_id = None

    if thread_id:
        return thread_id

    try:
        topic = await bot.create_forum_topic(chat_id=ADMIN_FORUM_CHAT_ID, name=str(user_id))
        thread_id = getattr(topic, "message_thread_id", None)
        if not thread_id:
            return None
        data[key] = {
            "thread_id": int(thread_id),
            "created_at": int(time.time()),
        }
        _save_threads(data)
        header_parts = [f"user_id: {user_id}"]
        if username:
            header_parts.append(f"@{username}")
        name = " ".join([x for x in [first_name, last_name] if x]).strip()
        if name:
            header_parts.append(name)
        header = " | ".join(header_parts)
        await bot.send_message(
            chat_id=ADMIN_FORUM_CHAT_ID,
            message_thread_id=int(thread_id),
            text=header,
        )
        return int(thread_id)
    except Exception:
        logger.exception("admin_forum: create thread failed")
        return None


async def mirror_user_message(bot, message, text: str) -> None:
    if not ADMIN_FORUM_CHAT_ID:
        logger.warning("admin_forum: ADMIN_FORUM_CHAT_ID not set (user)")
        return
    if not message:
        return
    user = getattr(message, "from_user", None)
    if not user:
        return
    user_id = int(user.id)
    username = (getattr(user, "username", "") or "").strip()
    first_name = getattr(user, "first_name", "") or ""
    last_name = getattr(user, "last_name", "") or ""
    thread_id = await _ensure_thread(bot, user_id, username=username, first_name=first_name, last_name=last_name)
    if not thread_id:
        return
    prefix = f"👤 {user_id}"
    if username:
        prefix += f" @{username}"
    name = " ".join([x for x in [first_name, last_name] if x]).strip()
    if name:
        prefix += f" ({name})"
    body = (text or "").strip()
    if not body:
        return
    try:
        await bot.send_message(
            chat_id=ADMIN_FORUM_CHAT_ID,
            message_thread_id=int(thread_id),
            text=f"{prefix}\n{body}",
        )
    except Exception:
        logger.exception("admin_forum: failed to send user message")


async def mirror_bot_message(message, text: str, *, bot=None) -> None:
    if not ADMIN_FORUM_CHAT_ID:
        logger.warning("admin_forum: ADMIN_FORUM_CHAT_ID not set (bot)")
        return
    if not message:
        return
    user = getattr(message, "from_user", None)
    if not user:
        return
    user_id = int(user.id)
    tg_bot = bot or getattr(message, "bot", None)
    if not tg_bot:
        logger.warning("admin_forum: no bot instance for mirror")
        return
    thread_id = await _ensure_thread(
        tg_bot,
        user_id,
        username=(getattr(user, "username", "") or "").strip(),
        first_name=getattr(user, "first_name", "") or "",
        last_name=getattr(user, "last_name", "") or "",
    )
    if not thread_id:
        return
    body = (text or "").strip()
    if not body:
        return
    prefix = "🤖 "
    chunks = []
    # simple chunking to avoid 4096 limit
    while body:
        chunk = body[: MAX_TG_LEN - len(prefix)]
        chunks.append(chunk)
        body = body[len(chunk) :]
    for i, chunk in enumerate(chunks):
        text_out = f"{prefix}{chunk}" if i == 0 else chunk
        try:
            await tg_bot.send_message(
                chat_id=ADMIN_FORUM_CHAT_ID,
                message_thread_id=int(thread_id),
                text=text_out,
            )
        except Exception:
            logger.exception("admin_forum: failed to send bot message")
