# handlers/common.py
import logging
import asyncio
import re
from html import escape
import html
from typing import Optional, Dict, Any, List, Tuple

from telegram.constants import ChatAction
from config import MAX_HISTORY_CHARS, MAX_HISTORY_MESSAGES

logger = logging.getLogger(__name__)

_ALLOWED_TAGS = {"b", "i", "u", "s", "code", "pre"}


# =========================================================
# MESSAGE TEXT EXTRACTOR (ЕДИНЫЙ ИСТОЧНИК ИСТИНЫ)
# =========================================================
def _cut(s: Optional[str], n: int) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    return s[:n]


def _get_message_main_text(msg) -> str:
    """
    Основной текст сообщения:
    - text для обычных сообщений
    - caption для медиа (photo/document/etc)
    """
    if not msg:
        return ""
    # PTB: msg.text / msg.caption
    base = (getattr(msg, "text", None) or getattr(msg, "caption", None) or "").strip()
    return base


def _get_forwarded_text(msg) -> str:
    """
    Достаём 'пересланное' содержимое максимально безопасно.
    В Telegram forwarded message может не иметь "оригинального текста" отдельно,
    поэтому считаем forwarded == msg.text/caption, но помечаем источник.
    """
    if not msg:
        return ""

    # Признаки форварда в PTB (v20+): forward_origin, forward_date, forward_from, forward_sender_name, forward_from_chat
    forward_origin = getattr(msg, "forward_origin", None)
    forward_date = getattr(msg, "forward_date", None)
    forward_from = getattr(msg, "forward_from", None)
    forward_sender_name = getattr(msg, "forward_sender_name", None)
    forward_from_chat = getattr(msg, "forward_from_chat", None)

    is_forwarded = any([forward_origin, forward_date, forward_from, forward_sender_name, forward_from_chat])
    if not is_forwarded:
        return ""

    # Сам текст/подпись форварда обычно и есть msg.text/caption
    return _get_message_main_text(msg)


def _get_reply_to_text(msg) -> str:
    """
    Достаём текст сообщения, на которое ответили (reply_to_message).
    """
    if not msg:
        return ""
    reply = getattr(msg, "reply_to_message", None)
    if not reply:
        return ""
    replied_text = (getattr(reply, "text", None) or getattr(reply, "caption", None) or "").strip()
    return replied_text


def extract_message_text(
    msg,
    *,
    # Позволяет переопределить "main" текст для нестандартных типов сообщений.
    # Пример: для voice мы делаем main = transcript.
    override_main_text: Optional[str] = None,
    max_main: int = 4000,
    max_reply: int = 800,
    max_forwarded: int = 1200,
) -> Dict[str, Any]:
    """
    Единая функция извлечения текста для ВСЕХ типов сообщений (text/photo/voice/etc).

    Возвращает dict:
    {
      "clean_text": "...",          # итоговый комбинированный текст для модели
      "parts": {                    # исходники
         "main": "...",             # msg.text / msg.caption
         "reply_to": "...",         # reply_to_message.text/caption
         "forwarded": "...",        # msg.text/caption, если это forwarded
      },
      "flags": {
         "is_reply": bool,
         "is_forwarded": bool,
         "has_caption": bool,
         "has_text": bool,
      }
    }

    ВАЖНО: "clean_text" включает источники:
      FORWARDED: ...
      REPLY_TO: ...
      USER: ...
    """
    main_raw = (override_main_text if override_main_text is not None else _get_message_main_text(msg))
    main = _cut(main_raw, max_main)
    reply_to = _cut(_get_reply_to_text(msg), max_reply)
    forwarded = _cut(_get_forwarded_text(msg), max_forwarded)

    has_text = bool(getattr(msg, "text", None))
    has_caption = bool(getattr(msg, "caption", None))

    is_forwarded = bool(forwarded)
    is_reply = bool(reply_to)

    # Собираем clean_text с источниками.
    # Приоритет логики:
    # 1) если есть reply_to — добавляем REPLY_TO (чтобы "это" понималось)
    # 2) если forwarded — добавляем FORWARDED
    # 3) затем USER (то что юзер написал сейчас)
    # Чтобы вопрос "а вот это что значит?" имел контекст.
    blocks: List[str] = []

    if is_reply and reply_to:
        blocks.append(f'REPLY_TO:\n"{reply_to}"')

    if is_forwarded and forwarded:
        blocks.append(f'FORWARDED:\n"{forwarded}"')

    if main:
        blocks.append(f"USER:\n{main}")

    clean_text = "\n\n".join(blocks).strip()

    return {
        "clean_text": clean_text,
        "parts": {
            "main": main,
            "reply_to": reply_to,
            "forwarded": forwarded,
        },
        "flags": {
            "is_reply": is_reply,
            "is_forwarded": is_forwarded,
            "has_caption": has_caption,
            "has_text": has_text,
        },
    }


# =========================================================
# HTML SANITIZER
# =========================================================
def sanitize_html_keep_basic(text: str) -> str:
    """
    Делает HTML безопасным для Telegram parse_mode=HTML:
    - разрешает только базовые теги (<b>, <i>, <u>, <s>, <code>, <pre>)
    - экранирует всё остальное
    - чинит незакрытые теги
    - не ломает emoji
    """
    if not text:
        return ""

    # 1) нормализуем переносы
    t = text.replace("\r\n", "\n").replace("\r", "\n")

    # 2) временно защищаем разрешённые теги
    # ВАЖНО:
    # regex groups:
    #   group(1) -> "/" (если закрывающий тег)
    #   group(2) -> имя тега
    def _protect_tag(m: re.Match) -> str:
        slash = m.group(1) or ""
        tag = (m.group(2) or "").lower()
        if tag in _ALLOWED_TAGS:
            # сохраняем тег маркером
            return f"[[[TAG:{slash}{tag}]]]"
        # всё остальное пусть потом экранируется
        return m.group(0)

    t = re.sub(
        r"<\s*(/?)\s*([a-zA-Z0-9]+)\s*>",
        _protect_tag,
        t,
    )

    # 3) экранируем всё как обычный текст
    # (включая остаточные < > &)
    t = html.escape(t, quote=False)

    # 4) возвращаем разрешённые теги обратно
    def _restore_tag(m: re.Match) -> str:
        slash = "/" if m.group(1) else ""
        tag = m.group(2)
        return f"<{slash}{tag}>"

    t = re.sub(
        r"\[\[\[TAG:(/)?([a-z0-9]+)\]\]\]",
        _restore_tag,
        t,
    )

    # 5) балансировка тегов (закрываем незакрытые)
    for tag in _ALLOWED_TAGS:
        opens = len(re.findall(fr"<{tag}>", t))
        closes = len(re.findall(fr"</{tag}>", t))
        if opens > closes:
            t += ("</" + tag + ">") * (opens - closes)

    return t


# =========================================================
# HISTORY TRIMMER
# =========================================================
def trim_history_for_model(
    history: List[Dict[str, Any]],
    *,
    max_chars: int = MAX_HISTORY_CHARS,
    max_items: int = MAX_HISTORY_MESSAGES,
) -> List[Dict[str, Any]]:
    """
    Trim history by max items and total characters to control token usage.
    Keeps the most recent messages.
    """
    if not history:
        return []
    items = history[-max_items:] if max_items else list(history)
    if not max_chars:
        return items

    total = 0
    trimmed: List[Dict[str, Any]] = []
    # iterate from newest to oldest, then reverse
    for m in reversed(items):
        content = (m.get("content") or "").strip()
        role = (m.get("role") or "").strip()
        size = len(content) + len(role) + 2
        if total + size > max_chars:
            continue
        trimmed.append({"role": role, "content": content})
        total += size
    trimmed.reverse()
    return trimmed


# =========================================================
# MEDIA LOCK (ВАЖНО)
# =========================================================
def get_media_lock(context) -> asyncio.Lock:
    """
    Один общий lock на чат, чтобы текст не обгонял обработку фото/voice.
    """
    lock = context.chat_data.get("media_lock")
    if lock is None:
        lock = asyncio.Lock()
        context.chat_data["media_lock"] = lock
    return lock


async def wait_for_media_if_needed(context) -> None:
    """
    Если в данный момент идёт обработка photo/voice — ждём, пока закончится.
    """
    lock = context.chat_data.get("media_lock")
    if not lock:
        return
    try:
        if lock.locked():
            await lock.acquire()
            lock.release()
    except Exception:
        # если что-то пошло не так — не ломаем чат
        return


# =========================================================
# SMART SENDER
# =========================================================
def split_answer_into_blocks(answer: str):
    """
    Делит ответ на блоки:
    - обычный текст
    - блоки кода между ``` ```

    Возвращает список словарей:
    {"type": "text", "content": "..."}
    или
    {"type": "code", "content": "...", "lang": "python"}
    """
    blocks = []
    if not answer:
        return blocks

    in_code = False
    code_lang = None
    buf = []

    lines = answer.splitlines(keepends=True)

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("```"):
            fence_lang = stripped[3:].strip() or None

            if not in_code:
                # открытие блока кода
                if buf:
                    blocks.append({"type": "text", "content": "".join(buf)})
                    buf = []
                in_code = True
                code_lang = fence_lang
            else:
                # закрытие блока кода
                blocks.append(
                    {"type": "code", "content": "".join(buf), "lang": code_lang}
                )
                buf = []
                in_code = False
                code_lang = None
        else:
            buf.append(line)

    # остаток
    if buf:
        blocks.append(
            {
                "type": "code" if in_code else "text",
                "content": "".join(buf),
                "lang": code_lang if in_code else None,
            }
        )

    return blocks


async def send_smart_answer(message, answer: str, reply_markup=None):
    """
    "Умная" отправка ответа:
    - делим текст на блоки (обычный текст + ```код```),
    - текстовые блоки отправляем как HTML,
    - кодовые блоки — отдельными сообщениями <pre><code>…</code></pre>.

    reply_markup (клавиатура) вешаем только к ПЕРВОМУ текстовому сообщению.
    """
    answer = answer or ""
    blocks = split_answer_into_blocks(answer)

    if not blocks:
        safe = sanitize_html_keep_basic(answer)
        await message.reply_text(
            safe,
            reply_markup=reply_markup,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
        return

    first_text_sent = False

    for block in blocks:
        btype = block.get("type")
        content = (block.get("content") or "").strip("\n")
        if not content.strip():
            continue

        if btype == "code":
            code_text = escape(content)
            html_code = f"<pre><code>{code_text}</code></pre>"
            try:
                await message.reply_text(
                    html_code,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )
            except Exception as e:
                logger.warning("Не удалось отправить код блоком, шлём plain: %s", e)
                await message.reply_text(content)
        else:
            safe = sanitize_html_keep_basic(content)
            try:
                if not first_text_sent:
                    await message.reply_text(
                        safe,
                        reply_markup=reply_markup,
                        parse_mode="HTML",
                        disable_web_page_preview=True,
                    )
                    first_text_sent = True
                else:
                    await message.reply_text(
                        safe,
                        parse_mode="HTML",
                        disable_web_page_preview=True,
                    )
            except Exception as e:
                logger.warning("Ошибка при отправке HTML-блока, шлём plain: %s", e)
                plain = re.sub(r"<[^>]+>", "", content)
                if not first_text_sent:
                    await message.reply_text(plain, reply_markup=reply_markup)
                    first_text_sent = True
                else:
                    await message.reply_text(plain)


# =========================================================
# TYPING ACTION
# =========================================================
async def send_typing_action(bot, chat_id: int, stop_event: asyncio.Event):
    """
    Пока stop_event не выставлен — каждые ~4 сек отправляем 'typing'.
    """
    try:
        while not stop_event.is_set():
            await bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)
            await asyncio.sleep(4)
    except Exception as e:
        logger.warning("Ошибка в send_typing_action: %s", e)
