import logging
import asyncio
from html import escape

from telegram.constants import ChatAction

logger = logging.getLogger(__name__)


def split_answer_into_blocks(answer: str):
    """
    Делит ответ на блоки:
    - обычный текст
    - блоки кода между ``` ``` (с возможным указанием языка).

    Возвращает список словарей вида:
    { "type": "text", "content": "..." }
    или
    { "type": "code", "content": "...", "lang": "python" }
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
                    blocks.append({
                        "type": "text",
                        "content": "".join(buf),
                    })
                    buf = []
                in_code = True
                code_lang = fence_lang
            else:
                # закрытие блока кода
                blocks.append({
                    "type": "code",
                    "content": "".join(buf),
                    "lang": code_lang,
                })
                buf = []
                in_code = False
                code_lang = None
        else:
            buf.append(line)

    # остаток
    if buf:
        blocks.append({
            "type": "code" if in_code else "text",
            "content": "".join(buf),
            "lang": code_lang if in_code else None,
        })

    return blocks


async def send_smart_answer(msg, answer: str):
    """
    "Умная" отправка ответа БЕЗ стриминга:
    - делим текст на блоки (обычный текст + ```код```),
    - все текстовые блоки отправляем как обычные сообщения,
    - кодовые блоки отправляем отдельными сообщениями в формате <pre><code> с HTML.
    """
    answer = answer or ""
    blocks = split_answer_into_blocks(answer)

    # если разметки нет — просто одно сообщение целиком
    if not blocks:
        await msg.reply_text(answer)
        return

    for block in blocks:
        btype = block.get("type")
        content = (block.get("content") or "").strip("\n")
        if not content.strip():
            continue

        if btype == "code":
            # отправляем код отдельным сообщением, красиво оформленным
            code_text = escape(content)
            html_code = f"<pre><code>{code_text}</code></pre>"
            try:
                await msg.reply_text(html_code, parse_mode="HTML")
            except Exception as e:
                logger.warning("Не удалось отправить код блоком, шлём plain: %s", e)
                await msg.reply_text(content)
        else:
            # обычный текстовый блок — просто как есть
            await msg.reply_text(content)


async def send_typing_action(bot, chat_id: int, stop_event: asyncio.Event):
    """
    Пока stop_event не выставлен — каждые ~4 сек отправляем 'typing'.
    Telegram держит индикатор 'печатает...' ~5 секунд после каждого вызова.
    """
    try:
        while not stop_event.is_set():
            await bot.send_chat_action(
                chat_id=chat_id,
                action=ChatAction.TYPING,
            )
            await asyncio.sleep(4)
    except Exception as e:
        logger.warning("Ошибка в send_typing_action: %s", e)