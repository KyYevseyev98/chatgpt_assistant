# handlers/common.py
import logging
import asyncio
from html import escape

from telegram.constants import ChatAction

logger = logging.getLogger(__name__)


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
                    blocks.append(
                        {
                            "type": "text",
                            "content": "".join(buf),
                        }
                    )
                    buf = []
                in_code = True
                code_lang = fence_lang
            else:
                # закрытие блока кода
                blocks.append(
                    {
                        "type": "code",
                        "content": "".join(buf),
                        "lang": code_lang,
                    }
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
    - текстовые блоки отправляем как HTML (поддержка <b>, <i> и т.п.),
    - кодовые блоки — отдельными сообщениями <pre><code>…</code></pre>.

    reply_markup (клавиатура) вешаем только к ПЕРВОМУ текстовому сообщению,
    чтобы кнопки не дублировались.
    """
    answer = answer or ""
    blocks = split_answer_into_blocks(answer)

    # если разметки нет — одно сообщение целиком
    if not blocks:
        await message.reply_text(
            answer,
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
            # отправляем код отдельным сообщением, красиво оформленным
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
            # обычный текстовый блок (с <b>, эмодзи и т.п.)
            try:
                if not first_text_sent:
                    await message.reply_text(
                        content,
                        reply_markup=reply_markup,
                        parse_mode="HTML",
                        disable_web_page_preview=True,
                    )
                    first_text_sent = True
                else:
                    await message.reply_text(
                        content,
                        parse_mode="HTML",
                        disable_web_page_preview=True,
                    )
            except Exception as e:
                # если телега ругнулась на HTML (например, сломанный тег) — шлём как plain
                logger.warning("Ошибка при отправке HTML-блока, шлём plain: %s", e)
                if not first_text_sent:
                    await message.reply_text(content, reply_markup=reply_markup)
                    first_text_sent = True
                else:
                    await message.reply_text(content)


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