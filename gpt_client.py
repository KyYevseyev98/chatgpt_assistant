from typing import List, Dict
import base64
from io import BytesIO

from openai import OpenAI

from config import OPENAI_API_KEY, MODEL_NAME

# Отдельная модель для картинок (если есть доступ — лучше gpt-4o-mini)
IMAGE_MODEL_NAME = MODEL_NAME  # при желании поменяешь на "gpt-4o-mini"

client = OpenAI(api_key=OPENAI_API_KEY)

FORBIDDEN_KEYWORDS = [
    "api key",
    "api-ключ",
    "openai key",
    "токен",
    "token",
    "какая модель",
    "версия гпт",
    "модель гпт",
    "как тебя сделали",
    "как написать бота",
    "how to build",
    "what model are you",
    "what version",
    "сколько стоят токены",
]


def is_forbidden_topic(text: str) -> bool:
    t = (text or "").lower()
    return any(k in t for k in FORBIDDEN_KEYWORDS)


def lang_instruction(lang: str) -> str:
    if lang.startswith("uk"):
        return "Відповідай українською."
    elif lang.startswith("en"):
        return "Answer in English."
    else:
        return "Отвечай по-русски."


def _base_system_prompt() -> str:
    """
    Описание контекста бота: Telegram, лимиты, PRO, фото и т.д.
    Это видит только модель, пользователь этот текст не видит.
    """
    return (
        "Ты — дружелюбный и лаконичный AI-ассистент внутри Telegram-бота ChatGPT | bot.\n"
        "Важные правила и контекст:\n"
        "1) Ты работаешь именно в Telegram-боте, а не в браузерной версии ChatGPT.\n"
        "2) У бота есть дневные бесплатные лимиты: ограниченное количество текстовых сообщений, аудио сообщений и ограниченное количество анализов фото.\n"
        "3) У некоторых пользователей есть PRO-подписка (оплачивается через Telegram Stars) — для них лимитов нет, ответы могут быть длиннее и быстрее.\n"
        "4) Пользователь может отправлять только ОДНО фото за раз для анализа. Если он спрашивает про несколько фото — объясни, что бот обрабатывает по одному фото за запрос.\n"
        "5) Ты НЕ управляешь подписками и оплатой, не можешь сам включать/отключать PRO. Если спрашивают — объясни общую логику (есть бесплатный лимит и PRO через звёзды), но без технических деталей.\n"
        "6) Никогда не обсуждай внутренние параметры модели, версии GPT, токены, ключи, стоимость API и внутреннюю реализацию бота. "
        "Если спрашивают об этом — мягко уходи от темы и переключайся на полезный ответ по сути вопроса.\n"
        "Отвечай понятно, по делу, без лишней воды.\n"
        
    )


async def ask_gpt(history: List[Dict[str, str]], lang: str) -> str:
    """
    Обычный текстовый запрос.
    """
    system_prompt = _base_system_prompt() + lang_instruction(lang)

    messages = [{"role": "system", "content": system_prompt}] + history

    resp = client.chat.completions.create(
        model=MODEL_NAME,
        messages=messages,
    )
    return resp.choices[0].message.content.strip()


async def ask_gpt_with_image(
    history: List[Dict[str, str]],
    lang: str,
    image_bytes: bytes,
    user_question: str,
) -> str:
    """
    Мультимодальный запрос: история + текстовый вопрос + изображение.
    history — история только с текстовыми сообщениями.
    """
    system_prompt = (
        _base_system_prompt()
        + "Пользователь прислал изображение. Отвечай, опираясь и на картинку, и на текст вопроса.\n"
        + lang_instruction(lang)
    )

    # кодируем фото в base64
    b64_image = base64.b64encode(image_bytes).decode("utf-8")
    data_url = f"data:image/jpeg;base64,{b64_image}"

    messages = [{"role": "system", "content": system_prompt}] + history

    messages.append(
        {
            "role": "user",
            "content": [
                {"type": "text", "text": user_question},
                {"type": "image_url", "image_url": {"url": data_url}},
            ],
        }
    )

    resp = client.chat.completions.create(
        model=IMAGE_MODEL_NAME,
        messages=messages,
    )
    return resp.choices[0].message.content.strip()


async def transcribe_voice(voice_bytes: bytes) -> str:
    """
    Расшифровка голосового в текст с помощью Whisper.
    Возвращает строку-расшифровку.
    """
    audio_file = BytesIO(voice_bytes)
    audio_file.name = "voice.ogg"  # имя нужно клиенту OpenAI

    resp = client.audio.transcriptions.create(
        model="whisper-1",
        file=audio_file,
    )
    # В новом клиенте ответ обычно в поле .text
    text = getattr(resp, "text", "").strip()
    return text