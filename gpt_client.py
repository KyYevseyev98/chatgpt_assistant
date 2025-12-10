#gpt_client.py

from typing import List, Dict, Optional
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
        "Ты — дружелюбный и лаконичный AI-ассистент внутри Telegram-бота FOXY.\n"
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

async def generate_followup_text(
    lang: str,
    ignored_days: int,
    stage: int,
    last_user_message: Optional[str] = None,
    last_bot_message: Optional[str] = None,
    last_followup_text: Optional[str] = None,
) -> str:
    """
    Генерирует короткое follow-up сообщение от Foxy с учётом контекста:
    - сколько дней человек молчит
    - какой по счёту follow-up
    - опционально: последний вопрос, последний ответ, последний follow-up
    """
    context_block = ""

    if last_user_message:
        context_block += f"Последний вопрос пользователя: «{last_user_message[:400]}».\n"
    if last_bot_message:
        context_block += f"Твой последний ответ ему: «{last_bot_message[:400]}».\n"
    if last_followup_text:
        context_block += (
            f"Последнее напоминание, которое ты отправлял: «{last_followup_text[:400]}».\n"
            "Сделай новое сообщение другим по формулировкам, не повторяй его дословно.\n"
        )

    if not context_block:
        context_block = (
            "Контекст диалога отсутствует. Пользователь запускал бота, "
            "но давно ничего не писал.\n"
        )

    if lang.startswith("uk"):
        lang_block = "Пиши українською, легко й дружньо."
    elif lang.startswith("en"):
        lang_block = "Write in English, friendly and concise."
    else:
        lang_block = "Пиши по-русски, дружелюбно и по делу."

    # Спец-инструкция для самого первого follow-up сразу после /start
    first_followup_hint = ""
    if ignored_days == 0 and stage == 0:
        first_followup_hint = (
            "Ситуация: користувач/пользователь тільки що запустил бота, уже увидел "
            "приветственное сообщение (например, 'Привет! Я AI-ассистент...'), но сам "
            "ничего не написал.\n"
            "В ЭТОМ случае:\n"
            "- не начинай текст со слова 'Привет' / 'Hi' / 'Hello';\n"
            "- не представляйся заново и не повторяй, что ты AI-ассистент;\n"
            "- сделай одно мягкое напоминание в духе: если появится вопрос — просто напиши, ты здесь и готов помочь.\n"
        )

    system_prompt = (
        "Ты — Foxy, дружелюбный AI-ассистент в Telegram-боте.\n"
        "Твоя задача — аккуратно напомнить пользователю о себе и пригласить продолжить диалог.\n"
        "Не дави, не выпрашивай денег, не извиняйся по 10 раз.\n"
        "Формат сообщения: 1–3 коротких предложения, максимум 2–4 строки.\n"
        "Без эмодзи в начале строки, максимум 1–2 эмодзи в конце, если уместно.\n"
        "Не используй разметку, только обычный текст.\n"
        f"{lang_block}\n"
        f"\nИнформация о контексте:\n{context_block}\n"
        f"Пользователь молчит уже {ignored_days} дней. Текущий номер напоминания (от 0): {stage}.\n"
        f"{first_followup_hint}"
        "Сделай текст так, чтобы он ощущался живым, но не навязчивым.\n"
    )

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": "Сгенерируй текст такого follow-up сообщения."},
    ]

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