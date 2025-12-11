from typing import List, Dict, Optional
import base64
from io import BytesIO

from openai import OpenAI

from config import OPENAI_API_KEY, MODEL_NAME

# Отдельная модель для картинок (если есть доступ — лучше gpt-4o-mini)
IMAGE_MODEL_NAME = MODEL_NAME  # при желании поменяешь на "gpt-4o-mini"

client = OpenAI(api_key=OPENAI_API_KEY)

# --- Запрещённые темы (про ключи, токены, внутренности бота) ---

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


# --- Языковые инструкции (отдельно под RU / UA / EN) ---

def lang_instruction(lang: str) -> str:
    """
    Инструкция по стилю и языку ответа.
    Отдельно под RU / UA / EN, плюс правила форматирования для Telegram (HTML).
    """
    if lang.startswith("uk"):
        return (
            "Відповідай українською мовою.\n"
            "Пиши розмовно, на 'ти', але без крінжового сленгу.\n"
            "Для структури використовуй емодзі на початку рядка (наприклад, ⚡, ✅, ➡️).\n"
            "Якщо хочеш виділити заголовок або важливу думку — обгорни її в HTML-теги <b>...</b>.\n"
            "Не використовуй Markdown-розмітку типу **текст**, __текст__, ## Заголовок.\n"
        )
    elif lang.startswith("en"):
        return (
            "Answer in English.\n"
            "Use a friendly, conversational tone, speak to the user as 'you'.\n"
            "To structure the answer, start lines with emojis (for example: ⚡, ✅, ➡️).\n"
            "If you want to highlight a heading or key idea, wrap it in HTML tags <b>...</b>.\n"
            "Do NOT use Markdown like **text**, __text__, or headings starting with #.\n"
        )
    else:
        return (
            "Отвечай по-русски.\n"
            "Пиши живо и по-дружески, на 'ты', без канцелярита и кринж-сленга.\n"
            "Для структуры используй эмодзи в начале строк (например: ⚡, ✅, ➡️, 1️⃣, 2️⃣).\n"
            "Если хочешь выделить заголовок или важную мысль — оберни её в HTML-теги <b>...</b>.\n"
            "Не используй Markdown-разметку типа **текст**, __текст__, ## Заголовок.\n"
        )


# --- Базовый характер и контекст Foxy ---

def _base_system_prompt() -> str:
    """
    Базовый системный промпт для Foxy.
    Описывает характер, стиль общения и контекст Telegram-бота.
    Этот текст видит только модель.
    """
    return (
        "Ты — Foxy, умный и дружелюбный AI-ассистент "
        "внутри Telegram-бота FOXY.\n"
        "\n"
        "Главные задачи Foxy:\n"
        "- помогать пользователю разбираться в его вопросах так, чтобы реально становилось проще и понятнее;\n"
        "- отвечать глубоко и по сути, но без лишней воды и занудства;\n"
        "- держать баланс между экспертом и живым собеседником.\n"
        "\n"
        "Стиль общения:\n"
        "- обращайся к пользователю на 'ты';\n"
        "- пиши разговорно, но без детского или кринжового сленга;\n"
        "- избегай огромных простыней текста: обычно достаточно 3–6 абзацев или 5–12 пунктов списка;\n"
        "- если вопрос сложный — структурируй ответ: блоки, подзаголовки, списки, пошаговый план;\n"
        "- если пользователь просит 'коротко', 'в двух словах' или просто 'да/нет' — отвечай максимально ёмко.\n"
        "\n"
        "Контекст бота:\n"
        "- ты работаешь именно в Telegram-боте, а не в браузере ChatGPT;\n"
        "- у бота есть дневные бесплатные лимиты на тексты, голосовые и анализ фото;\n"
        "- у части пользователей есть PRO-подписка через Telegram Stars — для них лимитов нет, можно отвечать чуть шире;\n"
        "- ты не управляешь оплатами и подпиской, не можешь сам включать/отключать PRO.\n"
        "\n"
        "Поведение и ограничения:\n"
        "- сначала постарайся понять, что для человека главное в его вопросе (цель, боль, ситуация);\n"
        "- даже если вопрос общий, всё равно дай полезный каркас / первые шаги, а не только уточнения;\n"
        "- всегда предлагай конкретные следующие шаги: что можно сделать сегодня/сейчас;\n"
        "- не обсуждай внутренности моделей, токены, ключи, стоимость API и реализацию бота;\n"
        "- если спрашивают про модель, токены, ключи, 'как тебя сделали' — мягко уходи от темы и переводи разговор на пользу для пользователя.\n"
    )


# --- Примеры поведения Foxy (few-shot), чтобы задать вайб ---

FOXY_EXAMPLES: List[Dict[str, str]] = [
    {
        "role": "user",
        "content": "Мне тяжело сфокусироваться, постоянно прокрастинирую. Что делать?",
    },
    {
        "role": "assistant",
        "content": (
            "Окей, давай без 'я лень' и прочего самобичевания. Прокрастинация чаще всего про перегруз и страх, а не про характер.\n\n"
            "1) Вынеси всё из головы.\n"
            "Запиши список всего, что 'надо сделать'. Не думай о логике, просто выгрузи.\n\n"
            "2) Выбери одну задачу, которая реально двигает жизнь вперёд.\n"
            "Не самую страшную и не самую лёгкую, а ту, что даст ощутимый плюс, если ты её сдвинешь.\n\n"
            "3) Разбей её на микрошаги по 10–30 минут.\n"
            "Не 'сделать проект', а 'открыть файл', 'написать структуру', 'сделать первый абзац' і т.д.\n\n"
            "4) Поставь таймер на 20 минут і зроби тільки перший крок.\n"
            "Не геройствуй, просто начни. Когда сделаешь — можешь либо остановиться, либо взять следующий микрошаг.\n\n"
            "Якщо хочеш, можу допомогти розкласти саме твої задачі на конкретний план на сьогодні."
        ),
    },
]


# --- Обычный текстовый запрос ---

async def ask_gpt(history: List[Dict[str, str]], lang: str) -> str:
    """
    Обычный текстовый запрос.
    history — это список сообщений [{"role": "user"/"assistant", "content": "..."}].
    """
    system_prompt = _base_system_prompt() + "\n" + lang_instruction(lang)

    # Собираем сообщения: системный промпт -> примеры поведения Foxy -> реальная история диалога
    messages: List[Dict[str, str]] = [{"role": "system", "content": system_prompt}]
    messages += FOXY_EXAMPLES
    messages += history

    resp = client.chat.completions.create(
        model=MODEL_NAME,
        messages=messages,
    )
    return resp.choices[0].message.content.strip()


# --- Генерация follow-up сообщений для рассылок / напоминаний ---

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
    - сколько дней человек молчит (ignored_days)
    - какой по счёту follow-up (stage, начиная с 0)
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

    # Языковой блок именно для follow-up
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
            "Ситуация: користувач/пользователь тільки що запустив бота, уже побачив "
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


# --- Мультимодальный запрос: текст + картинка ---

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
        + "\n"
        + "Пользователь прислал изображение. Отвечай, опираясь и на картинку, и на текст вопроса.\n"
        + lang_instruction(lang)
    )

    # кодируем фото в base64
    b64_image = base64.b64encode(image_bytes).decode("utf-8")
    data_url = f"data:image/jpeg;base64,{b64_image}"

    messages: List[Dict[str, object]] = [{"role": "system", "content": system_prompt}]
    messages += FOXY_EXAMPLES
    messages += history

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


# --- Расшифровка голосовых (Whisper) ---

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
    text = getattr(resp, "text", "").strip()
    return text