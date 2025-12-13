from typing import List, Dict, Optional, Any
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
    Для Telegram используем HTML (<b>...</b>), НЕ Markdown.
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
            "Use a friendly, conversational tone.\n"
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
    return (
        "Ты — Foxy, умный и дружелюбный AI-ассистент внутри Telegram-бота.\n"
        "Главные задачи:\n"
        "- помогать пользователю разбираться в вопросах так, чтобы реально становилось проще;\n"
        "- отвечать глубоко и по сути, без воды;\n"
        "- держать баланс между экспертом и живым собеседником.\n\n"
        "Стиль:\n"
        "- обращайся на 'ты';\n"
        "- без канцелярита и без кринж-сленга;\n"
        "- обычно 3–6 абзацев или 5–12 пунктов.\n\n"
        "Контекст бота:\n"
        "- бот имеет дневные бесплатные лимиты на текст/голос/фото;\n"
        "- у части пользователей есть PRO через Telegram Stars — для них лимитов нет;\n"
        "- ты не управляешь оплатами и подпиской.\n\n"
        "Ограничения:\n"
        "- не обсуждай ключи, токены, внутренности моделей и реализацию бота;\n"
        "- если спрашивают про модель/ключи/токены — мягко уходи от темы.\n"
    )


# --- Few-shot примеры ---
FOXY_EXAMPLES: List[Dict[str, str]] = [
    {
        "role": "user",
        "content": "Мне тяжело сфокусироваться, постоянно прокрастинирую. Что делать?",
    },
    {
        "role": "assistant",
        "content": (
            "Окей, без самобичевания. Прокрастинация чаще всего про перегруз и страх, а не про характер.\n\n"
            "1) Выгрузи всё из головы — выпиши список задач.\n"
            "2) Выбери одну, которая реально двигает жизнь вперёд.\n"
            "3) Разбей на микрошаги по 10–30 минут.\n"
            "4) Поставь таймер на 20 минут и сделай только первый шаг.\n\n"
            "Хочешь — помогу разложить именно твои задачи на план на сегодня."
        ),
    },
]


# -------------------- ОСНОВНОЙ GPT --------------------
async def ask_gpt(history: List[Dict[str, str]], lang: str) -> str:
    system_prompt = _base_system_prompt() + "\n" + lang_instruction(lang)

    messages: List[Dict[str, str]] = [{"role": "system", "content": system_prompt}]
    messages += FOXY_EXAMPLES
    messages += history

    resp = client.chat.completions.create(
        model=MODEL_NAME,
        messages=messages,
    )
    return resp.choices[0].message.content.strip()


# -------------------- SOFT UPSELL (внутри ответа) --------------------
async def generate_soft_upsell_text(lang: str, topic: Optional[str] = None) -> str:
    if lang.startswith("uk"):
        return "Якщо захочеш — можемо розібрати це глибше і без обмежень 💡"
    elif lang.startswith("en"):
        return "If you want, we can go deeper into this without limits 💡"
    else:
        return "Если хочешь — можем разобрать это глубже, без ограничений 💡"


# -------------------- PAYWALL: АДАПТИВНЫЙ ДОЖИМ PRO --------------------
def _topic_hint(topic: Optional[str], lang: str) -> str:
    topic = (topic or "").strip().lower()
    if topic == "fitness":
        return "fitness"
    if topic == "travel":
        return "travel"
    if topic == "content":
        return "content"
    return "chat"


async def generate_limit_paywall_text(
    *,
    lang: str,
    limit_type: str,  # "text" | "photo" | "voice"
    topic: Optional[str] = None,
    last_user_message: Optional[str] = None,
    user_profile: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Генерит 2–4 строки, которые:
    - максимально про ценность "прямо сейчас"
    - без давления/выпрашивания
    - без повторов
    - в стиле Foxy, но короче
    """
    t = _topic_hint(topic, lang)
    last_user_message = (last_user_message or "").strip()
    if len(last_user_message) > 400:
        last_user_message = last_user_message[:400]

    # профиль — опционально, но если есть, можно намекнуть на привычку юзера
    prof = user_profile or {}
    total_messages = int(prof.get("total_messages") or 0)
    total_photos = int(prof.get("total_photos") or 0)
    total_voice = int(prof.get("total_voice") or 0)
    pro_payments_count = int(prof.get("pro_payments_count") or 0)

    if lang.startswith("uk"):
        lang_block = "Пиши українською, на 'ти'."
    elif lang.startswith("en"):
        lang_block = "Write in English."
    else:
        lang_block = "Пиши по-русски, на 'ты'."

    if limit_type not in ("text", "photo", "voice"):
        limit_type = "text"

    system_prompt = (
        "Ты — Foxy, дружелюбный ассистент в Telegram.\n"
        "Пользователь упёрся в бесплатный лимит.\n"
        "Нужно написать ОДНО короткое сообщение-дожим (2–4 строки).\n"
        "Задача — показать ценность PRO конкретно для его ситуации, БЕЗ давления.\n"
        "Нельзя:\n"
        "- обещать невозможное;\n"
        "- давить 'купи', 'срочно', 'последний шанс';\n"
        "- писать длинные простыни.\n"
        "Можно:\n"
        "- подчеркнуть выгоду: не терять контекст, продолжить прямо сейчас, глубже разбор.\n"
        "Тон: живо, по делу.\n"
        f"{lang_block}\n"
        f"Тип лимита: {limit_type}.\n"
        f"Тема: {t}.\n"
        f"Последний запрос пользователя (если есть): {last_user_message!r}\n"
        f"Профиль: total_messages={total_messages}, total_photos={total_photos}, total_voice={total_voice}, pro_payments_count={pro_payments_count}\n"
        "Сформулируй так, чтобы звучало персонально, но без упоминания цифр статистики.\n"
        "Не упоминай цены и кнопки — они будут ниже.\n"
    )

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": "Сгенерируй этот paywall-текст."},
    ]

    resp = client.chat.completions.create(
        model=MODEL_NAME,
        messages=messages,
    )
    return resp.choices[0].message.content.strip()


# -------------------- FOLLOW-UP: рассылки/пинги --------------------
async def generate_followup_text(
    lang: str,
    ignored_days: int,
    stage: int,
    last_user_message: Optional[str] = None,
    last_bot_message: Optional[str] = None,
    last_followup_text: Optional[str] = None,
    user_profile: Optional[Dict[str, Any]] = None,
) -> str:
    """
    1–3 коротких предложения.
    stage: 0..n (обычные фоллоу-апы)
    stage=99 (после лимита)
    """
    last_user_message = (last_user_message or "").strip()
    last_bot_message = (last_bot_message or "").strip()
    last_followup_text = (last_followup_text or "").strip()

    if len(last_user_message) > 350:
        last_user_message = last_user_message[:350]
    if len(last_bot_message) > 350:
        last_bot_message = last_bot_message[:350]
    if len(last_followup_text) > 350:
        last_followup_text = last_followup_text[:350]

    prof = user_profile or {}
    topic_counts = prof.get("topic_counts") or {}
    # попробуем вытащить "самую частую" тему для аккуратного намёка
    best_topic = None
    try:
        if isinstance(topic_counts, dict) and topic_counts:
            best_topic = max(topic_counts.items(), key=lambda x: int(x[1] or 0))[0]
    except Exception:
        best_topic = None

    if lang.startswith("uk"):
        lang_block = "Пиши українською, дружньо."
    elif lang.startswith("en"):
        lang_block = "Write in English, friendly and concise."
    else:
        lang_block = "Пиши по-русски, дружелюбно и по делу."

    context_block = ""
    if last_user_message:
        context_block += f"Последний вопрос пользователя: «{last_user_message}».\n"
    if last_bot_message:
        context_block += f"Твой последний ответ: «{last_bot_message}».\n"
    if last_followup_text:
        context_block += (
            f"Последнее напоминание: «{last_followup_text}».\n"
            "Сделай новый текст другими словами, не повторяй дословно.\n"
        )

    if not context_block:
        context_block = "Контекст диалога отсутствует или пустой.\n"

    hint = ""
    if stage == 99:
        # follow-up после лимита
        hint = (
            "Это follow-up после того, как пользователь упёрся в лимит и ушёл.\n"
            "Сообщение должно быть мягким: продолжить мысль/помочь закончить разбор.\n"
            "Без слова 'лимит', без давления, без цен.\n"
        )
    else:
        # обычный follow-up
        hint = "Это обычное напоминание вернуться в диалог.\n"

    if best_topic:
        hint += f"Если уместно, намекни на тему, которая ему интересна: {best_topic}.\n"

    system_prompt = (
        "Ты — Foxy, дружелюбный AI-ассистент в Telegram.\n"
        "Твоя задача — аккуратно напомнить пользователю о себе и пригласить продолжить диалог.\n"
        "Формат: 1–3 коротких предложения, максимум 2–4 строки.\n"
        "Без разметки, без длинных списков. Допустимо 0–1 эмодзи в конце.\n"
        f"{lang_block}\n"
        f"{hint}\n"
        f"Информация о контексте:\n{context_block}\n"
        f"Пользователь молчит уже {ignored_days} дней. Номер напоминания: {stage}.\n"
    )

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": "Сгенерируй текст follow-up."},
    ]

    resp = client.chat.completions.create(
        model=MODEL_NAME,
        messages=messages,
    )
    return resp.choices[0].message.content.strip()


# -------------------- IMAGE --------------------
async def ask_gpt_with_image(
    history: List[Dict[str, str]],
    lang: str,
    image_bytes: bytes,
    user_question: str,
) -> str:
    system_prompt = (
        _base_system_prompt()
        + "\n"
        + "Пользователь прислал изображение. Отвечай, опираясь и на картинку, и на текст вопроса.\n"
        + lang_instruction(lang)
    )

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


# -------------------- VOICE TRANSCRIBE --------------------
async def transcribe_voice(voice_bytes: bytes) -> str:
    audio_file = BytesIO(voice_bytes)
    audio_file.name = "voice.ogg"

    resp = client.audio.transcriptions.create(
        model="whisper-1",
        file=audio_file,
    )
    text = getattr(resp, "text", "").strip()
    return text