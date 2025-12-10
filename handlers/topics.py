# handlers/topics.py
import logging
from typing import Dict

from telegram import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    Update,
)
from telegram.ext import ContextTypes

from localization import get_lang

logger = logging.getLogger(__name__)

# Тема по умолчанию
DEFAULT_TOPIC = "chat"

# Описание тем
TOPICS: Dict[str, Dict] = {
    "chat": {
        "icon": "💬",
        "titles": {
            "ru": "Общий",
            "uk": "Загальний",
            "en": "General",
        },
    },
    "travel": {
        "icon": "✈️",
        "titles": {
            "ru": "Путешествия",
            "uk": "Подорожі",
            "en": "Travel",
        },
    },
    "fitness": {
        "icon": "🏋️",
        "titles": {
            "ru": "Фитнес",
            "uk": "Фітнес",
            "en": "Fitness",
        },
    },
    "content": {
        "icon": "🎬",
        "titles": {
            "ru": "Контент",
            "uk": "Контент",
            "en": "Content",
        },
    },
}


def _lang_code(lang: str) -> str:
    if lang.startswith("uk"):
        return "uk"
    if lang.startswith("en"):
        return "en"
    return "ru"


def _topic_title(topic_id: str, lang: str) -> str:
    """Человекочитаемое название темы на нужном языке."""
    info = TOPICS.get(topic_id, TOPICS[DEFAULT_TOPIC])
    code = _lang_code(lang)
    return info["titles"].get(code, info["titles"]["ru"])


# ---------------- ТЕКУЩАЯ ТЕМА В chat_data ----------------

def get_current_topic(context: ContextTypes.DEFAULT_TYPE) -> str:
    """Берём текущую тему из chat_data."""
    return context.chat_data.get("current_topic", DEFAULT_TOPIC)


def set_current_topic(context: ContextTypes.DEFAULT_TYPE, topic_id: str) -> None:
    """Сохраняем выбранную тему в chat_data."""
    if topic_id not in TOPICS:
        topic_id = DEFAULT_TOPIC
    context.chat_data["current_topic"] = topic_id


# ---------------- КЛАВИАТУРА С ТЕМАМИ ----------------

def build_topics_keyboard(lang: str, current_topic: str) -> InlineKeyboardMarkup:
    """
    Строим inline-клавиатуру с темами.
    Активная тема помечена ✅.
    """

    buttons_rows = []

    # порядок показа тем
    order = ["chat", "travel", "fitness", "content"]

    row = []
    for idx, topic_id in enumerate(order, start=1):
        info = TOPICS[topic_id]
        title = _topic_title(topic_id, lang)
        icon = info["icon"]

        if topic_id == current_topic:
            text = f"✅ {icon} {title}"
        else:
            text = f"{icon} {title}"

        # ВАЖНО: callback_data начинается с "topic_",
        # чтобы совпадать с pattern в main.py: r"^(topic_|topics_close)"
        row.append(
            InlineKeyboardButton(
                text=text,
                callback_data=f"topic_{topic_id}",
            )
        )

        # 2 кнопки в строке
        if idx % 2 == 0:
            buttons_rows.append(row)
            row = []

    if row:
        buttons_rows.append(row)

    return InlineKeyboardMarkup(buttons_rows)


def get_topics_keyboard(lang: str, context: ContextTypes.DEFAULT_TYPE) -> InlineKeyboardMarkup:
    """
    Удобный хелпер: получить клавиатуру с учётом текущей темы из chat_data.
    """
    current = get_current_topic(context)
    return build_topics_keyboard(lang, current)


# ---------------- /topics команда (опционально) ----------------

async def topics_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Просто отдельно показать клавиатуру тем.
    В реальной жизни мы ещё и в ответах к GPT подвешиваем эту же клаву.
    """
    user = update.effective_user
    if not user:
        return

    lang = get_lang(user)
    kb = get_topics_keyboard(lang, context)

    if lang.startswith("uk"):
        text = "Оберіть тему діалогу:"
    elif lang.startswith("en"):
        text = "Choose a topic for the chat:"
    else:
        text = "Выбери тему диалога:"

    await update.message.reply_text(text, reply_markup=kb)


# ---------------- ОБРАБОТЧИК НАЖАТИЙ ПО КНОПКАМ ----------------

async def topic_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обработчик callback'ов по кнопкам тем.
    Имя функции именно topic_button — чтобы совпадать с импортом в main.py.
    """
    query = update.callback_query
    if not query or not query.data:
        return

    data = query.data
    if not data.startswith("topic_"):
        return

    await query.answer()

    topic_id = data.split("_", 1)[1]
    if topic_id not in TOPICS:
        return

    # записываем текущую тему
    set_current_topic(context, topic_id)

    user = query.from_user
    lang = get_lang(user)

    # обновляем клавиатуру у того сообщения, где она висела
    kb = build_topics_keyboard(lang, topic_id)
    try:
        await query.edit_message_reply_markup(reply_markup=kb)
    except Exception as e:
        logger.warning("Не удалось обновить клавиатуру тем: %s", e)

    #title = _topic_title(topic_id, lang)
    #if lang.startswith("uk"):
    #    text = f"Тема перемкнена на: {title}"
    #elif lang.startswith("en"):
    #    text = f"Topic switched to: {title}"
    #else:
    #    text = f"Тема переключена на: {title}"

    # отдельным сообщением говорим о смене темы
    #try:
    #    await query.message.reply_text(text)
    #except Exception as e:
    #    logger.warning("Не удалось отправить подтверждение смены темы: %s", e)