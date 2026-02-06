# handlers/topics.py

from __future__ import annotations
from typing import Optional

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from handlers.common import reply_and_mirror

# =========================
#  TOPIC MODEL
# =========================

DEFAULT_TOPIC = "general"


def get_current_topic(context: ContextTypes.DEFAULT_TYPE) -> str:
    """
    Текущий topic используется ТОЛЬКО для:
    - аналитики
    - лимитов
    - профиля
    - логирования

    Он НЕ влияет на:
    - принятие решений
    - маршрутизацию
    - историю диалога
    """
    topic = context.chat_data.get("current_topic")
    if not topic:
        topic = DEFAULT_TOPIC
        context.chat_data["current_topic"] = topic
    return topic


def set_current_topic(context: ContextTypes.DEFAULT_TYPE, topic: str) -> None:
    """
    Безопасно обновляет topic.
    """
    topic = (topic or "").strip().lower()
    if not topic:
        topic = DEFAULT_TOPIC
    context.chat_data["current_topic"] = topic


# =========================
#  UI: кнопки выбора темы
# =========================

def _topics_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("💬 Общение", callback_data="topic:general")],
        [InlineKeyboardButton("❤️ Отношения", callback_data="topic:love")],
        [InlineKeyboardButton("💼 Деньги / Работа", callback_data="topic:money")],
        [InlineKeyboardButton("🌿 Саморазвитие", callback_data="topic:self")],
        [InlineKeyboardButton("🔮 Таро (контекст)", callback_data="topic:tarot")],
        [InlineKeyboardButton("❌ Закрыть", callback_data="topics_close")],
    ]
    return InlineKeyboardMarkup(rows)


async def topics_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Команда /topics — просто UI для выбора темы.
    Никакой логики маршрутизации здесь быть не должно.
    """
    await reply_and_mirror(
        update.message,
        "Выбери тему (это влияет только на контекст и аналитику):",
        reply_markup=_topics_keyboard(),
    )


async def topic_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Callback от кнопок выбора темы.
    """
    q = update.callback_query
    if not q:
        return

    await q.answer()

    data = q.data or ""
    if data == "topics_close":
        try:
            await q.message.delete()
        except Exception:
            pass
        return

    if not data.startswith("topic:"):
        return

    topic = data.split(":", 1)[1]
    set_current_topic(context, topic)

    try:
        await q.message.edit_text(f"✅ Текущая тема: <b>{topic}</b>", parse_mode="HTML")
    except Exception:
        pass
