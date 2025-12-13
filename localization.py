from config import (
    FREE_TEXT_LIMIT_PER_DAY,
    FREE_PHOTO_LIMIT_PER_DAY,
    PRO_WEEK_STARS,
    PRO_MONTH_STARS,
    PRO_QUARTER_STARS,
    format_stars,
)


def get_lang(user) -> str:
    code = (getattr(user, "language_code", "") or "").lower()
    if code.startswith("uk"):
        return "uk"
    if code.startswith("en"):
        return "en"
    return "ru"


def start_text(lang: str) -> str:
    if lang.startswith("uk"):
        return (
            "Привіт! Я AI-асистент на базі GPT прямо в Telegram.\n"
            "Просто напиши запитання — і я допоможу."
        )
    elif lang.startswith("en"):
        return (
            "Hi! I'm an AI assistant powered by GPT right inside Telegram.\n"
            "Just send your question and I'll help."
        )
    else:
        return (
            "Привет! Я AI-ассистент на базе GPT прямо в Telegram.\n"
            "Просто напиши свой вопрос — и я помогу."
        )


def reset_text(lang: str) -> str:
    if lang.startswith("uk"):
        return "Я забув попередній діалог. Почнемо з чистого аркуша 🙂"
    elif lang.startswith("en"):
        return "I’ve forgotten our previous conversation. Let’s start fresh 🙂"
    else:
        return "Я забыл предыдущий диалог. Начнём с чистого листа 🙂"


def forbidden_reply(lang: str) -> str:
    if lang.startswith("uk"):
        return "Технічні деталі (ключі, токени, моделі) я не обговорюю, але із задоволенням допоможу з будь-якими задачами 🙂"
    elif lang.startswith("en"):
        return "I don’t discuss internal technical details (keys, tokens, models), but I’m happy to help with any other tasks 🙂"
    else:
        return "Технические детали (ключи, токены, модели) я не обсуждаю, но с удовольствием помогу с любыми другими задачами 🙂"


def _pro_prices_block(lang: str) -> str:
    w = format_stars(PRO_WEEK_STARS)
    m = format_stars(PRO_MONTH_STARS)
    q = format_stars(PRO_QUARTER_STARS)

    if lang.startswith("uk"):
        return (
            "PRO-підписка без лімітів:\n"
            f"• 7 днів — {w}\n"
            f"• 30 днів — {m}\n"
            f"• 90 днів — {q}"
        )
    elif lang.startswith("en"):
        return (
            "PRO subscription with no limits:\n"
            f"• 7 days — {w}\n"
            f"• 30 days — {m}\n"
            f"• 90 days — {q}"
        )
    else:
        return (
            "PRO-подписка без лимитов:\n"
            f"• 7 дней — {w}\n"
            f"• 30 дней — {m}\n"
            f"• 90 дней — {q}"
        )


def text_limit_reached(lang: str) -> str:
    if lang.startswith("uk"):
        return f"Ви використали безкоштовний денний ліміт у {FREE_TEXT_LIMIT_PER_DAY} текстових повідомлень."
    elif lang.startswith("en"):
        return f"You’ve used today’s free limit of {FREE_TEXT_LIMIT_PER_DAY} text messages."
    else:
        return f"Вы использовали сегодняшний бесплатный лимит в {FREE_TEXT_LIMIT_PER_DAY} текстовых сообщений."


def photo_limit_reached(lang: str) -> str:
    if lang.startswith("uk"):
        return f"Ви вже використали сьогодні {FREE_PHOTO_LIMIT_PER_DAY} безкоштовний аналіз фото."
    elif lang.startswith("en"):
        return f"You’ve already used your {FREE_PHOTO_LIMIT_PER_DAY} free photo analysis for today."
    else:
        return f"Вы уже использовали сегодня {FREE_PHOTO_LIMIT_PER_DAY} бесплатный анализ фото."


def photo_placeholder_text(lang: str) -> str:
    if lang.startswith("uk"):
        return "Не вдалося обробити фото. Спробуй ще раз трохи пізніше."
    elif lang.startswith("en"):
        return "I couldn’t process this image. Please try again a bit later."
    else:
        return "Не удалось обработать фото. Попробуй ещё раз чуть позже."


def multi_photo_not_allowed(lang: str) -> str:
    """
    Текст, если пользователь отправляет несколько фото (альбом).
    """
    if lang.startswith("uk"):
        return "Я можу аналізувати лише одне фото за раз. Будь ласка, надішли одне зображення окремим повідомленням."
    elif lang.startswith("en"):
        return "I can only analyze one photo at a time. Please send a single image in a separate message."
    else:
        return "Я могу анализировать только одно фото за раз. Пожалуйста, отправь одно изображение отдельным сообщением."


def pro_offer_text(lang: str) -> str:
    prices = _pro_prices_block(lang)
    if lang.startswith("uk"):
        return (
            "PRO-підписка відкриває:\n"
            "• безлімітні текстові запити\n"
            "• безлімітний аналіз фото\n"
            "• швидші відповіді без очікування\n\n"
            + prices
            + "\n\nОберіть тариф на кнопках нижче."
        )
    elif lang.startswith("en"):
        return (
            "PRO subscription gives you:\n"
            "• unlimited text requests\n"
            "• unlimited image analysis\n"
            "• faster replies with no waiting\n\n"
            + prices
            + "\n\nChoose a plan using the buttons below."
        )
    else:
        return (
            "PRO-подписка даёт:\n"
            "• безлимитные текстовые запросы\n"
            "• безлимитный анализ фото\n"
            "• более быстрые ответы без ожидания\n\n"
            + prices
            + "\n\nВыбери тариф по кнопкам ниже."
        )

def pro_error_text(lang: str) -> str:
    if lang.startswith("uk"):
        return "Оплата не пройшла. Спробуй ще раз або обери інший тариф."
    elif lang.startswith("en"):
        return "Payment failed. Please try again or choose another plan."
    else:
        return "Оплата не прошла. Попробуй ещё раз или выбери другой тариф."

def pro_success_text(lang: str, days: int) -> str:
    if lang.startswith("uk"):
        return f"Готово! PRO-режим активований на {days} днів. Можеш користуватися без лімітів 🚀"
    elif lang.startswith("en"):
        return f"Done! PRO mode is active for {days} days. Enjoy unlimited usage 🚀"
    else:
        return f"Готово! PRO-режим активирован на {days} дней. Теперь можно пользоваться без лимитов 🚀"