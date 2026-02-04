# gpt_client.py
from __future__ import annotations

import asyncio
import base64
import json
import re
from typing import Any, Dict, List, Optional

from openai import OpenAI

import logging

from config import MODEL_NAME, OPENAI_API_KEY, MAX_HISTORY_CHARS, MAX_USER_QUESTION_CHARS
from gpt_prompts import (
    astra_system_prompt,
    chat_system_prompt,
    messages_base,
    messages_chat_base,
    messages_tarot_base,
)
from gpt_router import (
    safe_json_loads,
    is_followup_like,
    history_tail,
    format_history_for_router,
)

History = List[Dict[str, str]]
MessageList = List[Dict[str, Any]]


logger = logging.getLogger(__name__)
client = OpenAI(api_key=OPENAI_API_KEY)
if not OPENAI_API_KEY:
    logger.warning("OPENAI_API_KEY is missing or empty")

# =========================
# FORBIDDEN TOPICS (минимально необходимое)
# =========================
FORBIDDEN_KEYWORDS = [
    "api key", "api-ключ", "openai key",
    "provider_token",
    "webhook", "polling",
    "tokens price", "token pricing", "сколько стоят токены",
    "what model are you", "какая модель", "версия gpt", "версия гпт",
]



def _looks_like_fake_tarot(text: str) -> bool:
    """Heuristic to detect tarot-like output in chat mode."""
    t = (text or "").lower()
    # В чате Астра может *упоминать* слово "расклад" как предложение.
    # Поэтому детектируем только явный формат трактовки/перечень карт.
    markers = [
        "(тасую", "(вытягиваю", "тасую колоду", "вытягиваю карты",
        "🧠", "🃏", "💡", "⚡",
        "<b>главное</b>", "<b>карты</b>", "<b>итог</b>",
        "пентак", "кубк", "меч", "жезл",
    ]
    if any(m in t for m in markers):
        return True

    # Частый паттерн фейкового "расклада": нумерованный список карт.
    if re.search(r"\n\s*1\)\s*<b>.*?</b>", text or "", flags=re.IGNORECASE):
        return True
    return False


def is_forbidden_topic(text: str) -> bool:
    """Return True for disallowed queries that should be blocked."""
    t = (text or "").lower()
    return any(k in t for k in FORBIDDEN_KEYWORDS)


def _extract_json_block(text: str) -> str:
    """Extract first JSON object from text."""
    if not text:
        return ""
    t = (text or "").strip()
    if t.startswith("{") and t.endswith("}"):
        return t
    m = re.search(r"\{.*\}", t, flags=re.DOTALL)
    return m.group(0) if m else ""


def _trim_history_for_router(history: Optional[History]) -> Optional[History]:
    if not history:
        return history
    items = history_tail(history, n=10)
    if not MAX_HISTORY_CHARS:
        return items
    total = 0
    trimmed: History = []
    for m in reversed(items):
        content = (m.get("content") or "").strip()
        role = (m.get("role") or "").strip()
        size = len(content) + len(role) + 2
        if total + size > int(MAX_HISTORY_CHARS):
            continue
        trimmed.append({"role": role, "content": content})
        total += size
    trimmed.reverse()
    return trimmed


async def _chat_complete(
    messages: MessageList,
    *,
    temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
) -> str:
    """Unified OpenAI chat call.

    Some call sites (tarot_intro_post) pass temperature/max_tokens.
    Previously _chat_complete didn't accept those kwargs, which caused
    the intro-post to fail and the bot to skip the required "post answer".
    """

    def _run() -> str:
        kwargs: Dict[str, Any] = {
            "model": MODEL_NAME,
            "messages": messages,
        }
        try:
            total_chars = sum(len(m.get("content") or "") for m in messages)
            logger.debug("gpt_call messages=%s total_chars=%s", len(messages), total_chars)
        except Exception:
            pass
        if temperature is not None:
            kwargs["temperature"] = float(temperature)
        if max_tokens is not None:
            kwargs["max_tokens"] = int(max_tokens)

        try:
            resp = client.chat.completions.create(**kwargs)
        except Exception as e:
            logger.warning("gpt_call failed: %s", e)
            raise
        content = (resp.choices[0].message.content or "").strip()
        if not content:
            logger.warning("gpt_call returned empty content")
        return content

    return await asyncio.to_thread(_run)


# =========================
# MAIN CHAT (text-only)
# =========================
async def ask_tarot(history: List[Dict[str, str]], lang: str = "ru") -> str:
    """Tarot model call for reading content (not routing)."""
    messages: MessageList = messages_tarot_base(lang=lang)
    messages += (history or [])
    return await _chat_complete(messages)


async def ask_chat(history: History, lang: str = "ru") -> str:
    """Обычный чат: строго без таро-формата и имитации раскладов."""
    messages: MessageList = messages_chat_base(lang=lang)
    messages += (history or [])
    answer = await _chat_complete(messages)
    # Страховка: если модель всё равно начала «гадать», переформулируем один раз.
    if _looks_like_fake_tarot(answer):
        messages2: MessageList = messages_chat_base(lang=lang)
        messages2 += (history or [])
        messages2.append(
            {
                "role": "system",
                "content": (
                    "Запрещено делать расклады, упоминать карты и имитировать гадание. "
                    "Ответь как обычный помощник."
                ),
            }
        )
        answer = await _chat_complete(messages2)
    return answer


async def summarize_long_memory(
    *,
    history: History,
    lang: str = "ru",
    current_profile: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Summarize recent dialog into a compact, structured long-term memory block."""
    if not history:
        return {}

    current_profile = current_profile or {}
    profile_hint = json.dumps(current_profile, ensure_ascii=False)[:1200]

    if (lang or "ru").lower().startswith("uk"):
        system_prompt = (
            "Ти — помічник з памʼяті. Виділи тільки те, що користувач ЯВНО сказав.\n"
            "Не вигадуй. Без персональних даних.\n"
            "Поверни ТІЛЬКИ JSON без markdown."
        )
        user_prompt = (
            "Ось останній діалог. Онови довготривалу памʼять у JSON.\n"
            "Схема JSON:\n"
            "{"
            '"summary": "1–3 короткі речення",'
            '"themes": ["..."],'
            '"goals": ["..."],'
            '"facts": ["..."],'
            '"boundaries": ["..."],'
            '"taboos": ["..."],'
            '"preferences": ["..."],'
            '"events": ["..."]'
            "}\n"
            "Якщо поля порожні — став порожні масиви.\n"
            f"Поточний профіль (може допомогти, не повторюй зайве): {profile_hint}\n"
            "Діалог:\n"
        )
    else:
        system_prompt = (
            "Ты — помощник по памяти. Выделяй только то, что пользователь ЯВНО сказал.\n"
            "Не выдумывай. Без персональных данных.\n"
            "Верни ТОЛЬКО JSON без markdown."
        )
        user_prompt = (
            "Ниже последний диалог. Обнови долгосрочную память в JSON.\n"
            "Схема JSON:\n"
            "{"
            '"summary": "1–3 коротких предложения",'
            '"themes": ["..."],'
            '"goals": ["..."],'
            '"facts": ["..."],'
            '"boundaries": ["..."],'
            '"taboos": ["..."],'
            '"preferences": ["..."],'
            '"events": ["..."]'
            "}\n"
            "Если поля пустые — ставь пустые массивы.\n"
            f"Текущий профиль (может помочь, не повторяй лишнее): {profile_hint}\n"
            "Диалог:\n"
        )

    dialogue_text = "\n".join([f"{m.get('role')}: {m.get('content')}" for m in history])[:3000]
    prompt = user_prompt + dialogue_text

    raw = await _chat_complete(
        [{"role": "system", "content": system_prompt}, {"role": "user", "content": prompt}],
        temperature=0.2,
        max_tokens=260,
    )

    json_block = _extract_json_block(raw or "")
    data = safe_json_loads(json_block) if json_block else None
    if isinstance(data, dict):
        return data

    # fallback: store as a free-form summary
    text = (raw or "").strip()
    return {"summary": text} if text else {}


# Backward compatibility
async def ask_gpt(history: History, lang: str = "ru") -> str:
    """Compatibility wrapper for chat responses."""
    # Для обычного чата можно также использовать общий системный промпт,
    # но чтобы не “ломать” существующее поведение — оставляем Астру как основной стиль.
    return await ask_chat(history, lang=lang)


async def route_tarot_action(
    user_text: str,
    lang: str = "ru",
    *,
    history_hint: str = "",
    history: Optional[History] = None,
) -> Dict[str, Any]:
    """
    Возвращает dict:
      action: chat|reading|clarify
      cards: 0 или 1..7
      spread_name: str
      clarify_question: str
      reason: str

    ВАЖНО:
    - Не запускаем reading без явного запроса.
    - Не запускаем reading в follow-up контексте (подробнее/расшифруй/итог).
    - Кнопки (mode_hint:...) считаем явным выбором ФОРМАТА.
    """

    text = (user_text or "").strip()
    if not text:
        return {"action": "chat", "cards": 0, "spread_name": "", "clarify_question": "", "reason": "empty"}

    # Локальный safety: follow-up не должен превращаться в новый расклад
    if is_followup_like(text) and "mode_hint:" not in (history_hint or ""):
        return {"action": "chat", "cards": 0, "spread_name": "", "clarify_question": "", "reason": "followup_like"}

    history = _trim_history_for_router(history)
    hist_block = format_history_for_router(history)

    prompt = (
        "Ты — Астра. Нужно выбрать действие.\n"
        "Верни СТРОГО JSON, без пояснений и без markdown.\n\n"
        "ЖЕЛЕЗНЫЕ правила:\n"
        "1) reading — ТОЛЬКО если человек ЯВНО попросил расклад/таро/карту дня/да-нет.\n"
        "   Если явной просьбы нет — action=chat.\n"
        "2) clarify — если человек ЯВНО просит расклад, но слишком мало данных и можно сильно ошибиться.\n"
        "   Тогда задай ОДИН уточняющий вопрос.\n"
        "3) cards: если reading — ВСЕГДА 1..7. Если chat/clarify — 0.\n"
        "4) spread_name — коротко: 'Карта дня', 'Да/Нет', 'Отношения', 'Деньги/работа', 'Расклад'.\n"
        "5) Если это похоже на follow-up после расклада (например: 'подробнее', 'что значит 2 карта') — action=chat.\n\n"
        "Подсказки режима (если есть):\n"
        "- mode_hint:card_day или mode_hint:yesno — это явный выбор формата пользователем.\n"
        "- mode_hint:love или mode_hint:money — явный выбор темы.\n\n"
        f"MODE_HINT: {(history_hint or '')[:200]}\n"
        f"LAST_DIALOG (если есть):\n{hist_block}\n\n"
        f"USER_MESSAGE:\n{text}\n\n"
        "{"
        "\"action\":\"chat|reading|clarify\","
        "\"cards\":0,"
        "\"spread_name\":\"...\","
        "\"clarify_question\":\"...\","
        "\"reason\":\"...\""
        "}"
    )

    raw = await _chat_complete(
        [
            {"role": "system", "content": astra_system_prompt(lang)},
            {"role": "user", "content": prompt},
        ]
    )

    data = safe_json_loads(raw)

    # fallback: НИКОГДА не включаем reading “по ошибке”
    if not isinstance(data, dict):
        return {
            "action": "chat",
            "cards": 0,
            "spread_name": "",
            "clarify_question": "",
            "reason": "fallback_bad_json",
        }

    action = str(data.get("action", "chat")).strip().lower()

    try:
        cards = int(data.get("cards", 0) or 0)
    except Exception:
        cards = 0

    if action not in ("chat", "reading", "clarify"):
        action = "chat"

    # защита: follow-up не должен уйти в reading
    if action == "reading" and is_followup_like(text) and "mode_hint:" not in (history_hint or ""):
        action = "chat"
        cards = 0

    if action != "reading":
        cards = 0
    else:
        cards = max(1, min(cards, 7))

    spread_name = str(data.get("spread_name", "")).strip()[:48] or "Расклад"
    clarify_q = str(data.get("clarify_question", "")).strip()[:300]
    reason = str(data.get("reason", "")).strip()[:140]

    if action == "clarify" and not clarify_q:
        clarify_q = "Уточни, пожалуйста: ты хочешь именно расклад Таро? Если да — про какую сферу и на какой срок?"

    return {
        "action": action,
        "cards": cards,
        "spread_name": spread_name,
        "clarify_question": clarify_q,
        "reason": reason,
    }


# =========================
# TAROT READING ANSWER (after cards are drawn)
# =========================

async def tarot_intro_post(
    lang: str,
    user_question: str,
    spread_name: str,
    n_cards: int,
    history: list | None = None,
    history_hint: str = "",
) -> str:
    """Живой post-ответ перед раскладом. Только для раскладов (Arcana-стиль)."""
    sys = astra_system_prompt(lang)
    user_question = (user_question or "").strip()
    if MAX_USER_QUESTION_CHARS:
        user_question = user_question[: int(MAX_USER_QUESTION_CHARS)]
    history_hint = (history_hint or "").strip()
    if MAX_HISTORY_CHARS:
        history_hint = history_hint[: int(MAX_HISTORY_CHARS)]
    prompt = (
        "Напиши короткий живой пост-ответ перед раскладом Таро. "
        "Это отдельное сообщение ПЕРЕД тем, как показать карты. "
        "Тон: как продолжение диалога, с той же эмоциональной нотой (поддержка/напряжение/радость), "
        "чтобы звучало естественно и по делу. "
        "Если запрос тревожный — мягко признавай чувства; если позитивный — поддержи и усили optimism. "
        "Структура: 2–4 коротких абзаца, можно с одним выделенным акцентом. "
        "Используй 1–3 эмодзи максимум, уместно и без перегруза. "
        "Вариативность: избегай повторяющихся шаблонов, меняй вступления и завершения. "
        "Нельзя: описывать процесс 'тасую/вытягиваю'. "
        "Обязательно: дать ощущение, что ты поняла контекст, и что сейчас посмотришь через карты.\n\n"
        f"Название расклада: {spread_name}. Карт: {n_cards}.\n"
        f"Запрос: {user_question}\n"
        + (f"\nКонтекст: {history_hint}\n" if history_hint else "")
    )
    msgs: MessageList = [{"role": "system", "content": sys}]
    if history:
        # ограничим, чтобы не переполнять
        msgs.extend(history[-50:])
    msgs.append({"role": "user", "content": prompt})
    # В этом репозитории реальная обёртка называется _chat_complete.
    out = await _chat_complete(msgs, temperature=0.8, max_tokens=220)
    return (out or "").strip()

async def tarot_reading_answer(
    *,
    lang: str,
    user_question: str,
    spread_name: str,
    cards_payload: List[Dict[str, Any]],
    history_hint: str = "",
) -> str:
    """
    cards_payload приходит из build_cards_payload()
    Там поля: ru_name, keywords, short, shadow, advice, key, file...
    """

    q = (user_question or "").strip()
    if MAX_USER_QUESTION_CHARS:
        q = q[: int(MAX_USER_QUESTION_CHARS)]
    spread_name = (spread_name or "").strip()
    history_hint = (history_hint or "").strip()
    if MAX_HISTORY_CHARS:
        history_hint = history_hint[: int(MAX_HISTORY_CHARS)]

    prompt = (
        "Сделай глубокий, живой расклад.\n"
        "Пиши тепло и по-человечески. Без мистической воды.\n"
        "Формулируй как тенденции/вероятности, не как 100% приговор.\n\n"
        f"<b>Тема:</b> {spread_name}\n"
        f"<b>Вопрос:</b> {q}\n"
        f"<b>Контекст:</b> {history_hint}\n\n"
        "<b>Карты:</b>\n"
    )

    for i, c in enumerate(cards_payload or [], start=1):
        card_name = c.get("name") or c.get("ru_name") or c.get("key") or "Карта"
        keywords = c.get("keywords", "")
        short = c.get("short", "")
        shadow = c.get("shadow", "")
        advice = c.get("advice", "")
        prompt += (
            f"\n{i}) {card_name}\n"
            f"Ключевые: {keywords}\n"
            f"Смысл: {short}\n"
            f"Тень: {shadow}\n"
            f"Совет: {advice}\n"
        )

    n_cards = len(cards_payload or [])
    # Требования ТЗ: без слова «Суть», название каждой карты отдельной строкой,
    # длина и конкретика растут с количеством карт.
    prompt += (
        "\n\nТребования к ответу (важно):\n"
        "1) Пиши тепло и живо, с лёгкой мистикой, без воды и канцелярита.\n"
        "2) НЕ используй слово \"Суть\" и заголовок \"Суть\".\n"
        "3) Не повторяй список «Карты и позиции» — он уже отправлен отдельным сообщением.\n"
        "4) В толковании: каждая карта — отдельным блоком. Привязывай смысл карты к конкретным аспектам запроса.\n"
        "   Если в запросе есть: тема/горизонт/контекст/цель — учитывай их явно.\n"
        "   Каждая карта отвечает за свою часть: что происходит, что влияет, что важно сделать, чего избегать.\n"
        "   Не будь размытым — объясняй, как это относится к запросу пользователя.\n"
        "   Дай конкретные интерпретации по карте: причины, состояния, возможные мотивы, скрытые факторы.\n"
        "   Допускается мягкая психологическая гипотеза («скорее всего», «похоже», «возможно»), без категоричных утверждений.\n"
        "   Периодически поясняй, почему именно такое значение карты здесь уместно (контекст, позиция, тема).\n"
        "   Если карта обычно воспринимается как трудная/негативная — можно показать, как в этом контексте она становится ресурсом.\n"
        "   Формат строго:\n"
        "   <b>1) Название карты</b>\n"
        "   2–5 коротких предложений трактовки (чем больше карт, тем детальнее)\n"
        "5) Дай конкретику: что происходит, где риск/точка роста, что делать.\n"
        "6) В конце предложи 2–4 варианта следующих шагов/вопросов, чтобы человек захотел продолжить диалог.\n"
        "7) Структура: короткие абзацы (1–3 строки), логические блоки, аккуратные маркеры (•).\n"
        "8) Эмодзи: 0–3 на весь ответ, уместно и без перегруза.\n"
        "9) Эмоциональные реплики допустимы 1–2 раза на ответ (например: «Ого, интересный расклад…», «Вау, это сильный знак…»), но без театральности.\n"
        f"10) Длина: 1 карта = коротко; 3 карты = подробно; 5–7 карт = очень подробно. (Сейчас карт: {n_cards}).\n"
        "\nФормат ответа:\n"
        "✨ <b>Главное</b> (2–3 строки)\n"
        "🔍 <b>Толкование</b> (по карте, коротко)\n"
        "🧭 <b>Итог + 2–4 шага</b>\n"
    )


    return await _chat_complete(
        [
            {"role": "system", "content": astra_system_prompt(lang)},
            {"role": "user", "content": prompt},
        ]
    )


# =========================
# PAYWALL TEXT
# =========================

async def generate_limit_paywall_text(
    *,
    lang: str,
    limit_type: str,
    topic: str | None = None,
    last_user_message: str | None = None,
    user_profile: dict | None = None,
    history: Optional[History] = None,
    context_hint: str = "",
) -> str:
    """Generate a short paywall message for text/tarot limits."""
    if not OPENAI_API_KEY:
        logger.warning("OPENAI_API_KEY is missing (paywall)")
    logger.info("paywall gpt generate start")
    # user_profile можно использовать мягко, но без персональных данных.
    last_user_message = (last_user_message or "").strip()[:260]
    topic = (topic or "").strip()[:64]
    limit_type = (limit_type or "").strip()[:16]
    context_hint = (context_hint or "").strip()[:600]

    history_lines = []
    if history:
        for m in history[-6:]:
            role = (m.get("role") or "").strip()
            content = (m.get("content") or "").strip().replace("\n", " ")
            if content:
                if len(content) > 220:
                    content = content[:220] + "…"
                history_lines.append(f"{role}: {content}")
    history_block = "\n".join(history_lines)

    prompt = (
        "Сгенерируй короткое, человечное сообщение по-русски.\n"
        "Смысл: бесплатная часть закончилась. Предложи купить расклады, без давления.\n"
        "Без цен и без слова 'лимит' и без упоминания сообщений/количества.\n"
        "Никаких кнопок в тексте (кнопки добавит бот).\n"
        "2–3 коротких абзаца, 1–2 эмодзи обязательно (эмодзи должны быть разными между сообщениями).\n"
        "Важно: пакеты раскладов остаются на балансе, и можно делать глубокие расклады с учётом контекста.\n"
        "Также можно получить расклады бесплатно, если пригласить подругу по реферальной ссылке.\n"
        "Сделай текст естественным продолжением диалога.\n"
        f"Тема: {topic}\n"
        f"Тип: {limit_type}\n"
        f"Последнее сообщение: {last_user_message!r}\n"
        + (f"\nКонтекст диалога:\n{history_block}\n" if history_block else "")
        + (f"\nДоп. контекст:\n{context_hint}\n" if context_hint else "")
    )

    try:
        out = await _chat_complete(
            [
                {"role": "system", "content": astra_system_prompt(lang)},
                {"role": "user", "content": prompt},
            ]
        )
        out = (out or "").strip()
        if out:
            return out
        logger.warning("paywall gpt empty (primary)")
    except Exception as e:
        logger.warning("paywall gpt primary failed: %s", e)

    # retry with a shorter prompt if the first attempt failed or returned empty
    try:
        retry_prompt = (
            "Сгенерируй 2–3 коротких абзаца по-русски: "
            "мягко предложи купить расклады, без давления, без цен и без слова 'лимит'. "
            "Укажи, что пакеты остаются на балансе, и что ты будешь опираться на контекст. "
            "Обязательно добавь 1–2 эмодзи, меняй их между сообщениями. "
            "Также упомяни, что можно получить расклады бесплатно, пригласив подругу по реферальной ссылке."
        )
        out2 = await _chat_complete(
            [
                {"role": "system", "content": astra_system_prompt(lang)},
                {"role": "user", "content": retry_prompt},
            ],
            temperature=0.6,
            max_tokens=220,
        )
        out2 = (out2 or "").strip()
        if out2:
            return out2
        logger.warning("paywall gpt empty (retry)")
    except Exception as e:
        logger.warning("paywall gpt retry failed: %s", e)

    return (
        "Похоже, сейчас бесплатная часть уже закончилась.\n\n"
        "Если хочешь, я могу продолжить и сделать глубокий расклад с учётом контекста. "
        "Пакеты раскладов остаются на балансе — можно использовать их в удобное время.\n\n"
        "Готова предложить варианты, чтобы мы шли дальше спокойно и по делу."
    )


async def generate_limit_paywall_text_via_chat(
    *,
    history: History,
    lang: str,
) -> str:
    """Fallback paywall generator using chat prompt with history."""
    if not OPENAI_API_KEY:
        logger.warning("OPENAI_API_KEY is missing (paywall fallback)")
    logger.info("paywall gpt fallback start")
    prompt = (
        "Сгенерируй 2–3 коротких абзаца по-русски. "
        "Мягко объясни, что для продолжения нужна покупка раскладов. "
        "Без цен, без слова 'лимит', без упоминания количества сообщений. "
        "Подчеркни, что пакеты раскладов остаются на балансе, и ты учитываешь контекст."
    )
    msgs: MessageList = messages_chat_base(lang=lang)
    msgs += (history or [])
    msgs.append({"role": "user", "content": prompt})
    try:
        out = await _chat_complete(msgs, temperature=0.7, max_tokens=220)
        return (out or "").strip()
    except Exception as e:
        logger.warning("paywall gpt fallback failed: %s", e)
        return ""


# =========================
# FOLLOW-UP (for jobs.py)
# =========================

async def generate_followup_text(
    lang: str,
    ignored_days: int,
    stage: int,
    last_user_message: Optional[str] = None,
    last_bot_message: Optional[str] = None,
    last_followup_text: Optional[str] = None,
    user_profile: Optional[Dict[str, Any]] = None,
) -> str:
    """Generate a follow-up message used by scheduled jobs."""
    last_user_message = (last_user_message or "").strip()[:260]
    last_bot_message = (last_bot_message or "").strip()[:260]
    last_followup_text = (last_followup_text or "").strip()[:260]

    system_prompt = (
        "Ты — Астра ✨, тёплый и внимательный ИИ-таролог.\n"
        "Сгенерируй короткое follow-up сообщение (1–2 предложения).\n"
        "Тон живой, человечный, без давления.\n"
        "Строго по-русски.\n"
        "Без ссылок, без цен, без слова 'лимит'.\n"
        "Используй 1 подходящий эмодзи.\n"
    )

    user_prompt = (
        f"Пользователь молчит уже {ignored_days} дней. Стадия: {stage}.\n"
        f"Последнее сообщение пользователя: {last_user_message!r}\n"
        f"Последний ответ бота: {last_bot_message!r}\n"
        f"Последнее напоминание: {last_followup_text!r}\n"
        "Не повторяй предыдущий текст.\n"
        "Не упоминай оплату прямо.\n"
        "Сделай сообщение естественным, как будто ты просто рядом."
    )

    try:
        return await _chat_complete(
            [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ]
        )
    except Exception:
        return "✨ Если захочешь — можешь написать, я рядом."


# =========================
# IMAGE SUPPORT (handlers/photo.py)
# =========================

async def ask_gpt_with_image(
    history: History,
    lang: str,
    image_bytes: bytes,
    user_question: str,
) -> str:
    """Vision-enabled chat call for photo handler."""
    # NOTE: держим совместимость с текущим handlers/photo.py
    b64_image = base64.b64encode(image_bytes).decode("utf-8")
    data_url = f"data:image/jpeg;base64,{b64_image}"

    messages: MessageList = messages_chat_base(lang=lang)
    messages += (history or [])
    messages.append(
        {
            "role": "user",
            "content": [
                {"type": "text", "text": user_question},
                {"type": "image_url", "image_url": {"url": data_url}},
            ],
        }
    )

    return await _chat_complete(messages)


# =========================
# VOICE SUPPORT (handlers/voice.py)
# =========================

async def transcribe_voice(voice_bytes: bytes) -> str:
    """Transcribe OGG/voice bytes using Whisper."""
    from io import BytesIO

    audio_file = BytesIO(voice_bytes)
    audio_file.name = "voice.ogg"

    def _run():
        resp = client.audio.transcriptions.create(
            model="whisper-1",
            file=audio_file,
        )
        return (getattr(resp, "text", "") or "").strip()

    return await asyncio.to_thread(_run)
