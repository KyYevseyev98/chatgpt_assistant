# config.py
import os
import datetime as dt
from dotenv import load_dotenv

load_dotenv()

TG_TOKEN = os.getenv("TG_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
WEBAPP_URL = os.getenv("WEBAPP_URL", "")
BOT_USERNAME = os.getenv("BOT_USERNAME", "")
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "")

DB_PATH = "chatgpt_users.db"
MODEL_NAME = "gpt-4.1-mini"

FREE_TAROT_LIFETIME = 3
FREE_TAROT_LIMIT_PER_DAY = 999999

# бесплатные лимиты
FREE_TEXT_LIMIT_PER_DAY = 50
FREE_PHOTO_LIMIT_PER_DAY = 300

# новые лимиты (без изменения БД)
FREE_TAROT_FREE_COUNT = 3
FREE_TEXT_FREE_COUNT = 50

# ограничения БД/истории (чтобы база не раздувалась)
MAX_DB_MESSAGES_PER_CHAT = 100  # хранить последние N сообщений на чат
MAX_TAROT_HISTORY_PER_USER = 100  # хранить последние N раскладов на пользователя

# сколько сообщений контекста помним (для GPT)
MAX_HISTORY_MESSAGES = 20
MAX_HISTORY_CHARS = 0
MAX_MESSAGE_CHARS_DB = 2000
MAX_USER_QUESTION_CHARS = 1200
PRE_DIALOG_TTL_SEC = 15 * 60
PRE_DIALOG_MAX_QUESTIONS = 3

# долгосрочная память (без изменения схемы БД)
LONG_MEMORY_SUMMARY_EVERY = 8
LONG_MEMORY_SUMMARY_HISTORY = 12
LONG_MEMORY_MAX_ITEMS = 10
LONG_MEMORY_MAX_SUMMARIES = 20
LONG_MEMORY_MAX_EVENTS = 30
LONG_MEMORY_BLOCK_MAX_CHARS = 900

# Tarot session continuation TTL (seconds)
TAROT_SESSION_TTL_SEC = 15 * 60

# пользователи без лимитов (по username)
UNLIMITED_USERNAMES = {
    "dasha_mitchell",
    "kirillevseev",
}

# рефералы
REFERRAL_REWARD_SPREADS = 3

# пакеты раскладов
TAROT_PACKS = [
    {"key": "20", "spreads": 20, "stars": 100},
    {"key": "60", "spreads": 60, "stars": 250},
    {"key": "150", "spreads": 150, "stars": 500},
    {"key": "350", "spreads": 350, "stars": 1000},
]
# Тарифы PRO в звёздах
PRO_WEEK_STARS = 79      # ~1.9$
PRO_MONTH_STARS = 149    # ~3.6$
PRO_QUARTER_STARS = 399  # ~9.5$

# примерный курс звёзд к доллару (по скрину: 100⭐ ≈ 2.39$)
STAR_USD_RATE = 2.39 / 100.0  # ≈ 0.0239$

def format_stars(stars: int) -> str:
    usd = stars * STAR_USD_RATE
    return f"⭐{stars} (~${usd:.2f})"

def today_iso() -> str:
    return dt.date.today().isoformat()

# --- настройки админ-бота ---

# токен админ-бота (заполняется через .env)
ADMIN_TG_TOKEN = os.getenv("ADMIN_TG_TOKEN")

# список ID админов, через .env вида: ADMIN_IDS="12345,67890"
_admin_ids_raw = os.getenv("ADMIN_IDS", "")
ADMIN_IDS = [
    int(x.strip()) for x in _admin_ids_raw.split(",") if x.strip().isdigit()
]

# форум-группа для зеркалирования диалогов (опционально)
_forum_chat_id = os.getenv("ADMIN_FORUM_CHAT_ID", "").strip()
ADMIN_FORUM_CHAT_ID = int(_forum_chat_id) if _forum_chat_id.lstrip("-").isdigit() else None

# --- PRO constants aliases (compat layer) ---

# stars
PRO_STARS_7_DAYS = PRO_WEEK_STARS
PRO_STARS_30_DAYS = PRO_MONTH_STARS
PRO_STARS_90_DAYS = PRO_QUARTER_STARS

# days (если где-то используются)
PRO_DAYS_7 = 7
PRO_DAYS_30 = 30
PRO_DAYS_90 = 90
