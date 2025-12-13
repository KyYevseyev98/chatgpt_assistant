# config.py
import os
import datetime as dt
from dotenv import load_dotenv

load_dotenv()

TG_TOKEN = os.getenv("TG_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

DB_PATH = "chatgpt_users.db"
MODEL_NAME = "gpt-4.1-mini"

# бесплатные лимиты
FREE_TEXT_LIMIT_PER_DAY = 20
FREE_PHOTO_LIMIT_PER_DAY = 1

# сколько сообщений контекста помним
MAX_HISTORY_MESSAGES = 20

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

# --- PRO constants aliases (compat layer) ---

# stars
PRO_STARS_7_DAYS = PRO_WEEK_STARS
PRO_STARS_30_DAYS = PRO_MONTH_STARS
PRO_STARS_90_DAYS = PRO_QUARTER_STARS

# days (если где-то используются)
PRO_DAYS_7 = 7
PRO_DAYS_30 = 30
PRO_DAYS_90 = 90