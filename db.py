# db.py (facade)
from __future__ import annotations

# Важно: этот файл оставлен как совместимый слой, чтобы остальной код проекта
# продолжал работать без изменений. Вся логика вынесена в db_layer/*.

from db_layer.connection import conn, cur, connect_ctx, MAX_EVENTS_ROWS
from db_layer.utils import required_ignored_days_for_stage

# schema / migrations
from db_layer.schema import *  # noqa

# users / limits
from db_layer.users import *  # noqa

# messages + profile
from db_layer.messages import *  # noqa
from db_layer.user_profile import *  # noqa

# memory / profiles
from db_layer.memory import *  # noqa
from db_layer.profiles import *  # noqa
from db_layer.billing import *  # noqa
from db_layer.support import *  # noqa

# events / payments
from db_layer.events import *  # noqa

# paywall + triggers
from db_layer.paywall import *  # noqa

# tarot
from db_layer.tarot_limits import *  # noqa
from db_layer.personalization import *  # noqa
