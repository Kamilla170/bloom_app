"""
Ручная правка расписания полива: расчёт даты и проверка границ.

Модуль намеренно не импортирует ничего из проекта (БД, config, FastAPI):
формулу можно проверить обычными assert'ами, не поднимая окружение бэкенда.
"""

from datetime import date, datetime, timedelta
from typing import Optional, Union

# Границы ручного интервала (дней). Шире коридора ИИ (3..28): пользователь знает
# своё растение лучше — кактус зимой спокойно ждёт и полтора месяца.
MIN_WATERING_INTERVAL = 1
MAX_WATERING_INTERVAL = 60

# Окно допустимой даты следующего полива относительно серверного «сегодня».
# Один день назад — люфт на часовые пояса: у пользователя западнее сервера ещё
# может быть «вчера». Год вперёд — защита от опечатки в годе.
MANUAL_DATE_MAX_DAYS_BACK = 1
MANUAL_DATE_MAX_DAYS_AHEAD = 365


def compute_next_watering_date(
    last_watered: Optional[Union[datetime, date]],
    interval_days: int,
    today: date,
) -> date:
    """
    Дата следующего полива после ручной смены интервала.

    Считаем от последнего полива, а не от сегодня: иначе каждая правка
    интервала отодвигала бы полив, будто растение только что полили.
    Если поливов ещё не было — от сегодня.

    Результат не бывает в прошлом: при давнем поливе и коротком интервале
    дата получилась бы просроченной на недели, а честный ответ тут один —
    «поливать сегодня».
    """
    # datetime — подкласс date, поэтому проверяем именно его
    if isinstance(today, datetime):
        today = today.date()

    if last_watered is None:
        return today + timedelta(days=interval_days)

    last_date = last_watered.date() if isinstance(last_watered, datetime) else last_watered
    return max(last_date + timedelta(days=interval_days), today)


def validate_manual_watering_date(target: date, today: date) -> Optional[str]:
    """
    Проверить дату полива, выбранную пользователем вручную.
    Возвращает текст ошибки для пользователя или None, если дата допустима.
    """
    if target < today - timedelta(days=MANUAL_DATE_MAX_DAYS_BACK):
        return "Дата следующего полива не может быть в прошлом"
    if target > today + timedelta(days=MANUAL_DATE_MAX_DAYS_AHEAD):
        return "Дата следующего полива не может быть позже, чем через год"
    return None
