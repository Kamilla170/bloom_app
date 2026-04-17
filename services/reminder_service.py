"""
Сервис напоминаний (API-версия)
Только create_plant_reminder — без Telegram-специфичного кода
"""

import logging
from datetime import timedelta

from utils.time_utils import get_moscow_now
from database import get_db

logger = logging.getLogger(__name__)


async def create_plant_reminder(plant_id: int, user_id: int, interval_days: int = 5):
    """Создать напоминание о поливе"""
    try:
        db = await get_db()
        moscow_now = get_moscow_now()
        next_watering = moscow_now + timedelta(days=interval_days)
        next_watering_naive = next_watering.replace(tzinfo=None)

        async with db.pool.acquire() as conn:
            await conn.execute("""
                UPDATE reminders
                SET is_active = FALSE
                WHERE user_id = $1 AND plant_id = $2
                AND reminder_type = 'watering' AND is_active = TRUE
            """, user_id, plant_id)

            reminder_id = await conn.fetchval("""
                INSERT INTO reminders (user_id, plant_id, reminder_type, next_date, is_active)
                VALUES ($1, $2, 'watering', $3, TRUE)
                RETURNING id
            """, user_id, plant_id, next_watering_naive)

        logger.info(
            f"✅ Напоминание ID={reminder_id} для растения {plant_id} "
            f"(user {user_id}) на {next_watering.date()} (через {interval_days} дней)"
        )

    except Exception as e:
        logger.error(f"❌ Ошибка создания напоминания для растения {plant_id}: {e}", exc_info=True)
        raise
