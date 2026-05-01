"""
Планировщик фоновых задач:
- send_watering_reminders: каждые 5 минут проверяет, кому из юзеров пора слать пуш о поливе
- process_auto_payments_job: раз в день в 10:00 МСК — продление подписок через YooKassa
"""

import logging
from datetime import timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from config import MOSCOW_TZ
from utils.time_utils import get_moscow_now
from database import get_db
from services.fcm_service import send_push_to_user

logger = logging.getLogger(__name__)

_scheduler: AsyncIOScheduler | None = None


def _plural_plants(n: int) -> str:
    """растение / растения / растений"""
    if n % 100 in (11, 12, 13, 14):
        return "растений"
    if n % 10 == 1:
        return "растение"
    if n % 10 in (2, 3, 4):
        return "растения"
    return "растений"


async def send_watering_reminders():
    """Раз в 5 минут проверяет, кому пора слать пуши о поливе."""
    try:
        moscow_now = get_moscow_now()
        today = moscow_now.date()
        now_time_str = moscow_now.strftime("%H:%M")

        db = await get_db()

        # Берём всех, у кого reminder_time уже наступил сегодня и кому ещё не слали
        async with db.pool.acquire() as conn:
            users = await conn.fetch("""
                SELECT user_id, reminder_time
                FROM user_settings
                WHERE reminder_enabled = TRUE
                  AND reminder_time IS NOT NULL
                  AND reminder_time <= $1
                  AND (last_reminder_sent IS NULL OR last_reminder_sent < $2)
            """, now_time_str, today)

        if not users:
            return

        logger.info(f"🔔 Reminder check at {now_time_str}: {len(users)} пользователей")

        for user in users:
            user_id = user['user_id']

            try:
                # Растения, которые ждут полива сегодня или раньше
                async with db.pool.acquire() as conn:
                    plants = await conn.fetch("""
                        SELECT
                            p.id,
                            COALESCE(p.custom_name, p.plant_name, 'Растение #' || p.id) AS display_name
                        FROM plants p
                        WHERE p.user_id = $1
                          AND (p.plant_type = 'regular' OR p.plant_type IS NULL)
                          AND COALESCE(p.reminder_enabled, TRUE) = TRUE
                          AND p.next_watering_date IS NOT NULL
                          AND p.next_watering_date <= $2
                        ORDER BY p.next_watering_date ASC
                    """, user_id, today)

                # Помечаем юзера как обработанного на сегодня — даже если растений 0,
                # чтобы не пробегать его каждые 5 минут до конца дня.
                async with db.pool.acquire() as conn:
                    await conn.execute("""
                        UPDATE user_settings
                        SET last_reminder_sent = $1
                        WHERE user_id = $2
                    """, today, user_id)

                if not plants:
                    logger.info(f"🔔 user={user_id}: нет растений для полива")
                    continue

                count = len(plants)

                if count == 1:
                    title = "🌱 Время полить!"
                    body = f"{plants[0]['display_name']} ждёт воды"
                else:
                    title = f"🌱 Пора полить {count} {_plural_plants(count)}"
                    names = ", ".join(p['display_name'] for p in plants[:3])
                    if count > 3:
                        body = f"{names} и ещё {count - 3}"
                    else:
                        body = names

                sent = await send_push_to_user(
                    user_id=user_id,
                    title=title,
                    body=body,
                    data={
                        "type": "watering_reminder",
                        "plants_count": str(count),
                    },
                )

                logger.info(
                    f"🔔 user={user_id}: отправлено пушей={sent}, растений={count}"
                )

            except Exception as e:
                logger.error(
                    f"❌ Ошибка отправки напоминания user={user_id}: {e}",
                    exc_info=True,
                )

    except Exception as e:
        logger.error(f"❌ send_watering_reminders fatal: {e}", exc_info=True)


async def process_auto_payments_job():
    """Раз в день — продление подписок с автоплатежом."""
    try:
        from services.payment_service import process_auto_payments
        logger.info("💳 Запуск ежедневной обработки автоплатежей")
        await process_auto_payments()
    except Exception as e:
        logger.error(f"❌ process_auto_payments_job: {e}", exc_info=True)


def start_scheduler():
    """Запустить планировщик. Вызывается из api/main.py lifespan startup."""
    global _scheduler
    if _scheduler is not None:
        logger.warning("⚠️ Scheduler уже запущен")
        return

    _scheduler = AsyncIOScheduler(timezone=MOSCOW_TZ)

    # Каждые 5 минут — проверка напоминаний о поливе
    _scheduler.add_job(
        send_watering_reminders,
        trigger="cron",
        minute="*/5",
        id="watering_reminders",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    # Раз в день в 10:00 МСК — автоплатежи
    _scheduler.add_job(
        process_auto_payments_job,
        trigger="cron",
        hour=10,
        minute=0,
        id="auto_payments",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    _scheduler.start()
    logger.info(
        "✅ Scheduler запущен: "
        "watering_reminders каждые 5 мин, auto_payments в 10:00 МСК"
    )


def stop_scheduler():
    """Остановить планировщик. Вызывается из api/main.py lifespan shutdown."""
    global _scheduler
    if _scheduler is not None:
        _scheduler.shutdown(wait=False)
        _scheduler = None
        logger.info("🛑 Scheduler остановлен")
