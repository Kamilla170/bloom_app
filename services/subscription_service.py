import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple

from database import get_db
from config import FREE_LIMITS, PRO_DURATION_DAYS, PRO_GRACE_PERIOD_DAYS, PRO_PRICE, ADMIN_USER_IDS
from services.analytics_recorder import record_subscription_event

logger = logging.getLogger(__name__)


async def ensure_plan_columns():
    """Миграция: добавляем plan_amount, plan_days и plan_id в subscriptions"""
    db = await get_db()
    async with db.pool.acquire() as conn:
        await conn.execute("""
            ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS plan_amount INTEGER DEFAULT 199;
        """)
        await conn.execute("""
            ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS plan_days INTEGER DEFAULT 30;
        """)
        await conn.execute("""
            ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS plan_id VARCHAR(20);
        """)
    logger.info("✅ Миграция plan_amount/plan_days/plan_id применена")


async def get_user_plan(user_id: int) -> Dict:
    """
    Получить текущий план пользователя.
    """
    db = await get_db()
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT plan, expires_at, auto_pay_method_id, granted_by_admin, plan_id
            FROM subscriptions
            WHERE user_id = $1
        """, user_id)

    if not row or row['plan'] == 'free':
        return {
            'plan': 'free',
            'expires_at': None,
            'is_grace_period': False,
            'days_left': None,
            'auto_pay': False,
            'plan_id': None,
        }

    now = datetime.now()
    expires_at = row['expires_at']

    if expires_at and expires_at > now:
        days_left = (expires_at - now).days
        return {
            'plan': 'pro',
            'expires_at': expires_at,
            'is_grace_period': False,
            'days_left': days_left,
            'auto_pay': bool(row['auto_pay_method_id']),
            'plan_id': row['plan_id'],
        }

    # Проверяем grace period
    if expires_at:
        grace_end = expires_at + timedelta(days=PRO_GRACE_PERIOD_DAYS)
        if now < grace_end:
            return {
                'plan': 'pro',
                'expires_at': expires_at,
                'is_grace_period': True,
                'days_left': 0,
                'auto_pay': bool(row['auto_pay_method_id']),
                'plan_id': row['plan_id'],
            }

    # Подписка истекла: переводим на free.
    # ВАЖНО: это lazy-expiration. В аналитике factual expiration считается
    # по subscriptions.expires_at + grace_period (в SQL-views). Не пишем
    # subscription_event здесь - время будет искажено (момент входа,
    # а не момент реального истечения).
    await downgrade_to_free(user_id, _from_lazy_expiration=True)
    return {
        'plan': 'free',
        'expires_at': None,
        'is_grace_period': False,
        'days_left': None,
        'auto_pay': False,
        'plan_id': None,
    }


async def is_pro(user_id: int) -> bool:
    """Быстрая проверка - PRO ли пользователь"""
    if user_id in ADMIN_USER_IDS:
        return True
    plan = await get_user_plan(user_id)
    return plan['plan'] == 'pro'


async def check_limit(user_id: int, action: str) -> Tuple[bool, Optional[str]]:
    """
    Проверить лимит действия.
    action: 'plants' | 'analyses' | 'questions'
    """
    if user_id in ADMIN_USER_IDS:
        return True, None

    if await is_pro(user_id):
        return True, None

    db = await get_db()
    usage = await get_or_create_usage(user_id)

    limit = FREE_LIMITS.get(action, 0)

    if action == 'plants':
        async with db.pool.acquire() as conn:
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM plants WHERE user_id = $1 AND plant_type = 'regular'",
                user_id
            )
        if count >= limit:
            return False, (
                f"🌱 Достигнут лимит бесплатного плана: <b>{limit} растений</b>\n\n"
                f"Оформите <b>подписку</b> для неограниченного доступа!"
            )
        return True, None

    elif action == 'analyses':
        if usage['analyses_used'] >= limit:
            return False, (
                f"📸 Достигнут лимит бесплатного плана: <b>{limit} анализа фото</b> в месяц\n\n"
                f"Оформите <b>подписку</b> для неограниченного доступа!"
            )
        return True, None

    elif action == 'questions':
        if usage['questions_used'] >= limit:
            return False, (
                f"🤖 Достигнут лимит бесплатного плана: <b>{limit} вопроса</b> в месяц\n\n"
                f"Оформите <b>подписку</b> для неограниченного доступа!"
            )
        return True, None

    return True, None


async def increment_usage(user_id: int, action: str):
    if await is_pro(user_id):
        return

    db = await get_db()
    await get_or_create_usage(user_id)

    column_map = {
        'analyses': 'analyses_used',
        'questions': 'questions_used',
    }

    column = column_map.get(action)
    if not column:
        return

    async with db.pool.acquire() as conn:
        await conn.execute(f"""
            UPDATE usage_limits
            SET {column} = {column} + 1
            WHERE user_id = $1
        """, user_id)


async def get_or_create_usage(user_id: int) -> Dict:
    db = await get_db()
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM usage_limits WHERE user_id = $1", user_id
        )

        if row:
            now = datetime.now()
            if row['reset_date'] and row['reset_date'] <= now:
                next_reset = (now.replace(day=1) + timedelta(days=32)).replace(day=1)
                await conn.execute("""
                    UPDATE usage_limits
                    SET analyses_used = 0, questions_used = 0, reset_date = $2
                    WHERE user_id = $1
                """, user_id, next_reset)
                return {
                    'analyses_used': 0,
                    'questions_used': 0,
                    'reset_date': next_reset,
                }
            return dict(row)

        now = datetime.now()
        next_reset = (now.replace(day=1) + timedelta(days=32)).replace(day=1)
        await conn.execute("""
            INSERT INTO usage_limits (user_id, analyses_used, questions_used, reset_date)
            VALUES ($1, 0, 0, $2)
            ON CONFLICT (user_id) DO NOTHING
        """, user_id, next_reset)

        return {
            'analyses_used': 0,
            'questions_used': 0,
            'reset_date': next_reset,
        }


async def get_usage_stats(user_id: int) -> Dict:
    plan_info = await get_user_plan(user_id)
    usage = await get_or_create_usage(user_id)

    db = await get_db()
    async with db.pool.acquire() as conn:
        plants_count = await conn.fetchval(
            "SELECT COUNT(*) FROM plants WHERE user_id = $1 AND plant_type = 'regular'",
            user_id
        )

    return {
        'plan': plan_info['plan'],
        'expires_at': plan_info.get('expires_at'),
        'days_left': plan_info.get('days_left'),
        'auto_pay': plan_info.get('auto_pay', False),
        'is_grace_period': plan_info.get('is_grace_period', False),
        'plants_count': plants_count or 0,
        'plants_limit': FREE_LIMITS['plants'] if plan_info['plan'] == 'free' else '∞',
        'analyses_used': usage['analyses_used'],
        'analyses_limit': FREE_LIMITS['analyses'] if plan_info['plan'] == 'free' else '∞',
        'questions_used': usage['questions_used'],
        'questions_limit': FREE_LIMITS['questions'] if plan_info['plan'] == 'free' else '∞',
    }


def _classify_subscription_event(
    existing_plan: Optional[str],
    existing_plan_id: Optional[str],
    existing_plan_days: Optional[int],
    existing_expires_at: Optional[datetime],
    new_plan_id: Optional[str],
    new_plan_days: int,
    granted_by: Optional[int],
    now: datetime,
) -> str:
    """
    Определить тип события для subscription_events.

    Логика:
        - granted_by != None: granted_by_admin
        - existing нет / plan='free' / истёк (с учётом grace): created
        - тот же plan_id: renewed
        - другой plan_id, plan_days больше: upgraded
        - другой plan_id, plan_days меньше: downgraded
        - plan_id отсутствует с одной из сторон: best-effort renewed
    """
    if granted_by is not None:
        return 'granted_by_admin'

    grace_end = None
    if existing_expires_at:
        grace_end = existing_expires_at + timedelta(days=PRO_GRACE_PERIOD_DAYS)

    is_active_pro = (
        existing_plan == 'pro'
        and existing_expires_at is not None
        and grace_end is not None
        and grace_end > now
    )

    if not is_active_pro:
        return 'created'

    # Идёт активная подписка PRO. Определяем upgrade/downgrade/renew.
    if existing_plan_id and new_plan_id and existing_plan_id == new_plan_id:
        return 'renewed'

    if existing_plan_days and new_plan_days:
        if new_plan_days > existing_plan_days:
            return 'upgraded'
        if new_plan_days < existing_plan_days:
            return 'downgraded'

    return 'renewed'


async def activate_pro(user_id: int, days: int = PRO_DURATION_DAYS, amount: int = None,
                       payment_method_id: str = None, granted_by: int = None,
                       plan_id: str = None,
                       payment_id: str = None,
                       source: str = None):
    """Активировать PRO подписку"""
    db = await get_db()
    now = datetime.now()

    if amount is None:
        amount = PRO_PRICE

    await ensure_plan_columns()

    async with db.pool.acquire() as conn:
        existing = await conn.fetchrow("""
            SELECT expires_at, plan, plan_id, plan_days
            FROM subscriptions WHERE user_id = $1
        """, user_id)

        if existing and existing['plan'] == 'pro' and existing['expires_at'] and existing['expires_at'] > now:
            expires_at = existing['expires_at'] + timedelta(days=days)
        else:
            expires_at = now + timedelta(days=days)

        await conn.execute("""
            INSERT INTO subscriptions (user_id, plan, expires_at, auto_pay_method_id,
                                       granted_by_admin, plan_amount, plan_days, plan_id, updated_at)
            VALUES ($1, 'pro', $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP)
            ON CONFLICT (user_id)
            DO UPDATE SET
                plan = 'pro',
                expires_at = $2,
                auto_pay_method_id = COALESCE($3, subscriptions.auto_pay_method_id),
                granted_by_admin = $4,
                plan_amount = $5,
                plan_days = $6,
                plan_id = COALESCE($7, subscriptions.plan_id),
                updated_at = CURRENT_TIMESTAMP
        """, user_id, expires_at, payment_method_id, granted_by, amount, days, plan_id)

    logger.info(f"✅ PRO активирован: user_id={user_id}, plan_id={plan_id}, {amount}₽/{days}д, expires={expires_at}")

    # Аналитика: классифицируем событие и пишем в subscription_events.
    event_type = _classify_subscription_event(
        existing_plan=existing['plan'] if existing else None,
        existing_plan_id=existing['plan_id'] if existing else None,
        existing_plan_days=existing['plan_days'] if existing else None,
        existing_expires_at=existing['expires_at'] if existing else None,
        new_plan_id=plan_id,
        new_plan_days=days,
        granted_by=granted_by,
        now=now,
    )

    await record_subscription_event(
        user_id=user_id,
        event_type=event_type,
        old_plan_id=existing['plan_id'] if existing else None,
        new_plan_id=plan_id,
        old_expires_at=existing['expires_at'] if existing else None,
        new_expires_at=expires_at,
        amount_rub=amount if granted_by is None else 0,
        payment_id=payment_id,
        source=source or ('admin' if granted_by else 'unknown'),
        metadata={
            'days': days,
            'has_payment_method': payment_method_id is not None,
            'granted_by': granted_by,
        },
    )

    return expires_at


async def downgrade_to_free(user_id: int, _from_lazy_expiration: bool = False):
    db = await get_db()

    # Получаем старое состояние для аналитики
    async with db.pool.acquire() as conn:
        old = await conn.fetchrow("""
            SELECT plan_id, expires_at FROM subscriptions WHERE user_id = $1
        """, user_id)

        await conn.execute("""
            UPDATE subscriptions
            SET plan = 'free', auto_pay_method_id = NULL, updated_at = CURRENT_TIMESTAMP
            WHERE user_id = $1
        """, user_id)

    logger.info(f"⬇️ Пользователь {user_id} переведён на FREE план")

    # Аналитика: lazy-expiration НЕ пишется в события (искажение времени).
    # Реальное истечение считается из subscriptions.expires_at в SQL-views.
    if _from_lazy_expiration:
        return

    # Если попали сюда не по lazy-expiration: значит revoke вручную
    # (revoke_pro). Логируем как cancelled.
    await record_subscription_event(
        user_id=user_id,
        event_type='cancelled',
        old_plan_id=old['plan_id'] if old else None,
        new_plan_id=None,
        old_expires_at=old['expires_at'] if old else None,
        source='manual_downgrade',
    )


async def revoke_pro(user_id: int):
    db = await get_db()

    # Получаем старое состояние для аналитики
    async with db.pool.acquire() as conn:
        old = await conn.fetchrow("""
            SELECT plan_id, expires_at FROM subscriptions WHERE user_id = $1
        """, user_id)

        await conn.execute("""
            UPDATE subscriptions
            SET plan = 'free', auto_pay_method_id = NULL, updated_at = CURRENT_TIMESTAMP
            WHERE user_id = $1
        """, user_id)

    logger.info(f"⬇️ revoke_pro: user_id={user_id}")

    await record_subscription_event(
        user_id=user_id,
        event_type='revoked_by_admin',
        old_plan_id=old['plan_id'] if old else None,
        new_plan_id=None,
        old_expires_at=old['expires_at'] if old else None,
        source='admin',
    )


async def reset_all_usage_limits():
    db = await get_db()
    now = datetime.now()
    next_reset = (now.replace(day=1) + timedelta(days=32)).replace(day=1)

    async with db.pool.acquire() as conn:
        await conn.execute("""
            UPDATE usage_limits
            SET analyses_used = 0, questions_used = 0, reset_date = $1
            WHERE reset_date <= $2
        """, next_reset, now)

    logger.info(f"🔄 Лимиты использования сброшены, следующий сброс: {next_reset}")


async def get_expiring_subscriptions(days_before: int = 1) -> list:
    """Получить подписки, истекающие через N дней (для автоплатежей)"""
    db = await get_db()
    now = datetime.now()
    target_date = now + timedelta(days=days_before)

    await ensure_plan_columns()

    async with db.pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT s.user_id, s.expires_at, s.auto_pay_method_id,
                   COALESCE(s.plan_amount, 199) as plan_amount,
                   COALESCE(s.plan_days, 30) as plan_days,
                   s.plan_id
            FROM subscriptions s
            WHERE s.plan = 'pro'
              AND s.auto_pay_method_id IS NOT NULL
              AND s.expires_at BETWEEN $1 AND $2
              AND s.granted_by_admin IS NULL
        """, now, target_date)

    return [dict(row) for row in rows]
