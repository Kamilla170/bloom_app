"""
Слой «право на скидку» (discount eligibility).

Единая точка: и ручные скидки (админ), и будущие автоправила создают строки в
таблице `discounts`. Клиент читает результат через /payments/plans без изменений.

Скидка новичкам (`new_user`) считается на лету по возрасту аккаунта и ВСЕГДА
доступна — она не хранится строкой и не управляется правилами/переключателями.
"""

import logging
from datetime import datetime, timedelta
from typing import Optional

from config import SUBSCRIPTION_PLANS, DISCOUNT_PLANS, DISCOUNT_DURATION_DAYS
from database import get_db
from services.subscription_service import is_pro

logger = logging.getLogger(__name__)


def _new_user_percent() -> int:
    """Процент скидки новичка для баннера. Как раньше — от месячного тарифа."""
    regular = SUBSCRIPTION_PLANS["1month"]["price"]
    promo = DISCOUNT_PLANS["1month"]["price"]
    if regular <= 0:
        return 0
    return round((1 - promo / regular) * 100)


async def _new_user_discount(user_id: int) -> Optional[dict]:
    """Скидка новичкам по возрасту аккаунта. None, если аккаунт старше окна."""
    try:
        db = await get_db()
        async with db.pool.acquire() as conn:
            created_at = await conn.fetchval(
                "SELECT created_at FROM users WHERE user_id = $1", user_id
            )
        if not created_at:
            return None
        if created_at.tzinfo:
            created_at = created_at.replace(tzinfo=None)
        days_since = (datetime.utcnow() - created_at).total_seconds() / 86400
        if days_since > DISCOUNT_DURATION_DAYS:
            return None
        return {
            "kind": "new_user",
            "percent": _new_user_percent(),
            "ends_at": created_at + timedelta(days=DISCOUNT_DURATION_DAYS),
            "label": "Скидка для новых пользователей",
        }
    except Exception as e:
        logger.error(f"❌ new_user discount check failed for user_id={user_id}: {e}")
        return None


async def get_active_discount(user_id: int) -> Optional[dict]:
    """
    Активная скидка юзера или None. Возвращает dict {kind, percent, ends_at, label}:
      - kind='flat'     — строка из discounts, цена = регуляр * (1 - percent/100)
      - kind='new_user' — скидка новичка, цена берётся из DISCOUNT_PLANS (пер-план)
    Не выдаём Pro-юзерам и тем, кто в holdout (контрольная группа).
    """
    if await is_pro(user_id):
        return None

    db = await get_db()
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT percent, label, expires_at
            FROM discounts
            WHERE user_id = $1
              AND is_holdout = FALSE
              AND expires_at > NOW()
            ORDER BY percent DESC, expires_at DESC
            LIMIT 1
            """,
            user_id,
        )
    if row:
        return {
            "kind": "flat",
            "percent": row["percent"],
            "ends_at": row["expires_at"],
            "label": row["label"] or "Персональная скидка",
        }

    # Всегда доступная скидка новичкам — вне таблицы и правил.
    return await _new_user_discount(user_id)


def price_for_plan(plan_id: str, regular: dict, discount: Optional[dict]) -> int:
    """Цена тарифа с учётом активной скидки (считается на сервере, не с клиента)."""
    if not discount:
        return regular["price"]
    if discount["kind"] == "new_user":
        promo = DISCOUNT_PLANS.get(plan_id)
        return promo["price"] if promo else regular["price"]
    # flat percent
    return round(regular["price"] * (100 - discount["percent"]) / 100)


async def create_discount(
    user_id: int,
    percent: int,
    days: int,
    source: str,
    label: Optional[str] = None,
    created_by: Optional[int] = None,
    allow_holdout: bool = False,
) -> bool:
    """
    Создать скидку юзеру. Возвращает False, если пропущено (уже есть активная).

    allow_holdout=True (для автоправил) → 20% детерминированно уходят в контроль
    (is_holdout=TRUE, скидку не видят) для замера lift. Ручные гранты
    (allow_holdout=False) в контроль не уходят — админ явно хочет выдать.
    """
    db = await get_db()
    async with db.pool.acquire() as conn:
        existing = await conn.fetchval(
            "SELECT 1 FROM discounts WHERE user_id = $1 AND expires_at > NOW() LIMIT 1",
            user_id,
        )
        if existing:
            return False

        is_holdout = allow_holdout and ((user_id * 2654435761) % 100 >= 80)
        expires_at = datetime.utcnow() + timedelta(days=days)
        await conn.execute(
            """
            INSERT INTO discounts (user_id, percent, source, label, expires_at,
                                   is_holdout, created_by)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            user_id, percent, source, label, expires_at, is_holdout, created_by,
        )
    return True


async def list_active_discounts(user_id: int) -> list[dict]:
    """Активные скидки юзера (для админки)."""
    db = await get_db()
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, percent, source, label, starts_at, expires_at,
                   is_holdout, used_at, created_by, created_at
            FROM discounts
            WHERE user_id = $1 AND expires_at > NOW()
            ORDER BY created_at DESC
            """,
            user_id,
        )
    return [dict(r) for r in rows]


async def revoke_discount(discount_id: int) -> bool:
    """Отозвать активную скидку (истечь сейчас). False, если такой активной нет."""
    db = await get_db()
    async with db.pool.acquire() as conn:
        result = await conn.execute(
            "UPDATE discounts SET expires_at = NOW() WHERE id = $1 AND expires_at > NOW()",
            discount_id,
        )
    try:
        return int(result.split()[-1]) > 0
    except (ValueError, IndexError):
        return False
