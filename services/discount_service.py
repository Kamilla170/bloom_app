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


# ======================= Автоправила (фаза 2) =======================
#
# Каждое правило — SELECT, возвращающий user_id подходящих под критерии.
# Общие гарды во всех: не активный Pro; нет активной скидки (не стакаем две живые).
# Ни кулдауна, ни лимита на число скидок — частоту регулируешь включением/выключением
# правила вручную (иначе, оставив правило включённым, юзер будет получать скидку
# снова при каждом истечении). Скидка новичкам сюда НЕ входит — всегда включена.
# Глубину/срок берём НЕ отсюда, а из таблицы discount_rules (тюнинг из админки).

ELIGIBILITY_SQL = {
    # Начал оплату, но не завершил, и до сих пор free.
    "abandoned_checkout": """
        SELECT DISTINCT p.user_id AS user_id
        FROM payments p
        WHERE p.status IN ('pending', 'canceled', 'waiting_for_capture')
          AND p.created_at > NOW() - INTERVAL '3 days'
          AND p.created_at < NOW() - INTERVAL '2 hours'
          AND NOT EXISTS (SELECT 1 FROM payments ps
                          WHERE ps.user_id = p.user_id AND ps.status = 'succeeded')
          AND NOT EXISTS (SELECT 1 FROM subscriptions s WHERE s.user_id = p.user_id
                          AND s.plan = 'pro' AND (s.expires_at IS NULL OR s.expires_at > NOW()))
          AND NOT EXISTS (SELECT 1 FROM discounts d WHERE d.user_id = p.user_id
                          AND d.expires_at > NOW())
    """,
    # Активный free-юзер, много раз возвращавшийся к ИИ (лимит 1/день →
    # questions_asked ≈ число дней с упором в пейвол).
    "repeated_paywall": """
        SELECT u.user_id AS user_id
        FROM users u
        WHERE u.questions_asked >= 5
          AND COALESCE(u.last_activity, u.created_at) > NOW() - INTERVAL '14 days'
          AND NOT EXISTS (SELECT 1 FROM payments ps
                          WHERE ps.user_id = u.user_id AND ps.status = 'succeeded')
          AND NOT EXISTS (SELECT 1 FROM subscriptions s WHERE s.user_id = u.user_id
                          AND s.plan = 'pro' AND (s.expires_at IS NULL OR s.expires_at > NOW()))
          AND NOT EXISTS (SELECT 1 FROM discounts d WHERE d.user_id = u.user_id
                          AND d.expires_at > NOW())
    """,
    # Бывший платник (был succeeded-платёж), подписка истекла 3..60 дней назад.
    "winback": """
        SELECT u.user_id AS user_id
        FROM users u
        JOIN subscriptions s ON s.user_id = u.user_id
        WHERE EXISTS (SELECT 1 FROM payments ps
                      WHERE ps.user_id = u.user_id AND ps.status = 'succeeded')
          AND s.expires_at IS NOT NULL
          AND s.expires_at < NOW() - INTERVAL '3 days'
          AND s.expires_at > NOW() - INTERVAL '60 days'
          AND NOT EXISTS (SELECT 1 FROM discounts d WHERE d.user_id = u.user_id
                          AND d.expires_at > NOW())
    """,
}


async def get_rules() -> list[dict]:
    """Конфиг автоправил (для админки)."""
    db = await get_db()
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT source, enabled, percent, duration_days, auto_disable_at, "
            "updated_at, updated_by FROM discount_rules ORDER BY source"
        )
    return [dict(r) for r in rows]


async def set_rule(
    source: str,
    enabled: Optional[bool] = None,
    percent: Optional[int] = None,
    duration_days: Optional[int] = None,
    enable_days: Optional[int] = None,
    updated_by: Optional[int] = None,
) -> bool:
    """
    Обновить правило (только переданные поля). False, если source неизвестен.

    enable_days действует только при включении (enabled=True): правило само
    выключится через столько дней (auto_disable_at). Если при включении
    enable_days не задан — правило работает до ручного выключения. При
    выключении (enabled=False) auto_disable_at сбрасывается.
    """
    if source not in ELIGIBILITY_SQL:
        return False
    db = await get_db()
    async with db.pool.acquire() as conn:
        if enabled is True:
            auto_disable_at = (
                datetime.utcnow() + timedelta(days=enable_days)
                if enable_days and enable_days > 0 else None
            )
            result = await conn.execute(
                """
                UPDATE discount_rules
                SET enabled = TRUE, auto_disable_at = $2,
                    percent = COALESCE($3, percent),
                    duration_days = COALESCE($4, duration_days),
                    updated_by = $5, updated_at = CURRENT_TIMESTAMP
                WHERE source = $1
                """,
                source, auto_disable_at, percent, duration_days, updated_by,
            )
        elif enabled is False:
            result = await conn.execute(
                """
                UPDATE discount_rules
                SET enabled = FALSE, auto_disable_at = NULL,
                    percent = COALESCE($2, percent),
                    duration_days = COALESCE($3, duration_days),
                    updated_by = $4, updated_at = CURRENT_TIMESTAMP
                WHERE source = $1
                """,
                source, percent, duration_days, updated_by,
            )
        else:
            result = await conn.execute(
                """
                UPDATE discount_rules
                SET percent = COALESCE($2, percent),
                    duration_days = COALESCE($3, duration_days),
                    updated_by = COALESCE($4, updated_by),
                    updated_at = CURRENT_TIMESTAMP
                WHERE source = $1
                """,
                source, percent, duration_days, updated_by,
            )
    try:
        return int(result.split()[-1]) > 0
    except (ValueError, IndexError):
        return False


async def _disable_expired_rules() -> None:
    """Выключить правила, у которых наступил срок авто-выключения."""
    db = await get_db()
    async with db.pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE discount_rules
            SET enabled = FALSE, auto_disable_at = NULL, updated_at = CURRENT_TIMESTAMP
            WHERE enabled = TRUE
              AND auto_disable_at IS NOT NULL
              AND auto_disable_at < NOW()
            """
        )


async def eligible_count(source: str) -> int:
    """Сколько юзеров подходят под правило прямо сейчас (у кого нет активной скидки)."""
    sql = ELIGIBILITY_SQL.get(source)
    if not sql:
        return 0
    db = await get_db()
    async with db.pool.acquire() as conn:
        return (await conn.fetchval(f"SELECT COUNT(*) FROM ({sql}) sub")) or 0


async def run_auto_discounts(only_source: Optional[str] = None) -> dict:
    """
    Прогнать включённые автоправила — выдать скидки подходящим (с holdout).
    only_source — прогнать одно правило (например, сразу при включении в админке).
    Возвращает {source: сколько выдано}.
    """
    # Сначала гасим правила, у которых истёк срок авто-выключения.
    await _disable_expired_rules()

    result: dict = {}
    for rule in await get_rules():
        source = rule["source"]
        if only_source and source != only_source:
            continue
        if not rule["enabled"]:
            continue
        sql = ELIGIBILITY_SQL.get(source)
        if not sql:
            continue
        db = await get_db()
        async with db.pool.acquire() as conn:
            rows = await conn.fetch(sql)
        granted = 0
        for row in rows:
            ok = await create_discount(
                row["user_id"],
                percent=rule["percent"],
                days=rule["duration_days"],
                source=source,
                allow_holdout=True,
            )
            if ok:
                granted += 1
        result[source] = granted
        logger.info(f"🏷️ auto-discount '{source}': кандидатов={len(rows)}, выдано={granted}")
    return result


async def list_free_users(limit: int = 200) -> list[dict]:
    """
    Free-юзеры (не активный Pro) для ручной выдачи скидки, свежие сверху.
    Поля для решения: вовлечённость (растения/вопросы/активность), регистрация,
    платежи (был платящим / начинал оплату), есть ли уже активная скидка.
    """
    db = await get_db()
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                u.user_id, u.email, u.first_name, u.created_at,
                u.plants_count, u.questions_asked, u.last_activity,
                EXISTS (SELECT 1 FROM payments p
                        WHERE p.user_id = u.user_id AND p.status = 'succeeded') AS was_payer,
                EXISTS (SELECT 1 FROM payments p
                        WHERE p.user_id = u.user_id
                          AND p.status IN ('pending','canceled','waiting_for_capture')) AS had_checkout,
                EXISTS (SELECT 1 FROM discounts d
                        WHERE d.user_id = u.user_id AND d.expires_at > NOW()) AS has_active_discount
            FROM users u
            WHERE NOT EXISTS (SELECT 1 FROM subscriptions s WHERE s.user_id = u.user_id
                              AND s.plan = 'pro' AND (s.expires_at IS NULL OR s.expires_at > NOW()))
            ORDER BY u.last_activity DESC NULLS LAST, u.created_at DESC
            LIMIT $1
            """,
            limit,
        )
    return [dict(r) for r in rows]
