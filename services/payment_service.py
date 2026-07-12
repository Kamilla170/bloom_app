"""
Сервис платежей (API-версия, in-app через YooKassa Mobile SDK)
"""

import logging
import uuid
import aiohttp
from datetime import datetime
from typing import Dict, Optional
from base64 import b64encode

from config import YOOKASSA_SHOP_ID, YOOKASSA_SECRET_KEY, PRO_PRICE
from services.analytics_recorder import (
    record_yookassa_webhook,
    mark_webhook_processed,
    record_subscription_event,
)

logger = logging.getLogger(__name__)

YOOKASSA_API_URL = "https://api.yookassa.ru/v3"

# Флаг, что колонка payments.subscription_granted уже гарантированно создана
# в текущем процессе. Чтобы не гонять ALTER TABLE на каждый запрос статуса.
_granted_column_ensured = False


def _get_auth_header() -> str:
    credentials = f"{YOOKASSA_SHOP_ID}:{YOOKASSA_SECRET_KEY}"
    encoded = b64encode(credentials.encode()).decode()
    return f"Basic {encoded}"


def _get_headers(idempotency_key: str = None) -> dict:
    headers = {
        "Content-Type": "application/json",
        "Authorization": _get_auth_header(),
    }
    if idempotency_key:
        headers["Idempotence-Key"] = idempotency_key
    return headers


async def _ensure_subscription_granted_column():
    """
    Миграция: колонка-флаг payments.subscription_granted.
    Защищает от двойной выдачи подписки за один платёж (webhook + поллинг).
    Выполняется один раз за жизнь процесса (IF NOT EXISTS — безопасно).
    """
    global _granted_column_ensured
    if _granted_column_ensured:
        return
    try:
        from database import get_db
        db = await get_db()
        async with db.pool.acquire() as conn:
            await conn.execute(
                "ALTER TABLE payments ADD COLUMN IF NOT EXISTS "
                "subscription_granted BOOLEAN DEFAULT FALSE"
            )
        _granted_column_ensured = True
    except Exception as e:
        logger.error(f"❌ Не удалось создать колонку subscription_granted: {e}", exc_info=True)


async def _try_claim_payment(payment_id: str) -> bool:
    """
    Атомарно "занять" платёж для выдачи подписки.

    Возвращает True, если этот вызов первым перевёл subscription_granted в TRUE
    (значит можно выдавать подписку). Возвращает False, если флаг уже был занят
    (значит подписку за этот платёж уже выдал кто-то другой - webhook или
    предыдущий поллинг). Атомарность гарантирует БД: UPDATE ... WHERE
    subscription_granted = FALSE отработает ровно у одного конкурента.
    """
    await _ensure_subscription_granted_column()
    try:
        from database import get_db
        db = await get_db()
        async with db.pool.acquire() as conn:
            claimed = await conn.fetchval(
                """
                UPDATE payments
                SET subscription_granted = TRUE, updated_at = CURRENT_TIMESTAMP
                WHERE payment_id = $1 AND subscription_granted = FALSE
                RETURNING payment_id
                """,
                payment_id,
            )
        return claimed is not None
    except Exception as e:
        logger.error(f"❌ Ошибка claim платежа {payment_id}: {e}", exc_info=True)
        return False


async def _release_claim(payment_id: str):
    """
    Откатить захват флага (если после claim не удалось довести выдачу подписки,
    например не получили данные платежа). Чтобы webhook/следующий поллинг
    смогли попробовать снова.
    """
    try:
        from database import get_db
        db = await get_db()
        async with db.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE payments
                SET subscription_granted = FALSE, updated_at = CURRENT_TIMESTAMP
                WHERE payment_id = $1
                """,
                payment_id,
            )
    except Exception as e:
        logger.error(f"❌ Ошибка release claim {payment_id}: {e}", exc_info=True)


async def _fetch_payment_from_yookassa(payment_id: str) -> Optional[Dict]:
    """Запросить актуальный объект платежа у YooKassa."""
    if not YOOKASSA_SHOP_ID or not YOOKASSA_SECRET_KEY:
        return None
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{YOOKASSA_API_URL}/payments/{payment_id}",
                headers=_get_headers(),
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
    except Exception as e:
        logger.error(f"❌ Не удалось получить платёж {payment_id} из YooKassa: {e}")
    return None


async def _maybe_grant_subscription(
    payment_id: str,
    user_id: int,
    yookassa_data: Optional[Dict] = None,
    source: str = "polling_fallback",
):
    """
    Фоллбэк-выдача подписки при поллинге, если платёж succeeded, но подписка
    ещё не выдана (например, webhook не дошёл).

    Защита от дублей: сначала атомарно занимаем флаг. Если заняли - выдаём,
    если нет - значит webhook (или другой поллинг) уже выдал, выходим.

    metadata (days/amount/plan_id) и payment_method берём из объекта платежа
    YooKassa - так же, как это делает webhook из своего payload.
    """
    claimed = await _try_claim_payment(payment_id)
    if not claimed:
        # Подписку за этот платёж уже выдали (webhook успел) - не дублируем.
        return

    # Данные платежа нужны для activate_pro. Если не передали - запросим.
    if yookassa_data is None:
        yookassa_data = await _fetch_payment_from_yookassa(payment_id)

    if not yookassa_data:
        # Не смогли получить данные - откатываем захват, чтобы webhook/следующий
        # поллинг попробовали снова. Иначе подписка потеряется навсегда.
        await _release_claim(payment_id)
        logger.warning(
            f"⚠️ Фоллбэк: не удалось получить данные платежа {payment_id}, claim откатан"
        )
        return

    metadata = yookassa_data.get("metadata", {}) or {}
    days = int(metadata.get("days", 30))
    amount = int(metadata.get("amount", PRO_PRICE))
    plan_id = metadata.get("plan_id") or None
    plan_label = metadata.get("plan_label", f"{days} дней")

    payment_method = yookassa_data.get("payment_method", {}) or {}
    payment_method_id = payment_method.get("id") if payment_method.get("saved") else None

    # Сохраняем payment_method_id (для будущих автоплатежей), как делает webhook.
    if payment_method_id:
        try:
            from database import get_db
            db = await get_db()
            async with db.pool.acquire() as conn:
                await conn.execute(
                    "UPDATE payments SET payment_method_id = $1, updated_at = CURRENT_TIMESTAMP "
                    "WHERE payment_id = $2",
                    payment_method_id, payment_id,
                )
        except Exception as e:
            logger.error(f"❌ Не удалось сохранить payment_method_id (фоллбэк): {e}")

    from services.subscription_service import activate_pro
    expires_at = await activate_pro(
        user_id,
        days=days,
        amount=amount,
        payment_method_id=payment_method_id,
        plan_id=plan_id,
        payment_id=payment_id,
        source=source,
    )

    logger.info(
        f"✅ Подписка активирована ({source}): user_id={user_id}, "
        f"plan_id={plan_id}, план={plan_label}, expires={expires_at}"
    )

    await _notify_user_payment_success(user_id, expires_at, plan_label)


async def create_payment(
    user_id: int,
    payment_token: str,
    amount: int = None,
    days: int = 30,
    plan_label: str = "1 месяц",
    plan_id: str = None,
    save_method: bool = True,
) -> Optional[Dict]:
    """
    Создать платёж в YooKassa из payment_token, полученного на клиенте через SDK.
    """
    if not YOOKASSA_SHOP_ID or not YOOKASSA_SECRET_KEY:
        logger.error("❌ YooKassa не настроена")
        return None

    if not payment_token:
        logger.error("❌ Не передан payment_token")
        return None

    if amount is None:
        amount = PRO_PRICE

    idempotency_key = str(uuid.uuid4())
    description = f"Bloom AI подписка - {plan_label} (пользователь {user_id})"

    payload = {
        "amount": {"value": f"{amount}.00", "currency": "RUB"},
        "capture": True,
        "payment_token": payment_token,
        "description": description,
        "metadata": {
            "user_id": str(user_id),
            "type": "subscription",
            "days": str(days),
            "amount": str(amount),
            "plan_label": plan_label,
            "plan_id": plan_id or "",
        },
        "save_payment_method": save_method,
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{YOOKASSA_API_URL}/payments",
                headers=_get_headers(idempotency_key),
                json=payload,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                data = await resp.json()

                if resp.status == 200:
                    logger.info(
                        f"✅ Платёж создан: {data['id']} "
                        f"user_id={user_id}, plan_id={plan_id}, {plan_label}, {amount}₽, "
                        f"status={data.get('status')}"
                    )

                    from database import get_db
                    db = await get_db()
                    async with db.pool.acquire() as conn:
                        await conn.execute(
                            """
                            INSERT INTO payments (payment_id, user_id, amount, currency, status, description, plan_id, created_at)
                            VALUES ($1, $2, $3, 'RUB', $4, $5, $6, CURRENT_TIMESTAMP)
                            """,
                            data["id"], user_id, amount, data["status"], description, plan_id,
                        )

                    # Если YooKassa требует подтверждение (3DS), вернём confirmation_url.
                    # SDK на клиенте сам откроет его в WebView.
                    confirmation = data.get("confirmation") or {}
                    confirmation_url = confirmation.get("confirmation_url")

                    return {
                        "payment_id": data["id"],
                        "status": data["status"],
                        "confirmation_url": confirmation_url,
                    }
                else:
                    logger.error(f"❌ Ошибка создания платежа: {resp.status} {data}")
                    return None

    except Exception as e:
        logger.error(f"❌ Ошибка запроса к YooKassa: {e}", exc_info=True)
        return None


async def get_payment_status(payment_id: str) -> Optional[Dict]:
    """
    Получить актуальный статус платежа.
    Сначала смотрим в БД, если статус не финальный - запрашиваем YooKassa.

    Фоллбэк: если платёж succeeded, но подписка ещё не выдана (webhook не дошёл),
    выдаём подписку прямо здесь - с защитой от двойной выдачи через claim-флаг.
    """
    from database import get_db
    db = await get_db()

    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT payment_id, user_id, amount, status, plan_id FROM payments WHERE payment_id = $1",
            payment_id,
        )

    if not row:
        return None

    status = row["status"]

    # canceled - финальный статус, ничего не выдаём.
    if status == "canceled":
        return {
            "payment_id": row["payment_id"],
            "status": status,
            "amount": row["amount"],
            "plan_id": row["plan_id"],
        }

    # succeeded в базе: статус уже финальный, но могла не выполниться выдача
    # подписки (например, webhook записал статус, а activate_pro упал). Пробуем
    # выдать через фоллбэк - claim не даст задвоить, если подписка уже выдана.
    if status == "succeeded":
        await _maybe_grant_subscription(payment_id, row["user_id"])
        return {
            "payment_id": row["payment_id"],
            "status": status,
            "amount": row["amount"],
            "plan_id": row["plan_id"],
        }

    # Статус не финальный - спросим YooKassa
    if not YOOKASSA_SHOP_ID or not YOOKASSA_SECRET_KEY:
        return {
            "payment_id": row["payment_id"],
            "status": status,
            "amount": row["amount"],
            "plan_id": row["plan_id"],
        }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{YOOKASSA_API_URL}/payments/{payment_id}",
                headers=_get_headers(),
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    fresh_status = data.get("status", status)
                    if fresh_status != status:
                        async with db.pool.acquire() as conn:
                            await conn.execute(
                                "UPDATE payments SET status = $1, updated_at = CURRENT_TIMESTAMP WHERE payment_id = $2",
                                fresh_status, payment_id,
                            )

                    # Фоллбэк: платёж только что стал succeeded - выдаём подписку,
                    # если её ещё не выдал webhook. Данные платежа уже есть в data.
                    if fresh_status == "succeeded":
                        await _maybe_grant_subscription(
                            payment_id, row["user_id"], yookassa_data=data
                        )

                    return {
                        "payment_id": payment_id,
                        "status": fresh_status,
                        "amount": row["amount"],
                        "plan_id": row["plan_id"],
                    }
    except Exception as e:
        logger.error(f"❌ Не удалось получить статус платежа {payment_id}: {e}")

    return {
        "payment_id": row["payment_id"],
        "status": status,
        "amount": row["amount"],
        "plan_id": row["plan_id"],
    }


async def create_recurring_payment(
    user_id: int,
    payment_method_id: str,
    amount: int = None,
    days: int = 30,
    plan_id: str = None,
) -> Optional[Dict]:
    """Создать рекуррентный платёж по сохранённому методу."""
    if not YOOKASSA_SHOP_ID or not YOOKASSA_SECRET_KEY:
        return None

    if amount is None:
        amount = PRO_PRICE

    idempotency_key = str(uuid.uuid4())

    payload = {
        "amount": {"value": f"{amount}.00", "currency": "RUB"},
        "capture": True,
        "payment_method_id": payment_method_id,
        "description": f"Bloom AI - автопродление {days}д (пользователь {user_id})",
        "metadata": {
            "user_id": str(user_id),
            "type": "recurring",
            "days": str(days),
            "amount": str(amount),
            "plan_id": plan_id or "",
        },
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{YOOKASSA_API_URL}/payments",
                headers=_get_headers(idempotency_key),
                json=payload,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                data = await resp.json()

                if resp.status == 200:
                    logger.info(
                        f"✅ Рекуррентный платёж: {data['id']} user_id={user_id}, plan_id={plan_id}, {amount}₽/{days}д"
                    )

                    from database import get_db
                    db = await get_db()
                    async with db.pool.acquire() as conn:
                        await conn.execute(
                            """
                            INSERT INTO payments (payment_id, user_id, amount, currency, status, description, is_recurring, plan_id, created_at)
                            VALUES ($1, $2, $3, 'RUB', $4, $5, TRUE, $6, CURRENT_TIMESTAMP)
                            """,
                            data["id"], user_id, amount, data["status"], payload["description"], plan_id,
                        )

                    return {"payment_id": data["id"], "status": data["status"]}
                else:
                    logger.error(f"❌ Рекуррентный платёж ошибка: {resp.status} {data}")

                    await record_subscription_event(
                        user_id=user_id,
                        event_type="payment_failed",
                        new_plan_id=plan_id,
                        amount_rub=amount,
                        source="recurring_create_error",
                        metadata={
                            "http_status": resp.status,
                            "yookassa_response": data,
                            "days": days,
                        },
                    )
                    return None

    except Exception as e:
        logger.error(f"❌ Рекуррентный платёж ошибка: {e}", exc_info=True)

        await record_subscription_event(
            user_id=user_id,
            event_type="payment_failed",
            new_plan_id=plan_id,
            amount_rub=amount,
            source="recurring_create_exception",
            metadata={"error": str(e), "days": days},
        )
        return None


async def handle_payment_webhook(payload: dict) -> bool:
    """Обработка webhook от YooKassa."""
    webhook_id = await record_yookassa_webhook(payload)

    try:
        event_type = payload.get("event")
        payment_data = payload.get("object", {})
        payment_id = payment_data.get("id")
        status = payment_data.get("status")
        metadata = payment_data.get("metadata", {})
        user_id = metadata.get("user_id")

        if not payment_id or not user_id:
            logger.warning(f"⚠️ Webhook без payment_id или user_id")
            await mark_webhook_processed(webhook_id, error="missing payment_id or user_id")
            return False

        user_id = int(user_id)
        days = int(metadata.get("days", 30))
        amount = int(metadata.get("amount", PRO_PRICE))
        plan_id = metadata.get("plan_id") or None

        logger.info(
            f"💳 Webhook: event={event_type}, payment_id={payment_id}, "
            f"user_id={user_id}, plan_id={plan_id}, {amount}₽/{days}д"
        )

        from database import get_db
        db = await get_db()

        async with db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE payments SET status = $1, updated_at = CURRENT_TIMESTAMP WHERE payment_id = $2",
                status, payment_id,
            )

        if event_type == "payment.succeeded" and status == "succeeded":
            # БЕЗОПАСНОСТЬ: эндпоинт вебхука открыт и не проверяет подпись,
            # поэтому телу запроса доверять НЕЛЬЗЯ — иначе кто угодно мог бы
            # прислать поддельный payment.succeeded и получить PRO бесплатно.
            # Перезапрашиваем платёж напрямую у YooKassa и выдаём подписку
            # ТОЛЬКО если YooKassa подтверждает статус succeeded.
            verified = await _fetch_payment_from_yookassa(payment_id)
            if not verified or verified.get("status") != "succeeded":
                real_status = verified.get("status") if verified else "нет данных"
                logger.warning(
                    f"⛔ Webhook {payment_id}: платёж НЕ подтверждён YooKassa "
                    f"(статус: {real_status}) — выдача отклонена"
                )
                await mark_webhook_processed(
                    webhook_id, error="payment not verified with yookassa"
                )
                return False

            # user_id тоже берём из проверенных данных YooKassa, не из тела.
            verified_meta = verified.get("metadata", {}) or {}
            verified_user_id = verified_meta.get("user_id")
            if not verified_user_id:
                logger.warning(
                    f"⚠️ Webhook {payment_id}: нет user_id в metadata YooKassa"
                )
                await mark_webhook_processed(
                    webhook_id, error="no user_id in verified metadata"
                )
                return False

            # Единый безопасный путь выдачи (claim от дублей + активация по
            # данным YooKassa), тот же, что и фоллбэк-поллинг.
            await _maybe_grant_subscription(
                payment_id,
                int(verified_user_id),
                yookassa_data=verified,
                source="yookassa_webhook",
            )
            await mark_webhook_processed(webhook_id)
            return True

        elif event_type == "payment.canceled" and status == "canceled":
            cancellation = payment_data.get("cancellation_details", {})
            reason = cancellation.get("reason", "unknown")
            logger.warning(f"❌ Платёж отменён: user_id={user_id}, reason={reason}")

            if metadata.get("type") == "recurring":
                await record_subscription_event(
                    user_id=user_id,
                    event_type="payment_failed",
                    new_plan_id=plan_id,
                    amount_rub=amount,
                    payment_id=payment_id,
                    source="yookassa_webhook_recurring_canceled",
                    metadata={"cancellation_reason": reason, "days": days},
                )
                await _notify_user_payment_failed(user_id, reason)

            await mark_webhook_processed(webhook_id)
            return True

        elif event_type == "refund.succeeded":
            refund_amount_obj = payment_data.get("amount", {})
            refund_amount = (
                refund_amount_obj.get("value") if isinstance(refund_amount_obj, dict) else None
            )
            try:
                refund_amount_rub = int(float(refund_amount)) if refund_amount else None
            except (TypeError, ValueError):
                refund_amount_rub = None

            await record_subscription_event(
                user_id=user_id,
                event_type="refunded",
                new_plan_id=plan_id,
                amount_rub=refund_amount_rub,
                payment_id=payment_id,
                source="yookassa_webhook_refund",
                metadata={"refund_payload": payment_data},
            )
            logger.info(
                f"💸 Возврат: user_id={user_id}, payment_id={payment_id}, amount={refund_amount}"
            )
            await mark_webhook_processed(webhook_id)
            return True

        await mark_webhook_processed(webhook_id)
        return True

    except Exception as e:
        logger.error(f"❌ Ошибка webhook: {e}", exc_info=True)
        await mark_webhook_processed(webhook_id, error=str(e))
        return False


async def process_auto_payments():
    """Обработка автоплатежей - вызывается scheduler'ом."""
    from services.subscription_service import get_expiring_subscriptions

    expiring = await get_expiring_subscriptions(days_before=1)
    if not expiring:
        logger.info("💳 Нет подписок для автопродления")
        return

    logger.info(f"💳 Найдено {len(expiring)} подписок для автопродления")

    for sub in expiring:
        user_id = sub["user_id"]
        method_id = sub["auto_pay_method_id"]
        if not method_id:
            continue

        result = await create_recurring_payment(
            user_id, method_id,
            amount=sub.get("plan_amount", PRO_PRICE),
            days=sub.get("plan_days", 30),
            plan_id=sub.get("plan_id"),
        )

        if result:
            logger.info(f"✅ Автоплатёж: user_id={user_id}: {result['payment_id']}")
        else:
            logger.error(f"❌ Автоплатёж не создан: user_id={user_id}")
            await _notify_user_payment_failed(user_id, "auto_payment_creation_failed")


async def _notify_user_payment_success(user_id: int, expires_at: datetime, plan_label: str = ""):
    try:
        from services.fcm_service import send_push_to_user
        expires_str = expires_at.strftime("%d.%m.%Y")
        await send_push_to_user(
            user_id=user_id,
            title="🎉 Подписка активирована!",
            body=f"План: {plan_label}. Активна до {expires_str}",
            data={"type": "subscription_activated"},
        )
    except Exception as e:
        logger.error(f"❌ Не удалось уведомить user_id={user_id}: {e}")


async def _notify_user_payment_failed(user_id: int, reason: str):
    try:
        from services.fcm_service import send_push_to_user
        await send_push_to_user(
            user_id=user_id,
            title="⚠️ Не удалось продлить подписку",
            body="Откройте приложение, чтобы продлить вручную",
            data={"type": "payment_failed"},
        )
    except Exception as e:
        logger.error(f"❌ Не удалось уведомить user_id={user_id}: {e}")


async def cancel_auto_payment(user_id: int):
    from database import get_db
    db = await get_db()

    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT plan_id, expires_at FROM subscriptions WHERE user_id = $1",
            user_id,
        )

        await conn.execute(
            """
            UPDATE subscriptions
            SET auto_pay_method_id = NULL, updated_at = CURRENT_TIMESTAMP
            WHERE user_id = $1
            """,
            user_id,
        )

    logger.info(f"🔕 Автоплатёж отключён для user_id={user_id}")

    await record_subscription_event(
        user_id=user_id,
        event_type="auto_pay_disabled",
        old_plan_id=row["plan_id"] if row else None,
        new_plan_id=row["plan_id"] if row else None,
        new_expires_at=row["expires_at"] if row else None,
        source="user_action",
    )
