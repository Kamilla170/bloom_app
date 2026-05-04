"""
Запись аналитических событий: AI-запросы, события подписок, webhooks YooKassa.

ПРИНЦИП БЕЗОПАСНОСТИ:
Все функции защищены try/except. Любая ошибка в аналитике логируется,
но НЕ пробрасывается дальше: аналитика никогда не должна сломать
основную бизнес-логику (AI-ответы, платежи, подписки).
"""

import logging
import json
from datetime import datetime
from typing import Optional, Any, Dict

logger = logging.getLogger(__name__)


# ============================================================
# Цены OpenAI моделей (USD за 1M токенов).
#
# При изменении цен или подключении новой модели править ТОЛЬКО этот словарь.
# Если модели нет в словаре, используется FALLBACK_PRICE с предупреждением в лог.
# Запись всё равно создастся, просто стоимость может быть приблизительной.
# ============================================================
MODEL_PRICES = {
    # gpt-4o (актуально на конец 2024 - начало 2025)
    "gpt-4o": {"input": 2.50, "output": 10.00},
    "gpt-4o-2024-08-06": {"input": 2.50, "output": 10.00},
    "gpt-4o-mini": {"input": 0.15, "output": 0.60},

    # gpt-5.1: PLACEHOLDER. Уточнить актуальные цены и поправить здесь.
    "gpt-5.1-2025-11-13": {"input": 5.00, "output": 20.00},
    "gpt-5.1": {"input": 5.00, "output": 20.00},
}

# Fallback если модели нет в словаре. Консервативная (завышенная) оценка.
FALLBACK_PRICE = {"input": 5.00, "output": 20.00}


def calculate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """
    Рассчитать стоимость запроса в USD. Точность: 6 знаков (микроценты).
    """
    if not model:
        prices = FALLBACK_PRICE
    else:
        prices = MODEL_PRICES.get(model)
        if prices is None:
            logger.warning(
                f"⚠️ Цена для модели '{model}' не задана в MODEL_PRICES. "
                f"Используется FALLBACK_PRICE. Обновите services/analytics_recorder.py."
            )
            prices = FALLBACK_PRICE

    input_cost = (input_tokens or 0) * prices["input"] / 1_000_000
    output_cost = (output_tokens or 0) * prices["output"] / 1_000_000
    return round(input_cost + output_cost, 6)


# ============================================================
# AI-запросы
# ============================================================

async def record_ai_request(
    user_id: Optional[int],
    request_type: str,
    model: str,
    response: Any,
    latency_ms: Optional[int] = None,
    plant_id: Optional[int] = None,
    metadata: Optional[Dict] = None,
) -> Optional[int]:
    """
    Записать AI-запрос в ai_requests.

    Параметры:
        user_id: ID пользователя. Если None - запись пропускается.
        request_type: 'photo_analysis' | 'qa' | 'growing_plan' и т.п.
        model: имя модели OpenAI (точно как использовалось в API-вызове).
        response: объект ответа OpenAI (откуда тянем response.usage).
        latency_ms: время выполнения в мс (опционально).
        plant_id: связанное растение (опционально).
        metadata: произвольный JSON для отладки/контекста.

    Возвращает: id записи или None при ошибке.
    Никогда не пробрасывает исключения.
    """
    if user_id is None:
        return None

    try:
        usage = getattr(response, 'usage', None)
        if usage is None:
            logger.warning(f"⚠️ Нет usage в ответе модели {model}, ai_request не записан")
            return None

        input_tokens = getattr(usage, 'prompt_tokens', 0) or 0
        output_tokens = getattr(usage, 'completion_tokens', 0) or 0
        total_tokens = getattr(usage, 'total_tokens', input_tokens + output_tokens) or 0

        cost_usd = calculate_cost(model, input_tokens, output_tokens)

        from database import get_db
        db = await get_db()

        async with db.pool.acquire() as conn:
            request_id = await conn.fetchval("""
                INSERT INTO ai_requests
                    (user_id, request_type, model, input_tokens, output_tokens,
                     total_tokens, cost_usd, latency_ms, plant_id, metadata, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, CURRENT_TIMESTAMP)
                RETURNING id
            """,
                user_id, request_type, model,
                input_tokens, output_tokens, total_tokens,
                cost_usd, latency_ms, plant_id,
                json.dumps(metadata) if metadata else None,
            )

        return request_id

    except Exception as e:
        logger.error(f"❌ Ошибка записи ai_request: {e}", exc_info=True)
        return None


# ============================================================
# События подписок
# ============================================================

async def record_subscription_event(
    user_id: int,
    event_type: str,
    old_plan_id: Optional[str] = None,
    new_plan_id: Optional[str] = None,
    old_expires_at: Optional[datetime] = None,
    new_expires_at: Optional[datetime] = None,
    amount_rub: Optional[int] = None,
    payment_id: Optional[str] = None,
    source: Optional[str] = None,
    metadata: Optional[Dict] = None,
) -> Optional[int]:
    """
    Записать событие подписки в subscription_events.

    event_type:
        'created' | 'renewed' | 'upgraded' | 'downgraded' |
        'auto_pay_disabled' | 'auto_pay_enabled' |
        'cancelled' | 'payment_failed' |
        'granted_by_admin' | 'revoked_by_admin' |
        'refunded'

    Никогда не пробрасывает исключения.
    """
    try:
        from database import get_db
        db = await get_db()

        async with db.pool.acquire() as conn:
            event_id = await conn.fetchval("""
                INSERT INTO subscription_events
                    (user_id, event_type, old_plan_id, new_plan_id,
                     old_expires_at, new_expires_at,
                     amount_rub, payment_id, source, metadata, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, CURRENT_TIMESTAMP)
                RETURNING id
            """,
                user_id, event_type,
                old_plan_id, new_plan_id,
                old_expires_at, new_expires_at,
                amount_rub, payment_id, source,
                json.dumps(metadata) if metadata else None,
            )

        logger.info(
            f"📊 subscription_event: user_id={user_id}, type={event_type}, "
            f"plan={new_plan_id}, amount={amount_rub}"
        )
        return event_id

    except Exception as e:
        logger.error(f"❌ Ошибка записи subscription_event: {e}", exc_info=True)
        return None


# ============================================================
# Webhooks YooKassa
# ============================================================

async def record_yookassa_webhook(payload: dict) -> Optional[int]:
    """
    Записать raw payload webhook'а ЮKassa СРАЗУ при получении,
    до любой бизнес-обработки.

    Возвращает id записи (или None при ошибке).
    Никогда не пробрасывает исключения.
    """
    try:
        event_type = payload.get('event') if isinstance(payload, dict) else None
        payment_obj = payload.get('object', {}) if isinstance(payload, dict) else {}
        payment_id = payment_obj.get('id') if isinstance(payment_obj, dict) else None

        from database import get_db
        db = await get_db()

        async with db.pool.acquire() as conn:
            webhook_id = await conn.fetchval("""
                INSERT INTO yookassa_webhooks_log
                    (event_type, payment_id, payload, processed, received_at)
                VALUES ($1, $2, $3, FALSE, CURRENT_TIMESTAMP)
                RETURNING id
            """, event_type, payment_id, json.dumps(payload))

        return webhook_id

    except Exception as e:
        logger.error(f"❌ Ошибка записи yookassa_webhook: {e}", exc_info=True)
        return None


async def mark_webhook_processed(webhook_id: Optional[int], error: Optional[str] = None):
    """
    Пометить webhook как обработанный (или с ошибкой).
    Если webhook_id is None: ничего не делает (защита от случая, когда
    запись raw payload не удалась).
    """
    if webhook_id is None:
        return

    try:
        from database import get_db
        db = await get_db()

        async with db.pool.acquire() as conn:
            await conn.execute("""
                UPDATE yookassa_webhooks_log
                SET processed = $1, processed_at = CURRENT_TIMESTAMP, error = $2
                WHERE id = $3
            """, error is None, error, webhook_id)

    except Exception as e:
        logger.error(f"❌ Ошибка mark_webhook_processed: {e}", exc_info=True)
