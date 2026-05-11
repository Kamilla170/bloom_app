"""
Слой работы с БД.
Подключается к основной Postgres Bloom AI через DATABASE_URL.
Использует пул соединений asyncpg.
Кэширует результаты запросов в памяти на CACHE_TTL_SECONDS секунд.
"""
import os
import time
import logging
from typing import Any, Optional

import asyncpg

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
CACHE_TTL_SECONDS = 60

_pool: Optional[asyncpg.Pool] = None
_cache: dict[str, tuple[float, Any]] = {}


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL не задан в env")
        _pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=1,
            max_size=5,
            command_timeout=30,
        )
        logger.info("✅ DB pool создан")
    return _pool


async def close_pool():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


def _cache_get(key: str):
    entry = _cache.get(key)
    if not entry:
        return None
    ts, value = entry
    if time.time() - ts > CACHE_TTL_SECONDS:
        return None
    return value


def _cache_set(key: str, value: Any):
    _cache[key] = (time.time(), value)


async def fetch(query: str, *args, cache_key: Optional[str] = None) -> list[dict]:
    """Выполнить SELECT-запрос и вернуть список dict-ов. С опциональным кэшем."""
    if cache_key:
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached

    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *args)
        result = [dict(row) for row in rows]

    if cache_key:
        _cache_set(cache_key, result)
    return result


async def fetchrow(query: str, *args, cache_key: Optional[str] = None) -> Optional[dict]:
    """Выполнить SELECT и вернуть одну строку как dict (или None)."""
    if cache_key:
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(query, *args)
        result = dict(row) if row else None

    if cache_key:
        _cache_set(cache_key, result)
    return result


# ============================================================
# Обзор
# ============================================================

async def get_kpi_summary() -> dict:
    """Сводные KPI для верхних плашек дашборда."""
    mrr = await fetchrow(
        "SELECT * FROM analytics.v_mrr_current",
        cache_key="kpi:mrr",
    )

    total_users = await fetchrow(
        "SELECT COUNT(*) AS total_users FROM users",
        cache_key="kpi:total_users",
    )

    mau_row = await fetchrow(
        """
        SELECT COUNT(DISTINCT user_id) AS mau
        FROM users
        WHERE last_activity >= NOW() - INTERVAL '30 days'
        """,
        cache_key="kpi:mau",
    )

    ai_cost_this_month = await fetchrow(
        """
        SELECT
            COALESCE(SUM(cost_usd), 0)::numeric AS cost_usd,
            COALESCE(SUM(cost_usd), 0)::numeric * 95 AS cost_rub,
            COUNT(*) AS requests
        FROM ai_requests
        WHERE created_at >= DATE_TRUNC('month', NOW())
        """,
        cache_key="kpi:ai_cost_month",
    )

    return {
        "mrr_rub": float(mrr["mrr_rub"] or 0) if mrr else 0,
        "arr_rub": float(mrr["arr_rub"] or 0) if mrr else 0,
        "active_subscriptions": int(mrr["active_subscriptions"] or 0) if mrr else 0,
        "arpu_rub": float(mrr["arpu_rub"] or 0) if mrr else 0,
        "total_users": int(total_users["total_users"]) if total_users else 0,
        "mau": int(mau_row["mau"]) if mau_row else 0,
        "ai_cost_rub_month": float(ai_cost_this_month["cost_rub"]) if ai_cost_this_month else 0,
        "ai_cost_usd_month": float(ai_cost_this_month["cost_usd"]) if ai_cost_this_month else 0,
        "ai_requests_month": int(ai_cost_this_month["requests"]) if ai_cost_this_month else 0,
    }


async def get_revenue_monthly() -> list[dict]:
    return await fetch(
        """
        SELECT
            month,
            payments_count,
            unique_payers,
            revenue_rub,
            recurring_revenue_rub,
            new_revenue_rub
        FROM analytics.v_revenue_monthly
        ORDER BY month ASC
        LIMIT 12
        """,
        cache_key="revenue_monthly",
    )


async def get_ai_costs_monthly() -> list[dict]:
    return await fetch(
        """
        SELECT
            month,
            requests_count,
            unique_users,
            cost_usd,
            cost_rub
        FROM analytics.v_ai_costs_total_monthly
        ORDER BY month ASC
        LIMIT 12
        """,
        cache_key="ai_costs_monthly",
    )


async def get_subscriptions_by_plan() -> list[dict]:
    return await fetch(
        """
        SELECT
            plan_id,
            plan_label,
            plan_days,
            active_count,
            mrr_rub,
            arpu_rub,
            with_auto_pay
        FROM analytics.v_subscriptions_by_plan
        ORDER BY plan_days ASC
        """,
        cache_key="subs_by_plan",
    )


async def get_heavy_ai_users(limit: int = 10) -> list[dict]:
    return await fetch(
        """
        SELECT
            user_id,
            user_created_at,
            lifetime_revenue_rub,
            lifetime_ai_cost_rub,
            lifetime_ai_requests,
            gross_margin_rub,
            current_plan_id,
            is_currently_paying
        FROM analytics.v_user_economics
        WHERE lifetime_ai_requests > 0
        ORDER BY lifetime_ai_cost_rub DESC
        LIMIT $1
        """,
        limit,
        cache_key=f"heavy_ai:{limit}",
    )


async def get_dau_mau() -> list[dict]:
    return await fetch(
        """
        SELECT date, dau, wau, mau
        FROM analytics.v_dau_mau_daily
        ORDER BY date ASC
        """,
        cache_key="dau_mau",
    )


async def get_signup_funnel() -> list[dict]:
    return await fetch(
        """
        SELECT
            signup_week,
            signups,
            converted_to_paid,
            conversion_rate_pct,
            cohort_revenue_rub
        FROM analytics.v_signup_to_paid
        ORDER BY signup_week DESC
        LIMIT 12
        """,
        cache_key="signup_funnel",
    )


async def get_ai_cost_breakdown() -> list[dict]:
    """AI расходы по типам запросов за последний месяц."""
    return await fetch(
        """
        SELECT
            request_type,
            COUNT(*) AS requests,
            COUNT(DISTINCT user_id) AS unique_users,
            ROUND(SUM(cost_usd)::numeric, 4) AS cost_usd,
            ROUND(SUM(cost_usd)::numeric * 95, 2) AS cost_rub,
            ROUND(AVG(cost_usd)::numeric * 95, 2) AS avg_cost_rub,
            ROUND(AVG(latency_ms)::numeric, 0) AS avg_latency_ms
        FROM ai_requests
        WHERE created_at >= NOW() - INTERVAL '30 days'
        GROUP BY request_type
        ORDER BY cost_rub DESC
        """,
        cache_key="ai_breakdown",
    )


async def get_overall_economics() -> dict:
    """Сводная картина: total revenue, total AI cost, gross margin lifetime."""
    row = await fetchrow(
        """
        SELECT
            COUNT(*) AS total_users,
            COUNT(*) FILTER (WHERE lifetime_payments > 0) AS paying_users_ever,
            COUNT(*) FILTER (WHERE is_currently_paying) AS currently_paying,
            COALESCE(SUM(lifetime_revenue_rub), 0) AS total_revenue_rub,
            COALESCE(SUM(lifetime_ai_cost_rub), 0) AS total_ai_cost_rub,
            COALESCE(SUM(gross_margin_rub), 0) AS total_gross_margin_rub
        FROM analytics.v_user_economics
        """,
        cache_key="overall_econ",
    )
    return dict(row) if row else {}


# ============================================================
# Подписки
# ============================================================

async def get_churn_summary() -> dict:
    """KPI плашки для таба Подписки."""
    row = await fetchrow(
        "SELECT * FROM analytics.v_churn_summary",
        cache_key="subs:churn_summary",
    )
    return dict(row) if row else {}


async def get_churn_by_month() -> list[dict]:
    """Voluntary vs involuntary churn по месяцам, 12 мес."""
    return await fetch(
        """
        SELECT month, voluntary, involuntary, admin_revoked, total
        FROM analytics.v_churn_by_month
        ORDER BY month ASC
        """,
        cache_key="subs:churn_by_month",
    )


async def get_days_to_churn_distribution() -> list[dict]:
    """Гистограмма дней до churn."""
    return await fetch(
        """
        SELECT bucket_order, bucket_label, count
        FROM analytics.v_days_to_churn_distribution
        ORDER BY bucket_order ASC
        """,
        cache_key="subs:days_to_churn",
    )


async def get_reactivation() -> dict:
    """Reactivation rate 30d/60d/90d."""
    row = await fetchrow(
        "SELECT * FROM analytics.v_reactivation",
        cache_key="subs:reactivation",
    )
    return dict(row) if row else {}


async def get_plan_switching() -> list[dict]:
    """Матрица переходов между планами за 90 дней."""
    return await fetch(
        """
        SELECT from_plan, to_plan, transitions, unique_users
        FROM analytics.v_plan_switching
        ORDER BY from_plan, to_plan
        """,
        cache_key="subs:plan_switching",
    )


async def get_mrr_movement_monthly() -> list[dict]:
    """Net MRR Movement по месяцам, 12 мес."""
    return await fetch(
        """
        SELECT
            month,
            new_mrr,
            expansion_mrr,
            contraction_mrr,
            churn_mrr,
            net_mrr_change,
            quick_ratio
        FROM analytics.v_mrr_movement_monthly
        ORDER BY month ASC
        """,
        cache_key="subs:mrr_movement",
    )


async def get_refund_rate_monthly() -> list[dict]:
    """Refund rate по месяцам, 12 мес."""
    return await fetch(
        """
        SELECT month, payments, refunds, refund_rate_pct
        FROM analytics.v_refund_rate_monthly
        ORDER BY month ASC
        """,
        cache_key="subs:refund_rate",
    )


async def get_failed_payment_rate_monthly() -> list[dict]:
    """Failed recurring payment rate по месяцам, 12 мес."""
    return await fetch(
        """
        SELECT month, success, failed, failed_rate_pct
        FROM analytics.v_failed_payment_rate_monthly
        ORDER BY month ASC
        """,
        cache_key="subs:failed_payment_rate",
    )
