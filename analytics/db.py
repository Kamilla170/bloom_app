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
# ОБЗОР
# ============================================================

async def get_kpi_summary() -> dict:
    mrr = await fetchrow("SELECT * FROM analytics.v_mrr_current", cache_key="kpi:mrr")
    total_users = await fetchrow("SELECT COUNT(*) AS total_users FROM users", cache_key="kpi:total_users")
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
        SELECT month, payments_count, unique_payers, revenue_rub,
               recurring_revenue_rub, new_revenue_rub
        FROM analytics.v_revenue_monthly
        ORDER BY month ASC LIMIT 12
        """,
        cache_key="revenue_monthly",
    )


async def get_ai_costs_monthly() -> list[dict]:
    return await fetch(
        """
        SELECT month, requests_count, unique_users, cost_usd, cost_rub
        FROM analytics.v_ai_costs_total_monthly
        ORDER BY month ASC LIMIT 12
        """,
        cache_key="ai_costs_monthly",
    )


async def get_subscriptions_by_plan() -> list[dict]:
    return await fetch(
        """
        SELECT plan_id, plan_label, plan_days, active_count,
               mrr_rub, arpu_rub, with_auto_pay
        FROM analytics.v_subscriptions_by_plan
        ORDER BY plan_days ASC
        """,
        cache_key="subs_by_plan",
    )


async def get_heavy_ai_users(limit: int = 10) -> list[dict]:
    return await fetch(
        """
        SELECT user_id, user_created_at, lifetime_revenue_rub,
               lifetime_ai_cost_rub, lifetime_ai_requests,
               gross_margin_rub, current_plan_id, is_currently_paying
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
        "SELECT date, dau, wau, mau FROM analytics.v_dau_mau_daily ORDER BY date ASC",
        cache_key="dau_mau",
    )


async def get_signup_funnel() -> list[dict]:
    return await fetch(
        """
        SELECT signup_week, signups, converted_to_paid,
               conversion_rate_pct, cohort_revenue_rub
        FROM analytics.v_signup_to_paid
        ORDER BY signup_week DESC LIMIT 12
        """,
        cache_key="signup_funnel",
    )


async def get_ai_cost_breakdown() -> list[dict]:
    return await fetch(
        """
        SELECT request_type,
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
    row = await fetchrow(
        """
        SELECT COUNT(*) AS total_users,
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
# ПОДПИСКИ
# ============================================================

async def get_churn_summary() -> dict:
    row = await fetchrow("SELECT * FROM analytics.v_churn_summary", cache_key="subs:churn_summary")
    return dict(row) if row else {}


async def get_churn_by_month() -> list[dict]:
    return await fetch(
        "SELECT month, voluntary, involuntary, admin_revoked, total FROM analytics.v_churn_by_month ORDER BY month ASC",
        cache_key="subs:churn_by_month",
    )


async def get_days_to_churn_distribution() -> list[dict]:
    return await fetch(
        "SELECT bucket_order, bucket_label, count FROM analytics.v_days_to_churn_distribution ORDER BY bucket_order ASC",
        cache_key="subs:days_to_churn",
    )


async def get_reactivation() -> dict:
    row = await fetchrow("SELECT * FROM analytics.v_reactivation", cache_key="subs:reactivation")
    return dict(row) if row else {}


async def get_plan_switching() -> list[dict]:
    return await fetch(
        "SELECT from_plan, to_plan, transitions, unique_users FROM analytics.v_plan_switching ORDER BY from_plan, to_plan",
        cache_key="subs:plan_switching",
    )


async def get_mrr_movement_monthly() -> list[dict]:
    return await fetch(
        """
        SELECT month, new_mrr, expansion_mrr, contraction_mrr,
               churn_mrr, net_mrr_change, quick_ratio
        FROM analytics.v_mrr_movement_monthly ORDER BY month ASC
        """,
        cache_key="subs:mrr_movement",
    )


async def get_refund_rate_monthly() -> list[dict]:
    return await fetch(
        "SELECT month, payments, refunds, refund_rate_pct FROM analytics.v_refund_rate_monthly ORDER BY month ASC",
        cache_key="subs:refund_rate",
    )


async def get_failed_payment_rate_monthly() -> list[dict]:
    return await fetch(
        "SELECT month, success, failed, failed_rate_pct FROM analytics.v_failed_payment_rate_monthly ORDER BY month ASC",
        cache_key="subs:failed_payment_rate",
    )


# ============================================================
# ЮНИТ-ЭКОНОМИКА
# ============================================================

async def get_unit_econ_summary() -> dict:
    row = await fetchrow("SELECT * FROM analytics.v_unit_econ_summary", cache_key="unit:summary")
    return dict(row) if row else {}


async def get_ai_cost_by_feature() -> list[dict]:
    return await fetch(
        """
        SELECT request_type, requests, unique_users, total_cost_usd,
               total_cost_rub, avg_cost_per_request_rub, avg_latency_ms, share_pct
        FROM analytics.v_ai_cost_by_feature
        """,
        cache_key="unit:cost_by_feature",
    )


async def get_margin_distribution() -> list[dict]:
    return await fetch(
        "SELECT bucket_order, bucket_label, count FROM analytics.v_margin_distribution ORDER BY bucket_order ASC",
        cache_key="unit:margin_dist",
    )


async def get_heavy_users_extended(limit: int = 20) -> list[dict]:
    return await fetch(
        f"SELECT * FROM analytics.v_heavy_users_extended LIMIT {int(limit)}",
        cache_key=f"unit:heavy_ext:{limit}",
    )


async def get_ltv_by_cohort() -> list[dict]:
    return await fetch(
        """
        SELECT cohort_month, cohort_size, life_month,
               cumulative_revenue_rub, ltv_per_user_rub
        FROM analytics.v_ltv_by_cohort
        ORDER BY cohort_month DESC, life_month ASC
        """,
        cache_key="unit:ltv",
    )


async def get_payback_by_cohort() -> list[dict]:
    return await fetch(
        """
        SELECT cohort_month, cohort_size, paid_back_users,
               payback_rate_pct, median_payback_days, avg_payback_days
        FROM analytics.v_payback_by_cohort
        """,
        cache_key="unit:payback",
    )


# ============================================================
# КОГОРТЫ
# ============================================================

async def get_cohort_retention_triangle() -> list[dict]:
    return await fetch(
        """
        SELECT cohort_month, cohort_size, life_month, active_users, retention_pct
        FROM analytics.v_cohort_retention_triangle
        ORDER BY cohort_month DESC, life_month ASC
        """,
        cache_key="cohorts:retention_triangle",
    )


async def get_cohort_dn_retention() -> list[dict]:
    return await fetch(
        "SELECT cohort_week, cohort_size, d7_pct, d14_pct, d28_pct, d90_pct FROM analytics.v_cohort_dn_retention",
        cache_key="cohorts:dn_retention",
    )


async def get_subscription_retention_curve() -> list[dict]:
    return await fetch(
        "SELECT cohort_month, cohort_size, d30_pct, d60_pct, d90_pct, d180_pct FROM analytics.v_subscription_retention_curve",
        cache_key="cohorts:sub_retention",
    )


async def get_cumulative_cohort_revenue() -> list[dict]:
    return await fetch(
        "SELECT cohort_month, cohort_size, total_revenue_rub, revenue_per_user_rub FROM analytics.v_cumulative_cohort_revenue",
        cache_key="cohorts:cumulative_revenue",
    )


# ============================================================
# ПРОДУКТ
# ============================================================

async def get_plants_per_user_distribution() -> list[dict]:
    return await fetch(
        "SELECT bucket_order, bucket_label, count FROM analytics.v_plants_per_user_distribution ORDER BY bucket_order ASC",
        cache_key="product:plants_dist",
    )


async def get_plants_per_user_stats() -> dict:
    row = await fetchrow("SELECT * FROM analytics.v_plants_per_user_stats", cache_key="product:plants_stats")
    return dict(row) if row else {}


async def get_ai_questions_weekly() -> list[dict]:
    return await fetch(
        """
        SELECT week, active_qa_users, total_questions, avg_per_user, median_per_user
        FROM analytics.v_ai_questions_per_user_weekly ORDER BY week ASC
        """,
        cache_key="product:qa_weekly",
    )


async def get_photos_per_user() -> dict:
    row = await fetchrow("SELECT * FROM analytics.v_photos_per_user", cache_key="product:photos")
    return dict(row) if row else {}


async def get_care_actions_summary() -> list[dict]:
    return await fetch(
        "SELECT action_type, actions_count, unique_users, unique_plants, avg_per_user FROM analytics.v_care_actions_summary",
        cache_key="product:care_actions",
    )


async def get_plants_by_state() -> list[dict]:
    return await fetch(
        "SELECT state, plants_count, share_pct FROM analytics.v_plants_by_state",
        cache_key="product:plants_by_state",
    )


# ============================================================
# STREAK (engagement маркер)
# ============================================================

async def get_streak_summary() -> dict:
    row = await fetchrow("SELECT * FROM analytics.v_streak_summary", cache_key="streak:summary")
    return dict(row) if row else {}


async def get_streak_distribution() -> list[dict]:
    return await fetch(
        "SELECT bucket_order, bucket_label, count FROM analytics.v_streak_distribution ORDER BY bucket_order ASC",
        cache_key="streak:distribution",
    )


async def get_top_streak_users() -> list[dict]:
    return await fetch(
        """
        SELECT user_id, plants_count, best_current_streak,
               best_max_streak, avg_current_streak, plants_active_now
        FROM analytics.v_top_streak_users
        """,
        cache_key="streak:top_users",
    )


# ============================================================
# ACTIVATION FUNNEL
# ============================================================

async def get_activation_summary() -> dict:
    row = await fetchrow(
        "SELECT * FROM analytics.v_activation_summary",
        cache_key="activation:summary",
    )
    return dict(row) if row else {}


async def get_activation_funnel_weekly() -> list[dict]:
    return await fetch(
        """
        SELECT cohort_week, signups, step_plant_added, step_photo_analysis,
               step_activated, step_ai_question,
               pct_plant, pct_photo, pct_activated, pct_qa
        FROM analytics.v_activation_funnel_weekly
        ORDER BY cohort_week DESC
        """,
        cache_key="activation:funnel_weekly",
    )


# ============================================================
# AARRR (пиратские метрики - сводка)
# ============================================================

async def get_aarrr_acquisition() -> dict:
    row = await fetchrow(
        "SELECT * FROM analytics.v_aarrr_acquisition",
        cache_key="aarrr:acquisition",
    )
    return dict(row) if row else {}


async def get_aarrr_revenue_trend() -> dict:
    row = await fetchrow(
        "SELECT * FROM analytics.v_aarrr_revenue_trend",
        cache_key="aarrr:revenue",
    )
    return dict(row) if row else {}


async def get_aarrr_retention() -> dict:
    row = await fetchrow(
        "SELECT * FROM analytics.v_aarrr_retention",
        cache_key="aarrr:retention",
    )
    return dict(row) if row else {}
