"""
Bloom AI Analytics Dashboard.
"""

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import db
from auth import require_auth

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.get_pool()
    yield
    await db.close_pool()


app = FastAPI(title="Bloom AI Analytics", lifespan=lifespan)

import os as _os
if _os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="templates")


def _format_rub(value) -> str:
    if value is None:
        return "0 ₽"
    try:
        n = round(float(value))
    except (TypeError, ValueError):
        return f"{value} ₽"
    return f"{n:,}".replace(",", " ") + " ₽"


def _format_int(value) -> str:
    if value is None:
        return "0"
    try:
        return f"{int(value):,}".replace(",", " ")
    except (TypeError, ValueError):
        return str(value)


def _format_pct(value) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value):.1f}%"
    except (TypeError, ValueError):
        return str(value)


def _format_date(value) -> str:
    if value is None:
        return ""
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    return str(value)


def _format_month(value) -> str:
    if value is None:
        return ""
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m")
    return str(value)


PLAN_LABELS = {
    "1month": "1 мес",
    "3months": "3 мес",
    "6months": "6 мес",
    "12months": "12 мес",
}
PLAN_ORDER = ["1month", "3months", "6months", "12months"]


def _build_plan_switching_matrix(rows: list[dict]) -> dict:
    matrix = {fp: {tp: 0 for tp in PLAN_ORDER} for fp in PLAN_ORDER}
    for r in rows:
        fp = r.get("from_plan")
        tp = r.get("to_plan")
        if fp in matrix and tp in matrix[fp]:
            matrix[fp][tp] = int(r.get("transitions") or 0)
    return {
        "rows": [PLAN_LABELS[p] for p in PLAN_ORDER],
        "cols": [PLAN_LABELS[p] for p in PLAN_ORDER],
        "matrix": [[matrix[fp][tp] for tp in PLAN_ORDER] for fp in PLAN_ORDER],
    }


def _build_retention_triangle(rows: list[dict]) -> dict:
    by_cohort: dict = {}
    for r in rows:
        cm = r["cohort_month"]
        lm = int(r["life_month"])
        pct = r.get("retention_pct")
        if cm not in by_cohort:
            by_cohort[cm] = {
                "month": cm,
                "size": int(r["cohort_size"]),
                "cells": {},
            }
        by_cohort[cm]["cells"][lm] = float(pct) if pct is not None else None

    cohorts_sorted = sorted(by_cohort.values(), key=lambda x: x["month"], reverse=True)
    cohorts = [
        {"month": _format_month(c["month"]), "size": c["size"]}
        for c in cohorts_sorted
    ]
    cells = []
    for c in cohorts_sorted:
        row = []
        for lm in range(13):
            row.append(c["cells"].get(lm))
        cells.append(row)
    return {"cohorts": cohorts, "cells": cells, "life_months": list(range(13))}


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, _: str = Depends(require_auth)):
    try:
        # Обзор
        kpi = await db.get_kpi_summary()
        revenue_monthly = await db.get_revenue_monthly()
        ai_costs_monthly = await db.get_ai_costs_monthly()
        subs_by_plan = await db.get_subscriptions_by_plan()
        heavy_users = await db.get_heavy_ai_users(limit=10)
        dau_mau = await db.get_dau_mau()
        signup_funnel = await db.get_signup_funnel()
        ai_breakdown = await db.get_ai_cost_breakdown()
        overall = await db.get_overall_economics()

        # Подписки
        churn_summary = await db.get_churn_summary()
        churn_by_month = await db.get_churn_by_month()
        days_to_churn = await db.get_days_to_churn_distribution()
        reactivation = await db.get_reactivation()
        plan_switching_raw = await db.get_plan_switching()
        mrr_movement = await db.get_mrr_movement_monthly()
        refund_rate = await db.get_refund_rate_monthly()
        failed_payment_rate = await db.get_failed_payment_rate_monthly()

        # Юнит-экономика
        unit_summary = await db.get_unit_econ_summary()
        ai_by_feature = await db.get_ai_cost_by_feature()
        margin_dist = await db.get_margin_distribution()
        heavy_users_ext = await db.get_heavy_users_extended(limit=20)
        ltv_cohorts = await db.get_ltv_by_cohort()
        payback_cohorts = await db.get_payback_by_cohort()

        # Когорты
        retention_triangle_raw = await db.get_cohort_retention_triangle()
        cohort_dn = await db.get_cohort_dn_retention()
        sub_retention = await db.get_subscription_retention_curve()
        cumulative_revenue = await db.get_cumulative_cohort_revenue()

        # Продукт
        plants_dist = await db.get_plants_per_user_distribution()
        plants_stats = await db.get_plants_per_user_stats()
        qa_weekly = await db.get_ai_questions_weekly()
        photos_stats = await db.get_photos_per_user()
        care_summary = await db.get_care_actions_summary()
        plants_by_state = await db.get_plants_by_state()

        # Streak
        streak_summary = await db.get_streak_summary()
        streak_dist = await db.get_streak_distribution()
        top_streak_users = await db.get_top_streak_users()

        # Activation
        activation_summary = await db.get_activation_summary()
        activation_funnel = await db.get_activation_funnel_weekly()
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки данных: {e}", exc_info=True)
        return HTMLResponse(
            f"<h1>Ошибка загрузки данных</h1><pre>{e}</pre>",
            status_code=500,
        )

    # Графики Обзора
    revenue_chart = {
        "labels": [_format_month(r["month"]) for r in revenue_monthly],
        "revenue": [float(r["revenue_rub"] or 0) for r in revenue_monthly],
        "recurring": [float(r["recurring_revenue_rub"] or 0) for r in revenue_monthly],
        "new": [float(r["new_revenue_rub"] or 0) for r in revenue_monthly],
    }
    ai_cost_by_month = {_format_month(r["month"]): float(r["cost_rub"] or 0) for r in ai_costs_monthly}
    revenue_by_month = {_format_month(r["month"]): float(r["revenue_rub"] or 0) for r in revenue_monthly}
    all_months = sorted(set(ai_cost_by_month.keys()) | set(revenue_by_month.keys()))
    cost_vs_revenue = {
        "labels": all_months,
        "ai_cost": [ai_cost_by_month.get(m, 0) for m in all_months],
        "revenue": [revenue_by_month.get(m, 0) for m in all_months],
    }
    dau_mau_chart = {
        "labels": [_format_date(r["date"]) for r in dau_mau],
        "dau": [int(r["dau"] or 0) for r in dau_mau],
        "wau": [int(r["wau"] or 0) for r in dau_mau],
        "mau": [int(r["mau"] or 0) for r in dau_mau],
    }

    # Подписки
    churn_chart = {
        "labels": [_format_month(r["month"]) for r in churn_by_month],
        "voluntary": [int(r["voluntary"] or 0) for r in churn_by_month],
        "involuntary": [int(r["involuntary"] or 0) for r in churn_by_month],
        "admin_revoked": [int(r["admin_revoked"] or 0) for r in churn_by_month],
    }
    mrr_movement_chart = {
        "labels": [_format_month(r["month"]) for r in mrr_movement],
        "new": [float(r["new_mrr"] or 0) for r in mrr_movement],
        "expansion": [float(r["expansion_mrr"] or 0) for r in mrr_movement],
        "contraction": [float(r["contraction_mrr"] or 0) for r in mrr_movement],
        "churn": [float(r["churn_mrr"] or 0) for r in mrr_movement],
        "net": [float(r["net_mrr_change"] or 0) for r in mrr_movement],
        "quick_ratio": [float(r["quick_ratio"]) if r["quick_ratio"] is not None else None for r in mrr_movement],
    }
    days_to_churn_chart = {
        "labels": [r["bucket_label"] for r in days_to_churn],
        "counts": [int(r["count"] or 0) for r in days_to_churn],
    }
    refund_chart = {
        "labels": [_format_month(r["month"]) for r in refund_rate],
        "rate": [float(r["refund_rate_pct"]) if r["refund_rate_pct"] is not None else 0 for r in refund_rate],
    }
    failed_chart = {
        "labels": [_format_month(r["month"]) for r in failed_payment_rate],
        "rate": [float(r["failed_rate_pct"]) if r["failed_rate_pct"] is not None else 0 for r in failed_payment_rate],
    }
    plan_switching_matrix = _build_plan_switching_matrix(plan_switching_raw)

    # Юнит-экономика
    margin_dist_chart = {
        "labels": [r["bucket_label"] for r in margin_dist],
        "counts": [int(r["count"] or 0) for r in margin_dist],
    }

    ltv_by_cohort_grouped: dict = {}
    for r in ltv_cohorts:
        cm = _format_month(r["cohort_month"])
        if cm not in ltv_by_cohort_grouped:
            ltv_by_cohort_grouped[cm] = {}
        ltv_by_cohort_grouped[cm][int(r["life_month"])] = float(r["ltv_per_user_rub"] or 0)
    ltv_chart_labels = list(range(13))
    ltv_chart_datasets = []
    for cm in sorted(ltv_by_cohort_grouped.keys(), reverse=True)[:6]:
        data = [ltv_by_cohort_grouped[cm].get(lm) for lm in ltv_chart_labels]
        ltv_chart_datasets.append({"label": cm, "data": data})
    ltv_chart = {
        "labels": [f"М{lm}" for lm in ltv_chart_labels],
        "datasets": ltv_chart_datasets,
    }

    # Когорты
    retention_triangle = _build_retention_triangle(retention_triangle_raw)

    retention_curve_datasets = []
    for cohort_idx, cohort in enumerate(retention_triangle["cohorts"][:6]):
        retention_curve_datasets.append({
            "label": cohort["month"],
            "data": retention_triangle["cells"][cohort_idx],
        })
    retention_curve_chart = {
        "labels": [f"М{lm}" for lm in retention_triangle["life_months"]],
        "datasets": retention_curve_datasets,
    }

    # Продукт
    plants_dist_chart = {
        "labels": [r["bucket_label"] for r in plants_dist],
        "counts": [int(r["count"] or 0) for r in plants_dist],
    }
    qa_weekly_chart = {
        "labels": [_format_date(r["week"]) for r in qa_weekly],
        "users": [int(r["active_qa_users"] or 0) for r in qa_weekly],
        "avg": [float(r["avg_per_user"] or 0) for r in qa_weekly],
        "total": [int(r["total_questions"] or 0) for r in qa_weekly],
    }
    streak_dist_chart = {
        "labels": [r["bucket_label"] for r in streak_dist],
        "counts": [int(r["count"] or 0) for r in streak_dist],
    }

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "now": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            # Обзор
            "kpi": kpi,
            "overall": overall,
            "subs_by_plan": subs_by_plan,
            "heavy_users": heavy_users,
            "signup_funnel": signup_funnel,
            "ai_breakdown": ai_breakdown,
            "revenue_chart": revenue_chart,
            "cost_vs_revenue": cost_vs_revenue,
            "dau_mau_chart": dau_mau_chart,
            # Подписки
            "churn_summary": churn_summary,
            "reactivation": reactivation,
            "mrr_movement": mrr_movement,
            "refund_rate": refund_rate,
            "failed_payment_rate": failed_payment_rate,
            "churn_chart": churn_chart,
            "mrr_movement_chart": mrr_movement_chart,
            "days_to_churn_chart": days_to_churn_chart,
            "refund_chart": refund_chart,
            "failed_chart": failed_chart,
            "plan_switching_matrix": plan_switching_matrix,
            # Юнит-экономика
            "unit_summary": unit_summary,
            "ai_by_feature": ai_by_feature,
            "margin_dist": margin_dist,
            "margin_dist_chart": margin_dist_chart,
            "heavy_users_ext": heavy_users_ext,
            "ltv_chart": ltv_chart,
            "payback_cohorts": payback_cohorts,
            # Когорты
            "retention_triangle": retention_triangle,
            "retention_curve_chart": retention_curve_chart,
            "cohort_dn": cohort_dn,
            "sub_retention": sub_retention,
            "cumulative_revenue": cumulative_revenue,
            # Продукт
            "plants_stats": plants_stats,
            "plants_dist_chart": plants_dist_chart,
            "qa_weekly_chart": qa_weekly_chart,
            "photos_stats": photos_stats,
            "care_summary": care_summary,
            "plants_by_state": plants_by_state,
            # Streak
            "streak_summary": streak_summary,
            "streak_dist_chart": streak_dist_chart,
            "top_streak_users": top_streak_users,
            # Activation
            "activation_summary": activation_summary,
            "activation_funnel": activation_funnel,
            # Хелперы
            "fmt_rub": _format_rub,
            "fmt_int": _format_int,
            "fmt_pct": _format_pct,
            "fmt_date": _format_date,
            "fmt_month": _format_month,
        },
    )
