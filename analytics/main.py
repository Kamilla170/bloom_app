"""
Bloom AI Analytics Dashboard.
Одна страница со всеми ключевыми метриками.
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
        return "—"
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
    """
    Превращает плоский список переходов в матрицу 4x4.
    Возвращает: {'rows': [...], 'cols': [...], 'matrix': [[count or 0]]}
    """
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
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки данных: {e}", exc_info=True)
        return HTMLResponse(
            f"<h1>Ошибка загрузки данных</h1><pre>{e}</pre>",
            status_code=500,
        )

    # Графики обзора
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

    # Графики таба Подписки
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
        "payments": [int(r["payments"] or 0) for r in refund_rate],
        "refunds": [int(r["refunds"] or 0) for r in refund_rate],
    }

    failed_chart = {
        "labels": [_format_month(r["month"]) for r in failed_payment_rate],
        "rate": [float(r["failed_rate_pct"]) if r["failed_rate_pct"] is not None else 0 for r in failed_payment_rate],
        "success": [int(r["success"] or 0) for r in failed_payment_rate],
        "failed": [int(r["failed"] or 0) for r in failed_payment_rate],
    }

    plan_switching_matrix = _build_plan_switching_matrix(plan_switching_raw)

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
            # Хелперы
            "fmt_rub": _format_rub,
            "fmt_int": _format_int,
            "fmt_pct": _format_pct,
            "fmt_date": _format_date,
            "fmt_month": _format_month,
        },
    )
