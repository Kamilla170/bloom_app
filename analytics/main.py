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

# Монтируем static только если папка существует.
# Сейчас static не используется (Tailwind/Chart.js через CDN, стили инлайн),
# но оставлено на будущее.
import os as _os
if _os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="templates")


def _format_rub(value) -> str:
    """1234567.89 -> '1 234 568 ₽'"""
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
    """date('2026-05-01') -> '2026-05'"""
    if value is None:
        return ""
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m")
    return str(value)


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, _: str = Depends(require_auth)):
    try:
        kpi = await db.get_kpi_summary()
        revenue_monthly = await db.get_revenue_monthly()
        ai_costs_monthly = await db.get_ai_costs_monthly()
        subs_by_plan = await db.get_subscriptions_by_plan()
        heavy_users = await db.get_heavy_ai_users(limit=10)
        dau_mau = await db.get_dau_mau()
        signup_funnel = await db.get_signup_funnel()
        ai_breakdown = await db.get_ai_cost_breakdown()
        overall = await db.get_overall_economics()
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки данных: {e}", exc_info=True)
        return HTMLResponse(
            f"<h1>Ошибка загрузки данных</h1><pre>{e}</pre>",
            status_code=500,
        )

    # Подготовка данных для Chart.js
    revenue_chart = {
        "labels": [_format_month(r["month"]) for r in revenue_monthly],
        "revenue": [float(r["revenue_rub"] or 0) for r in revenue_monthly],
        "recurring": [float(r["recurring_revenue_rub"] or 0) for r in revenue_monthly],
        "new": [float(r["new_revenue_rub"] or 0) for r in revenue_monthly],
    }

    # AI cost vs Revenue
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

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "now": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            "kpi": kpi,
            "overall": overall,
            "subs_by_plan": subs_by_plan,
            "heavy_users": heavy_users,
            "signup_funnel": signup_funnel,
            "ai_breakdown": ai_breakdown,
            "revenue_chart": revenue_chart,
            "cost_vs_revenue": cost_vs_revenue,
            "dau_mau_chart": dau_mau_chart,
            "fmt_rub": _format_rub,
            "fmt_int": _format_int,
            "fmt_pct": _format_pct,
            "fmt_date": _format_date,
            "fmt_month": _format_month,
        },
    )
