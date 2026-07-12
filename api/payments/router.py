"""
Эндпоинты подписки и платежей
"""

import logging
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse

from api.auth.dependencies import get_current_user
from api.schemas import (
    SubscriptionPlan,
    PlansResponse,
    DiscountInfo,
    CreatePaymentRequest,
    CreatePaymentResponse,
    PaymentStatusResponse,
    SuccessResponse,
)
from config import SUBSCRIPTION_PLANS
from database import get_db
from services.payment_service import (
    create_payment,
    handle_payment_webhook,
    cancel_auto_payment,
    get_payment_status,
)
from services.subscription_service import is_pro
from services.discount_service import get_active_discount, price_for_plan

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/payments", tags=["payments"])


@router.get("/plans", response_model=PlansResponse)
async def list_plans(user_id: int = Depends(get_current_user)):
    """Список тарифов с учётом активной скидки юзера (см. discount_service)."""
    discount = await get_active_discount(user_id)

    plans: list[SubscriptionPlan] = []
    for plan_id, regular in SUBSCRIPTION_PLANS.items():
        plans.append(SubscriptionPlan(
            id=plan_id,
            label=regular["label"],
            days=regular["days"],
            price=price_for_plan(plan_id, regular, discount),
            original_price=regular["price"],
            per_month=regular.get("per_month"),
            is_popular=(plan_id == "3months"),
        ))

    discount_info: Optional[DiscountInfo] = None
    if discount:
        discount_info = DiscountInfo(
            percent=discount["percent"],
            ends_at=discount["ends_at"],
            label=discount["label"],
        )

    return PlansResponse(plans=plans, discount=discount_info)


@router.post("/create", response_model=CreatePaymentResponse)
async def create_new_payment(
    req: CreatePaymentRequest,
    user_id: int = Depends(get_current_user),
):
    """
    Создать платёж из payment_token, полученного на клиенте через YooKassa SDK.
    """
    if await is_pro(user_id):
        raise HTTPException(status_code=400, detail="У вас уже есть подписка")

    if not req.payment_token:
        raise HTTPException(status_code=400, detail="Не передан payment_token")

    regular = SUBSCRIPTION_PLANS.get(req.plan_id)
    if not regular:
        raise HTTPException(status_code=404, detail="Тариф не найден")

    # Цену со скидкой считаем на СЕРВЕРЕ из активной скидки юзера, не доверяя клиенту.
    discount = await get_active_discount(user_id)
    amount = price_for_plan(req.plan_id, regular, discount)

    save_method = (req.plan_id == "1month")

    result = await create_payment(
        user_id=user_id,
        payment_token=req.payment_token,
        amount=amount,
        days=regular["days"],
        plan_label=regular["label"],
        plan_id=req.plan_id,
        save_method=save_method,
    )

    if not result:
        return CreatePaymentResponse(
            success=False,
            error="Платёжная система недоступна",
        )

    return CreatePaymentResponse(
        success=True,
        payment_id=result["payment_id"],
        status=result["status"],
        confirmation_url=result.get("confirmation_url"),
    )


@router.get("/status/{payment_id}", response_model=PaymentStatusResponse)
async def payment_status(
    payment_id: str,
    user_id: int = Depends(get_current_user),
):
    """
    Получить статус платежа. Используется фронтом для поллинга после tokenization.
    """
    info = await get_payment_status(payment_id)
    if not info:
        raise HTTPException(status_code=404, detail="Платёж не найден")

    return PaymentStatusResponse(
        payment_id=info["payment_id"],
        status=info["status"],
        amount=info["amount"],
        plan_id=info.get("plan_id"),
    )


@router.post("/webhook")
async def payment_webhook(request: Request):
    """Webhook от YooKassa (без авторизации)"""
    try:
        payload = await request.json()
        success = await handle_payment_webhook(payload)
        if success:
            return {"status": "ok"}
        return JSONResponse({"status": "error"}, status_code=400)
    except Exception as e:
        logger.error(f"❌ Payment webhook error: {e}", exc_info=True)
        return JSONResponse({"status": "error"}, status_code=500)


@router.post("/cancel-auto", response_model=SuccessResponse)
async def cancel_auto(user_id: int = Depends(get_current_user)):
    """Отключить автопродление"""
    await cancel_auto_payment(user_id)
    return SuccessResponse(message="Автопродление отключено")
