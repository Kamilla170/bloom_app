"""
Админ-эндпоинты. Пока — ручное управление скидками (выдача/список/отзыв).
Авторизация: require_admin (только ADMIN_USER_IDS из config).
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from api.auth.dependencies import require_admin
from api.schemas import SuccessResponse
from services.discount_service import (
    create_discount,
    list_active_discounts,
    revoke_discount,
    get_rules,
    set_rule,
    eligible_count,
    run_auto_discounts,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["admin"])


class GrantDiscountRequest(BaseModel):
    user_ids: list[int]
    percent: int
    days: int = 7
    label: Optional[str] = None


@router.post("/discounts", response_model=SuccessResponse)
async def grant_discounts(
    req: GrantDiscountRequest,
    admin_id: int = Depends(require_admin),
):
    """Выдать скидку списку юзеров вручную (source='manual')."""
    if not req.user_ids:
        raise HTTPException(status_code=400, detail="Не переданы user_ids")
    if not (1 <= req.percent <= 90):
        raise HTTPException(status_code=400, detail="percent должен быть в диапазоне 1..90")
    if req.days <= 0:
        raise HTTPException(status_code=400, detail="days должен быть > 0")

    granted = 0
    for uid in req.user_ids:
        ok = await create_discount(
            uid,
            percent=req.percent,
            days=req.days,
            source="manual",
            label=req.label,
            created_by=admin_id,
        )
        if ok:
            granted += 1

    return SuccessResponse(
        message=f"Скидка выдана: {granted} из {len(req.user_ids)} "
                f"(остальные уже имеют активную)"
    )


@router.get("/discounts")
async def get_user_discounts(
    user_id: int,
    admin_id: int = Depends(require_admin),
):
    """Активные скидки конкретного юзера."""
    return {"discounts": await list_active_discounts(user_id)}


@router.post("/discounts/{discount_id}/revoke", response_model=SuccessResponse)
async def revoke_user_discount(
    discount_id: int,
    admin_id: int = Depends(require_admin),
):
    """Отозвать активную скидку по id."""
    ok = await revoke_discount(discount_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Активная скидка не найдена")
    return SuccessResponse(message="Скидка отозвана")


# ===================== Автоправила (вкл/выкл + счётчик) =====================


@router.get("/discount-rules")
async def list_discount_rules(admin_id: int = Depends(require_admin)):
    """Автоправила со статусом, глубиной, сроком и числом подходящих сейчас."""
    rules = []
    for r in await get_rules():
        rules.append({**r, "eligible_count": await eligible_count(r["source"])})
    return {"rules": rules}


class SetRuleRequest(BaseModel):
    enabled: Optional[bool] = None
    percent: Optional[int] = None
    duration_days: Optional[int] = None


@router.post("/discount-rules/{source}", response_model=SuccessResponse)
async def update_discount_rule(
    source: str,
    req: SetRuleRequest,
    admin_id: int = Depends(require_admin),
):
    """Вкл/выкл и тюнинг правила. При включении сразу прогоняет выдачу по нему."""
    if req.percent is not None and not (1 <= req.percent <= 90):
        raise HTTPException(status_code=400, detail="percent должен быть 1..90")
    if req.duration_days is not None and req.duration_days <= 0:
        raise HTTPException(status_code=400, detail="duration_days должен быть > 0")

    ok = await set_rule(
        source,
        enabled=req.enabled,
        percent=req.percent,
        duration_days=req.duration_days,
        updated_by=admin_id,
    )
    if not ok:
        raise HTTPException(status_code=404, detail="Правило не найдено")

    msg = f"Правило '{source}' обновлено"
    if req.enabled:
        # Немедленный прогон по текущему множеству («раз и включил на них»).
        res = await run_auto_discounts(only_source=source)
        msg += f", выдано сразу: {res.get(source, 0)}"
    return SuccessResponse(message=msg)
