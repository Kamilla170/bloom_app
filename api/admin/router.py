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
