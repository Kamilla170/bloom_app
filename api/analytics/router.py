"""
Этап 9: Эндпоинты аналитики и достижений
"""

import logging

from fastapi import APIRouter, Depends, HTTPException

from api.auth.dependencies import get_current_user
from achievements import get_analytics_data, get_next_achievement

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/analytics", tags=["analytics"])


@router.get("")
async def analytics(user_id: int = Depends(get_current_user)):
    """Экран аналитики: статистика + стрик + достижения."""
    data = await get_analytics_data(user_id)
    if not data:
        raise HTTPException(status_code=404, detail="User not found")
    return data


@router.get("/next-achievement")
async def next_achievement(user_id: int = Depends(get_current_user)):
    """Ближайшее достижение для Home Screen empty state."""
    result = await get_next_achievement(user_id)
    return {"next_achievement": result}
