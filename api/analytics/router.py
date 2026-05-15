"""
Этап 9: Эндпоинты аналитики и достижений
"""

import logging

from fastapi import APIRouter, Body, Depends, HTTPException
from typing import Optional

from database import get_db
from api.auth.dependencies import get_current_user
from api.schemas import (
    AchievementOut,
    UnseenAchievementsResponse,
    MarkSeenRequest,
    SuccessResponse,
)
from achievements import (
    get_analytics_data,
    get_next_achievement,
    ACHIEVEMENTS_MAP,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/analytics", tags=["analytics"])

# Отдельный роутер для глобальных путей /achievements/*.
# Подключается в main.py через app.include_router(achievements_router).
achievements_router = APIRouter(prefix="/achievements", tags=["achievements"])


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


@achievements_router.get("/unseen", response_model=UnseenAchievementsResponse)
async def get_unseen_achievements(user_id: int = Depends(get_current_user)):
    """
    Список разблокированных, но ещё не показанных пользователю достижений.
    Используется фронтом на старте приложения, чтобы дотащить хвост из
    предыдущей сессии (например если приложение закрыли до показа тоста).
    Сортировка по unlocked_at ASC, чтобы тосты показывались в естественном порядке.
    """
    db = await get_db()
    async with db.pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT achievement_code, unlocked_at
            FROM user_achievements
            WHERE user_id = $1 AND seen_at IS NULL
            ORDER BY unlocked_at ASC
        """, user_id)

    achievements = []
    for row in rows:
        ach = ACHIEVEMENTS_MAP.get(row['achievement_code'])
        if ach is None:
            # Защита от ситуации, когда в БД остался код от удалённой ачивки.
            logger.warning(f"⚠️ unseen ачивка {row['achievement_code']} не в ACHIEVEMENTS_MAP")
            continue
        achievements.append(AchievementOut(
            code=ach['code'],
            title=ach['title'],
            category=ach['category'],
            icon=ach['icon'],
            description=ach['description_unlocked'],
            unlocked_at=row['unlocked_at'],
        ))

    return UnseenAchievementsResponse(
        count=len(achievements),
        achievements=achievements,
    )


@achievements_router.post("/mark-seen", response_model=SuccessResponse)
async def mark_achievements_seen(
    req: Optional[MarkSeenRequest] = Body(None),
    user_id: int = Depends(get_current_user),
):
    """
    Пометить достижения как показанные пользователю.

    Без тела (или с пустым codes) маркает ВСЕ unseen текущего юзера.
    С codes маркает только указанные.

    Идемпотентно: UPDATE ... WHERE seen_at IS NULL не трогает уже виденные.
    Кросс-юзер безопасен: фильтр по user_id отрезает чужие записи.
    """
    codes = req.codes if (req and req.codes) else None

    db = await get_db()
    async with db.pool.acquire() as conn:
        if codes:
            await conn.execute("""
                UPDATE user_achievements
                SET seen_at = NOW()
                WHERE user_id = $1
                  AND achievement_code = ANY($2)
                  AND seen_at IS NULL
            """, user_id, codes)
        else:
            await conn.execute("""
                UPDATE user_achievements
                SET seen_at = NOW()
                WHERE user_id = $1 AND seen_at IS NULL
            """, user_id)

    return SuccessResponse(success=True, message="OK")
