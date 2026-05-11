"""
Эндпоинты авторизации: вход через провайдеров, обновление токена
"""

import os
import logging
from fastapi import APIRouter, HTTPException, status

from google.oauth2 import id_token as google_id_token
from google.auth.transport import requests as google_requests

from database import get_db
from api.schemas import (
    GoogleAuthRequest, RefreshRequest, TokenResponse
)
from api.auth.jwt import create_tokens, decode_token

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["auth"])

# Стартовый user_id для app-пользователей (чтобы не пересекаться с Telegram ID)
APP_USER_ID_START = 5_000_000_000

# OAuth client IDs из переменных окружения
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")


async def _get_next_app_user_id(conn) -> int:
    """Получить следующий свободный user_id для app-пользователя"""
    max_id = await conn.fetchval("""
        SELECT COALESCE(MAX(user_id), $1) FROM users WHERE user_id >= $1
    """, APP_USER_ID_START)
    return max_id + 1


async def _find_or_create_user(
    conn,
    provider: str,
    provider_user_id: str,
    email: str = None,
    first_name: str = None,
) -> int:
    """
    Найти юзера по (provider, provider_user_id) или создать нового.
    Возвращает user_id.
    """
    # Ищем существующего юзера
    existing = await conn.fetchval("""
        SELECT user_id FROM users
        WHERE auth_provider = $1 AND provider_user_id = $2
    """, provider, provider_user_id)

    if existing:
        # Обновляем активность
        await conn.execute("""
            UPDATE users
            SET last_activity = CURRENT_TIMESTAMP, last_action = $2
            WHERE user_id = $1
        """, existing, f"{provider}_login")
        return existing

    # Создаём нового
    user_id = await _get_next_app_user_id(conn)

    await conn.execute("""
        INSERT INTO users (
            user_id, email, first_name,
            auth_provider, provider_user_id,
            last_activity, last_action
        )
        VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, $6)
    """, user_id, email, first_name, provider, provider_user_id, f"{provider}_register")

    await conn.execute("""
        INSERT INTO user_settings (user_id) VALUES ($1)
        ON CONFLICT (user_id) DO NOTHING
    """, user_id)

    await conn.execute("""
        INSERT INTO subscriptions (user_id, plan) VALUES ($1, 'free')
        ON CONFLICT (user_id) DO NOTHING
    """, user_id)

    logger.info(f"✅ Новый {provider}-пользователь: user_id={user_id}, email={email}")
    return user_id


@router.post("/google", response_model=TokenResponse)
async def auth_google(req: GoogleAuthRequest):
    """Вход или регистрация через Google. Принимает id_token от Google Sign-In."""
    if not GOOGLE_CLIENT_ID:
        logger.error("GOOGLE_CLIENT_ID не задан в переменных окружения")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Google авторизация не настроена",
        )

    # Валидируем id_token у Google
    try:
        idinfo = google_id_token.verify_oauth2_token(
            req.id_token,
            google_requests.Request(),
            GOOGLE_CLIENT_ID,
        )
    except ValueError as e:
        logger.warning(f"Невалидный Google id_token: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Невалидный Google токен",
        )

    # Извлекаем данные
    google_sub = idinfo.get("sub")
    email = idinfo.get("email")
    first_name = idinfo.get("given_name") or idinfo.get("name")

    if not google_sub:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Не удалось получить идентификатор пользователя от Google",
        )

    # Находим или создаём юзера
    db = await get_db()
    async with db.pool.acquire() as conn:
        user_id = await _find_or_create_user(
            conn,
            provider="google",
            provider_user_id=google_sub,
            email=email,
            first_name=first_name,
        )

    return TokenResponse(**create_tokens(user_id))


@router.post("/refresh", response_model=TokenResponse)
async def refresh(req: RefreshRequest):
    """Обновление пары токенов по refresh_token"""
    payload = decode_token(req.refresh_token)

    if payload is None or payload.get("type") != "refresh":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Невалидный refresh token",
        )

    user_id_str = payload.get("sub")
    if not user_id_str:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Невалидный токен",
        )

    user_id = int(user_id_str)

    db = await get_db()
    async with db.pool.acquire() as conn:
        exists = await conn.fetchval(
            "SELECT 1 FROM users WHERE user_id = $1", user_id
        )

    if not exists:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Пользователь не найден",
        )

    return TokenResponse(**create_tokens(user_id))
