"""
Эндпоинты авторизации: вход через провайдеров, обновление токена
"""

import os
import logging
import httpx
from fastapi import APIRouter, HTTPException, status

from google.oauth2 import id_token as google_id_token
from google.auth.transport import requests as google_requests

from database import get_db
from api.schemas import (
    GoogleAuthRequest, YandexAuthRequest, RefreshRequest, TokenResponse
)
from api.auth.jwt import create_tokens, decode_token

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["auth"])

# Стартовый user_id для app-пользователей (чтобы не пересекаться с Telegram ID)
APP_USER_ID_START = 5_000_000_000

# OAuth client IDs из переменных окружения
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
YANDEX_CLIENT_ID = os.getenv("YANDEX_CLIENT_ID")


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


@router.post("/yandex", response_model=TokenResponse)
async def auth_yandex(req: YandexAuthRequest):
    """
    Вход или регистрация через Yandex ID.
    Принимает access_token, полученный Flutter-приложением в результате OAuth flow
    через WebView (Custom Tabs / ASWebAuthenticationSession).
    """
    # Валидируем токен через login.yandex.ru/info
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                "https://login.yandex.ru/info",
                params={"format": "json"},
                headers={"Authorization": f"OAuth {req.access_token}"},
            )
    except httpx.HTTPError as e:
        logger.warning(f"Ошибка запроса к Yandex info: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Не удалось проверить токен Yandex",
        )

    if response.status_code != 200:
        logger.warning(
            f"Невалидный Yandex token: status={response.status_code}, "
            f"body={response.text[:200]}"
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Невалидный Yandex токен",
        )

    info = response.json()

    yandex_id = info.get("id")
    if not yandex_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Не удалось получить идентификатор пользователя от Yandex",
        )

    # Опционально проверяем, что токен выдан именно для нашего приложения.
    # Защита от подсовывания токенов от других приложений Яндекса.
    if YANDEX_CLIENT_ID:
        token_client_id = info.get("client_id")
        if token_client_id and token_client_id != YANDEX_CLIENT_ID:
            logger.warning(
                f"Yandex client_id mismatch: получен {token_client_id}, "
                f"ожидался {YANDEX_CLIENT_ID}"
            )
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Токен выдан для другого приложения",
            )

    # Извлекаем email и имя
    email = info.get("default_email")
    if not email:
        emails = info.get("emails") or []
        email = emails[0] if emails else None

    first_name = (
        info.get("first_name")
        or info.get("display_name")
        or info.get("login")
    )

    # Находим или создаём юзера
    db = await get_db()
    async with db.pool.acquire() as conn:
        user_id = await _find_or_create_user(
            conn,
            provider="yandex",
            provider_user_id=str(yandex_id),
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
