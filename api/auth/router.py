"""
Эндпоинты авторизации: вход через провайдеров, magic-link по email, обновление токена
"""

import os
import re
import hashlib
import secrets
import logging
import httpx
from fastapi import APIRouter, HTTPException, status
from fastapi.responses import RedirectResponse

from database import get_db
from api.schemas import (
    YandexAuthRequest, RefreshRequest, TokenResponse,
    EmailLoginRequest, EmailLoginResponse, EmailExchangeRequest,
)
from api.auth.jwt import create_tokens, decode_token
from api.auth.email_service import send_login_email

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["auth"])

# Стартовый user_id для app-пользователей (чтобы не пересекаться с Telegram ID)
APP_USER_ID_START = 5_000_000_000

# Версия юр-документов, которую принимает пользователь при регистрации.
# Держать в синхроне с DOCS_VERSION в api/legal.py.
TERMS_VERSION = "2026-06-19"

# OAuth client IDs из переменных окружения
YANDEX_CLIENT_ID = os.getenv("YANDEX_CLIENT_ID")
YANDEX_CLIENT_SECRET = os.getenv("YANDEX_CLIENT_SECRET")

# === Magic-link по email ===
# Публичный адрес API (попадает в ссылку письма) и схема возврата в приложение
PUBLIC_API_BASE = os.getenv("PUBLIC_API_BASE", "https://api.bloomai.ru").rstrip("/")
APP_EMAIL_CALLBACK = "bloomai://oauth/email/callback"

EMAIL_TOKEN_TTL_MINUTES = 15          # срок жизни ссылки из письма
EMAIL_CODE_TTL_MINUTES = 5            # срок жизни кода для обмена на JWT
EMAIL_RATELIMIT_PER_HOUR = 5          # макс. запросов на один адрес в час
EMAIL_RATELIMIT_COOLDOWN_SECONDS = 60  # не чаще одного запроса в минуту

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# Таблица создаётся лениво при первом обращении (одна попытка на процесс).
# Если захочешь, перенесём это в стартовые миграции, покажи main.py / database.py.
_email_table_ready = False


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
            last_activity, last_action,
            terms_accepted_at, terms_version
        )
        VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, $6, CURRENT_TIMESTAMP, $7)
    """, user_id, email, first_name, provider, provider_user_id,
         f"{provider}_register", TERMS_VERSION)

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


# === Хелперы magic-link ===

def _hash_token(value: str) -> str:
    """SHA-256 хеш секрета (в БД храним только хеши, не сами токены)."""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


async def _ensure_email_tokens_table(conn) -> None:
    """Создаёт таблицу одноразовых токенов входа, если её ещё нет."""
    global _email_table_ready
    if _email_table_ready:
        return
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS email_login_tokens (
            id BIGSERIAL PRIMARY KEY,
            email TEXT NOT NULL,
            token_hash TEXT NOT NULL UNIQUE,
            code_hash TEXT UNIQUE,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            token_expires_at TIMESTAMP NOT NULL,
            code_expires_at TIMESTAMP,
            verified_at TIMESTAMP,
            exchanged_at TIMESTAMP
        )
    """)
    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_email_login_token_hash
        ON email_login_tokens (token_hash)
    """)
    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_email_login_code_hash
        ON email_login_tokens (code_hash)
    """)
    _email_table_ready = True


async def _exchange_yandex_code_for_token(code: str) -> str:
    """
    Обменивает authorization code на access_token через oauth.yandex.ru/token.
    Возвращает access_token. Бросает HTTPException при ошибках.
    """
    if not YANDEX_CLIENT_ID or not YANDEX_CLIENT_SECRET:
        logger.error("YANDEX_CLIENT_ID или YANDEX_CLIENT_SECRET не заданы")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Yandex авторизация не настроена",
        )

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                "https://oauth.yandex.ru/token",
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                },
                auth=(YANDEX_CLIENT_ID, YANDEX_CLIENT_SECRET),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
    except httpx.HTTPError as e:
        logger.warning(f"Ошибка запроса к Yandex token endpoint: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Не удалось обменять код Yandex",
        )

    if response.status_code != 200:
        logger.warning(
            f"Yandex token обмен не удался: status={response.status_code}, "
            f"body={response.text[:300]}"
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Невалидный или просроченный код Yandex",
        )

    payload = response.json()
    access_token = payload.get("access_token")
    if not access_token:
        logger.warning(f"Yandex token endpoint: нет access_token в ответе: {payload}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Yandex не вернул access_token",
        )

    return access_token


async def _fetch_yandex_user_info(access_token: str) -> dict:
    """Запрашивает данные пользователя через login.yandex.ru/info."""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                "https://login.yandex.ru/info",
                params={"format": "json"},
                headers={"Authorization": f"OAuth {access_token}"},
            )
    except httpx.HTTPError as e:
        logger.warning(f"Ошибка запроса к Yandex info: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Не удалось проверить токен Yandex",
        )

    if response.status_code != 200:
        logger.warning(
            f"Yandex info вернул ошибку: status={response.status_code}, "
            f"body={response.text[:200]}"
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Невалидный Yandex токен",
        )

    return response.json()


@router.post("/yandex", response_model=TokenResponse)
async def auth_yandex(req: YandexAuthRequest):
    """
    Вход или регистрация через Yandex ID.
    Принимает authorization code, полученный мобильным клиентом из OAuth
    callback (?code=...). Обменивает его на access_token через
    oauth.yandex.ru/token и валидирует через login.yandex.ru/info.
    """
    # Шаг 1: обмен code на access_token
    access_token = await _exchange_yandex_code_for_token(req.code)

    # Шаг 2: валидируем токен и получаем данные пользователя
    info = await _fetch_yandex_user_info(access_token)

    yandex_id = info.get("id")
    if not yandex_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Не удалось получить идентификатор пользователя от Yandex",
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


@router.post("/email/request", response_model=EmailLoginResponse)
async def auth_email_request(req: EmailLoginRequest):
    """
    Шаг 1 magic-link: принять email, отправить письмо со ссылкой входа.
    Ответ одинаковый при любом исходе валидного запроса (не раскрываем,
    зарегистрирован адрес или нет).
    """
    email = req.email.strip().lower()
    if len(email) > 254 or not email.isascii() or not _EMAIL_RE.match(email):
        raise HTTPException(status_code=400, detail="Некорректный email")

    db = await get_db()
    async with db.pool.acquire() as conn:
        await _ensure_email_tokens_table(conn)

        # Антиспам по адресу
        hour_count = await conn.fetchval(f"""
            SELECT COUNT(*) FROM email_login_tokens
            WHERE email = $1
              AND created_at > CURRENT_TIMESTAMP - INTERVAL '1 hour'
        """, email)
        if hour_count and hour_count >= EMAIL_RATELIMIT_PER_HOUR:
            raise HTTPException(
                status_code=429,
                detail="Слишком много запросов, попробуйте позже",
            )

        cooldown_count = await conn.fetchval(f"""
            SELECT COUNT(*) FROM email_login_tokens
            WHERE email = $1
              AND created_at > CURRENT_TIMESTAMP
                  - INTERVAL '{EMAIL_RATELIMIT_COOLDOWN_SECONDS} seconds'
        """, email)
        if cooldown_count and cooldown_count >= 1:
            raise HTTPException(
                status_code=429,
                detail="Письмо уже отправлено, подождите минуту",
            )

        token = secrets.token_urlsafe(32)
        token_hash = _hash_token(token)

        await conn.execute(f"""
            INSERT INTO email_login_tokens (email, token_hash, token_expires_at)
            VALUES ($1, $2,
                    CURRENT_TIMESTAMP + INTERVAL '{EMAIL_TOKEN_TTL_MINUTES} minutes')
        """, email, token_hash)

    link = f"{PUBLIC_API_BASE}/auth/email/verify?token={token}"

    try:
        await send_login_email(email, link)
    except Exception as e:  # noqa: BLE001
        logger.error(f"Не удалось отправить письмо входа на {email}: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Не удалось отправить письмо, попробуйте позже",
        )

    return EmailLoginResponse(success=True, message="Письмо со ссылкой отправлено")


@router.get("/email/verify")
async def auth_email_verify(token: str):
    """
    Шаг 2 magic-link: юзер открыл ссылку из письма.
    Гасим токен, выдаём короткий код и 302-редиректом возвращаем в приложение.
    """
    token_hash = _hash_token(token)

    code = secrets.token_urlsafe(32)
    code_hash = _hash_token(code)

    db = await get_db()
    async with db.pool.acquire() as conn:
        await _ensure_email_tokens_table(conn)
        # Атомарно: гасим токен и записываем код только если токен валиден
        row = await conn.fetchrow(f"""
            UPDATE email_login_tokens
            SET verified_at = CURRENT_TIMESTAMP,
                code_hash = $2,
                code_expires_at = CURRENT_TIMESTAMP
                    + INTERVAL '{EMAIL_CODE_TTL_MINUTES} minutes'
            WHERE token_hash = $1
              AND verified_at IS NULL
              AND token_expires_at > CURRENT_TIMESTAMP
            RETURNING id
        """, token_hash, code_hash)

    if row is None:
        return RedirectResponse(
            url=f"{APP_EMAIL_CALLBACK}?error=invalid_or_expired",
            status_code=302,
        )

    return RedirectResponse(
        url=f"{APP_EMAIL_CALLBACK}?code={code}",
        status_code=302,
    )


@router.post("/email/exchange", response_model=TokenResponse)
async def auth_email_exchange(req: EmailExchangeRequest):
    """
    Шаг 3 magic-link: клиент обменивает код из deep link на JWT.
    """
    code_hash = _hash_token(req.code)

    db = await get_db()
    async with db.pool.acquire() as conn:
        await _ensure_email_tokens_table(conn)
        # Атомарно гасим код и забираем email
        row = await conn.fetchrow("""
            UPDATE email_login_tokens
            SET exchanged_at = CURRENT_TIMESTAMP
            WHERE code_hash = $1
              AND exchanged_at IS NULL
              AND code_expires_at > CURRENT_TIMESTAMP
            RETURNING email
        """, code_hash)

        if row is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Невалидный или просроченный код",
            )

        email = row["email"]
        user_id = await _find_or_create_user(
            conn,
            provider="email",
            provider_user_id=email,
            email=email,
            first_name=None,
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
