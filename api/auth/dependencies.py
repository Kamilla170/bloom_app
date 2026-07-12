"""
FastAPI зависимость для авторизации
"""

import os
import secrets
from typing import Optional

from fastapi import Depends, HTTPException, status, Request, Header

from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from api.auth.jwt import decode_token
from config import ADMIN_USER_IDS

security = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> int:
    """
    Извлекает user_id из JWT токена.
    Используется как зависимость в эндпоинтах.
    
    Returns:
        int: user_id текущего пользователя
    """
    token = credentials.credentials
    payload = decode_token(token)

    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Невалидный или просроченный токен",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token_type = payload.get("type")
    if token_type != "access":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Нужен access token",
        )

    user_id_str = payload.get("sub")
    if not user_id_str:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Невалидный токен",
        )

    try:
        return int(user_id_str)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Невалидный user_id в токене",
        )


async def require_admin(
    request: Request,
    x_admin_key: Optional[str] = Header(default=None),
) -> int:
    """
    Пропускает админа двумя путями:

    1) Сервисный ключ `X-Admin-Key` == env `ADMIN_API_KEY` — для дашборда
       (server-to-server). Работает ТОЛЬКО если ADMIN_API_KEY задан в окружении;
       если не задан — этот путь полностью отключён (fail-closed). Возвращает 0
       как id «системного» админа (пишется в created_by).
    2) Access-JWT пользователя из `ADMIN_USER_IDS` — для ручных вызовов.

    Не задан ключ и не админ-JWT → 403.
    """
    admin_key = os.getenv("ADMIN_API_KEY", "")
    if admin_key and x_admin_key and secrets.compare_digest(x_admin_key, admin_key):
        return 0

    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        payload = decode_token(auth[len("Bearer "):])
        if payload and payload.get("type") == "access":
            try:
                uid = int(payload.get("sub"))
            except (TypeError, ValueError):
                uid = None
            if uid is not None and uid in ADMIN_USER_IDS:
                return uid

    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Требуются права администратора",
    )
