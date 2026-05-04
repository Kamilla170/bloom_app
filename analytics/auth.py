"""
HTTP Basic Auth.
Логин и пароль из переменных окружения BASIC_AUTH_USER / BASIC_AUTH_PASSWORD.
"""

import os
import secrets
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

security = HTTPBasic()

BASIC_AUTH_USER = os.getenv("BASIC_AUTH_USER", "bloom")
BASIC_AUTH_PASSWORD = os.getenv("BASIC_AUTH_PASSWORD", "qweasd")


def require_auth(credentials: HTTPBasicCredentials = Depends(security)) -> str:
    """
    Проверяем логин/пароль через secrets.compare_digest для защиты
    от timing-атак (хоть и оверкилл для нашего случая, лучше так).
    """
    user_ok = secrets.compare_digest(
        credentials.username.encode("utf-8"),
        BASIC_AUTH_USER.encode("utf-8"),
    )
    pwd_ok = secrets.compare_digest(
        credentials.password.encode("utf-8"),
        BASIC_AUTH_PASSWORD.encode("utf-8"),
    )
    if not (user_ok and pwd_ok):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неверный логин или пароль",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username
