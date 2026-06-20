"""
Rate limiting через slowapi.

Лимитер и обработчик 429 живут в отдельном модуле, чтобы их могли
импортировать и main.py, и роутеры, без циклических импортов.

ВАЖНО: приложение стоит за nginx, поэтому request.client.host это IP nginx,
один на всех. Реальный IP клиента берём из заголовков, которые проставляет
nginx (X-Real-IP / X-Forwarded-For). Без этих заголовков лимит станет общим
на всех пользователей сразу, см. примечание в чате про настройку nginx.
"""

import logging

from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from starlette.requests import Request
from starlette.responses import JSONResponse

logger = logging.getLogger(__name__)


def client_ip(request: Request) -> str:
    """
    Реальный IP клиента за nginx.

    Приоритет:
    1. X-Real-IP   (nginx ставит как $remote_addr, одно значение, спуфить трудно)
    2. X-Forwarded-For (берём первый адрес в цепочке)
    3. адрес соединения (это будет IP nginx; значит заголовки не настроены)
    """
    real_ip = request.headers.get("x-real-ip")
    if real_ip:
        return real_ip.strip()

    fwd = request.headers.get("x-forwarded-for")
    if fwd:
        return fwd.split(",")[0].strip()

    return get_remote_address(request)


# Хранилище по умолчанию in-memory и на процесс. Для текущего масштаба
# (один контейнер api) этого достаточно. Если позже включишь несколько
# воркеров, лимиты станут на воркер, тогда вынесем хранилище в Redis.
limiter = Limiter(key_func=client_ip)


async def rate_limit_handler(request: Request, exc: RateLimitExceeded) -> JSONResponse:
    """Человеческий ответ на 429 вместо сырой ошибки slowapi. Логируем срабатывание."""
    logger.warning(
        f"⛔ Rate limit: ip={client_ip(request)} "
        f"path={request.url.path} limit={getattr(exc, 'detail', '')}"
    )
    return JSONResponse(
        status_code=429,
        content={
            "detail": "Слишком много запросов. Подождите немного и попробуйте снова."
        },
    )
