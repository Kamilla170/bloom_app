"""
Отправка писем через Yandex Cloud Postbox (SES v2 API).
Используется для входа по коду из email.
"""

from __future__ import annotations

import os
import logging

import boto3
from botocore.config import Config
from fastapi.concurrency import run_in_threadpool

logger = logging.getLogger(__name__)

POSTBOX_ENDPOINT = "https://postbox.cloud.yandex.net"
POSTBOX_REGION = "ru-central1"

MAIL_FROM = os.getenv("MAIL_FROM", "Bloom AI <noreply@bloomai.ru>")

_client = None


def _get_client():
    """Лениво создаёт и кеширует boto3-клиент Postbox."""
    global _client
    if _client is None:
        key_id = os.getenv("POSTBOX_KEY_ID")
        secret = os.getenv("POSTBOX_SECRET_KEY")
        if not key_id or not secret:
            raise RuntimeError("POSTBOX_KEY_ID / POSTBOX_SECRET_KEY не заданы")
        _client = boto3.client(
            "sesv2",
            endpoint_url=POSTBOX_ENDPOINT,
            region_name=POSTBOX_REGION,
            aws_access_key_id=key_id,
            aws_secret_access_key=secret,
            config=Config(retries={"max_attempts": 3, "mode": "standard"}),
        )
    return _client


def _build_login_code_email(code: str) -> tuple[str, str, str]:
    """Возвращает (subject, text, html) письма с кодом входа."""
    subject = "Код для входа в Bloom AI"
    text = (
        "Здравствуйте!\n\n"
        f"Ваш код для входа в Bloom AI: {code}\n\n"
        "Код действует 10 минут.\n"
        "Никому не сообщайте этот код.\n"
        "Если вы не запрашивали вход, просто проигнорируйте это письмо."
    )
    html = f"""\
<div style="font-family: Arial, Helvetica, sans-serif; font-size: 16px; color: #2b3629; max-width: 480px; margin: 0 auto;">
  <p>Здравствуйте!</p>
  <p>Ваш код для входа в Bloom AI:</p>
  <p style="margin: 28px 0;">
    <span style="display: inline-block; background: #f6f7f6; color: #2b3629;
                 border: 1px solid #e2e0d8; border-radius: 16px;
                 padding: 14px 28px; font-size: 32px; font-weight: bold;
                 letter-spacing: 8px; font-family: 'Courier New', monospace;">{code}</span>
  </p>
  <p style="color: #737a6f; font-size: 14px;">
    Код действует 10 минут. Никому не сообщайте этот код.<br>
    Если вы не запрашивали вход, просто проигнорируйте это письмо.
  </p>
</div>"""
    return subject, text, html


async def send_login_code_email(to_email: str, code: str) -> None:
    """
    Отправляет письмо с кодом входа.
    Бросает исключение при ошибке отправки, вызывающий код решает, что показать.
    """
    subject, text, html = _build_login_code_email(code)
    client = _get_client()

    def _send():
        return client.send_email(
            FromEmailAddress=MAIL_FROM,
            Destination={"ToAddresses": [to_email]},
            Content={
                "Simple": {
                    "Subject": {"Data": subject, "Charset": "UTF-8"},
                    "Body": {
                        "Text": {"Data": text, "Charset": "UTF-8"},
                        "Html": {"Data": html, "Charset": "UTF-8"},
                    },
                }
            },
        )

    resp = await run_in_threadpool(_send)
    logger.info(
        f"📧 Письмо с кодом входа отправлено на {to_email}, MessageId={resp.get('MessageId')}"
    )
