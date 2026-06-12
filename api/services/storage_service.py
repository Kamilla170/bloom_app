"""
Сервис хранения фото растений в S3-совместимом хранилище (Timeweb Cloud S3).

Замена Cloudinary. Локализация ПД по 152-ФЗ: бакет создаётся в РФ-регионе.

Контракт (важно):
- upload_plant_photo() возвращает БАЗОВЫЙ КЛЮЧ без размера и расширения,
  например "bloom_plants/user_5000000001/Фикус_a1b2c3d4".
  Именно он сохраняется в БД (plants.photo_file_id и plant_photos.photo_url).
- При загрузке генерируются три квадратных размера: 200, 400, 800.
  Файлы лежат в S3 как "<ключ>_200.jpg", "<ключ>_400.jpg", "<ключ>_800.jpg".
- get_photo_url(ключ, width) собирает публичный URL ближайшего размера.

Совместимость:
- Если в поле лежит старый Cloudinary URL (начинается с http), get_photo_url
  возвращает его как есть, без ресайза. Это страховка на случай старых записей.
- Если в поле лежит неизвестный формат (например, старый Telegram file_id),
  get_photo_url возвращает None, как это делал cloudinary_service.
"""

import os
import io
import asyncio
import logging
import hashlib

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from PIL import Image

logger = logging.getLogger(__name__)

# === Конфигурация из переменных окружения ===
S3_ENDPOINT = os.getenv("TIMEWEB_S3_ENDPOINT")          # напр. https://s3.timeweb.cloud
S3_BUCKET = os.getenv("TIMEWEB_S3_BUCKET")              # имя бакета
S3_ACCESS_KEY = os.getenv("TIMEWEB_S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("TIMEWEB_S3_SECRET_KEY")
S3_REGION = os.getenv("TIMEWEB_S3_REGION", "ru-1")

# Публичный базовый URL для отдачи файлов (бакет должен быть открыт на чтение).
# Напр. "https://s3.timeweb.cloud/<bucket>" или "https://<bucket>.s3.timeweb.cloud".
# Точный формат зависит от настроек бакета, поэтому задаём через env.
S3_PUBLIC_BASE_URL = os.getenv("S3_PUBLIC_BASE_URL")

# Префикс всех ключей. Используется и для различения наших ключей от мусора.
KEY_PREFIX = "bloom_plants"

# Квадратные размеры, которые генерируем при загрузке.
# Должны покрывать все width, с которыми вызывается get_photo_url (200/400/800).
PHOTO_SIZES = (200, 400, 800)
JPEG_QUALITY = 75

_s3_client = None


def _get_client():
    """Ленивая инициализация S3-клиента (один на процесс)."""
    global _s3_client
    if _s3_client is None:
        if not (S3_ENDPOINT and S3_ACCESS_KEY and S3_SECRET_KEY and S3_BUCKET):
            logger.error("❌ S3 не сконфигурирован: проверьте TIMEWEB_S3_* переменные")
            return None
        _s3_client = boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            region_name=S3_REGION,
            config=Config(signature_version="s3v4"),
        )
    return _s3_client


def _make_square_jpeg(image_bytes: bytes, size: int) -> bytes:
    """
    Привести изображение к квадрату size x size (центральный кроп) и отдать JPEG.
    Повторяет поведение Cloudinary c_fill: заполняем квадрат, лишнее обрезаем.
    """
    img = Image.open(io.BytesIO(image_bytes))
    if img.mode != "RGB":
        img = img.convert("RGB")

    w, h = img.size
    side = min(w, h)
    left = (w - side) // 2
    top = (h - side) // 2
    img = img.crop((left, top, left + side, top + side))

    if side != size:
        img = img.resize((size, size), Image.Resampling.LANCZOS)

    out = io.BytesIO()
    img.save(out, format="JPEG", quality=JPEG_QUALITY, optimize=True)
    return out.getvalue()


def _upload_sync(image_bytes: bytes, user_id: int, plant_name: str) -> str | None:
    """
    Синхронная загрузка трёх размеров в S3. Вызывается из upload_plant_photo
    через asyncio.to_thread, чтобы не блокировать event loop.
    Возвращает базовый ключ или None при ошибке.
    """
    client = _get_client()
    if client is None:
        return None

    image_hash = hashlib.md5(image_bytes[:1024]).hexdigest()[:8]
    safe_name = (plant_name or "plant").replace(" ", "_").replace("/", "_")[:30]
    key_base = f"{KEY_PREFIX}/user_{user_id}/{safe_name}_{image_hash}"

    try:
        for size in PHOTO_SIZES:
            data = _make_square_jpeg(image_bytes, size)
            client.put_object(
                Bucket=S3_BUCKET,
                Key=f"{key_base}_{size}.jpg",
                Body=data,
                ContentType="image/jpeg",
            )
        logger.info(f"✅ Фото загружено в S3: {key_base} (размеры {PHOTO_SIZES})")
        return key_base
    except (BotoCoreError, ClientError) as e:
        logger.error(f"❌ Ошибка загрузки в S3: {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"❌ Ошибка обработки/загрузки фото: {e}", exc_info=True)
        return None


async def upload_plant_photo(image_bytes: bytes, user_id: int, plant_name: str = "plant") -> str | None:
    """
    Загрузить фото растения в S3 в трёх квадратных размерах (200/400/800).

    Возвращает базовый ключ (без размера и расширения) для сохранения в БД,
    либо None при ошибке.
    """
    return await asyncio.to_thread(_upload_sync, image_bytes, user_id, plant_name)


def _delete_user_photos_sync(user_id: int) -> int:
    """
    Синхронно удалить ВСЕ объекты пользователя из S3 под префиксом
    bloom_plants/user_{user_id}/ (все растения, все размеры).

    Возвращает количество удалённых объектов. Best-effort: ошибки логируются,
    наружу не пробрасываются (чтобы удаление аккаунта в БД не откатывалось
    из-за проблем с S3).
    """
    client = _get_client()
    if client is None:
        logger.error(f"❌ S3 не сконфигурирован, не могу удалить фото user_id={user_id}")
        return 0

    prefix = f"{KEY_PREFIX}/user_{user_id}/"
    deleted = 0
    try:
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
            if not objects:
                continue
            # delete_objects удаляет до 1000 ключей за один вызов
            for i in range(0, len(objects), 1000):
                batch = objects[i:i + 1000]
                resp = client.delete_objects(
                    Bucket=S3_BUCKET,
                    Delete={"Objects": batch, "Quiet": True},
                )
                deleted += len(batch)
                for err in (resp.get("Errors") or []):
                    logger.error(
                        f"❌ Не удалён объект S3 {err.get('Key')}: {err.get('Message')}"
                    )
                    deleted -= 1
        logger.info(
            f"🗑️ Удалено фото из S3 для user_id={user_id}: {deleted} объектов (префикс {prefix})"
        )
        return deleted
    except (BotoCoreError, ClientError) as e:
        logger.error(f"❌ Ошибка удаления фото из S3 (user_id={user_id}): {e}", exc_info=True)
        return deleted
    except Exception as e:
        logger.error(
            f"❌ Непредвиденная ошибка удаления фото из S3 (user_id={user_id}): {e}",
            exc_info=True,
        )
        return deleted


async def delete_user_photos(user_id: int) -> int:
    """
    Асинхронно удалить все фото пользователя из S3 (под префиксом user_{id}).
    Best-effort: при ошибке S3 возвращает, сколько успели удалить, и не падает.
    """
    return await asyncio.to_thread(_delete_user_photos_sync, user_id)


def _closest_size(width: int) -> int:
    """Ближайший доступный размер не меньше width (иначе максимальный)."""
    for s in PHOTO_SIZES:
        if width <= s:
            return s
    return PHOTO_SIZES[-1]


def get_photo_url(photo_field: str | None, width: int = 400) -> str | None:
    """
    Собрать публичный URL фото нужного размера.

    - Пусто -> None
    - Старый Cloudinary URL (http...) -> возвращаем как есть (страховка)
    - Базовый S3-ключ (начинается с KEY_PREFIX) -> публичный URL нужного размера
    - Любой другой формат (напр. старый Telegram file_id) -> None
    """
    if not photo_field:
        return None

    if photo_field.startswith("http"):
        return photo_field

    if not photo_field.startswith(f"{KEY_PREFIX}/"):
        return None

    if not S3_PUBLIC_BASE_URL:
        logger.error("❌ S3_PUBLIC_BASE_URL не задан, не могу собрать URL фото")
        return None

    size = _closest_size(width)
    base = S3_PUBLIC_BASE_URL.rstrip("/")
    return f"{base}/{photo_field}_{size}.jpg"
