"""
Bloom AI REST API: точка входа
Запуск: uvicorn api.main:app --host 0.0.0.0 --port 8001
"""
import os
import sys
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, JSONResponse
# Добавляем корневую директорию проекта в sys.path
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)
from database import init_database, get_db
from slowapi.errors import RateLimitExceeded
from api.rate_limit import limiter, rate_limit_handler
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)
# === Миграции ===
async def run_app_migrations():
    """Все миграции для REST API"""
    db = await get_db()
    async with db.pool.acquire() as conn:
        # --- Auth: OAuth провайдеры ---
        # email теперь просто инфо-поле (без UNIQUE), заполняется из OAuth
        await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS email TEXT")
        await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS auth_provider TEXT")
        await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS provider_user_id TEXT")
        await conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_users_auth_provider
            ON users(auth_provider, provider_user_id)
            WHERE auth_provider IS NOT NULL
        """)
        # Удаляем старый уникальный индекс по email (если был) - теперь email не уникален
        await conn.execute("DROP INDEX IF EXISTS idx_users_email")
        logger.info("✅ Миграция: auth_provider, provider_user_id")
        # --- Согласие с юр-документами (152-ФЗ) ---
        # Фиксируем момент и редакцию принятых документов при регистрации.
        await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS terms_accepted_at TIMESTAMP")
        await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS terms_version TEXT")
        logger.info("✅ Миграция: terms_accepted_at, terms_version")
        # --- Маркетинговое согласие (ст. 18 38-ФЗ «О рекламе») ---
        # Отдельный от 152-ФЗ тумблер «Новости и предложения». По умолчанию
        # выключен. marketing_consent_at фиксирует момент включения как
        # доказательство добровольного согласия на рекламные рассылки.
        await conn.execute(
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS marketing_consent BOOLEAN NOT NULL DEFAULT FALSE"
        )
        await conn.execute(
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS marketing_consent_at TIMESTAMPTZ"
        )
        logger.info("✅ Миграция: marketing_consent")
        # --- Этап 9: Аналитика и достижения ---
        await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS total_photos INT DEFAULT 0")
        await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS global_watering_streak INT DEFAULT 0")
        await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS global_max_watering_streak INT DEFAULT 0")
        await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_global_watering_date DATE")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_achievements (
                user_id BIGINT NOT NULL,
                achievement_code VARCHAR(50) NOT NULL,
                unlocked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (user_id, achievement_code)
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_user_achievements_user
            ON user_achievements(user_id)
        """)
        # Бэкфилл total_waterings
        await conn.execute("""
            UPDATE users u
            SET total_waterings = COALESCE((
                SELECT COUNT(*) FROM care_history ch
                WHERE ch.user_id = u.user_id AND ch.action_type = 'watered'
            ), 0)
            WHERE total_waterings IS NULL OR total_waterings = 0
        """)
        # Бэкфилл total_photos
        try:
            await conn.execute("""
                UPDATE users u
                SET total_photos = COALESCE((
                    SELECT COUNT(*) FROM plant_photos pp
                    JOIN plants p ON pp.plant_id = p.id
                    WHERE p.user_id = u.user_id
                ), 0)
                WHERE total_photos IS NULL OR total_photos = 0
            """)
        except Exception:
            pass
        logger.info("✅ Миграция: аналитика и достижения (Этап 9)")
        # --- Этап 9.1: уведомления о новых достижениях ---
        # Колонка seen_at фиксирует факт показа тоста пользователю.
        # Backfill критичен: при первом запуске после деплоя считаем все
        # уже разблокированные ачивки виденными, чтобы юзеру не свалилась
        # очередь из 20 тостов.
        await conn.execute(
            "ALTER TABLE user_achievements ADD COLUMN IF NOT EXISTS seen_at TIMESTAMPTZ"
        )
        await conn.execute("""
            UPDATE user_achievements
            SET seen_at = NOW()
            WHERE seen_at IS NULL
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_user_achievements_unseen
            ON user_achievements(user_id)
            WHERE seen_at IS NULL
        """)
        logger.info("✅ Миграция: user_achievements.seen_at (Этап 9.1)")
        # --- ИИ чат: общий чат без растения ---
        try:
            await conn.execute(
                "ALTER TABLE plant_qa_history ALTER COLUMN plant_id DROP NOT NULL"
            )
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_plant_qa_history_user_general
                ON plant_qa_history(user_id, question_date DESC)
                WHERE plant_id IS NULL
            """)
            logger.info("✅ Миграция: общий чат без растения")
        except Exception as e:
            logger.info(f"Миграция общего чата уже выполнена: {e}")
        # --- Scheduler пушей: дата последней рассылки напоминания юзеру ---
        await conn.execute(
            "ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS last_reminder_sent DATE"
        )
        logger.info("✅ Миграция: user_settings.last_reminder_sent")
# === Lifecycle ===
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Инициализация при старте, очистка при завершении"""
    logger.info("🚀 Bloom AI REST API: запуск...")
    await init_database()
    await run_app_migrations()
    # Инициализация Firebase (FCM пуши)
    from services.fcm_service import init_firebase
    init_firebase()
    # Запуск планировщика (напоминания о поливе + автоплатежи)
    from services.scheduler import start_scheduler
    start_scheduler()
    logger.info("✅ API готов к работе")
    yield
    logger.info("🛑 API: завершение")
    from services.scheduler import stop_scheduler
    stop_scheduler()
    try:
        db = await get_db()
        await db.close()
    except Exception:
        pass
# === App ===
# /docs, /redoc и /openapi.json открыты только если ENABLE_DOCS=true.
# В проде переменную не ставим, поэтому карта API закрыта от посторонних.
# Локально для разработки: ENABLE_DOCS=true uvicorn api.main:app ...
_DOCS_ENABLED = os.getenv("ENABLE_DOCS", "false").lower() == "true"
app = FastAPI(
    title="Bloom AI API",
    description="REST API для мобильного приложения Bloom AI",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs" if _DOCS_ENABLED else None,
    redoc_url="/redoc" if _DOCS_ENABLED else None,
    openapi_url="/openapi.json" if _DOCS_ENABLED else None,
)
# === Rate limiting (slowapi) ===
# Лимитер и обработчик 429 объявлены в api/rate_limit.py. Здесь подключаем
# их к приложению. Сами лимиты висят декораторами на конкретных ручках.
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, rate_limit_handler)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# === Security-заголовки на всех ответах ===
@app.middleware("http")
async def security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    return response
# === Роутеры ===
from api.auth.router import router as auth_router
from api.plants.router import router as plants_router
from api.ai.router import router as ai_router
from api.users.router import router as users_router
from api.payments.router import router as payments_router
from api.analytics.router import (
    router as analytics_router,
    achievements_router,
)
from api.legal import router as legal_router
app.include_router(auth_router)
app.include_router(plants_router)
app.include_router(ai_router)
app.include_router(users_router)
app.include_router(payments_router)
app.include_router(analytics_router)
app.include_router(achievements_router)
app.include_router(legal_router)
# === Health check ===
@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "service": "Bloom AI REST API",
        "version": "1.0.0",
    }
@app.get("/")
async def root():
    return {"message": "Bloom AI API"}
# === Android App Links: Digital Asset Links ===
#
# Файл по адресу https://api.bloomai.ru/.well-known/assetlinks.json связывает
# приложение ru.bloomai.app с этим доменом. Android по нему проверяет, что
# приложению разрешено открывать https-ссылки домена напрямую (App Links).
# Отпечаток сейчас debug-ключа; при release-сборке просто добавим второй
# отпечаток в массив sha256_cert_fingerprints, оба будут работать.
_ASSETLINKS = [
    {
        "relation": ["delegate_permission/common.handle_all_urls"],
        "target": {
            "namespace": "android_app",
            "package_name": "ru.bloomai.app",
            "sha256_cert_fingerprints": [
                "45:2A:9C:93:46:F9:02:A0:76:2D:89:5F:28:3E:F4:96:6C:19:E2:D8:D0:23:E4:73:2E:5F:3D:23:51:FD:51:3A"
            ],
        },
    }
]


@app.get("/.well-known/assetlinks.json")
async def assetlinks():
    """Digital Asset Links для Android App Links (домен ↔ приложение)."""
    return JSONResponse(content=_ASSETLINKS)
# === OAuth callback: серверный 302 на кастомную схему мобильного приложения ===
#
# Yandex OAuth не поддерживает кастомные схемы (bloomai://) в redirect_uri.
# Поэтому регистрируем HTTPS-страницу на нашем бэке, а она делает HTTP 302
# на кастомную схему приложения. intent-filter в манифесте приложения ловит
# этот редирект и доставляет Uri в приложение через app_links.
#
# Используем authorization code flow: code приходит в query параметрах
# (не в fragment), поэтому сервер видит его и может пробросить в редирект.
@app.get("/oauth/yandex/callback")
async def yandex_oauth_callback(request: Request):
    """
    Принимает редирект от oauth.yandex.ru и перенаправляет в мобильное
    приложение через кастомную схему bloomai://.
    Сохраняет все query параметры как есть (code, state, error и т.д.).
    """
    qs = request.url.query
    target = "bloomai://oauth/yandex/callback"
    if qs:
        target = f"{target}?{qs}"

    return RedirectResponse(url=target, status_code=302)
