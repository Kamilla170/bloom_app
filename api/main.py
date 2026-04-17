"""
Bloom AI REST API — точка входа
Запуск: uvicorn api.main:app --host 0.0.0.0 --port 8001
"""

import os
import sys
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Добавляем корневую директорию проекта в sys.path
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from database import init_database, get_db

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
        # --- Этап 0: app-авторизация ---
        await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS email TEXT UNIQUE")
        await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS password_hash TEXT")
        await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email ON users(email) WHERE email IS NOT NULL")
        logger.info("✅ Миграция: email, password_hash")

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

        # Бэкфилл total_waterings (если триггер ещё не заполнил)
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
            # plant_photos может не существовать
            pass

        logger.info("✅ Миграция: аналитика и достижения (Этап 9)")


# === Lifecycle ===

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Инициализация при старте, очистка при завершении"""
    logger.info("🚀 Bloom AI REST API — запуск...")
    await init_database()
    await run_app_migrations()

    # Инициализация Firebase (FCM пуши)
    from services.fcm_service import init_firebase
    init_firebase()

    logger.info("✅ API готов к работе")
    yield
    logger.info("🛑 API — завершение")
    try:
        db = await get_db()
        await db.close()
    except Exception:
        pass


# === App ===

app = FastAPI(
    title="Bloom AI API",
    description="REST API для мобильного приложения Bloom AI",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# === Роутеры ===

from api.auth.router import router as auth_router
from api.plants.router import router as plants_router
from api.ai.router import router as ai_router
from api.users.router import router as users_router
from api.payments.router import router as payments_router
from api.analytics.router import router as analytics_router

app.include_router(auth_router)
app.include_router(plants_router)
app.include_router(ai_router)
app.include_router(users_router)
app.include_router(payments_router)
app.include_router(analytics_router)


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
    return {"message": "Bloom AI API", "docs": "/docs"}
