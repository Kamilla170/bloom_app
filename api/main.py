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
from fastapi.responses import HTMLResponse
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
from api.analytics.router import (
    router as analytics_router,
    achievements_router,
)
app.include_router(auth_router)
app.include_router(plants_router)
app.include_router(ai_router)
app.include_router(users_router)
app.include_router(payments_router)
app.include_router(analytics_router)
app.include_router(achievements_router)
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
# === OAuth callback: возврат в мобильное приложение через кастомную схему ===
#
# Yandex OAuth не поддерживает кастомные схемы (bloomai://) в redirect_uri,
# поэтому redirect_uri указывает на эту HTTPS-страницу, а она возвращает
# пользователя в приложение по схеме bloomai://.
#
# ВАЖНО про Auth Tab: Custom Tab / Auth Tab на свежих Android НЕ переходит
# по кастомной схеме из HTTP 302 (Location header) — остаётся на странице и
# показывает "сайт недоступен". Поэтому отдаём HTML-страницу, которая:
#   1) сразу пытается перейти на bloomai://... через JavaScript;
#   2) если автопереход не сработал — показывает кнопку, клик по которой
#      открывает приложение (переход по клику Auth Tab пропускает).
#
# authorization code flow: code приходит в query (не в fragment), поэтому
# сервер видит его и пробрасывает в схему as is (вместе со state/error и т.д.).
@app.get("/oauth/yandex/callback")
async def yandex_oauth_callback(request: Request):
    """
    Принимает редирект от oauth.yandex.ru и возвращает в мобильное приложение
    через кастомную схему bloomai://. Сохраняет все query-параметры как есть.
    """
    qs = request.url.query
    target = "bloomai://oauth/yandex/callback"
    if qs:
        target = f"{target}?{qs}"

    # target идёт в href ссылки и в JS-редирект — экранируем кавычки.
    safe_target = target.replace('"', "%22")

    html = f"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Bloom AI</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    background: #F7F4ED;
    color: #2b3629;
    min-height: 100vh;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 24px;
    text-align: center;
  }}
  .logo {{
    width: 72px; height: 72px;
    background: #009850;
    border-radius: 20px;
    display: flex; align-items: center; justify-content: center;
    margin-bottom: 24px;
    font-size: 36px;
  }}
  h1 {{ font-size: 20px; font-weight: 700; margin-bottom: 8px; }}
  p {{ font-size: 15px; color: #737a6f; margin-bottom: 28px; max-width: 320px; line-height: 1.4; }}
  a.btn {{
    display: inline-block;
    background: #009850;
    color: #ffffff;
    text-decoration: none;
    font-size: 16px;
    font-weight: 600;
    padding: 14px 32px;
    border-radius: 28px;
  }}
  a.btn:active {{ opacity: 0.85; }}
</style>
</head>
<body>
  <div class="logo">🌱</div>
  <h1>Вход выполнен</h1>
  <p>Возвращаемся в приложение Bloom AI. Если это не произошло автоматически, нажмите кнопку ниже.</p>
  <a class="btn" href="{safe_target}">Открыть Bloom AI</a>
  <script>
    // Сразу пытаемся открыть приложение по кастомной схеме.
    // На части браузеров сработает автоматически; если нет — остаётся кнопка.
    window.location.href = "{safe_target}";
  </script>
</body>
</html>"""

    return HTMLResponse(content=html)
