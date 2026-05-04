"""
Эндпоинты для работы с растениями (Этап 3 + Этап 9 achievements)
"""

import logging
import uuid
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, status
from pydantic import BaseModel
from typing import Optional

from database import get_db
from api.auth.dependencies import get_current_user
from api.schemas import (
    PlantListResponse, PlantSummary, PlantDetail,
    AnalysisResponse, SavePlantRequest, WaterPlantResponse,
    UpdatePlantRequest, FertilizeResponse, PlantPhotoEntry, SuccessResponse,
)
from services.ai_service import analyze_plant_image, format_recommendations_text
from services.plant_service import (
    save_analyzed_plant, get_user_plants_list,
    water_plant, delete_plant, rename_plant,
    get_plant_details, fertilize_plant_action,
    update_plant_state_from_photo,
    _post_chat_auto_message,
)
from services.subscription_service import check_limit, increment_usage
from api.services.cloudinary_service import upload_plant_photo, get_photo_url
from config import STATE_EMOJI, STATE_NAMES
from achievements import (
    update_global_watering_streak,
    update_global_watering_streak_bulk,
    check_and_unlock,
    increment_photo_count,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/plants", tags=["plants"])

# Временное хранилище анализов
_app_temp_analyses: dict[str, dict] = {}


# === Схема для ответа на отправку фото в чат ===

class ChatPhotoMessageOut(BaseModel):
    """Ответ на отправку фото в чате растения. Совпадает с ChatMessageOut в ai/router.py."""
    id: int
    question: str = ""
    answer: str
    created_at: str
    plant_id: Optional[int] = None
    plant_name: Optional[str] = None
    photo_url: Optional[str] = None
    message_type: str = "user_photo"


def _plant_photo_url(photo_file_id: str | None, width: int = 400) -> str | None:
    return get_photo_url(photo_file_id, width)


async def _safe_check_achievements(user_id: int, category: str) -> None:
    try:
        await check_and_unlock(user_id, category=category)
    except Exception as e:
        logger.warning(f"⚠️ check_and_unlock({category}) упал: {e}", exc_info=True)


async def _safe_update_global_streak(user_id: int, plant_id: int) -> None:
    try:
        await update_global_watering_streak(user_id, plant_id)
    except Exception as e:
        logger.warning(f"⚠️ update_global_watering_streak упал: {e}", exc_info=True)


async def _safe_update_global_streak_bulk(user_id: int, plant_ids: list[int]) -> None:
    try:
        await update_global_watering_streak_bulk(user_id, plant_ids)
    except Exception as e:
        logger.warning(f"⚠️ update_global_watering_streak_bulk упал: {e}", exc_info=True)


async def _safe_increment_photo_count(user_id: int) -> None:
    try:
        await increment_photo_count(user_id)
    except Exception as e:
        logger.warning(f"⚠️ increment_photo_count упал: {e}", exc_info=True)


def _plant_to_summary(p: dict) -> PlantSummary:
    pid = p.get("id")
    state = p.get("current_state") or "healthy"
    return PlantSummary(
        id=pid,
        display_name=p.get("display_name") or p.get("plant_name") or f"Растение #{pid}",
        plant_name=p.get("plant_name"),
        current_state=state,
        state_emoji=STATE_EMOJI.get(state, "🌱"),
        watering_interval=p.get("watering_interval") or 7,
        last_watered=p.get("last_watered"),
        next_watering_date=p.get("next_watering_date"),
        needs_watering=bool(p.get("needs_watering", False)),
        water_status=p.get("water_status", ""),
        photo_file_id=p.get("photo_file_id"),
        photo_url=_plant_photo_url(p.get("photo_file_id"), 400),
        saved_date=p.get("saved_date"),
        current_streak=p.get("current_streak", 0) or 0,
        max_streak=p.get("max_streak", 0) or 0,
        fertilizing_enabled=bool(p.get("fertilizing_enabled", False)),
        fertilizing_interval=p.get("fertilizing_interval"),
        last_fertilized=p.get("last_fertilized"),
        next_fertilizing_date=p.get("next_fertilizing_date"),
    )


def _plant_to_detail(plant: dict) -> PlantDetail:
    pid = plant.get("id")
    current_state = plant.get("current_state") or "healthy"
    photo_fid = plant.get("photo_file_id")
    return PlantDetail(
        id=pid,
        display_name=plant.get("display_name") or plant.get("plant_name") or f"Растение #{pid}",
        plant_name=plant.get("plant_name"),
        current_state=current_state,
        state_emoji=STATE_EMOJI.get(current_state, "🌱"),
        state_name=STATE_NAMES.get(current_state, "Здоровое"),
        watering_interval=plant.get("watering_interval") or 7,
        last_watered=plant.get("last_watered"),
        next_watering_date=plant.get("next_watering_date"),
        needs_watering=bool(plant.get("needs_watering") or False),
        water_status="",
        photo_file_id=photo_fid,
        photo_url=_plant_photo_url(photo_fid, 800),
        saved_date=plant.get("saved_date"),
        analysis=plant.get("analysis"),
        current_streak=plant.get("current_streak") or 0,
        max_streak=plant.get("max_streak") or 0,
        fertilizing_enabled=bool(plant.get("fertilizing_enabled") or False),
        fertilizing_interval=plant.get("fertilizing_interval"),
        last_fertilized=plant.get("last_fertilized"),
        next_fertilizing_date=plant.get("next_fertilizing_date"),
    )


@router.get("", response_model=PlantListResponse)
async def list_plants(user_id: int = Depends(get_current_user)):
    """Список растений пользователя"""
    plants = await get_user_plants_list(user_id, limit=50)

    items = []
    for p in plants:
        if p.get("type") == "growing":
            continue
        items.append(_plant_to_summary(p))

    return PlantListResponse(plants=items, total=len(items))


@router.get("/{plant_id}", response_model=PlantDetail)
async def get_plant(plant_id: int, user_id: int = Depends(get_current_user)):
    """Детали растения"""
    db = await get_db()
    plant = await db.get_plant_with_state(plant_id, user_id)

    if not plant:
        raise HTTPException(status_code=404, detail="Растение не найдено")

    return _plant_to_detail(plant)


@router.post("/analyze", response_model=AnalysisResponse)
async def analyze_photo(
    photo: UploadFile = File(...),
    user_id: int = Depends(get_current_user),
):
    """Загрузить фото и получить анализ растения"""
    allowed, error_msg = await check_limit(user_id, "analyses")
    if not allowed:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=error_msg)

    image_bytes = await photo.read()
    if len(image_bytes) < 1000:
        raise HTTPException(status_code=400, detail="Файл слишком маленький")
    if len(image_bytes) > 20 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="Файл слишком большой (макс. 20 МБ)")

    result = await analyze_plant_image(image_bytes, user_id=user_id)

    # На фото не растение — лимит не списываем, фото в Cloudinary не грузим
    if not result.get("success") and result.get("error_code") == "not_a_plant":
        raise HTTPException(status_code=400, detail=result["error"])

    if not result["success"]:
        return AnalysisResponse(success=False, error=result.get("error", "Ошибка анализа"))

    await increment_usage(user_id, "analyses")

    plant_name_safe = (result.get("plant_name") or "plant").replace(" ", "_")[:30]
    photo_url = await upload_plant_photo(image_bytes, user_id, plant_name_safe)

    state_info = result.get("state_info", {})
    recommendations = result.get("recommendations") or format_recommendations_text(result.get("raw_analysis", ""))

    temp_id = str(uuid.uuid4())
    _app_temp_analyses[temp_id] = {
        "user_id": user_id,
        "analysis": result.get("raw_analysis", result["analysis"]),
        "formatted_analysis": result["analysis"],
        "photo_bytes": image_bytes,
        "photo_file_id": photo_url or "app_photo_pending",
        "plant_name": result.get("plant_name", "Неизвестное растение"),
        "latin_name": result.get("latin_name"),
        "species_description": result.get("species_description"),
        "confidence": result.get("confidence", 0),
        "state_info": state_info,
        "watering_interval": result.get("watering_interval"),
        "recommendations": recommendations,
        "created_at": datetime.now(),
    }

    return AnalysisResponse(
        success=True,
        analysis=result["analysis"],
        plant_name=result.get("plant_name"),
        latin_name=result.get("latin_name"),
        species_description=result.get("species_description"),
        confidence=result.get("confidence"),
        watering_interval=result.get("watering_interval"),
        state=state_info.get("current_state", "healthy"),
        fertilizing_enabled=state_info.get("fertilizing_enabled", False),
        fertilizing_interval=state_info.get("fertilizing_interval"),
        temp_id=temp_id,
        photo_url=photo_url,
        recommendations=recommendations,
    )


@router.post("", response_model=PlantDetail)
async def save_plant(
    req: SavePlantRequest,
    user_id: int = Depends(get_current_user),
):
    """Сохранить проанализированное растение в коллекцию"""
    allowed, error_msg = await check_limit(user_id, "plants")
    if not allowed:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=error_msg)

    analysis_data = _app_temp_analyses.get(req.temp_id)
    if not analysis_data or analysis_data["user_id"] != user_id:
        raise HTTPException(status_code=404, detail="Анализ не найден или истёк")

    last_watered = None
    if req.last_watered_days_ago is not None and req.last_watered_days_ago >= 0:
        last_watered = datetime.now() - timedelta(days=req.last_watered_days_ago)

    result = await save_analyzed_plant(user_id, analysis_data, last_watered=last_watered)

    if not result["success"]:
        raise HTTPException(status_code=500, detail=result.get("error", "Ошибка сохранения"))

    _app_temp_analyses.pop(req.temp_id, None)

    await _safe_check_achievements(user_id, category='plants')

    plant_id = result["plant_id"]

    try:
        db = await get_db()
        plant = await db.get_plant_with_state(plant_id, user_id)
        if plant:
            return _plant_to_detail(plant)

        logger.warning(
            f"⚠️ Растение {plant_id} сохранено, но get_plant_with_state вернул None"
        )
    except Exception as e:
        logger.error(
            f"❌ Не удалось собрать PlantDetail для plant_id={plant_id}: {e}",
            exc_info=True,
        )

    state_info = analysis_data.get("state_info") or {}
    current_state = state_info.get("current_state") or "healthy"
    return PlantDetail(
        id=plant_id,
        display_name=analysis_data.get("plant_name") or f"Растение #{plant_id}",
        plant_name=analysis_data.get("plant_name"),
        current_state=current_state,
        state_emoji=STATE_EMOJI.get(current_state, "🌱"),
        state_name=STATE_NAMES.get(current_state, "Здоровое"),
        watering_interval=analysis_data.get("watering_interval") or 7,
        photo_file_id=analysis_data.get("photo_file_id"),
        photo_url=_plant_photo_url(analysis_data.get("photo_file_id"), 800),
        saved_date=datetime.now(),
        fertilizing_enabled=bool(state_info.get("fertilizing_enabled") or False),
        fertilizing_interval=state_info.get("fertilizing_interval"),
    )


@router.post("/{plant_id}/water", response_model=WaterPlantResponse)
async def water_single_plant(plant_id: int, user_id: int = Depends(get_current_user)):
    """Отметить полив растения (с расчётом серии)"""
    result = await water_plant(user_id, plant_id)

    if not result["success"]:
        raise HTTPException(status_code=404, detail=result.get("error", "Растение не найдено"))

    await _safe_update_global_streak(user_id, plant_id)
    await _safe_check_achievements(user_id, category='water')

    return WaterPlantResponse(
        success=True,
        plant_name=result["plant_name"],
        next_watering_days=result["next_watering_days"],
        next_watering_date=result.get("next_watering_date"),
        current_streak=result.get("current_streak", 0),
        max_streak=result.get("max_streak", 0),
        watered_at=datetime.now(),
    )


@router.post("/{plant_id}/fertilize", response_model=FertilizeResponse)
async def fertilize_single_plant(plant_id: int, user_id: int = Depends(get_current_user)):
    """Подкормка отключена. Эндпоинт оставлен для совместимости."""
    raise HTTPException(status_code=400, detail="Подкормка временно отключена")


@router.patch("/{plant_id}", response_model=SuccessResponse)
async def update_plant(
    plant_id: int,
    req: UpdatePlantRequest,
    user_id: int = Depends(get_current_user),
):
    """Обновить растение (имя и/или fertilizing_enabled)"""
    db = await get_db()
    plant = await db.get_plant_by_id(plant_id, user_id)
    if not plant:
        raise HTTPException(status_code=404, detail="Растение не найдено")

    updated_fields = []

    if req.name is not None:
        result = await rename_plant(user_id, plant_id, req.name)
        if not result["success"]:
            raise HTTPException(status_code=400, detail=result.get("error", "Ошибка"))
        updated_fields.append(f"имя: {result['new_name']}")

    if req.fertilizing_enabled is not None:
        await db.update_plant_fertilizing(plant_id, user_id, req.fertilizing_enabled)
        updated_fields.append(f"подкормка: {'вкл' if req.fertilizing_enabled else 'выкл'}")

    if not updated_fields:
        raise HTTPException(status_code=400, detail="Нет полей для обновления")

    return SuccessResponse(message=f"Обновлено ({', '.join(updated_fields)})")


@router.delete("/{plant_id}", response_model=SuccessResponse)
async def remove_plant(plant_id: int, user_id: int = Depends(get_current_user)):
    """Удалить растение"""
    result = await delete_plant(user_id, plant_id)

    if not result["success"]:
        raise HTTPException(status_code=404, detail="Растение не найдено")

    return SuccessResponse(message=f"{result['plant_name']} удалено")


@router.get("/{plant_id}/photos", response_model=list[PlantPhotoEntry])
async def get_photos(plant_id: int, user_id: int = Depends(get_current_user)):
    """История фото растения"""
    db = await get_db()
    plant = await db.get_plant_by_id(plant_id, user_id)
    if not plant:
        raise HTTPException(status_code=404, detail="Растение не найдено")

    photos = await db.get_plant_photos(plant_id, limit=50)

    return [
        PlantPhotoEntry(
            id=p["id"],
            photo_url=p["photo_url"],
            created_at=p["created_at"],
        )
        for p in photos
    ]


async def _process_photo_update(
    plant_id: int,
    user_id: int,
    image_bytes: bytes,
    message_type: str,
) -> tuple[dict, str | None, dict | None]:
    """
    Общий код для /photo и /chat-photo: лимит, анализ, апдейт состояния, plant_photos.
    Возвращает: (analyze_result, photo_url, update_result).

    Если на фото не растение — возвращает (analyze_result, None, None).
    Лимит не списывается, фото в Cloudinary не грузится, карточка растения не трогается.
    Решение что делать дальше принимает вызывающий эндпоинт.
    """
    allowed, error_msg = await check_limit(user_id, "analyses")
    if not allowed:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=error_msg)

    db = await get_db()
    plant = await db.get_plant_by_id(plant_id, user_id)
    if not plant:
        raise HTTPException(status_code=404, detail="Растение не найдено")

    if len(image_bytes) < 1000:
        raise HTTPException(status_code=400, detail="Файл слишком маленький")
    if len(image_bytes) > 20 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="Файл слишком большой")

    previous_state = plant.get("current_state", "healthy")
    result = await analyze_plant_image(
        image_bytes,
        previous_state=previous_state,
        user_id=user_id,
        plant_id=plant_id,
    )

    # На фото не растение — выходим до любых сайд-эффектов.
    if not result.get("success") and result.get("error_code") == "not_a_plant":
        return result, None, None

    if not result["success"]:
        raise HTTPException(status_code=500, detail=result.get("error", "Ошибка анализа"))

    await increment_usage(user_id, "analyses")

    plant_name_safe = (result.get("plant_name") or "plant").replace(" ", "_")[:30]
    photo_url = await upload_plant_photo(image_bytes, user_id, plant_name_safe)

    if not photo_url:
        raise HTTPException(status_code=500, detail="Ошибка загрузки фото")

    state_info = result.get("state_info", {})
    new_interval = result.get("watering_interval")

    update_result = await update_plant_state_from_photo(
        plant_id=plant_id,
        user_id=user_id,
        photo_file_id=photo_url,
        state_info=state_info,
        raw_analysis=result.get("raw_analysis", ""),
        new_watering_interval=new_interval,
        message_type=message_type,
    )

    if not update_result["success"]:
        raise HTTPException(status_code=500, detail=update_result.get("error", "Ошибка обновления"))

    # Кладём новое фото в plant_photos (история растения)
    try:
        await db.add_plant_photo(plant_id, user_id, photo_url)
    except Exception as e:
        logger.warning(f"⚠️ Не удалось добавить фото в plant_photos: {e}")

    await _safe_increment_photo_count(user_id)
    await _safe_check_achievements(user_id, category='photos')

    return result, photo_url, update_result


@router.post("/{plant_id}/photo", response_model=PlantDetail)
async def update_plant_photo(
    plant_id: int,
    photo: UploadFile = File(...),
    user_id: int = Depends(get_current_user),
):
    """
    Обновить главное фото растения с переанализом.
    Внутри также пишет сообщение с фото и анализом в чат растения.
    Используется кнопкой 'Обновить фото' в карточке.
    """
    image_bytes = await photo.read()
    analyze_result, _, _ = await _process_photo_update(
        plant_id=plant_id,
        user_id=user_id,
        image_bytes=image_bytes,
        message_type="auto_analysis",
    )

    # На фото не растение — карточку не трогаем, возвращаем понятную 400.
    if not analyze_result.get("success") and analyze_result.get("error_code") == "not_a_plant":
        raise HTTPException(status_code=400, detail=analyze_result["error"])

    db = await get_db()
    plant = await db.get_plant_with_state(plant_id, user_id)
    return _plant_to_detail(plant)


@router.post("/{plant_id}/chat-photo", response_model=ChatPhotoMessageOut)
async def send_chat_photo(
    plant_id: int,
    photo: UploadFile = File(...),
    user_id: int = Depends(get_current_user),
):
    """
    Отправить фото растения в чат.
    Делает то же самое что /photo (анализ + обновление главного фото и состояния),
    но возвращает новое сообщение чата (с фото пользователя и анализом ИИ).
    """
    image_bytes = await photo.read()
    analyze_result, photo_url, update_result = await _process_photo_update(
        plant_id=plant_id,
        user_id=user_id,
        image_bytes=image_bytes,
        message_type="user_photo",
    )

    # На фото не растение — пишем в чат сообщение от ИИ с отказом
    # и возвращаем его. Карточку растения не трогаем, лимит не списан.
    if not analyze_result.get("success") and analyze_result.get("error_code") == "not_a_plant":
        refusal_text = analyze_result["error"]

        try:
            await _post_chat_auto_message(
                plant_id=plant_id,
                user_id=user_id,
                photo_url=None,
                answer_text=refusal_text,
                message_type="user_photo",
            )
        except Exception as e:
            logger.warning(f"⚠️ Не удалось записать сообщение-отказ в чат: {e}")

        # Достаём только что записанное сообщение
        db = await get_db()
        history = await db.get_plant_qa_history(plant_id, limit=1)
        if history:
            qa = history[0]
            created = qa.get("question_date") or qa.get("created_at")
            return ChatPhotoMessageOut(
                id=qa.get("id", 0),
                question="",
                answer=qa.get("answer_text", refusal_text) or refusal_text,
                created_at=created.isoformat() if created else datetime.now().isoformat(),
                plant_id=plant_id,
                plant_name=None,
                photo_url=None,
                message_type="user_photo",
            )

        # Fallback если запись в БД не удалась
        return ChatPhotoMessageOut(
            id=0,
            question="",
            answer=refusal_text,
            created_at=datetime.now().isoformat(),
            plant_id=plant_id,
            plant_name=None,
            photo_url=None,
            message_type="user_photo",
        )

    # Достаём только что записанное сообщение из plant_qa_history
    db = await get_db()
    history = await db.get_plant_qa_history(plant_id, limit=1)

    if not history:
        # Не должно случаться, но fallback на собранное сообщение
        recommendations = update_result.get("recommendations") or analyze_result.get("recommendations") or ""
        return ChatPhotoMessageOut(
            id=0,
            question="",
            answer=recommendations,
            created_at=datetime.now().isoformat(),
            plant_id=plant_id,
            plant_name=update_result.get("plant_name"),
            photo_url=photo_url,
            message_type="user_photo",
        )

    qa = history[0]
    created = qa.get("question_date") or qa.get("created_at")

    return ChatPhotoMessageOut(
        id=qa.get("id", 0),
        question=qa.get("question_text", "") or "",
        answer=qa.get("answer_text", "") or "",
        created_at=created.isoformat() if created else datetime.now().isoformat(),
        plant_id=plant_id,
        plant_name=update_result.get("plant_name"),
        photo_url=photo_url,
        message_type="user_photo",
    )


@router.post("/water-all", response_model=SuccessResponse)
async def water_all(user_id: int = Depends(get_current_user)):
    """Полить все растения"""
    from services.plant_service import water_all_plants

    # Берём список ВСЕХ regular-растений ДО полива.
    # ON CONFLICT DO NOTHING в bulk-инсёрте защитит от двойного учёта тех,
    # что уже были политы сегодня по отдельной кнопке.
    db = await get_db()
    async with db.pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id FROM plants
            WHERE user_id = $1
              AND (plant_type = 'regular' OR plant_type IS NULL)
        """, user_id)
    plant_ids = [r['id'] for r in rows]

    result = await water_all_plants(user_id)
    if not result["success"]:
        raise HTTPException(status_code=500, detail="Ошибка")

    await _safe_update_global_streak_bulk(user_id, plant_ids)
    await _safe_check_achievements(user_id, category='water')

    return SuccessResponse(message="Все растения политы")
