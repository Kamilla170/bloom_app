import logging
from datetime import datetime, timedelta
from database import get_db
from services.ai_service import extract_watering_info, format_recommendations_text
from services.reminder_service import create_plant_reminder
from utils.time_utils import get_moscow_now, format_days_ago
from config import STATE_EMOJI, STATE_NAMES

logger = logging.getLogger(__name__)

# Временное хранилище для анализов
temp_analyses = {}


async def _post_chat_auto_message(
    plant_id: int,
    user_id: int,
    photo_url: str,
    answer_text: str,
    message_type: str,
) -> None:
    """
    Записать системное сообщение от ИИ в plant_qa_history.
    Используется при добавлении растения и при обновлении фото.

    message_type:
        'auto_analysis' - первоначальный анализ или обновление фото из карточки
        'user_photo' - фото отправлено пользователем в чате (анализ ответ)
    """
    try:
        if not answer_text or not answer_text.strip():
            return
        db = await get_db()
        await db.save_qa_interaction(
            plant_id=plant_id,
            user_id=user_id,
            question="",
            answer=answer_text,
            context_used={
                "type": message_type,
                "photo_url": photo_url,
            },
        )
    except Exception as e:
        # Не должно валить core-флоу
        logger.warning(f"⚠️ Не удалось записать авто-сообщение в чат: {e}", exc_info=True)


async def save_analyzed_plant(user_id: int, analysis_data: dict, last_watered: datetime = None) -> dict:
    """
    Сохранение проанализированного растения (Этап 3).
    Записываем интервал полива и next_watering_date.
    Также:
      - сохраняем первое фото в plant_photos
      - пишем первое сообщение в plant_qa_history (анализ + рекомендации)
    """
    try:
        raw_analysis = analysis_data.get("analysis", "")
        state_info = analysis_data.get("state_info", {})

        # Интервал полива
        ai_interval = analysis_data.get("watering_interval")
        if ai_interval is None:
            watering_info = extract_watering_info(raw_analysis)
            ai_interval = watering_info["interval_days"]

        ai_interval = max(3, min(28, ai_interval))
        logger.info(f"💧 Интервал полива: {ai_interval} дней")

        db = await get_db()
        plant_id = await db.save_plant(
            user_id=user_id,
            analysis=raw_analysis,
            photo_file_id=analysis_data["photo_file_id"],
            plant_name=analysis_data.get("plant_name", "Неизвестное растение")
        )

        # Устанавливаем интервалы
        await db.update_plant_watering_interval(plant_id, ai_interval)
        await db.set_base_watering_interval(plant_id, ai_interval)

        # Расчёт next_watering_date и last_watered
        today = datetime.now().date()
        next_watering_days = ai_interval

        if last_watered:
            days_since_watered = (datetime.now() - last_watered).days
            next_watering_days = max(1, ai_interval - days_since_watered)
            next_watering_date = today + timedelta(days=next_watering_days)
        else:
            next_watering_date = today + timedelta(days=ai_interval)

        async with db.pool.acquire() as conn:
            if last_watered:
                await conn.execute("""
                    UPDATE plants
                    SET watering_interval = $2,
                        next_watering_date = $3,
                        last_watered = $4
                    WHERE id = $1
                """, plant_id, ai_interval, next_watering_date, last_watered)
            else:
                await conn.execute("""
                    UPDATE plants
                    SET watering_interval = $2,
                        next_watering_date = $3
                    WHERE id = $1
                """, plant_id, ai_interval, next_watering_date)

        # Состояние
        current_state = state_info.get('current_state', 'healthy')
        state_reason = state_info.get('state_reason', 'Первичный анализ AI')

        await db.update_plant_state(
            plant_id=plant_id,
            user_id=user_id,
            new_state=current_state,
            change_reason=state_reason,
            photo_file_id=analysis_data["photo_file_id"],
            ai_analysis=raw_analysis,
        )

        # Полный анализ в историю
        await db.save_full_analysis(
            plant_id=plant_id,
            user_id=user_id,
            photo_file_id=analysis_data["photo_file_id"],
            full_analysis=raw_analysis,
            confidence=analysis_data.get("confidence", 0),
            identified_species=analysis_data.get("plant_name"),
            detected_state=current_state,
            watering_advice=None,
            lighting_advice=None
        )

        # Сохраняем первое фото в plant_photos (история растения)
        photo_url = analysis_data.get("photo_file_id")
        if photo_url and (photo_url.startswith("http") or photo_url.startswith("/")):
            try:
                await db.add_plant_photo(plant_id, user_id, photo_url)
                logger.info(f"📸 Первое фото сохранено в plant_photos для растения {plant_id}")
            except Exception as e:
                logger.warning(f"⚠️ Не удалось сохранить первое фото в plant_photos: {e}")

        # Пишем первое сообщение в чат растения (анализ + рекомендации)
        recommendations_text = analysis_data.get("recommendations") or format_recommendations_text(raw_analysis)
        if recommendations_text:
            await _post_chat_auto_message(
                plant_id=plant_id,
                user_id=user_id,
                photo_url=photo_url,
                answer_text=recommendations_text,
                message_type="auto_analysis",
            )

        # Напоминание
        await create_plant_reminder(plant_id, user_id, next_watering_days)

        plant_name = analysis_data.get("plant_name", "растение")
        state_emoji = STATE_EMOJI.get(current_state, '🌱')
        state_name = STATE_NAMES.get(current_state, 'Здоровое')

        logger.info(f"✅ Растение сохранено: {plant_name}, id={plant_id}")

        return {
            "success": True,
            "plant_id": plant_id,
            "plant_name": plant_name,
            "state": current_state,
            "state_emoji": state_emoji,
            "state_name": state_name,
            "interval": ai_interval,
            "next_watering_days": next_watering_days,
        }

    except Exception as e:
        logger.error(f"Ошибка сохранения растения: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


async def update_plant_state_from_photo(plant_id: int, user_id: int,
                                        photo_file_id: str, state_info: dict,
                                        raw_analysis: str,
                                        new_watering_interval: int = None,
                                        message_type: str = "auto_analysis") -> dict:
    """
    Обновление состояния растения по новому фото (Этап 3).
    Старое главное фото уезжает в plant_photos history.
    Также:
      - корректирует watering_interval если передан
      - пишет сообщение в plant_qa_history (анализ + рекомендации)

    message_type:
        'auto_analysis' - обновление фото из карточки растения
        'user_photo' - фото отправлено пользователем в чате
    """
    try:
        db = await get_db()
        plant = await db.get_plant_by_id(plant_id, user_id)

        if not plant:
            return {"success": False, "error": "Растение не найдено"}

        previous_state = plant.get('current_state', 'healthy')
        new_state = state_info.get('current_state', 'healthy')
        state_reason = state_info.get('state_reason', 'Анализ AI')
        state_changed = (new_state != previous_state)

        # Старое фото в историю
        old_photo = plant.get('photo_file_id')
        if old_photo and old_photo.startswith('http'):
            try:
                await db.add_plant_photo_to_history(plant_id, old_photo)
                logger.info(f"📸 Старое фото добавлено в историю растения {plant_id}")
            except Exception as e:
                logger.error(f"Ошибка сохранения фото в историю: {e}")

        # Обновляем состояние
        await db.update_plant_state(
            plant_id=plant_id,
            user_id=user_id,
            new_state=new_state,
            change_reason=state_reason,
            photo_file_id=photo_file_id,
            ai_analysis=raw_analysis,
        )

        # Обновляем главное фото и дату последнего анализа.
        # Если переданы новые рекомендации по интервалу полива, корректируем.
        async with db.pool.acquire() as conn:
            if new_watering_interval and 3 <= new_watering_interval <= 28:
                # Корректируем watering_interval и пересчитываем next_watering_date
                last_watered = plant.get('last_watered')
                today = datetime.now().date()
                if last_watered:
                    days_since_watered = (datetime.now() - last_watered).days
                    next_days = max(1, new_watering_interval - days_since_watered)
                    next_date = today + timedelta(days=next_days)
                else:
                    next_date = today + timedelta(days=new_watering_interval)

                await conn.execute("""
                    UPDATE plants
                    SET last_photo_analysis = CURRENT_TIMESTAMP,
                        photo_file_id = $1,
                        watering_interval = $2,
                        next_watering_date = $3
                    WHERE id = $4
                """, photo_file_id, new_watering_interval, next_date, plant_id)
                logger.info(
                    f"💧 Интервал полива обновлён по новому фото: "
                    f"{plant.get('watering_interval')} -> {new_watering_interval} дн."
                )
            else:
                await conn.execute("""
                    UPDATE plants
                    SET last_photo_analysis = CURRENT_TIMESTAMP,
                        photo_file_id = $1
                    WHERE id = $2
                """, photo_file_id, plant_id)

        # Пишем сообщение в чат растения с анализом
        recommendations_text = format_recommendations_text(raw_analysis)
        if recommendations_text:
            await _post_chat_auto_message(
                plant_id=plant_id,
                user_id=user_id,
                photo_url=photo_file_id,
                answer_text=recommendations_text,
                message_type=message_type,
            )

        return {
            "success": True,
            "state_changed": state_changed,
            "previous_state": previous_state,
            "new_state": new_state,
            "plant_name": plant['display_name'],
            "recommendations": recommendations_text,
        }

    except Exception as e:
        logger.error(f"Ошибка обновления состояния: {e}")
        return {"success": False, "error": str(e)}


async def get_user_plants_list(user_id: int, limit: int = 15) -> list:
    """Получить список растений с форматированием"""
    try:
        db = await get_db()
        plants = await db.get_user_plants(user_id, limit=limit)

        formatted_plants = []

        for plant in plants:
            plant_data = {
                "id": plant.get('id'),
                "display_name": plant.get('display_name'),
                "type": plant.get('type', 'regular'),
                "emoji": '🌱',
                "photo_file_id": plant.get('photo_file_id'),
            }

            if plant.get('type') == 'growing':
                plant_data["emoji"] = '🌱'
                plant_data["stage_info"] = plant.get('stage_info', 'В процессе')
                plant_data["growing_id"] = plant.get('growing_id')
            else:
                current_state = plant.get('current_state', 'healthy')
                plant_data["emoji"] = STATE_EMOJI.get(current_state, '🌱')
                plant_data["current_state"] = current_state
                plant_data["water_status"] = format_days_ago(plant.get('last_watered'))
                plant_data["last_watered"] = plant.get('last_watered')
                plant_data["watering_interval"] = plant.get('watering_interval', 7)
                plant_data["next_watering_date"] = plant.get('next_watering_date')
                plant_data["needs_watering"] = plant.get('needs_watering', False)
                plant_data["current_streak"] = plant.get('current_streak', 0)
                plant_data["max_streak"] = plant.get('max_streak', 0)
                plant_data["plant_name"] = plant.get('plant_name')
                plant_data["saved_date"] = plant.get('saved_date')

            formatted_plants.append(plant_data)

        return formatted_plants

    except Exception as e:
        logger.error(f"Ошибка получения списка растений: {e}", exc_info=True)
        return []


async def water_plant(user_id: int, plant_id: int) -> dict:
    """Полив растения с расчётом серии (Этап 3)"""
    try:
        db = await get_db()
        plant = await db.get_plant_by_id(plant_id, user_id)

        if not plant:
            return {"success": False, "error": "Растение не найдено"}

        result = await db.water_plant_with_streak(user_id, plant_id)

        if not result["success"]:
            return result

        await create_plant_reminder(plant_id, user_id, result["interval"])

        current_time = get_moscow_now().strftime("%d.%m.%Y в %H:%M")

        return {
            "success": True,
            "plant_name": plant['display_name'],
            "time": current_time,
            "next_watering_days": result["interval"],
            "next_watering_date": result["next_watering_date"],
            "current_streak": result["current_streak"],
            "max_streak": result["max_streak"],
        }

    except Exception as e:
        logger.error(f"Ошибка полива: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


async def water_all_plants(user_id: int) -> dict:
    """Полить все растения (legacy для бота)"""
    try:
        db = await get_db()
        await db.update_watering(user_id)
        return {"success": True}
    except Exception as e:
        logger.error(f"Ошибка массового полива: {e}")
        return {"success": False, "error": str(e)}


async def fertilize_plant_action(user_id: int, plant_id: int) -> dict:
    """Подкормка пока отключена. Заглушка для совместимости с роутерами/ботом."""
    return {"success": False, "error": "Подкормка временно отключена"}


async def delete_plant(user_id: int, plant_id: int) -> dict:
    try:
        db = await get_db()
        plant = await db.get_plant_by_id(plant_id, user_id)

        if not plant:
            return {"success": False, "error": "Растение не найдено"}

        plant_name = plant['display_name']
        await db.delete_plant(user_id, plant_id)
        return {"success": True, "plant_name": plant_name}

    except Exception as e:
        logger.error(f"Ошибка удаления: {e}")
        return {"success": False, "error": str(e)}


async def rename_plant(user_id: int, plant_id: int, new_name: str) -> dict:
    try:
        if len(new_name.strip()) < 2:
            return {"success": False, "error": "Слишком короткое название"}

        db = await get_db()
        await db.update_plant_name(plant_id, user_id, new_name.strip())
        return {"success": True, "new_name": new_name.strip()}

    except Exception as e:
        logger.error(f"Ошибка переименования: {e}")
        return {"success": False, "error": str(e)}


async def get_plant_details(plant_id: int, user_id: int) -> dict:
    """Получить детали растения"""
    try:
        db = await get_db()
        plant = await db.get_plant_with_state(plant_id, user_id)

        if not plant:
            return None

        plant_name = plant['display_name']
        current_state = plant.get('current_state', 'healthy')
        state_emoji = STATE_EMOJI.get(current_state, '🌱')
        state_name = STATE_NAMES.get(current_state, 'Здоровое')

        return {
            "plant_id": plant_id,
            "plant_name": plant_name,
            "current_state": current_state,
            "state_emoji": state_emoji,
            "state_name": state_name,
            "watering_interval": plant.get('watering_interval', 7),
            "state_changes_count": 0,
            "water_status": format_days_ago(plant.get('last_watered')),
        }

    except Exception as e:
        logger.error(f"Ошибка получения деталей: {e}")
        return None


async def get_plant_state_history(plant_id: int, limit: int = 10) -> list:
    """Заглушка для бота: после удаления plant_state_history возвращаем пустой список"""
    return []
