import logging
import base64
import re
from openai import AsyncOpenAI

from config import OPENAI_API_KEY, PLANT_IDENTIFICATION_PROMPT, LEGACY_STATE_MAPPING
from utils.image_utils import optimize_image_for_analysis
from utils.formatters import format_plant_analysis
from utils.season_utils import get_current_season

logger = logging.getLogger(__name__)

openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

GPT_5_1_MODEL = "gpt-5.1-2025-11-13"

# Допустимые статусы Этапа 3
ALLOWED_STATES = {'healthy', 'flowering', 'growing', 'needs_care', 'dormancy'}

# Фраза-отказ для off-topic вопросов в чате. Используется на роутере, чтобы
# не списывать лимит вопросов с пользователя если ИИ отказал отвечать.
OFFTOPIC_REFUSAL_PHRASE = "Я отвечаю только на вопросы о растениях"


def _normalize_state(state_text: str) -> str:
    """Нормализация статуса с учётом старых значений и алиасов"""
    if not state_text:
        return 'healthy'

    state_text = state_text.strip().lower()

    # Прямое совпадение
    if state_text in ALLOWED_STATES:
        return state_text

    # Маппинг старых статусов
    if state_text in LEGACY_STATE_MAPPING:
        return LEGACY_STATE_MAPPING[state_text]

    # Эвристика по русским ключевым словам
    if 'цвет' in state_text:
        return 'flowering'
    if 'актив' in state_text or 'рост' in state_text or 'growing' in state_text:
        return 'growing'
    if 'покой' in state_text or 'dormanc' in state_text:
        return 'dormancy'
    if 'стресс' in state_text or 'болезн' in state_text or 'проблем' in state_text or 'адаптац' in state_text:
        return 'needs_care'

    return 'healthy'


def extract_plant_state_from_analysis(raw_analysis: str) -> dict:
    """Извлечь информацию о состоянии и подкормке из анализа AI"""
    state_info = {
        'current_state': 'healthy',
        'state_reason': '',
        'recommendations': '',
        'fertilizing_enabled': False,
        'fertilizing_interval': None,
    }

    if not raw_analysis:
        return state_info

    lines = raw_analysis.split('\n')

    for line in lines:
        line = line.strip()

        if line.startswith("ТЕКУЩЕЕ_СОСТОЯНИЕ:"):
            state_text = line.replace("ТЕКУЩЕЕ_СОСТОЯНИЕ:", "").strip()
            state_info['current_state'] = _normalize_state(state_text)

        elif line.startswith("ПРИЧИНА_СОСТОЯНИЯ:"):
            state_info['state_reason'] = line.replace("ПРИЧИНА_СОСТОЯНИЯ:", "").strip()

        elif line.startswith("ПОДКОРМКА_НУЖНА:"):
            value = line.replace("ПОДКОРМКА_НУЖНА:", "").strip().lower()
            state_info['fertilizing_enabled'] = value.startswith('y') or 'да' in value or 'нужн' in value

        elif line.startswith("ПОДКОРМКА_ИНТЕРВАЛ:"):
            interval_text = line.replace("ПОДКОРМКА_ИНТЕРВАЛ:", "").strip()
            numbers = re.findall(r'\d+', interval_text)
            if numbers:
                try:
                    interval = int(numbers[0])
                    if 7 <= interval <= 90:
                        state_info['fertilizing_interval'] = interval
                except:
                    pass

    # Если подкормка нужна, но интервал не указан: ставим дефолт 21 день
    if state_info['fertilizing_enabled'] and not state_info['fertilizing_interval']:
        state_info['fertilizing_interval'] = 21

    # Если подкормка не нужна: обнуляем интервал
    if not state_info['fertilizing_enabled']:
        state_info['fertilizing_interval'] = None

    return state_info


def extract_species_meta(raw_analysis: str) -> dict:
    """
    Извлечь латинское название и описание вида из анализа AI.
    Возвращает: {'latin_name': str|None, 'species_description': str|None}
    """
    meta = {'latin_name': None, 'species_description': None}
    if not raw_analysis:
        return meta

    for line in raw_analysis.split('\n'):
        line = line.strip()

        if line.startswith("НАЗВАНИЕ_ЛАТ:"):
            value = line.replace("НАЗВАНИЕ_ЛАТ:", "").strip()
            value = value.strip(' "\'[]()')
            if value and value.lower() not in {'неизвестно', 'unknown', '-', 'нет', 'n/a'}:
                meta['latin_name'] = value

        elif line.startswith("ОПИСАНИЕ_ВИДА:"):
            value = line.replace("ОПИСАНИЕ_ВИДА:", "").strip()
            value = value.strip(' "\'')
            if value and value.lower() not in {'неизвестно', 'unknown', '-', 'нет', 'n/a'}:
                if len(value) > 320:
                    value = value[:317].rstrip() + '…'
                meta['species_description'] = value

    return meta


def extract_watering_info(analysis_text: str) -> dict:
    """Извлечь информацию о поливе"""
    watering_info = {
        "interval_days": 7,
        "personal_recommendations": "",
        "current_state": "",
        "needs_adjustment": False
    }

    if not analysis_text:
        return watering_info

    for line in analysis_text.split('\n'):
        line = line.strip()

        if line.startswith("ПОЛИВ_ИНТЕРВАЛ:"):
            interval_text = line.replace("ПОЛИВ_ИНТЕРВАЛ:", "").strip()
            numbers = re.findall(r'\d+', interval_text)
            if numbers:
                try:
                    interval = int(numbers[0])
                    if 2 <= interval <= 28:
                        watering_info["interval_days"] = interval
                except:
                    pass

        elif line.startswith("ПОЛИВ_АНАЛИЗ:"):
            current_state = line.replace("ПОЛИВ_АНАЛИЗ:", "").strip()
            watering_info["current_state"] = current_state

        elif line.startswith("ПОЛИВ_РЕКОМЕНДАЦИИ:"):
            watering_info["personal_recommendations"] = line.replace("ПОЛИВ_РЕКОМЕНДАЦИИ:", "").strip()

    return watering_info


def extract_and_remove_watering_interval(text: str, season_info: dict) -> tuple:
    """Извлечь интервал полива из текста и удалить эту строку."""
    default_interval = 10
    if season_info.get('season') == 'summer':
        default_interval = 7
    elif season_info.get('season') == 'winter':
        default_interval = 12

    interval = default_interval
    clean_text = text

    pattern = r'\n?ПОЛИВ_ИНТЕРВАЛ:\s*(\d+)\s*'
    match = re.search(pattern, text)

    if match:
        try:
            interval = int(match.group(1))
            interval = max(3, min(28, interval))
            logger.info(f"💧 Извлечён интервал полива: {interval} дней")
        except:
            interval = default_interval

        clean_text = re.sub(pattern, '', text).strip()
    else:
        logger.warning(f"⚠️ ПОЛИВ_ИНТЕРВАЛ не найден, default: {default_interval}")

    return interval, clean_text


def _extract_plant_name_rus(raw_analysis: str) -> str:
    """
    Извлечь русское название растения.
    Сначала ищем НАЗВАНИЕ_РУС: (новый формат), потом fallback на РАСТЕНИЕ:.
    """
    plant_name = "Неизвестное растение"

    # Сначала ищем новый формат
    for line in raw_analysis.split('\n'):
        line = line.strip()
        if line.startswith("НАЗВАНИЕ_РУС:"):
            value = line.replace("НАЗВАНИЕ_РУС:", "").strip().strip(' "\'[]()')
            if value and value.lower() not in {'неизвестно', 'unknown', '-', 'нет', 'n/a'}:
                return value

    # Fallback: старый формат "РАСТЕНИЕ:"
    for line in raw_analysis.split('\n'):
        line = line.strip()
        if line.startswith("РАСТЕНИЕ:"):
            raw_name = line.replace("РАСТЕНИЕ:", "").strip()
            if "неизвестное растение" in raw_name.lower() and "(" in raw_name:
                match = re.search(r'\((?:возможно,?\s*)?([^)]+)\)', raw_name, re.IGNORECASE)
                plant_name = match.group(1).strip() if match else raw_name
            else:
                # Из "Фикус каучуконосный (Ficus elastica)" оставим "Фикус каучуконосный"
                plant_name = re.split(r'[(\[]', raw_name)[0].strip()
            break

    return plant_name


def format_recommendations_text(raw_analysis: str) -> str:
    """
    Собрать читаемый markdown с рекомендациями для пользователя.
    Извлекает СОСТОЯНИЕ, ПОЛИВ_РЕКОМЕНДАЦИИ, СВЕТ, ТЕМПЕРАТУРА, ВЛАЖНОСТЬ, СОВЕТ
    из raw_analysis и собирает короткий markdown.
    """
    if not raw_analysis:
        return ""

    # Порядок важен — он сохраняется в выводе
    fields = [
        ("СОСТОЯНИЕ", "Состояние"),
        ("ПОЛИВ_РЕКОМЕНДАЦИИ", "Полив"),
        ("СВЕТ", "Свет"),
        ("ТЕМПЕРАТУРА", "Температура"),
        ("ВЛАЖНОСТЬ", "Влажность"),
        ("СОВЕТ", "Совет"),
    ]

    placeholders = {'-', 'нет', 'неизвестно', 'unknown', 'n/a', '—'}

    parts = []
    for key, label in fields:
        value = None
        for line in raw_analysis.split("\n"):
            stripped = line.strip()
            if stripped.startswith(f"{key}:"):
                value = stripped[len(key) + 1:].strip()
                break
        if value and value.lower() not in placeholders:
            parts.append(f"**{label}.** {value}")

    return "\n\n".join(parts)


def _extract_not_a_plant_subject(raw_analysis: str) -> str | None:
    """
    Если ответ модели начинается с НЕ_РАСТЕНИЕ: — вернуть описание того,
    что на фото (например, "кот"). Иначе None.
    Проверяем только начало ответа: модель должна вернуть РОВНО эту строку.
    """
    if not raw_analysis:
        return None
    text = raw_analysis.strip()
    # Маркер должен быть в первой строке (модель проинструктирована
    # вернуть РОВНО эту строку и больше ничего).
    first_line = text.split('\n', 1)[0].strip()
    if first_line.upper().startswith("НЕ_РАСТЕНИЕ:"):
        subject = first_line.split(':', 1)[1].strip()
        subject = subject.strip(' "\'.,!?()[]')
        return subject or "не растение"
    return None


async def analyze_with_openai_advanced(image_data: bytes, user_question: str = None,
                                        previous_state: str = None) -> dict:
    """Анализ через OpenAI с единым промптом."""
    if not openai_client:
        return {"success": False, "error": "❌ OpenAI API недоступен"}

    try:
        optimized_image = await optimize_image_for_analysis(image_data)
        base64_image = base64.b64encode(optimized_image).decode('utf-8')

        season_data = get_current_season()

        feeding_recommendations = {
            'spring': 'Каждые 14 дней',
            'summer': 'Каждые 14 дней',
            'autumn': 'Каждые 21 день',
            'winter': 'Не подкармливать или раз в 30-45 дней'
        }

        water_adjustment_days = 0
        if season_data['season'] == 'winter':
            water_adjustment_days = 5
        elif season_data['season'] == 'summer':
            water_adjustment_days = -2
        elif season_data['season'] == 'autumn':
            water_adjustment_days = 2

        prompt = PLANT_IDENTIFICATION_PROMPT.format(
            season_name=season_data['season_ru'],
            season_description=season_data['growth_phase'],
            season_water_note=season_data['watering_adjustment'],
            season_light_note=season_data['light_hours'],
            season_temperature_note=season_data['temperature_note'],
            season_feeding_note=feeding_recommendations.get(season_data['season'], 'Стандартный режим'),
            season_water_adjustment=f"{water_adjustment_days:+d} дня к базовому интервалу"
        )

        if previous_state:
            prompt += (
                f"\n\nКОНТЕКСТ ДЛЯ СРАВНЕНИЯ: при предыдущем анализе состояние растения было «{previous_state}». "
                f"Сравни текущее фото с этим состоянием и отметь изменения в поле ПРИЧИНА_СОСТОЯНИЯ. "
                f"ВАЖНО: это лишь дополнительный контекст. Всё равно заполни ВЕСЬ шаблон ответа полностью "
                f"(НАЗВАНИЕ_РУС, НАЗВАНИЕ_ЛАТ, ТЕКУЩЕЕ_СОСТОЯНИЕ, УВЕРЕННОСТЬ, ПОЛИВ_ИНТЕРВАЛ и все остальные поля), "
                f"как если бы анализировал растение впервые. "
                f"Но если на фото НЕ растение — всё равно действует правило вернуть только строку 'НЕ_РАСТЕНИЕ: ...'."
            )
        if user_question:
            prompt += (
                f"\n\nДополнительный вопрос пользователя: {user_question}. "
                f"Ответь на него в поле СОВЕТ, но ВЕСЬ остальной шаблон всё равно заполни полностью."
            )

        response = await openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "Вы - профессиональный ботаник-диагност с 30-летним опытом. Используйте только 5 статусов: healthy, flowering, growing, needs_care, dormancy."},
                {"role": "user", "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {
                        "url": f"data:image/jpeg;base64,{base64_image}",
                        "detail": "high"
                    }}
                ]}
            ],
            max_tokens=1500,
            temperature=0.2
        )

        raw_analysis = response.choices[0].message.content

        # Сначала проверяем явный отказ "это не растение"
        not_a_plant_subject = _extract_not_a_plant_subject(raw_analysis)
        if not_a_plant_subject is not None:
            logger.info(f"🚫 На фото не растение: {not_a_plant_subject!r}")
            return {
                "success": False,
                "error_code": "not_a_plant",
                "subject": not_a_plant_subject,
                "error": (
                    f"На фото не растение, а {not_a_plant_subject}. "
                    f"Загрузите, пожалуйста, фото вашего растения."
                ),
            }

        if not raw_analysis or len(raw_analysis) < 100:
            logger.warning(
                f"⚠️ Короткий/пустой ответ OpenAI (len={len(raw_analysis or '')}, "
                f"previous_state={previous_state!r}): {(raw_analysis or '')[:500]!r}"
            )
            raise Exception("Некачественный ответ")

        confidence = 0
        for line in raw_analysis.split('\n'):
            if line.startswith("УВЕРЕННОСТЬ:"):
                try:
                    confidence = float(line.replace("УВЕРЕННОСТЬ:", "").strip().replace("%", ""))
                except:
                    confidence = 70
                break

        plant_name = _extract_plant_name_rus(raw_analysis)

        state_info = extract_plant_state_from_analysis(raw_analysis)
        watering_info = extract_watering_info(raw_analysis)
        species_meta = extract_species_meta(raw_analysis)
        recommendations = format_recommendations_text(raw_analysis)

        formatted_analysis = format_plant_analysis(raw_analysis, confidence, state_info)

        logger.info(
            f"✅ Анализ: {plant_name} [{species_meta.get('latin_name')}], "
            f"состояние={state_info['current_state']}, "
            f"подкормка={state_info['fertilizing_enabled']} ({state_info['fertilizing_interval']}д)"
        )

        return {
            "success": True,
            "analysis": formatted_analysis,
            "raw_analysis": raw_analysis,
            "plant_name": plant_name,
            "latin_name": species_meta.get('latin_name'),
            "species_description": species_meta.get('species_description'),
            "confidence": confidence,
            "source": "openai_advanced",
            "state_info": state_info,
            "watering_interval": watering_info["interval_days"],
            "recommendations": recommendations,
            "season_data": season_data
        }

    except Exception as e:
        logger.error(f"❌ OpenAI error: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


async def analyze_plant_image(image_data: bytes, user_question: str = None,
                              previous_state: str = None, retry_count: int = 0,
                              plant_context: str = None) -> dict:
    """
    Главная функция анализа.
    Этап 3: используем единый промпт через analyze_with_openai_advanced,
    он корректно парсит состояние, интервал полива и подкормку.
    """
    logger.info("🔍 Запуск анализа растения")

    result = await analyze_with_openai_advanced(image_data, user_question, previous_state)

    # Прокидываем "не растение" как есть, чтобы роутер мог корректно обработать
    if not result.get("success") and result.get("error_code") == "not_a_plant":
        return result

    if result["success"]:
        if 'watering_interval' not in result:
            watering_info = extract_watering_info(result.get('raw_analysis', ''))
            result['watering_interval'] = watering_info['interval_days']

        # Гарантируем наличие recommendations
        if 'recommendations' not in result or not result['recommendations']:
            result['recommendations'] = format_recommendations_text(result.get('raw_analysis', ''))

        result['needs_retry'] = result.get('confidence', 50) < 50
        return result

    return {"success": False, "error": result.get("error", "Анализ не удался")}


async def answer_plant_question(question: str, plant_context: str = None) -> dict:
    """Ответить на вопрос о растении с контекстом"""
    if not openai_client:
        return {"error": "❌ OpenAI API недоступен"}

    try:
        season_info = get_current_season()

        seasonal_context = f"""
ТЕКУЩИЙ СЕЗОН: {season_info['season_ru']} ({season_info['month_name_ru']})
ФАЗА РОСТА: {season_info['growth_phase']}
КОРРЕКТИРОВКА ПОЛИВА: {season_info['watering_adjustment']}
"""

        # ЖЁСТКОЕ ОГРАНИЧЕНИЕ ТЕМАТИКИ.
        # Любой вопрос не про растения должен получать ровно одну фразу-отказ.
        system_prompt = f"""Вы - опытный ботаник-консультант приложения Bloom AI. Помогаете пользователям с уходом за комнатными и садовыми растениями.

ОГРАНИЧЕНИЕ ТЕМАТИКИ - КРИТИЧЕСКИ ВАЖНО:
Вы отвечаете ТОЛЬКО на вопросы о растениях, ботанике, садоводстве, уходе за комнатными и садовыми растениями, цветах, овощах, фруктах, грунтах, удобрениях, болезнях растений и вредителях.

Если вопрос НЕ относится к растениям (например: программирование, история, политика, отношения, спорт, рецепты блюд без упоминания растений, общие темы), отвечайте РОВНО одной фразой и больше НИЧЕГО:
"{OFFTOPIC_REFUSAL_PHRASE}. Что вас интересует про ваши зелёные питомцы?"

Не пытайтесь шутить, не извиняйтесь, не объясняйте почему отказываете. Только эта одна фраза.

Если вопрос двусмысленный (например про "уход" - но непонятно за чем), уточните что речь про растения - и попробуйте ответить как если бы это было о растении.

ПРАВИЛА ОТВЕТОВ ПО ТЕМЕ РАСТЕНИЙ:
- Отвечайте именно на тот вопрос, который задан
- Простой вопрос: короткий ответ (3-5 предложений)
- Сложный вопрос: можно структурировать
- Конкретные цифры где уместно
- Учитывайте текущий сезон
- Для выделения используйте ТОЛЬКО markdown: **жирный текст** и *курсив*
- НЕ используйте HTML-теги (<b>, <i>, <strong>, <em>, <p>, <br> и подобные)
- НЕ начинайте с шаблонных заголовков"""

        if plant_context:
            user_prompt = f"ИСТОРИЯ РАСТЕНИЯ:\n{plant_context}\n\n{seasonal_context}\n\nВОПРОС:\n{question}"
        else:
            user_prompt = f"{seasonal_context}\n\nВОПРОС:\n{question}"

        models_to_try = [GPT_5_1_MODEL, "gpt-4o"]

        for model_name in models_to_try:
            try:
                logger.info(f"🔄 Модель: {model_name}")

                api_params = {
                    "model": model_name,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ]
                }

                if model_name == GPT_5_1_MODEL:
                    api_params["max_completion_tokens"] = 4000
                    api_params["extra_body"] = {"reasoning_effort": "low"}
                else:
                    api_params["max_tokens"] = 1000
                    api_params["temperature"] = 0.4

                response = await openai_client.chat.completions.create(**api_params)
                answer = response.choices[0].message.content

                if answer and len(answer) > 10:
                    logger.info(f"✅ Ответ от {model_name}")
                    return {"answer": answer, "model": model_name}

            except Exception as model_error:
                logger.warning(f"⚠️ {model_name}: {model_error}")
                if model_name == models_to_try[-1]:
                    raise
                continue

        raise Exception("Все модели вернули пустой ответ")

    except Exception as e:
        logger.error(f"❌ Ошибка ответа: {e}", exc_info=True)
        return {"error": "❌ Не могу дать ответ. Попробуйте переформулировать вопрос."}


def is_offtopic_refusal(answer_text: str) -> bool:
    """Проверка: содержит ли ответ ИИ отказную фразу для off-topic вопроса."""
    if not answer_text:
        return False
    return OFFTOPIC_REFUSAL_PHRASE.lower() in answer_text.lower()


async def generate_growing_plan(plant_name: str) -> tuple:
    """Генерация плана выращивания (без изменений Этапа 3)"""
    if not openai_client:
        return None, None

    try:
        season_info = get_current_season()

        prompt = f"""
Составьте профессиональный план выращивания для: {plant_name}
ТЕКУЩИЙ СЕЗОН: {season_info['season_ru']}

Формат:
🌱 ЭТАП 1: Название (X дней)
• Задача
🌿 ЭТАП 2: Название (X дней)
🌸 ЭТАП 3: Название (X дней)
🌳 ЭТАП 4: Название (X дней)
"""

        response = await openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": f"Вы - агроном. Сейчас {season_info['season_ru']}."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=1200,
            temperature=0.2
        )

        plan_text = response.choices[0].message.content

        task_calendar = {
            "stage_1": {"name": "Подготовка", "duration_days": 7, "tasks": []},
            "stage_2": {"name": "Прорастание", "duration_days": 14, "tasks": []},
            "stage_3": {"name": "Активный рост", "duration_days": 30, "tasks": []},
            "stage_4": {"name": "Взрослое растение", "duration_days": 30, "tasks": []}
        }

        return plan_text, task_calendar

    except Exception as e:
        logger.error(f"Ошибка генерации плана: {e}")
        return None, None
