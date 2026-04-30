"""
Этап 9: Система достижений Bloom AI
Справочник 20 достижений + логика проверки и разблокировки
"""
from datetime import datetime
from config import MOSCOW_TZ, logger

# Таргеты водных ачивок — используются и для разблокировки,
# и для прогресса карточки "Серия полива" в аналитике.
WATER_ACHIEVEMENT_TARGETS = [1, 10, 30, 60, 100]

# =============================================
# Справочник достижений (4 категории × 5)
# =============================================

ACHIEVEMENTS = [
    # === Поливы (water) ===
    {'code': 'water_1', 'title': 'Первая капля', 'category': 'water', 'target': 1, 'icon': 'water_1', 'order': 0,
     'description_locked': 'Полейте своё первое растение.',
     'description_unlocked': 'Поздравляем! Ваше первое растение получило необходимую влагу. Это важный шаг к здоровому саду.'},
    {'code': 'water_10', 'title': 'Лейка наготове', 'category': 'water', 'target': 10, 'icon': 'water_2', 'order': 1,
     'description_locked': 'Полейте растения 10 раз.',
     'description_unlocked': '10 поливов — это уже привычка! Ваши растения чувствуют вашу заботу и отвечают вам свежими листьями. Вы доказали, что готовы заботиться о своём домашнем саду регулярно. Так держать!'},
    {'code': 'water_30', 'title': 'Водная стихия', 'category': 'water', 'target': 30, 'icon': 'water_3', 'order': 2,
     'description_locked': 'Полейте растения 30 раз.',
     'description_unlocked': '30 поливов позади — вы вошли в настоящий ритм! Ваш домашний сад процветает благодаря вашему вниманию. Немногие доходят до этой отметки — вы явно знаете, что растениям нужна не только вода, но и постоянство.'},
    {'code': 'water_60', 'title': 'Повелитель влаги', 'category': 'water', 'target': 60, 'icon': 'water_4', 'order': 3,
     'description_locked': 'Полейте растения 60 раз.',
     'description_unlocked': '60 поливов — это впечатляет! Ваша забота стала частью повседневности, а растения наверняка выглядят лучше, чем когда-либо. Вы — пример для каждого, кто мечтает о зелёном доме, но боится не справиться.'},
    {'code': 'water_100', 'title': 'Сотня поливов', 'category': 'water', 'target': 100, 'icon': 'water_5', 'order': 4,
     'description_locked': 'Полейте растения 100 раз.',
     'description_unlocked': '100 поливов — невероятный результат! Вы прошли путь от первой капли до настоящего мастера полива. Ваш сад — отражение вашей дисциплины и любви к растениям. Мы гордимся тем, что Bloom стал частью этого пути!'},

    # === Растения (plants) ===
    {'code': 'plants_1', 'title': 'Первый росток', 'category': 'plants', 'target': 1, 'icon': 'plants_1', 'order': 5,
     'description_locked': 'Добавьте своё первое растение.',
     'description_unlocked': 'Поздравляем! Вы добавили своё первое растение в Bloom. Это начало вашего зелёного путешествия — впереди много открытий, заботы и радости от наблюдения за ростом. Ваш домашний сад официально основан!'},
    {'code': 'plants_3', 'title': 'Зелёная компания', 'category': 'plants', 'target': 3, 'icon': 'plants_2', 'order': 6,
     'description_locked': 'Добавьте 3 растения.',
     'description_unlocked': '3 растения в вашей коллекции! Вы уже не просто владелец одного цветка — вы создаёте настоящий мини-сад. Каждое растение уникально, и забота о них подарит вам массу приятных моментов. Продолжайте в том же духе!'},
    {'code': 'plants_5', 'title': 'Мини-оранжерея', 'category': 'plants', 'target': 5, 'icon': 'plants_3', 'order': 7,
     'description_locked': 'Добавьте 5 растений.',
     'description_unlocked': '5 растений — ваш дом постепенно превращается в уютный зелёный уголок. Вы научились уделять внимание каждому питомцу и понимать их потребности. Ваши гости наверняка уже обращают внимание на то, как живо и свежо стало вокруг!'},
    {'code': 'plants_10', 'title': 'Домашние джунгли', 'category': 'plants', 'target': 10, 'icon': 'plants_4', 'order': 8,
     'description_locked': 'Добавьте 10 растений.',
     'description_unlocked': '10 растений — это уже серьёзная коллекция! Ваш дом можно смело называть личным ботаническим садом. Заботиться о таком количестве растений — настоящее искусство, и вы им явно овладели. Bloom гордится быть вашим помощником!'},
    {'code': 'plants_20', 'title': 'Ботанический сад', 'category': 'plants', 'target': 20, 'icon': 'plants_5', 'order': 9,
     'description_locked': 'Добавьте 20 растений.',
     'description_unlocked': '20 растений — вы настоящий коллекционер и эксперт! Ваш дом можно смело называть личным ботаническим садом. За этим достижением стоит огромная любовь к растениям, терпение и внимание к деталям. Вы вдохновляете других начать свой зелёный путь!'},

    # === Фото (photos) ===
    {'code': 'photos_1', 'title': 'Первый кадр', 'category': 'photos', 'target': 1, 'icon': 'photos_1', 'order': 10,
     'description_locked': 'Добавьте первое фото растения.',
     'description_unlocked': 'Поздравляем! Вы загрузили первое фото растения в Bloom. Теперь ваш зелёный питомец сохранён не только в сердце, но и в приложении. Фотографии помогут отслеживать рост и замечать малейшие изменения — впереди целая визуальная история вашего сада!'},
    {'code': 'photos_5', 'title': 'Фотоальбом новичка', 'category': 'photos', 'target': 5, 'icon': 'photos_2', 'order': 11,
     'description_locked': 'Добавьте 5 фото растений.',
     'description_unlocked': '5 фотографий в вашей коллекции! Вы начали собирать настоящий архив своего домашнего сада. С каждым снимком вы лучше видите, как меняются ваши растения, и замечаете детали, которые раньше ускользали от внимания. Отличное начало!'},
    {'code': 'photos_15', 'title': 'Зелёный фотограф', 'category': 'photos', 'target': 15, 'icon': 'photos_3', 'order': 12,
     'description_locked': 'Добавьте 15 фото растений.',
     'description_unlocked': '15 фото — вы всерьёз увлеклись документированием своего сада! Каждый кадр — это маленькая история о росте, заботе и внимании. Ваша коллекция снимков уже рассказывает о том, какой путь прошли ваши растения вместе с вами.'},
    {'code': 'photos_25', 'title': 'Мастер кадра', 'category': 'photos', 'target': 25, 'icon': 'photos_4', 'order': 13,
     'description_locked': 'Добавьте 25 фото растений.',
     'description_unlocked': '25 фотографий — впечатляющая галерея! Вы не просто ухаживаете за растениями, но и умеете запечатлеть их красоту. Такой архив — ценный инструмент: по нему легко отследить развитие каждого питомца и вспомнить, с чего всё начиналось.'},
    {'code': 'photos_40', 'title': 'Ботанический хроникёр', 'category': 'photos', 'target': 40, 'icon': 'photos_5', 'order': 14,
     'description_locked': 'Добавьте 40 фото растений.',
     'description_unlocked': '40 фото в вашей коллекции — это настоящая летопись вашего сада! Вы создали уникальный визуальный дневник, который отражает любовь, терпение и внимание к каждому растению. Ваши снимки вдохновляют и показывают, каким живым и красивым может быть дом, наполненный зеленью!'},

    # === Дни в приложении (days) ===
    {'code': 'days_7', 'title': 'Первая неделя вместе', 'category': 'days', 'target': 7, 'icon': 'days_1', 'order': 15,
     'description_locked': 'Проведите 7 дней с Bloom.',
     'description_unlocked': 'Поздравляем! Вы с Bloom уже целую неделю. За это короткое время вы успели сделать первые шаги в заботе о своих растениях и почувствовать, как приятно видеть их отклик. Это только начало большого зелёного путешествия — впереди ещё много открытий!'},
    {'code': 'days_30', 'title': 'Месяц в Bloom', 'category': 'days', 'target': 30, 'icon': 'days_2', 'order': 16,
     'description_locked': 'Проведите 30 дней с Bloom.',
     'description_unlocked': '30 дней вместе — это уже настоящая привычка! За этот месяц вы научились замечать потребности своих растений, выстроили ритм заботы и наверняка увидели первые результаты. Ваш домашний сад стал частью повседневной жизни, и это здорово!'},
    {'code': 'days_60', 'title': 'Два месяца заботы', 'category': 'days', 'target': 60, 'icon': 'days_3', 'order': 17,
     'description_locked': 'Проведите 60 дней с Bloom.',
     'description_unlocked': '60 дней с Bloom — вы уверенно двигаетесь вперёд! За это время вы прошли путь от новичка до заботливого хозяина, который понимает своих зелёных питомцев. Ваши растения чувствуют стабильность и отвечают вам здоровым видом и новыми листьями.'},
    {'code': 'days_90', 'title': 'Сезон вместе', 'category': 'days', 'target': 90, 'icon': 'days_4', 'order': 18,
     'description_locked': 'Проведите 90 дней с Bloom.',
     'description_unlocked': '90 дней — целый сезон рядом с Bloom! Вы пережили вместе со своими растениями смену погоды, освещения и настроений. Такой опыт делает вас настоящим знатоком своего сада. Мы рады быть частью вашей истории заботы о зелёных друзьях!'},
    {'code': 'days_365', 'title': 'Год в Bloom', 'category': 'days', 'target': 365, 'icon': 'days_5', 'order': 19,
     'description_locked': 'Проведите 365 дней с Bloom.',
     'description_unlocked': '365 дней вместе — это невероятно! Целый год заботы, внимания и любви к своим растениям. Вы прошли через все сезоны, научились справляться с любыми задачами и создали вокруг себя живую, дышащую зелёную атмосферу. Спасибо, что выбрали Bloom спутником в этом пути — впереди ещё много прекрасных моментов!'},
]

ACHIEVEMENTS_MAP = {a['code']: a for a in ACHIEVEMENTS}

ACHIEVEMENTS_BY_CATEGORY = {}
for a in ACHIEVEMENTS:
    ACHIEVEMENTS_BY_CATEGORY.setdefault(a['category'], []).append(a)


# =============================================
# Получение текущих значений прогресса
# =============================================

async def _get_user_stats(user_id: int) -> dict | None:
    """Получить все счётчики пользователя для расчёта прогресса достижений."""
    from database import get_db
    import pytz

    db = await get_db()
    async with db.pool.acquire() as conn:
        user_row = await conn.fetchrow("""
            SELECT total_photos, created_at
            FROM users WHERE user_id = $1
        """, user_id)

        if not user_row:
            return None

        plant_count = await conn.fetchval(
            "SELECT COUNT(*) FROM plants WHERE user_id = $1 "
            "AND (plant_type = 'regular' OR plant_type IS NULL)",
            user_id
        )

        # === % здоровых: healthy + flowering + growing, без срочных поливов ===
        # Колонки needs_watering нет в plants — определяем по next_watering_date.
        # Не пора поливать = next_watering_date >= сегодня (или вообще NULL).
        healthy_count = await conn.fetchval("""
            SELECT COUNT(*) FROM plants
            WHERE user_id = $1
              AND (plant_type = 'regular' OR plant_type IS NULL)
              AND current_state IN ('healthy', 'flowering', 'growing')
              AND (next_watering_date IS NULL OR next_watering_date >= CURRENT_DATE)
        """, user_id)

        # === Уникальные пары (plant, day) — наша новая "Серия полива" ===
        unique_waterings = await conn.fetchval(
            "SELECT COUNT(*) FROM daily_watering_log WHERE user_id = $1",
            user_id
        )

    now_moscow = datetime.now(MOSCOW_TZ)
    created_at = user_row['created_at']
    if created_at and created_at.tzinfo is None:
        created_at = pytz.utc.localize(created_at)
    days_in_app = (now_moscow - created_at).days if created_at else 0

    return {
        'unique_waterings': unique_waterings or 0,
        'total_photos': user_row['total_photos'] or 0,
        'total_plants': plant_count or 0,
        'healthy_count': healthy_count or 0,
        'days_in_app': days_in_app,
    }


def _current_value_for(achievement: dict, stats: dict) -> int:
    cat = achievement['category']
    if cat == 'water':
        return stats['unique_waterings']
    elif cat == 'plants':
        return stats['total_plants']
    elif cat == 'photos':
        return stats['total_photos']
    elif cat == 'days':
        return stats['days_in_app']
    return 0


# =============================================
# Проверка и разблокировка
# =============================================

async def check_and_unlock(user_id: int, category: str = None) -> list:
    """Проверить и разблокировать достижения. Возвращает список новых."""
    from database import get_db

    stats = await _get_user_stats(user_id)
    if not stats:
        return []

    categories = [category] if category else ['water', 'plants', 'photos', 'days']

    db = await get_db()
    async with db.pool.acquire() as conn:
        existing = await conn.fetch(
            "SELECT achievement_code FROM user_achievements WHERE user_id = $1", user_id
        )
    existing_codes = {row['achievement_code'] for row in existing}

    newly_unlocked = []
    for cat in categories:
        for ach in ACHIEVEMENTS_BY_CATEGORY.get(cat, []):
            if ach['code'] in existing_codes:
                continue
            if _current_value_for(ach, stats) >= ach['target']:
                async with db.pool.acquire() as conn:
                    await conn.execute("""
                        INSERT INTO user_achievements (user_id, achievement_code, unlocked_at)
                        VALUES ($1, $2, NOW()) ON CONFLICT DO NOTHING
                    """, user_id, ach['code'])
                newly_unlocked.append(ach)
                logger.info(f"🏆 Achievement unlocked: {ach['code']} for user {user_id}")

    return newly_unlocked


# =============================================
# Учёт полива: дедуп уникальных пар (plant, day)
# =============================================

async def update_global_watering_streak(user_id: int, plant_id: int) -> bool:
    """
    Вызывать при каждом поливе одного растения.

    INSERT в daily_watering_log с ON CONFLICT DO NOTHING.
    Если этот plant_id уже учтён сегодня — ничего не делает.
    Поле users.total_waterings НЕ трогаем (там старая логика осталась).

    Возвращает True, если запись новая.
    """
    from database import get_db

    today = datetime.now(MOSCOW_TZ).date()
    db = await get_db()

    async with db.pool.acquire() as conn:
        inserted = await conn.fetchval("""
            INSERT INTO daily_watering_log (user_id, plant_id, watered_date)
            VALUES ($1, $2, $3)
            ON CONFLICT DO NOTHING
            RETURNING 1
        """, user_id, plant_id, today)
        return inserted is not None


async def update_global_watering_streak_bulk(user_id: int, plant_ids: list) -> int:
    """
    Версия для water-all. Атомарно вставляет все (user, plant, today)
    с ON CONFLICT DO NOTHING.

    Возвращает количество добавленных уникальных пар.
    """
    from database import get_db

    if not plant_ids:
        return 0

    today = datetime.now(MOSCOW_TZ).date()
    db = await get_db()

    async with db.pool.acquire() as conn:
        rows = await conn.fetch("""
            INSERT INTO daily_watering_log (user_id, plant_id, watered_date)
            SELECT $1, plant_id, $2
            FROM unnest($3::int[]) AS t(plant_id)
            ON CONFLICT DO NOTHING
            RETURNING 1
        """, user_id, today, plant_ids)
        return len(rows)


# =============================================
# Инкремент фото
# =============================================

async def increment_photo_count(user_id: int):
    """Вызывать при добавлении фото растения."""
    from database import get_db
    db = await get_db()
    async with db.pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET total_photos = total_photos + 1 WHERE user_id = $1", user_id
        )


# =============================================
# Полная аналитика для эндпоинта
# =============================================

async def get_analytics_data(user_id: int) -> dict | None:
    """Собрать все данные для экрана аналитики."""
    from database import get_db

    stats = await _get_user_stats(user_id)
    if not stats:
        return None

    # Проверяем days-достижения при каждом запросе
    await check_and_unlock(user_id, category='days')

    db = await get_db()
    async with db.pool.acquire() as conn:
        unlocked_rows = await conn.fetch(
            "SELECT achievement_code, unlocked_at FROM user_achievements WHERE user_id = $1", user_id
        )
    unlocked_map = {r['achievement_code']: r['unlocked_at'] for r in unlocked_rows}

    achievements = []
    for a in ACHIEVEMENTS:
        current = _current_value_for(a, stats)
        unlocked_at = unlocked_map.get(a['code'])
        is_unlocked = unlocked_at is not None
        achievements.append({
            'code': a['code'],
            'title': a['title'],
            'category': a['category'],
            'target': a['target'],
            'current_value': min(current, a['target']),
            'icon': a['icon'],
            'order': a['order'],
            'is_unlocked': is_unlocked,
            'unlocked_at': unlocked_at.isoformat() if unlocked_at else None,
            'description': a['description_unlocked'] if is_unlocked else a['description_locked'],
        })

    total = stats['total_plants']
    healthy_pct = round((stats['healthy_count'] / total * 100) if total > 0 else 0)

    # === "Серия полива" = unique_waterings (уникальные plant×day) ===
    # Target = ближайшая невыполненная water-ачивка.
    waterings = stats['unique_waterings']
    streak_target = WATER_ACHIEVEMENT_TARGETS[-1]
    for t in WATER_ACHIEVEMENT_TARGETS:
        if waterings < t:
            streak_target = t
            break

    streak_percent = (
        round(min(waterings / streak_target * 100, 100))
        if streak_target > 0 else 0
    )

    return {
        'total_plants': total,
        'healthy_percent': healthy_pct,
        'watering_streak': {
            'current': waterings,
            'max': waterings,  # сохраняем поле для совместимости фронта
            'target': streak_target,
            'percent': streak_percent,
        },
        'achievements': achievements,
        'unlocked_count': len(unlocked_map),
        'total_achievements': len(ACHIEVEMENTS),
    }


# =============================================
# Ближайшее достижение для Home Screen
# =============================================

async def get_next_achievement(user_id: int) -> dict | None:
    """Найти ближайшее к разблокировке достижение."""
    from database import get_db

    stats = await _get_user_stats(user_id)
    if not stats:
        return None

    db = await get_db()
    async with db.pool.acquire() as conn:
        unlocked_rows = await conn.fetch(
            "SELECT achievement_code FROM user_achievements WHERE user_id = $1", user_id
        )
    unlocked_codes = {r['achievement_code'] for r in unlocked_rows}

    best = None
    best_pct = -1

    for a in ACHIEVEMENTS:
        if a['code'] in unlocked_codes:
            continue
        current = _current_value_for(a, stats)
        pct = current / a['target'] if a['target'] > 0 else 0
        if pct >= 1.0:
            continue
        if pct > best_pct:
            best_pct = pct
            best = {
                'code': a['code'],
                'title': a['title'],
                'category': a['category'],
                'target': a['target'],
                'current_value': current,
                'remaining': a['target'] - current,
                'percent': round(pct * 100),
                'icon': a['icon'],
            }

    return best
