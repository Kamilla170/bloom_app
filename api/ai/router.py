"""
Эндпоинты для ИИ-вопросов о растениях
"""

import logging
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

from api.auth.dependencies import get_current_user
from api.schemas import QuestionRequest, QuestionResponse
from services.ai_service import answer_plant_question
from services.subscription_service import check_limit, increment_usage
from plant_memory import get_plant_context, save_interaction
from database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ai", tags=["ai"])


# ---------- Схемы для истории чата ----------

class ChatMessageOut(BaseModel):
    id: int
    question: str
    answer: str
    created_at: str  # ISO datetime
    plant_id: Optional[int] = None
    plant_name: Optional[str] = None


class ChatHistoryResponse(BaseModel):
    messages: List[ChatMessageOut]
    total: int


def _qa_row_to_message(qa: dict, plant_name: Optional[str] = None) -> Optional[ChatMessageOut]:
    """Преобразование строки plant_qa_history в ChatMessageOut"""
    try:
        created = qa.get("question_date") or qa.get("created_at")
        return ChatMessageOut(
            id=qa.get("id", 0),
            question=qa.get("question_text", ""),
            answer=qa.get("answer_text", ""),
            created_at=created.isoformat() if created else "",
            plant_id=qa.get("plant_id"),
            plant_name=plant_name,
        )
    except Exception as e:
        logger.error(f"Ошибка форматирования сообщения: {e}")
        return None


# ---------- Получить общую историю чата (без растения) ----------

@router.get("/chat", response_model=ChatHistoryResponse)
async def get_general_chat_history(
    limit: int = Query(50, ge=1, le=200),
    user_id: int = Depends(get_current_user),
):
    """Получить общую историю чата пользователя (без привязки к растению)"""
    db = await get_db()
    qa_list = await db.get_user_chat_history(user_id, limit=limit)

    messages = []
    for qa in qa_list:
        msg = _qa_row_to_message(qa)
        if msg:
            messages.append(msg)

    # Старые сверху
    messages.reverse()

    return ChatHistoryResponse(messages=messages, total=len(messages))


# ---------- Получить историю чата по растению ----------

@router.get("/chat/{plant_id}", response_model=ChatHistoryResponse)
async def get_plant_chat_history(
    plant_id: int,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    user_id: int = Depends(get_current_user),
):
    """Получить историю чата по конкретному растению"""
    db = await get_db()

    # Проверяем, что растение принадлежит пользователю
    plant = await db.get_plant_with_state(plant_id, user_id)
    if not plant:
        raise HTTPException(status_code=404, detail="Растение не найдено")

    qa_list = await db.get_plant_qa_history(plant_id, limit=limit)
    plant_name = plant.get("display_name", "Растение")

    messages = []
    for qa in qa_list:
        msg = _qa_row_to_message(qa, plant_name=plant_name)
        if msg:
            messages.append(msg)

    # Старые сверху
    messages.reverse()

    return ChatHistoryResponse(messages=messages, total=len(messages))


# ---------- Задать вопрос ----------

@router.post("/question", response_model=QuestionResponse)
async def ask_question(
    req: QuestionRequest,
    user_id: int = Depends(get_current_user),
):
    """Задать вопрос ИИ о растении (или без растения: общий чат)"""
    # Проверяем лимит
    allowed, error_msg = await check_limit(user_id, "questions")
    if not allowed:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=error_msg)

    # Загружаем контекст растения если указано
    context_text = ""
    plant_name = None

    if req.plant_id:
        db = await get_db()
        plant = await db.get_plant_with_state(req.plant_id, user_id)
        if not plant:
            raise HTTPException(status_code=404, detail="Растение не найдено")

        plant_name = plant.get("display_name")
        context_text = await get_plant_context(req.plant_id, user_id, focus="general")

    # Получаем ответ от AI
    answer = await answer_plant_question(req.question, context_text)

    if isinstance(answer, dict):
        if "error" in answer:
            return QuestionResponse(success=False, error=answer["error"])

        answer_text = answer.get("answer", "")
        model_name = answer.get("model")
    else:
        answer_text = answer
        model_name = None

    if not answer_text or len(answer_text) < 20:
        return QuestionResponse(
            success=False,
            error="Не удалось сформировать ответ. Попробуйте переформулировать.",
        )

    # Увеличиваем счётчик
    await increment_usage(user_id, "questions")

    # Сохраняем взаимодействие
    if req.plant_id:
        # С растением: используем save_interaction (она пишет в plant_memory + БД)
        await save_interaction(
            req.plant_id, user_id, req.question, answer_text,
            context_used={"context_length": len(context_text)},
        )
    else:
        # Без растения: пишем напрямую в БД с plant_id=NULL
        db = await get_db()
        await db.save_qa_interaction(
            plant_id=None,
            user_id=user_id,
            question=req.question,
            answer=answer_text,
            context_used=None,
        )

    return QuestionResponse(
        success=True,
        answer=answer_text,
        model=model_name,
        plant_name=plant_name,
    )
