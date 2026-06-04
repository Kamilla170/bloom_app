FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . .

EXPOSE 8000

# Один процесс uvicorn без --workers.
# Это важно: внутри живут APScheduler (напоминания, автоплатежи) и
# in-memory словари временных анализов. Несколько воркеров их сломают
# (дублирование пушей, потеря temp-анализов между запросами).
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
