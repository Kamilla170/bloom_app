FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . .

EXPOSE 8000

# Shell-форма, а не JSON-массив: Railway передаёт порт через переменную
# PORT, и раскрыть её может только оболочка. В docker-compose на сервере
# PORT не задан — остаётся 8000, на который проксирует nginx.
CMD uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}
