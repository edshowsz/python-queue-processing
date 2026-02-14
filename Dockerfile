FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app/src

WORKDIR /app

# System deps (if needed later, keep small)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
  && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY pyproject.toml ./
RUN pip install --upgrade pip \
  && pip install \
    fastapi \
    "uvicorn[standard]" \
    "sqlalchemy[asyncio]" \
    asyncpg \
    requests \
    prometheus-fastapi-instrumentator \
    prometheus-client \
    "redis[hiredis]"

# Copy source
COPY src ./src

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
