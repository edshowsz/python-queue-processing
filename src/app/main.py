import json
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from datetime import datetime, timezone

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import start_http_server

from . import db
from .schemas import TokenCreate
from .metrics import QUEUE_ENQUEUED_TOTAL
from .redis_client import get_redis, close_redis, QUEUE_KEY


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    print("[api] Initializing database...")
    await db.init_db()
    
    # Start Prometheus metrics server for custom metrics
    print("[api] Starting Prometheus metrics server on :8002")
    start_http_server(8002)
    
    print("[api] API ready (workers run in separate container)")
    
    try:
        yield
    finally:
        await close_redis()


app = FastAPI(title="FastAPI Queue Processing", lifespan=lifespan)

# Expose Prometheus metrics at /metrics (configure before startup)
Instrumentator().instrument(app).expose(app, endpoint="/metrics")


@app.post("/tokens", status_code=201)
async def enqueue_token(payload: TokenCreate) -> dict:
    redis = await get_redis()
    
    # Enfileira no Redis com timestamp
    item = {
        "token": payload.token,
        "enqueued_at": datetime.now(timezone.utc).isoformat(),
    }
    await redis.lpush(QUEUE_KEY, json.dumps(item))
    
    QUEUE_ENQUEUED_TOTAL.inc()
    return {"status": "enqueued", "token": payload.token}


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
