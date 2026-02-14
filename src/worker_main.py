import asyncio
import os

from prometheus_client import start_http_server

from app import db
from app.db import AsyncSessionLocal
from app.worker import start_workers


async def main():

    print("[workers] Initializing database...")
    await db.init_db()
    
    print("[workers] Starting Prometheus metrics server on :8001")
    start_http_server(8001)
    
    worker_concurrency = int(os.getenv("WORKER_CONCURRENCY", "10"))
    print(f"[workers] Starting {worker_concurrency} worker(s)...")
    
    await start_workers(worker_concurrency, AsyncSessionLocal)


if __name__ == "__main__":
    asyncio.run(main())
