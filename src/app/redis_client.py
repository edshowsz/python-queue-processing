import os
from redis.asyncio import Redis
from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
QUEUE_KEY = "queue:tokens"

_redis_client: Redis | None = None


async def get_redis() -> Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = Redis.from_url(REDIS_URL, decode_responses=True)
    return _redis_client


async def close_redis() -> None:
    global _redis_client
    if _redis_client:
        await _redis_client.close()
        _redis_client = None
