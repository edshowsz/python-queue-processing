import asyncio
import json
import os
import random
from datetime import datetime, timezone
from time import monotonic

from sqlalchemy.ext.asyncio import AsyncSession

from .models import QueueProcessed
from .metrics import QUEUE_PROCESSED_TOTAL, QUEUE_PROCESSING_SECONDS, WORKER_ERRORS_TOTAL
from .redis_client import get_redis, QUEUE_KEY


BATCH_SIZE = int(os.getenv("QUEUE_BATCH_SIZE", "200"))
DLQ_KEY = "queue:failed"  # dead letter queue for failed items
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))  # max retry attempts
RETRY_DELAY_MINUTES = int(os.getenv("RETRY_DELAY_MINUTES", "5"))  # minutes before retry


async def fetch_batch_from_redis(limit: int) -> list[dict]:
    """Fetch up to 'limit' items from Redis queue using pipeline for performance"""
    redis = await get_redis()
    batch = []
    
    # Use pipeline to fetch multiple items at once (much faster than loop)
    async with redis.pipeline(transaction=False) as pipe:
        for _ in range(limit):
            pipe.rpop(QUEUE_KEY)
        results = await pipe.execute()
    
    for item_str in results:
        if item_str is not None:
            batch.append(json.loads(item_str))
    
    return batch


async def process_one_token(token_data: dict) -> dict | None:
    """
    Process a single token. Returns processed data on success, None on failure.
    Simulates heavy processing work (4-10 seconds randomly).
    """
    start = monotonic()
    try:
        # Simulate heavy processing work (random between 4 and 10 seconds)
        processing_time = random.uniform(4, 10)
        await asyncio.sleep(processing_time)
        
        duration = monotonic() - start
        QUEUE_PROCESSING_SECONDS.observe(duration)  # Record actual time per token
        
        return {
            "token": token_data["token"],
            "enqueued_at": token_data["enqueued_at"],
            "status": "success",
        }
    except Exception as e:
        print(f"[worker] error processing token {token_data.get('token')}: {e}")
        WORKER_ERRORS_TOTAL.labels(worker_id="batch", error_type=e.__class__.__name__).inc()
        return None


async def send_to_dlq(token_data: dict, error: str) -> None:
    """Send failed token to DLQ or discard if max retries exceeded"""
    redis = await get_redis()
    
    retry_count = token_data.get("retry_count", 0)
    
    # Check if max retries exceeded - permanently discard
    if retry_count >= MAX_RETRIES:
        print(f"[dlq] discarding token {token_data.get('token')} (max retries {MAX_RETRIES} exceeded)")
        return
    
    # Add to DLQ for retry after delay
    now = datetime.now(timezone.utc)
    retry_after = now.timestamp() + (RETRY_DELAY_MINUTES * 60)
    
    failed_item = {
        **token_data,
        "error": error,
        "failed_at": now.isoformat(),
        "retry_count": retry_count + 1,  # Increment for next attempt
    }
    
    await redis.zadd(DLQ_KEY, {json.dumps(failed_item): retry_after})
    print(f"[dlq] queued token {token_data.get('token')} for retry (attempt {retry_count + 1}/{MAX_RETRIES})")


async def process_batch(batch: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Process all tokens in parallel. Returns (successes, failures).
    Individual failures don't break the entire batch.
    """

    results = await asyncio.gather(
        *[process_one_token(token) for token in batch],
        return_exceptions=True
    )
    
    successes = []
    failures = []
    
    for token_data, result in zip(batch, results):
        
        if isinstance(result, Exception):
            failures.append(token_data)
            await send_to_dlq(token_data, str(result))
            
        elif result is None:
            failures.append(token_data)
            await send_to_dlq(token_data, "processing_failed")
       
        else:
            # Success
            successes.append(result)
    
    return successes, failures


async def save_processed(session: AsyncSession, batch: list[dict]) -> None:
    """Save all successfully processed tokens to Postgres in one transaction"""
    if not batch:
        return
    
    now = datetime.now(timezone.utc)
    items = [
        QueueProcessed(
            token=item["token"],
            enqueued_at=datetime.fromisoformat(item["enqueued_at"]),
            processed_at=now,
        )
        for item in batch
    ]
    session.add_all(items)
    await session.commit()


async def process_one_cycle(session_factory) -> int:
    """
    One processing cycle: fetch batch from Redis, process in parallel, save successes to Postgres.
    Failed items are sent to DLQ for later inspection/retry.
    """
    # Fetch batch from Redis (fast - in memory)
    batch = await fetch_batch_from_redis(BATCH_SIZE)
    
    if not batch:
        return 0
    
    # Measure only processing time (not including DB save)
    process_start = monotonic()
    successes, failures = await process_batch(batch)
    process_duration = monotonic() - process_start
    
    success_count = len(successes)
    failure_count = len(failures)
    
    # Update metrics immediately after processing (before DB save)
    if success_count > 0:
        QUEUE_PROCESSED_TOTAL.inc(success_count)
        # Note: QUEUE_PROCESSING_SECONDS is already recorded per token in process_one_token()
    
    # Save only successful items to Postgres (separate from processing metrics)
    save_start = monotonic()
    async with session_factory() as session:
        await save_processed(session, successes)
    save_duration = monotonic() - save_start
    
    total_duration = process_duration + save_duration
    print(f"[worker] batch: {success_count} ok, {failure_count} failed | process: {process_duration:.2f}s, save: {save_duration:.2f}s")
    return success_count


async def dlq_retry_worker() -> None:
    """Worker that moves items from DLQ back to main queue when ready for retry"""
    print(f"[dlq-retry] started (retry after {RETRY_DELAY_MINUTES} min)")
    redis = await get_redis()
    
    while True:
        try:
            now = datetime.now(timezone.utc).timestamp()
            
            # Get items ready for retry (score <= now)
            items_ready = await redis.zrangebyscore(DLQ_KEY, min=0, max=now)
            
            if not items_ready:
                await asyncio.sleep(30)  # Check every 30s when empty
                continue
            
            requeued_count = 0
            
            for item_str in items_ready:
                item = json.loads(item_str)
                
                # Remove from DLQ
                await redis.zrem(DLQ_KEY, item_str)
                
                # Send back to main queue (keeps retry_count for send_to_dlq logic)
                await redis.lpush(QUEUE_KEY, item_str)
                requeued_count += 1
            
            if requeued_count > 0:
                print(f"[dlq-retry] requeued {requeued_count} items back to main queue")
            
            await asyncio.sleep(30)  # Check every 30 seconds
            
        except Exception as e:
            print(f"[dlq-retry] error: {e}")
            await asyncio.sleep(30)


async def worker_loop(worker_id: int, session_factory) -> None:
    """Main worker loop - continuously processes batches until stopped"""
    print(f"[worker-{worker_id}] started")
    while True:
        try:
            count = await process_one_cycle(session_factory)
            if count == 0:
                await asyncio.sleep(0.1)  # queue empty - short wait
        except Exception as e:
            print(f"[worker-{worker_id}] cycle error: {e}")
            WORKER_ERRORS_TOTAL.labels(worker_id=str(worker_id), error_type=e.__class__.__name__).inc()
            await asyncio.sleep(0.1)


async def start_workers(concurrency: int, session_factory) -> None:
    """Start worker pool with specified concurrency + DLQ retry worker"""
    print(f"[workers] starting {concurrency} workers (BATCH_SIZE={BATCH_SIZE})")
    
    # Start main processing workers
    tasks = [
        asyncio.create_task(worker_loop(i, session_factory))
        for i in range(concurrency)
    ]
    
    # Start DLQ retry worker
    tasks.append(asyncio.create_task(dlq_retry_worker()))
    
    await asyncio.gather(*tasks)
