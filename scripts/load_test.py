import argparse
import asyncio
import time
import uuid

import httpx


async def worker(name: str, client: httpx.AsyncClient, url: str, requests_per_worker: int) -> int:
    success = 0
    for _ in range(requests_per_worker):
        token = str(uuid.uuid4())
        try:
            resp = await client.post(url, json={"token": token}, timeout=5.0)
            if resp.status_code == 201:
                success += 1
        except Exception as e:
            print(f"[worker-{name}] request failed for token {token}: {e}")
    return success


async def run_load(base_url: str, total_requests: int, concurrency: int) -> None:
    url = f"{base_url.rstrip('/')}/tokens"
    requests_per_worker = total_requests // concurrency
    extra = total_requests % concurrency

    async with httpx.AsyncClient() as client:
        tasks = []
        for i in range(concurrency):
            n = requests_per_worker + (1 if i < extra else 0)
            if n <= 0:
                continue
            tasks.append(worker(str(i), client, url, n))

        start = time.perf_counter()
        results = await asyncio.gather(*tasks)
        elapsed = time.perf_counter() - start

    sent = sum(results)
    rps = sent / elapsed if elapsed > 0 else 0.0
    print(f"Sent {sent} requests in {elapsed:.2f}s -> {rps:.2f} req/s (concurrency={concurrency})")


def main() -> None:
    parser = argparse.ArgumentParser(description="Simple load test for /tokens endpoint")
    parser.add_argument("--base-url", default="http://localhost:8000", help="Base URL of the API (default: http://localhost:8000)")
    parser.add_argument("--requests", type=int, default=100, help="Total number of requests to send")
    parser.add_argument("--concurrency", type=int, default=10, help="Number of concurrent workers")

    args = parser.parse_args()

    asyncio.run(run_load(args.base_url, args.requests, args.concurrency))


if __name__ == "__main__":
    main()
