from prometheus_client import Counter, Histogram

QUEUE_ENQUEUED_TOTAL = Counter(
    "queue_enqueued_total",
    "Total number of tokens enqueued",
)

QUEUE_PROCESSED_TOTAL = Counter(
    "queue_processed_total",
    "Total number of tokens processed",
)

QUEUE_PROCESSING_SECONDS = Histogram(
    "queue_processing_seconds",
    "Time spent processing a single token in seconds",
    buckets=(0.5, 1, 2, 5, 10, 30, 60),
)

WORKER_ERRORS_TOTAL = Counter(
    "worker_errors_total",
    "Total number of errors encountered by workers",
    ["worker_id", "error_type"],
)
