"""Prometheus collectors, declared once so both processes agree on names and labels.

The API serves these at GET /metrics via the ASGI app; the worker is not an ASGI app, so it
starts prometheus_client's own HTTP server on `worker_metrics_port` (see `app.worker`).
"""

from prometheus_client import Counter, Gauge, Histogram

EVENTS_PUBLISHED = Counter(
    "events_published_total",
    "Transaction events accepted by the API and written to the stream.",
)

EVENTS_PROCESSED = Counter(
    "events_processed_total",
    "Events consumed and acked, by outcome.",
    labelnames=("outcome",),  # processed | duplicate
)

EVENTS_FAILED = Counter(
    "events_failed_total",
    "Event deliveries that ended in failure, by kind.",
    labelnames=("kind",),  # transient | permanent | exhausted | unexpected
)

RETRIES = Counter(
    "retries_total",
    "Individual backoff retries, by the operation being retried.",
    labelnames=("operation",),  # rate_lookup | db_insert
)

CONSUMER_LAG = Gauge(
    "consumer_lag",
    "Stream entries not yet delivered to this consumer group.",
)

PROCESSING_SECONDS = Histogram(
    "event_processing_seconds",
    "Wall time for one full process() call, including retries.",
)
"""Recorded on both the success and the exception path (see the `with` block in
processor.process): "how long did this event take before we knew its outcome" is deliberately
the question this answers, not "how long did successful processing take" — a rate-service
outage inflating the p99 here IS the retry cost becoming visible, which is the point."""
