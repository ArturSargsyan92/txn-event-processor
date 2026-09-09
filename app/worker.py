"""The consumer process: read the stream, process events, decide what to ack.

Runs as the `worker` service in docker-compose (`python -m app.worker`).

Processing is sequential on purpose. Bounded concurrency would raise throughput, but the queue
already absorbs bursts and consumer groups already give horizontal scaling for free
(`docker compose up --scale worker=N`), so the simpler loop is the better trade here.
"""

from app.core.config import Settings
from app.queue.consumer import StreamConsumer, StreamMessage
from app.services.processor import EventProcessor


async def handle(
    msg: StreamMessage,
    processor: EventProcessor,
    consumer: StreamConsumer,
    settings: Settings,
) -> None:
    """Process one delivery and decide its fate. This function *is* the delivery policy.

    Three failure branches, each tested in tests/test_worker_handle.py:

    * PermanentError -> dead-letter with the original reason. Bad data must never come back.
    * TransientError, under max_deliveries -> do nothing at all. No ack, no DLQ: the message
      stays in the pending-entries list and `claim_stale` will redeliver it once it goes idle.
      Doing nothing is the deliberate behaviour, which is exactly why it is asserted.
    * TransientError, at or over max_deliveries -> dead-letter. This is the cutoff that stops a
      poison message being reclaimed forever and growing the pending list without bound.

    Success acks. Note that a DUPLICATE outcome is a success, not a failure — it is the expected
    shape of at-least-once delivery meeting an idempotent write.
    """
    ...


async def consume_loop(
    consumer: StreamConsumer,
    processor: EventProcessor,
    settings: Settings,
) -> None:
    """Read new messages ('>') and hand each to `handle`."""
    ...


async def reclaim_loop(
    consumer: StreamConsumer,
    processor: EventProcessor,
    settings: Settings,
) -> None:
    """Every `reclaim_interval_s`, XAUTOCLAIM idle messages and re-run them through `handle`."""
    ...


async def lag_loop(consumer: StreamConsumer) -> None:
    """Publish consumer lag to the gauge on a timer."""
    ...


async def run_worker(settings: Settings) -> None:
    """Build dependencies, start the metrics server, and run the three loops together.

    The worker is not an ASGI app, so Prometheus is served by prometheus_client's own HTTP
    server on `worker_metrics_port` rather than by a /metrics route.
    """
    ...


def main() -> None:
    """Entrypoint: load settings, configure logging, run until SIGTERM."""
    ...


if __name__ == "__main__":
    main()
