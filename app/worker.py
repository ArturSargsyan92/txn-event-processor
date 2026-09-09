"""The consumer process: read the stream, process events, decide what to ack.

Runs as the `worker` service in docker-compose (`python -m app.worker`).

Processing is sequential on purpose. Bounded concurrency would raise throughput, but the queue
already absorbs bursts and consumer groups already give horizontal scaling for free
(`docker compose up --scale worker=N`), so the simpler loop is the better trade here.
"""

import asyncio
import signal

import httpx
from prometheus_client import start_http_server

from app.core.config import Settings
from app.core.errors import PermanentError, TransientError
from app.core.logging import configure_logging, get_logger
from app.core.metrics import CONSUMER_LAG, EVENTS_FAILED, EVENTS_PROCESSED
from app.db.engine import create_engine, create_session_factory, wait_until_ready
from app.db.repository import TransactionRepository
from app.queue.client import close_redis, create_redis
from app.queue.consumer import StreamConsumer, StreamMessage
from app.services.processor import EventProcessor
from app.services.rate_client import RateClient

logger = get_logger(__name__)

LAG_POLL_INTERVAL_S = 5.0
"""Not a Settings knob: there's no operational reason to tune how often this gauge refreshes."""


class _ShutdownRequested(Exception):
    """Raised internally by the signal watcher to unwind the TaskGroup cleanly on SIGTERM/
    SIGINT — never raised or caught anywhere else."""


async def handle(
    msg: StreamMessage,
    processor: EventProcessor,
    consumer: StreamConsumer,
    settings: Settings,
) -> None:
    """Process one delivery and decide its fate. This function *is* the delivery policy.

    Four branches, each tested in tests/test_worker_handle.py:

    * PermanentError -> dead-letter with the original reason. Bad data must never come back.
    * TransientError, under max_deliveries -> do nothing at all. No ack, no DLQ: the message
      stays in the pending-entries list and `claim_stale` will redeliver it once it goes idle.
      Doing nothing is the deliberate behaviour, which is exactly why it is asserted.
    * TransientError, at or over max_deliveries -> dead-letter. This is the cutoff that stops a
      poison message being reclaimed forever and growing the pending list without bound.
    * Anything else — neither AppError subclass — dead-letters too, immediately. `classify_db_error`
      and RateClient both deliberately let some failures (a numeric overflow, an unexpected 4xx)
      propagate unclassified rather than mislabel them; without this branch, one of those crashes
      `consume_loop`, and since the message is never acked, it comes right back on restart via
      `claim_stale` and crashes the worker again — an unbounded crash loop, never reaching the DLQ,
      the exact failure the DLQ exists to prevent. `except Exception`, not `except BaseException`:
      CancelledError is a BaseException specifically so a handler like this one can't catch it,
      which is what keeps a SIGTERM-triggered shutdown mid-`handle` propagating correctly.

    Success acks. Note that a DUPLICATE outcome is a success, not a failure — it is the expected
    shape of at-least-once delivery meeting an idempotent write.
    """
    log_fields = {"event_id": msg.event.id, "msg_id": msg.msg_id}
    try:
        outcome = await processor.process(msg.event)
        await consumer.ack(msg.msg_id)
        EVENTS_PROCESSED.labels(outcome=outcome).inc()
        logger.info("processed", extra={**log_fields, "outcome": outcome})
    except PermanentError as exc:
        await consumer.dead_letter(msg, reason=str(exc))
        EVENTS_FAILED.labels(kind="permanent").inc()
        logger.warning("dead-lettered: permanent error", extra={**log_fields, "reason": str(exc)})
    except TransientError as exc:
        if msg.delivery_count >= settings.max_deliveries:
            await consumer.dead_letter(msg, reason="max deliveries exceeded")
            EVENTS_FAILED.labels(kind="exhausted").inc()
            logger.warning(
                "dead-lettered: max deliveries exceeded",
                extra={**log_fields, "delivery_count": msg.delivery_count},
            )
        else:
            EVENTS_FAILED.labels(kind="transient").inc()
            logger.info(
                "left pending after transient failure",
                extra={**log_fields, "delivery_count": msg.delivery_count, "error": str(exc)},
            )
            # deliberately no ack, no dead_letter — see the docstring above
    except Exception as exc:
        await consumer.dead_letter(msg, reason=f"unexpected error: {exc}")
        EVENTS_FAILED.labels(kind="unexpected").inc()
        logger.error(
            "dead-lettered: unexpected error",
            extra={**log_fields, "reason": str(exc), "error_type": type(exc).__name__},
        )


async def consume_loop(
    consumer: StreamConsumer,
    processor: EventProcessor,
    settings: Settings,
) -> None:
    """Read new messages ('>') and hand each to `handle`."""
    while True:
        for msg in await consumer.read():
            await handle(msg, processor, consumer, settings)


async def reclaim_loop(
    consumer: StreamConsumer,
    processor: EventProcessor,
    settings: Settings,
) -> None:
    """Every `reclaim_interval_s`, XAUTOCLAIM idle messages and re-run them through `handle`."""
    while True:
        await asyncio.sleep(settings.reclaim_interval_s)
        for msg in await consumer.claim_stale(min_idle_ms=settings.min_idle_ms):
            await handle(msg, processor, consumer, settings)


async def lag_loop(consumer: StreamConsumer) -> None:
    """Publish consumer lag to the gauge on a timer."""
    while True:
        CONSUMER_LAG.set(await consumer.lag())
        await asyncio.sleep(LAG_POLL_INTERVAL_S)


async def _watch_for_shutdown(stop_event: asyncio.Event) -> None:
    """Wait for SIGTERM/SIGINT, then raise to unwind the sibling loops.

    Raising from one task inside a TaskGroup cancels every other task in it and propagates the
    exception out — that's the mechanism, deliberately used here rather than reached for by
    accident.
    """
    await stop_event.wait()
    raise _ShutdownRequested


async def run_worker(settings: Settings) -> None:
    """Build dependencies, start the metrics server, and run the three loops together.

    The worker is not an ASGI app, so Prometheus is served by prometheus_client's own HTTP
    server on `worker_metrics_port` rather than by a /metrics route.
    """
    start_http_server(settings.worker_metrics_port)

    engine = create_engine(settings.database_url)
    await wait_until_ready(
        engine, attempts=settings.db_startup_attempts, base_delay=settings.db_startup_base_delay_s
    )
    repo = TransactionRepository(create_session_factory(engine))

    # +5s margin over block_ms: the client's own read timeout must never race the server-side
    # BLOCK duration XREADGROUP uses — see create_redis's docstring for why.
    redis = create_redis(settings.redis_url, socket_timeout=settings.block_ms / 1000 + 5.0)
    consumer = StreamConsumer(
        redis,
        stream=settings.stream_name,
        group=settings.consumer_group,
        consumer=settings.consumer_name,
        batch_size=settings.batch_size,
        block_ms=settings.block_ms,
        dlq_stream=settings.dlq_stream_name,
    )
    await consumer.ensure_group()

    http = httpx.AsyncClient(timeout=settings.rate_service_timeout_s)
    rates = RateClient(
        http, base_url=settings.rate_service_url, cache_ttl_s=settings.rate_cache_ttl_s
    )
    processor = EventProcessor(rates, repo, settings)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)

    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(consume_loop(consumer, processor, settings))
            tg.create_task(reclaim_loop(consumer, processor, settings))
            tg.create_task(lag_loop(consumer))
            tg.create_task(_watch_for_shutdown(stop_event))
    except* _ShutdownRequested:
        # Only this sentinel is matched here — except* partitions the group, so a genuine
        # crash in any sibling task (consume_loop, reclaim_loop, lag_loop) that happens to
        # land in the same group is *not* caught by this clause and is automatically
        # re-raised once the block exits. That's what keeps a real bug racing with a SIGTERM
        # from silently disappearing: verified directly by raising both a RuntimeError and
        # _ShutdownRequested in one TaskGroup and confirming the RuntimeError still escapes.
        #
        # A task cancelled mid-blocking-read can in principle surface as some client
        # library's own timeout exception rather than plain CancelledError, which would
        # slip past this clause too. create_redis's socket_timeout margin (see run_worker
        # above) is what actually prevents that for the one place it was observed to happen
        # (consumer.read()'s XREADGROUP) — measured at 0% occurrence with that margin in
        # place under realistic cancellation timing, versus close to it exactly at the
        # client's own (now much later) deadline. Not defended against with more code here:
        # that residual is real but exceedingly narrow, and the cost of catching it — the
        # broader except* this replaced — silently swallowed genuine crashes instead.
        logger.info("shutting down")
    finally:
        await http.aclose()
        await close_redis(redis)
        await engine.dispose()


def main() -> None:
    """Entrypoint: load settings, configure logging, run until SIGTERM."""
    settings = Settings()
    configure_logging(settings.log_level)
    asyncio.run(run_worker(settings))


if __name__ == "__main__":
    main()
