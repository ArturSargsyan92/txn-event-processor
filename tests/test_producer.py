"""EventProducer — verifies the actual wire contract with StreamConsumer.

Needs a real Redis; see the `redis` fixture in conftest.py. The producer and consumer are
tested separately elsewhere (this file; tests/test_consumer.py), but neither alone proves they
agree on field names or envelope shape — a round trip through both is what actually pins that.
"""

from datetime import UTC, datetime
from decimal import Decimal

from redis.asyncio import Redis

from app.queue.consumer import StreamConsumer
from app.queue.producer import EventProducer
from app.schemas.events import TransactionEvent

STREAM = "test-producer-transactions"


def _event() -> TransactionEvent:
    return TransactionEvent(
        id="evt-1",
        user_id="u1",
        amount=Decimal("99.99"),
        currency="EUR",
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        received_at=datetime(2026, 1, 1, tzinfo=UTC),
    )


async def test_published_event_round_trips_through_the_consumer(redis: Redis):
    producer = EventProducer(redis, stream=STREAM, maxlen=1000)
    consumer = StreamConsumer(
        redis,
        stream=STREAM,
        group="test-group",
        consumer="test-consumer",
        batch_size=10,
        block_ms=100,
        dlq_stream=f"{STREAM}:dlq",
    )
    await consumer.ensure_group()
    event = _event()

    stream_id = await producer.publish(event)
    (msg,) = await consumer.read()

    assert msg.msg_id == stream_id
    assert msg.event == event
    assert isinstance(msg.event.amount, Decimal)


async def test_maxlen_trims_the_stream(redis: Redis):
    """approximate=True means the trim isn't exact, but it must trim at all — otherwise
    passing maxlen would be silently doing nothing."""
    producer = EventProducer(redis, stream=STREAM, maxlen=5)
    for i in range(50):
        await producer.publish(
            TransactionEvent(
                id=f"evt-{i}",
                user_id="u1",
                amount=Decimal("1.00"),
                currency="EUR",
                timestamp=datetime(2026, 1, 1, tzinfo=UTC),
                received_at=datetime(2026, 1, 1, tzinfo=UTC),
            )
        )

    length = await redis.xlen(STREAM)

    assert length < 50
