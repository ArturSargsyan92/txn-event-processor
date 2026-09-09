"""Consumer-group mechanics. Implemented in step 5, alongside app/queue/consumer.py.

Needs a real Redis — see the `redis` fixture in conftest.py. Publishes test data directly via
`redis.xadd` rather than through `EventProducer`, keeping this file focused on `StreamConsumer`
alone.
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from redis.asyncio import Redis

from app.queue.consumer import StreamConsumer
from app.schemas.events import StreamEnvelope, TransactionEvent

STREAM = "test-transactions"
GROUP = "test-group"
CONSUMER_NAME = "test-consumer"
DLQ_STREAM = "test-transactions:dlq"


def _event(id: str = "t1", **overrides: object) -> TransactionEvent:
    fields = {
        "id": id,
        "user_id": "u1",
        "amount": Decimal("10.00"),
        "currency": "EUR",
        "timestamp": datetime(2026, 1, 1, tzinfo=UTC),
        "received_at": datetime(2026, 1, 1, tzinfo=UTC),
    }
    fields.update(overrides)
    return TransactionEvent(**fields)


async def _publish(redis: Redis, event: TransactionEvent) -> str:
    envelope = StreamEnvelope(payload=event.model_dump_json())
    return await redis.xadd(STREAM, envelope.to_fields())


@pytest.fixture
def consumer(redis: Redis) -> StreamConsumer:
    return StreamConsumer(
        redis,
        stream=STREAM,
        group=GROUP,
        consumer=CONSUMER_NAME,
        batch_size=10,
        block_ms=100,
        dlq_stream=DLQ_STREAM,
    )


async def test_ensure_group_is_idempotent(consumer: StreamConsumer):
    await consumer.ensure_group()
    await consumer.ensure_group()  # must not raise BUSYGROUP


async def test_read_returns_published_events(redis: Redis, consumer: StreamConsumer):
    await consumer.ensure_group()
    msg_id = await _publish(redis, _event("t1", amount=Decimal("42.50")))

    messages = await consumer.read()

    assert len(messages) == 1
    assert messages[0].msg_id == msg_id
    assert messages[0].event.id == "t1"
    assert messages[0].event.amount == Decimal("42.50")
    assert messages[0].delivery_count == 1


async def test_ack_removes_from_pending(redis: Redis, consumer: StreamConsumer):
    await consumer.ensure_group()
    await _publish(redis, _event())
    (msg,) = await consumer.read()

    await consumer.ack(msg.msg_id)

    pending = await redis.xpending(STREAM, GROUP)
    assert pending["pending"] == 0


async def test_unacked_message_stays_pending(redis: Redis, consumer: StreamConsumer):
    await consumer.ensure_group()
    await _publish(redis, _event())
    await consumer.read()

    pending = await redis.xpending(STREAM, GROUP)
    assert pending["pending"] == 1


async def test_claim_stale_reclaims_idle_message(redis: Redis, consumer: StreamConsumer):
    await consumer.ensure_group()
    msg_id = await _publish(redis, _event())
    await consumer.read()  # delivered, left unacked

    # min_idle_ms=0: reclaim anything pending at all, no need to actually wait.
    reclaimed = await consumer.claim_stale(min_idle_ms=0)

    assert len(reclaimed) == 1
    assert reclaimed[0].msg_id == msg_id


async def test_claim_stale_reports_delivery_count(redis: Redis, consumer: StreamConsumer):
    await consumer.ensure_group()
    await _publish(redis, _event())
    await consumer.read()  # delivery #1

    (reclaimed,) = await consumer.claim_stale(min_idle_ms=0)  # delivery #2

    assert reclaimed.delivery_count == 2


async def test_dead_letter_writes_to_dlq_and_acks(redis: Redis, consumer: StreamConsumer):
    await consumer.ensure_group()
    await _publish(redis, _event("t1"))
    (msg,) = await consumer.read()

    await consumer.dead_letter(msg, reason="max deliveries exceeded")

    dlq_entries = await redis.xrange(DLQ_STREAM)
    assert len(dlq_entries) == 1
    _dlq_id, fields = dlq_entries[0]
    assert fields["reason"] == "max deliveries exceeded"
    envelope = StreamEnvelope.from_fields({k: v for k, v in fields.items() if k != "reason"})
    assert envelope.to_event().id == "t1"

    pending = await redis.xpending(STREAM, GROUP)
    assert pending["pending"] == 0


async def test_unparseable_payload_is_dead_lettered(redis: Redis, consumer: StreamConsumer):
    await consumer.ensure_group()
    # Missing the "payload" field entirely — StreamEnvelope.from_fields can't rebuild this.
    bad_id = await redis.xadd(STREAM, {"schema_version": "1"})

    messages = await consumer.read()

    assert messages == []
    dlq_entries = await redis.xrange(DLQ_STREAM)
    assert len(dlq_entries) == 1
    assert "reason" in dlq_entries[0][1]

    pending = await redis.xpending(STREAM, GROUP)
    assert pending["pending"] == 0
    assert bad_id  # published id is real; just documents what we dead-lettered


async def test_lag_reports_undelivered_entries(redis: Redis):
    # batch_size=1 so read() below delivers exactly one of the three, proving lag tracks
    # "delivered", not "acked" — an unacked-but-read message still comes off the lag count.
    consumer = StreamConsumer(
        redis,
        stream=STREAM,
        group=GROUP,
        consumer=CONSUMER_NAME,
        batch_size=1,
        block_ms=100,
        dlq_stream=DLQ_STREAM,
    )
    for i in range(3):
        await _publish(redis, _event(f"t{i}"))
    await consumer.ensure_group()  # created after publishing — id="0" sees all 3 as undelivered

    assert await consumer.lag() == 3

    await consumer.read()  # delivers exactly one (unacked); lag counts delivery, not ack

    assert await consumer.lag() == 2
