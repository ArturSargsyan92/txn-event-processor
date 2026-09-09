"""The ack / dead-letter policy in `worker.handle`.

Implemented in step 7, in the same commit as the worker wiring.

`processor.process` is mocked to raise, and `StreamConsumer` is a mock, so `ack` and
`dead_letter` and their arguments are asserted directly. No Redis, no Postgres — this is a test
of the decision, not of the transport.
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from app.core.config import Settings
from app.core.errors import PermanentError, TransientError
from app.queue.consumer import StreamMessage
from app.schemas.events import TransactionEvent
from app.services.processor import Outcome
from app.worker import handle


def _msg(delivery_count: int = 1) -> StreamMessage:
    event = TransactionEvent(
        id="t1",
        user_id="u1",
        amount=Decimal("10.00"),
        currency="EUR",
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        received_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    return StreamMessage(
        msg_id="1-0",
        event=event,
        delivery_count=delivery_count,
        raw_fields={"schema_version": "1", "payload": event.model_dump_json()},
    )


@pytest.fixture
def processor() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def consumer() -> AsyncMock:
    return AsyncMock()


async def test_success_acks(processor: AsyncMock, consumer: AsyncMock, settings: Settings):
    processor.process.return_value = Outcome.PROCESSED
    msg = _msg()

    await handle(msg, processor, consumer, settings)

    consumer.ack.assert_awaited_once_with(msg.msg_id)
    consumer.dead_letter.assert_not_awaited()


async def test_duplicate_acks(processor: AsyncMock, consumer: AsyncMock, settings: Settings):
    """DUPLICATE is a success, not a failure: it acks too.

    Under at-least-once this is the normal path for a redelivered event, and treating it as an
    error would dead-letter perfectly good work.
    """
    processor.process.return_value = Outcome.DUPLICATE
    msg = _msg()

    await handle(msg, processor, consumer, settings)

    consumer.ack.assert_awaited_once_with(msg.msg_id)
    consumer.dead_letter.assert_not_awaited()


async def test_permanent_error_dead_letters_with_original_reason(
    processor: AsyncMock, consumer: AsyncMock, settings: Settings
):
    """PermanentError -> dead_letter(msg, reason) carrying the original error text.

    The reason must survive to the DLQ entry; 'unknown currency: XYZ' is what makes a parked
    message diagnosable later. No ack — dead_letter does that itself.
    """
    processor.process.side_effect = PermanentError("unknown currency: XYZ")
    msg = _msg()

    await handle(msg, processor, consumer, settings)

    consumer.dead_letter.assert_awaited_once_with(msg, reason="unknown currency: XYZ")
    consumer.ack.assert_not_awaited()


async def test_transient_error_under_limit_leaves_message_pending(
    processor: AsyncMock, consumer: AsyncMock, settings: Settings
):
    """TransientError below max_deliveries -> neither ack nor dead_letter is called.

    The assertion is that *nothing happens*. Leaving the message in the pending-entries list is
    what hands it to `claim_stale` for redelivery, and it is invisible in the code — a refactor
    that "tidied up" by acking here would silently start dropping events under load.
    """
    processor.process.side_effect = TransientError("rate-service unreachable")
    msg = _msg(delivery_count=settings.max_deliveries - 1)

    await handle(msg, processor, consumer, settings)

    consumer.ack.assert_not_awaited()
    consumer.dead_letter.assert_not_awaited()


async def test_transient_error_at_limit_dead_letters(
    processor: AsyncMock, consumer: AsyncMock, settings: Settings
):
    """delivery_count == max_deliveries -> dead_letter(msg, reason='max deliveries exceeded').

    Boundary case: at the limit, not merely past it.
    """
    processor.process.side_effect = TransientError("rate-service unreachable")
    msg = _msg(delivery_count=settings.max_deliveries)

    await handle(msg, processor, consumer, settings)

    consumer.dead_letter.assert_awaited_once_with(msg, reason="max deliveries exceeded")
    consumer.ack.assert_not_awaited()


async def test_transient_error_over_limit_dead_letters(
    processor: AsyncMock, consumer: AsyncMock, settings: Settings
):
    """delivery_count above max_deliveries dead-letters as well, in case a reclaim overshoots."""
    processor.process.side_effect = TransientError("rate-service unreachable")
    msg = _msg(delivery_count=settings.max_deliveries + 1)

    await handle(msg, processor, consumer, settings)

    consumer.dead_letter.assert_awaited_once_with(msg, reason="max deliveries exceeded")
    consumer.ack.assert_not_awaited()
