"""Consumer-group reads, acks, reclaims, and dead-lettering.

Delivery model is at-least-once. A message is acked only after its side effect is durable, so a
crash mid-processing means redelivery rather than loss — and the idempotent insert in
`TransactionRepository` is what makes that redelivery harmless.
"""

from dataclasses import dataclass

from redis.asyncio import Redis

from app.schemas.events import TransactionEvent


@dataclass(frozen=True)
class StreamMessage:
    """One delivery: the parsed event plus the delivery metadata the ack policy needs."""

    msg_id: str
    event: TransactionEvent
    delivery_count: int
    """How many times this message has been delivered. Drives the dead-letter cutoff."""


class StreamConsumer:
    """Wraps the XREADGROUP / XAUTOCLAIM / XACK cycle for one consumer in one group."""

    def __init__(
        self,
        redis: Redis,
        *,
        stream: str,
        group: str,
        consumer: str,
        batch_size: int,
        block_ms: int,
        dlq_stream: str,
    ) -> None:
        self._redis = redis
        self._stream = stream
        self._group = group
        self._consumer = consumer
        self._batch_size = batch_size
        self._block_ms = block_ms
        self._dlq_stream = dlq_stream

    async def ensure_group(self) -> None:
        """XGROUP CREATE with MKSTREAM, swallowing BUSYGROUP.

        Idempotent so every worker replica can call it at boot without coordinating.
        """
        ...

    async def read(self) -> list[StreamMessage]:
        """XREADGROUP for new messages ('>'), blocking up to `block_ms`.

        Messages that fail to parse are dead-lettered rather than returned — an unparseable
        payload is permanently bad and must not be allowed to circulate.
        """
        ...

    async def claim_stale(self, min_idle_ms: int) -> list[StreamMessage]:
        """XAUTOCLAIM messages pending longer than `min_idle_ms`.

        This is the recovery path for both a crashed consumer and a message this worker left
        deliberately unacked after exhausting its retries.
        """
        ...

    async def ack(self, msg_id: str) -> None:
        """XACK — the message is done and leaves the pending-entries list."""
        ...

    async def dead_letter(self, msg: StreamMessage, reason: str) -> None:
        """XADD the message to the DLQ stream with `reason`, then XACK the original.

        Acking after the DLQ write is the ordering that cannot lose an event: a crash in between
        leaves the message pending and it is simply reclaimed and dead-lettered again.
        """
        ...

    async def lag(self) -> int:
        """Entries not yet delivered to this group.

        Reads the `lag` field from XINFO GROUPS; that field can be None after a trim, in which
        case fall back to XLEN minus entries-read.
        """
        ...
