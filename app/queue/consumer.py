"""Consumer-group reads, acks, reclaims, and dead-lettering.

Delivery model is at-least-once. A message is acked only after its side effect is durable, so a
crash mid-processing means redelivery rather than loss — and the idempotent insert in
`TransactionRepository` is what makes that redelivery harmless.
"""

from dataclasses import dataclass

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from app.schemas.events import StreamEnvelope, TransactionEvent


@dataclass(frozen=True)
class StreamMessage:
    """One delivery: the parsed event plus the delivery metadata the ack policy needs."""

    msg_id: str
    event: TransactionEvent
    delivery_count: int
    """How many times this message has been delivered. Drives the dead-letter cutoff."""

    raw_fields: dict[str, str]
    """The exact original XADD fields. Dead-lettering replays these unchanged, rather than
    re-serializing `event` — which would silently drop `schema_version` (StreamEnvelope's
    default of 1 every time) if the entry actually arrived as some future v2."""


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

        Idempotent so every worker replica can call it at boot without coordinating. Starts
        the group from id "0" (the beginning of the stream), not "$" (only new entries): if
        the stream already has entries published before this group existed — the API started
        before the worker managed to create it, say — those still get delivered rather than
        silently skipped.
        """
        try:
            await self._redis.xgroup_create(self._stream, self._group, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def read(self) -> list[StreamMessage]:
        """XREADGROUP for new messages ('>'), blocking up to `block_ms`.

        Messages that fail to parse are dead-lettered rather than returned — an unparseable
        payload is permanently bad and must not be allowed to circulate.

        Every entry XREADGROUP '>' returns is, by definition, one this group has never been
        given before, so its delivery count is exactly 1 — no extra lookup needed here (unlike
        `claim_stale`, where the count can be anything).
        """
        response = await self._redis.xreadgroup(
            groupname=self._group,
            consumername=self._consumer,
            streams={self._stream: ">"},
            count=self._batch_size,
            block=self._block_ms,
        )
        if not response:
            return []

        _stream_name, entries = response[0]
        delivery_counts = {msg_id: 1 for msg_id, _fields in entries}
        return await self._parse_entries(entries, delivery_counts)

    async def claim_stale(self, min_idle_ms: int) -> list[StreamMessage]:
        """XAUTOCLAIM messages pending longer than `min_idle_ms`.

        This is the recovery path for both a crashed consumer and a message this worker left
        deliberately unacked after exhausting its retries. Note it does not exclude the calling
        consumer: a worker whose own in-flight message has been pending longer than
        `min_idle_ms` can reclaim and reprocess it — safe here only because the insert is
        idempotent. Needs Redis >= 7.0 for XAUTOCLAIM's three-element reply (older servers
        return two); pin that when the compose file is written.
        """
        _next_cursor, claimed, _deleted = await self._redis.xautoclaim(
            self._stream,
            self._group,
            self._consumer,
            min_idle_time=min_idle_ms,
            start_id="0-0",
            count=self._batch_size,
        )
        if not claimed:
            return []

        # XAUTOCLAIM increments each message's delivery count but doesn't return the new
        # value — only XPENDING's extended form does. One lookup per message, sequentially,
        # rather than a single range query: reclaim isn't the hot path (it runs on a timer,
        # not per-event), and this avoids needing any stream-id range/ordering logic to get
        # right (plain string comparison gets it wrong — "10-0" < "9-0" lexicographically).
        delivery_counts = {}
        for msg_id, _fields in claimed:
            delivery_counts[msg_id] = await self._delivery_count(msg_id)

        return await self._parse_entries(claimed, delivery_counts)

    async def _delivery_count(self, msg_id: str) -> int:
        """How many times `msg_id` has been delivered, via XPENDING's extended form."""
        entries = await self._redis.xpending_range(
            self._stream, self._group, min=msg_id, max=msg_id, count=1
        )
        return entries[0]["times_delivered"] if entries else 1

    async def _parse_entries(
        self,
        entries: list[tuple[str, dict[str, str]]],
        delivery_counts: dict[str, int],
    ) -> list[StreamMessage]:
        """Turn raw (id, fields) pairs into StreamMessages, dead-lettering unparseable ones."""
        messages = []
        for msg_id, fields in entries:
            try:
                event = StreamEnvelope.from_fields(fields).to_event()
            except (KeyError, ValueError) as exc:
                await self._dead_letter_fields(msg_id, fields, reason=f"unparseable: {exc}")
                continue

            messages.append(
                StreamMessage(
                    msg_id=msg_id,
                    event=event,
                    delivery_count=delivery_counts[msg_id],
                    raw_fields=fields,
                )
            )
        return messages

    async def ack(self, msg_id: str) -> None:
        """XACK — the message is done and leaves the pending-entries list."""
        await self._redis.xack(self._stream, self._group, msg_id)

    async def dead_letter(self, msg: StreamMessage, reason: str) -> None:
        """XADD the message to the DLQ stream with `reason`, then XACK the original.

        Acking after the DLQ write is the ordering that cannot lose an event: a crash in between
        leaves the message pending and it is simply reclaimed and dead-lettered again.

        Replays `msg.raw_fields` verbatim rather than re-serializing `msg.event` — the latter
        would silently reset schema_version to StreamEnvelope's default of 1 on every
        dead-letter, regardless of what the entry actually arrived as.
        """
        await self._dead_letter_fields(msg.msg_id, msg.raw_fields, reason)

    async def _dead_letter_fields(self, msg_id: str, fields: dict[str, str], reason: str) -> None:
        """Shared core for `dead_letter` and the unparseable-payload path in `_parse_entries` —
        both end up writing raw fields plus a reason to the DLQ, then acking the original.
        """
        await self._redis.xadd(self._dlq_stream, {**fields, "reason": reason})
        await self._redis.xack(self._stream, self._group, msg_id)

    async def lag(self) -> int:
        """Entries not yet delivered to this group.

        Reads the `lag` field from XINFO GROUPS. Redis sets that field to None exactly when it
        can no longer compute it — entries were deleted from the stream (by MAXLEN trimming,
        say) before this group read them — and `entries-read` goes None in that same situation,
        so there's no reliable fallback arithmetic to reconstruct the true count from XLEN
        either. Reports 0 rather than crashing or overcounting: an unknown lag is treated as
        "nothing measurable," not "assume the worst."
        """
        groups = await self._redis.xinfo_groups(self._stream)
        group = next(g for g in groups if g["name"] == self._group)
        return group["lag"] or 0
