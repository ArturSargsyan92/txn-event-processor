"""Publishes accepted events onto the stream.

This is the whole of the API's write path. XADD is O(1), so a 1k/sec burst lands in Redis at
full speed and the queue depth — not Postgres — absorbs the spike.
"""

from redis.asyncio import Redis

from app.schemas.events import StreamEnvelope, TransactionEvent


class EventProducer:
    """XADDs events onto the transaction stream."""

    def __init__(self, redis: Redis, stream: str, maxlen: int | None = None) -> None:
        self._redis = redis
        self._stream = stream
        self._maxlen = maxlen
        """Approximate cap (MAXLEN ~) so an unattended stream cannot grow without bound."""

    async def publish(self, event: TransactionEvent) -> str:
        """Wrap `event` in a StreamEnvelope and XADD it.

        Trimming uses `approximate=True`: exact trimming would make XADD O(n) and is not worth
        it just to hold the stream to a precise length.

        Returns:
            The stream message id, echoed back to the client for correlation.
        """
        envelope = StreamEnvelope(payload=event.model_dump_json())
        return await self._redis.xadd(
            self._stream,
            envelope.to_fields(),
            maxlen=self._maxlen,
            approximate=True,
        )
