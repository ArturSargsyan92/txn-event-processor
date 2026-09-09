"""The transaction event, in its three forms: inbound, internal, and on the stream.

Field names match the assignment's contract exactly: {id, user_id, amount, currency, timestamp}.
`amount` is Decimal end to end — never float — and crosses the queue as a JSON *string* so no
precision is lost in serialisation.
"""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel


class TransactionEventIn(BaseModel):
    """The POST /events request body."""

    id: str
    """Client-supplied. Doubles as the idempotency key — dedup is by this and nothing else."""

    user_id: str
    amount: Decimal
    currency: str
    """ISO-4217, upper-cased on validation."""

    timestamp: datetime


class TransactionEvent(TransactionEventIn):
    """The inbound event plus the moment the API accepted it."""

    received_at: datetime


class StreamEnvelope(BaseModel):
    """What actually goes onto the Redis stream.

    Stream entries are flat str->str maps, so the event is carried as a single JSON payload
    field alongside a schema version — the version is what makes a future field addition a
    non-event for consumers still running the old code.
    """

    schema_version: int = 1
    payload: str
    """`TransactionEvent.model_dump_json()`."""

    def to_fields(self) -> dict[str, str]:
        """Flatten for XADD."""
        ...

    @classmethod
    def from_fields(cls, fields: dict[str, str]) -> "StreamEnvelope":
        """Rebuild from an XREADGROUP / XAUTOCLAIM entry."""
        ...

    def to_event(self) -> TransactionEvent:
        """Parse the payload back into a validated event."""
        ...
