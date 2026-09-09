"""The transaction event, in its three forms: inbound, internal, and on the stream.

Field names match the assignment's contract exactly: {id, user_id, amount, currency, timestamp}.
`amount` is Decimal end to end — never float — and crosses the queue as a JSON *string* so no
precision is lost in serialisation.
"""

from datetime import UTC, datetime
from decimal import Decimal

from pydantic import BaseModel, field_validator


class TransactionEventIn(BaseModel):
    """The POST /events request body."""

    id: str
    """Client-supplied. Doubles as the idempotency key — dedup is by this and nothing else."""

    user_id: str
    amount: Decimal
    currency: str
    """ISO-4217, upper-cased on validation."""

    timestamp: datetime

    @field_validator("currency")
    @classmethod
    def _upper_currency(cls, value: str) -> str:
        """A lowercase "eur" must mean the same thing as "EUR": both reach the rate lookup
        and the DB with the identical, canonical code. Without this, "eur" 404s at the rate
        service and dead-letters as a bad currency — a data-shaped bug that's really an input
        normalization gap."""
        return value.upper()

    @field_validator("timestamp")
    @classmethod
    def _naive_timestamp_means_utc(cls, value: datetime) -> datetime:
        """A timestamp with no UTC offset must not be interpreted in whatever timezone the
        *process* happens to be running in — confirmed live: asyncpg encodes a naive
        `datetime` via `astimezone()`, which assumes local time, so a naive `13:00` accepted
        here lands in Postgres shifted by the worker container's own clock. This applies the
        same "naive means UTC" rule that `GET /users/{id}/transactions` applies to its own
        `from`/`to` query params (routes.py) — those aren't model fields, so they can't share
        this validator, but the rule itself is the one thing both boundaries agree on."""
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


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
        return {"schema_version": str(self.schema_version), "payload": self.payload}

    @classmethod
    def from_fields(cls, fields: dict[str, str]) -> "StreamEnvelope":
        """Rebuild from an XREADGROUP / XAUTOCLAIM entry.

        Raises:
            KeyError: a required field is missing.
            ValueError: `schema_version` isn't an integer, or `payload` isn't valid JSON for
                this model — both mean the entry is corrupt, not merely unfamiliar.
        """
        return cls(schema_version=int(fields["schema_version"]), payload=fields["payload"])

    def to_event(self) -> TransactionEvent:
        """Parse the payload back into a validated event.

        Raises:
            ValueError: the payload doesn't validate as a TransactionEvent (pydantic's
                ValidationError is a ValueError subclass) — same "corrupt, not unfamiliar"
                reasoning as `from_fields`.
        """
        return TransactionEvent.model_validate_json(self.payload)
