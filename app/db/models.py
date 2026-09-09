"""SQLModel tables.

One table. `id` is the natural primary key straight from the event, which is what makes the
whole dedup story a single INSERT ... ON CONFLICT DO NOTHING rather than an application-level
"have I seen this?" check that would race under concurrent workers.
"""

from datetime import datetime
from decimal import Decimal

from sqlalchemy import Column, DateTime, Index, Numeric, String
from sqlmodel import Field, SQLModel


class ProcessedTransaction(SQLModel, table=True):
    """A transaction event after successful rate lookup and conversion.

    `amount`/`amount_usd` are Numeric(18, 4) — four decimal places is enough for every real
    currency's minor unit and leaves headroom over plain cents. `rate` gets Numeric(18, 8) since
    an FX rate itself needs more precision than the amounts it's multiplied into. Storing the
    rate (rather than just the converted total) means a historical row can be explained without
    re-querying the rate service — the audit trail travels with the row.
    """

    __tablename__ = "processed_transactions"
    __table_args__ = (
        # Composite, not two single-column indexes: (user_id, timestamp) serves both read
        # endpoints — the summary's WHERE user_id=... uses its leftmost column alone, and the
        # paginated list's WHERE user_id=... ORDER BY timestamp uses both.
        Index("ix_processed_transactions_user_id_timestamp", "user_id", "timestamp"),
    )

    id: str = Field(sa_column=Column(String, primary_key=True))
    """The client-supplied event id — the dedup key, not a database-generated one."""

    user_id: str = Field(sa_column=Column(String, nullable=False))
    amount: Decimal = Field(sa_column=Column(Numeric(18, 4), nullable=False))
    currency: str = Field(sa_column=Column(String(3), nullable=False))
    rate: Decimal = Field(sa_column=Column(Numeric(18, 8), nullable=False))
    amount_usd: Decimal = Field(sa_column=Column(Numeric(18, 4), nullable=False))

    # timezone=True: the event's timestamp arrives with an offset (ISO-8601), and comparing a
    # naive column against an aware query bound is exactly the kind of bug that only shows up
    # once a deployment crosses a timezone boundary.
    timestamp: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))
    processed_at: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))
    """When the worker stored the row — distinct from `timestamp`, which is the event's own
    time. Useful for lag forensics: how long did this event sit in the queue?"""
