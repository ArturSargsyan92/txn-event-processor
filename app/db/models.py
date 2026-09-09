"""SQLModel tables.

One table. `id` is the natural primary key straight from the event, which is what makes the
whole dedup story a single INSERT ... ON CONFLICT DO NOTHING rather than an application-level
"have I seen this?" check that would race under concurrent workers.
"""

from datetime import datetime
from decimal import Decimal

from sqlmodel import Field, SQLModel


class ProcessedTransaction(SQLModel, table=True):
    """A transaction event after successful rate lookup and conversion.

    Columns to define in step 3:
        id: str          -- primary key, the client-supplied event id (the dedup key)
        user_id: str     -- indexed with timestamp for the range query
        amount: Decimal  -- Numeric(18, 4), the original amount
        currency: str    -- ISO-4217, as submitted
        rate: Decimal    -- Numeric(18, 8), USD per unit at processing time; stored so a
                            historical row can be explained without re-querying the rate service
        amount_usd: Decimal  -- Numeric(18, 4), what GET /summary sums
        timestamp: datetime  -- from the event; the range filter and sort key
        processed_at: datetime -- when the worker stored it; useful for lag forensics

    Index: (user_id, timestamp) serves both read endpoints.
    """

    __tablename__ = "processed_transactions"

    # Column types, precision and the composite index are filled in during step 3;
    # the primary key is here now because it is the dedup contract, not an implementation detail.
    id: str = Field(primary_key=True)
    user_id: str = Field(index=True)
    amount: Decimal
    currency: str
    rate: Decimal
    amount_usd: Decimal
    timestamp: datetime
    processed_at: datetime
