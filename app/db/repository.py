"""Data access for transactions.

Dedup lives here and nowhere else. No read-before-write, no Redis SETNX, no application-level
"seen" set — one atomic statement whose correctness does not depend on how many workers are
running or how they interleave.
"""

from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal

from sqlalchemy.ext.asyncio import async_sessionmaker

from app.db.models import ProcessedTransaction


class TransactionRepository:
    """Reads and writes for `processed_transactions`."""

    def __init__(self, session_factory: async_sessionmaker) -> None:
        self._session_factory = session_factory

    async def insert_ignore_duplicate(self, txn: ProcessedTransaction) -> bool:
        """Insert `txn`, doing nothing if its id is already present.

        Built with `sqlalchemy.dialects.postgresql.insert(...).on_conflict_do_nothing(
        index_elements=["id"]).returning(id)` — SQLModel's ORM API does not expose upsert, so
        this one statement drops to SQLAlchemy Core.

        Returns:
            True if a row was inserted, False if the id already existed (a duplicate delivery).

        Raises:
            DatabaseUnavailable: connection refused or dropped — retryable.
        """
        ...

    async def user_summary(self, user_id: str) -> tuple[Decimal, int]:
        """Total USD and transaction count for a user.

        `SELECT COALESCE(SUM(amount_usd), 0), COUNT(*) ... WHERE user_id = :user_id`, so an
        unknown user returns (0, 0) rather than erroring — a user with no transactions yet is a
        normal state, not a 404.
        """
        ...

    async def list_user_transactions(
        self,
        user_id: str,
        *,
        start: datetime | None,
        end: datetime | None,
        limit: int,
        offset: int,
    ) -> tuple[Sequence[ProcessedTransaction], int]:
        """One page of a user's transactions, newest first, plus the unpaginated total.

        `start`/`end` are the `from=`/`to=` query params and bound `timestamp` as a half-open
        interval [start, end) — so adjacent ranges tile without double-counting a boundary row.
        Both are optional; either side may be left open.

        Served by the (user_id, timestamp) index.
        """
        ...
