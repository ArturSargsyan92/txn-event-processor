"""Data access for transactions.

Dedup lives here and nowhere else. No read-before-write, no Redis SETNX, no application-level
"seen" set — one atomic statement whose correctness does not depend on how many workers are
running or how they interleave.
"""

from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from typing import NoReturn

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.errors import DB_EXCEPTIONS, classify_db_error
from app.db.models import ProcessedTransaction


class TransactionRepository:
    """Reads and writes for `processed_transactions`.

    Every method catches DB_EXCEPTIONS broadly, then hands it to classify_db_error: a
    connection problem re-raises as DatabaseUnavailable (retryable); a data or schema problem
    (a value too long for a column, a constraint violation) propagates as whatever SQLAlchemy
    or asyncpg actually raised, unclassified — retrying it can never help. See app/db/errors.py
    for why this can't be done by Python exception type alone on this driver.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    @staticmethod
    def _reraise_classified(exc: Exception) -> NoReturn:
        """Re-raise `exc` as DatabaseUnavailable if it looks retryable, else re-raise it as-is.

        Two separate raises rather than `raise (transient or exc) from exc` — chaining an
        exception from itself as its own cause is legal but produces a nonsense traceback.
        """
        transient = classify_db_error(exc)
        if transient is not None:
            raise transient from exc
        raise exc

    async def insert_ignore_duplicate(self, txn: ProcessedTransaction) -> bool:
        """Insert `txn`, doing nothing if its id is already present.

        Built with `sqlalchemy.dialects.postgresql.insert(...).on_conflict_do_nothing(
        index_elements=["id"]).returning(id)` against `.__table__` directly — SQLModel's ORM API
        (`session.add(...)`) has no upsert, so this one statement drops to SQLAlchemy Core.
        RETURNING yields a row only when a row was actually inserted, which is what turns this
        into a boolean without a second query.

        Returns:
            True if a row was inserted, False if the id already existed (a duplicate delivery).

        Raises:
            DatabaseUnavailable: connection refused or dropped — retryable.
        """
        table = ProcessedTransaction.__table__
        stmt = (
            pg_insert(table)
            .values(**txn.model_dump())
            .on_conflict_do_nothing(index_elements=["id"])
            .returning(table.c.id)
        )
        try:
            async with self._session_factory() as session:
                result = await session.execute(stmt)
                inserted = result.first() is not None
                await session.commit()
        except DB_EXCEPTIONS as exc:
            self._reraise_classified(exc)

        return inserted

    async def user_summary(self, user_id: str) -> tuple[Decimal, int]:
        """Total USD and transaction count for a user.

        `SELECT COALESCE(SUM(amount_usd), 0), COUNT(*) ... WHERE user_id = :user_id`, so an
        unknown user returns (0, 0) rather than erroring — a user with no transactions yet is a
        normal state, not a 404.
        """
        stmt = select(
            func.coalesce(func.sum(ProcessedTransaction.amount_usd), 0),
            func.count(),
        ).where(ProcessedTransaction.user_id == user_id)

        try:
            async with self._session_factory() as session:
                total, count = (await session.execute(stmt)).one()
        except DB_EXCEPTIONS as exc:
            self._reraise_classified(exc)

        return Decimal(total), count

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
        conditions = [ProcessedTransaction.user_id == user_id]
        if start is not None:
            conditions.append(ProcessedTransaction.timestamp >= start)
        if end is not None:
            conditions.append(ProcessedTransaction.timestamp < end)

        rows_stmt = (
            select(ProcessedTransaction)
            .where(*conditions)
            # id as a tiebreaker: timestamp alone isn't unique (a same-second ingest batch is
            # realistic), and without one, ties have no defined order — limit/offset paging
            # could then skip or repeat a row across two pages of the same query.
            .order_by(ProcessedTransaction.timestamp.desc(), ProcessedTransaction.id.desc())
            .limit(limit)
            .offset(offset)
        )
        count_stmt = select(func.count()).select_from(ProcessedTransaction).where(*conditions)

        try:
            async with self._session_factory() as session:
                rows = (await session.execute(rows_stmt)).scalars().all()
                total = (await session.execute(count_stmt)).scalar_one()
        except DB_EXCEPTIONS as exc:
            self._reraise_classified(exc)

        return rows, total
