"""Deduplication by event id — required by the assignment.

Implemented in step 3, alongside app/db/repository.py.

Needs a real Postgres (see the `session_factory` fixture in conftest.py): the repository uses
`sqlalchemy.dialects.postgresql.insert(...).on_conflict_do_nothing(...)`, a Postgres-specific
construct, so this is exactly the behaviour that can't be faked with a different backend.
"""

import asyncio
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from app.db.models import ProcessedTransaction
from app.db.repository import TransactionRepository


def _txn(id: str, **overrides: object) -> ProcessedTransaction:
    """Build a ProcessedTransaction with sane defaults, overriding only what a test cares about."""
    fields = {
        "id": id,
        "user_id": "u1",
        "amount": Decimal("10.00"),
        "currency": "EUR",
        "rate": Decimal("1.1"),
        "amount_usd": Decimal("11.00"),
        "timestamp": datetime(2026, 1, 1, tzinfo=UTC),
        "processed_at": datetime(2026, 1, 1, tzinfo=UTC),
    }
    fields.update(overrides)
    return ProcessedTransaction(**fields)


@pytest.fixture
def repo(session_factory) -> TransactionRepository:
    return TransactionRepository(session_factory)


async def test_first_insert_returns_true(repo: TransactionRepository):
    inserted = await repo.insert_ignore_duplicate(_txn("t1"))

    assert inserted is True


async def test_duplicate_insert_returns_false(repo: TransactionRepository):
    await repo.insert_ignore_duplicate(_txn("t1"))
    inserted_again = await repo.insert_ignore_duplicate(_txn("t1"))

    assert inserted_again is False


async def test_duplicate_does_not_overwrite(repo: TransactionRepository):
    await repo.insert_ignore_duplicate(_txn("t1", amount=Decimal("10.00")))
    await repo.insert_ignore_duplicate(_txn("t1", amount=Decimal("999.00")))

    _, count = await repo.user_summary("u1")
    rows, _ = await repo.list_user_transactions("u1", start=None, end=None, limit=10, offset=0)

    assert count == 1
    assert len(rows) == 1
    assert rows[0].amount == Decimal("10.00")


async def test_concurrent_inserts_of_same_id(repo: TransactionRepository):
    results = await asyncio.gather(
        repo.insert_ignore_duplicate(_txn("t1")),
        repo.insert_ignore_duplicate(_txn("t1")),
    )

    # Postgres's unique constraint makes this atomic, not a race the test has to get lucky
    # with: exactly one of the two concurrent inserts can win.
    assert sorted(results) == [False, True]
    _, count = await repo.user_summary("u1")
    assert count == 1


async def test_distinct_ids_both_inserted(repo: TransactionRepository):
    first = await repo.insert_ignore_duplicate(_txn("t1"))
    second = await repo.insert_ignore_duplicate(_txn("t2"))

    assert first is True
    assert second is True
    _, count = await repo.user_summary("u1")
    assert count == 2
