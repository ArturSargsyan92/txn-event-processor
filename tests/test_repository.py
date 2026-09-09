"""Read-path behavior for TransactionRepository: summary totals, pagination, and the
half-open [from, to) interval.

Needs a real Postgres — see the `session_factory` fixture in conftest.py. Kept separate from
test_dedup.py, which is the spec-required file and stays focused on the dedup contract alone.
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from app.db.models import ProcessedTransaction
from app.db.repository import TransactionRepository


def _txn(
    id: str,
    *,
    user_id: str = "u1",
    amount: Decimal = Decimal("10.00"),
    amount_usd: Decimal = Decimal("11.00"),
    timestamp: datetime,
) -> ProcessedTransaction:
    return ProcessedTransaction(
        id=id,
        user_id=user_id,
        amount=amount,
        currency="EUR",
        rate=Decimal("1.1"),
        amount_usd=amount_usd,
        timestamp=timestamp,
        processed_at=timestamp,
    )


@pytest.fixture
def repo(session_factory) -> TransactionRepository:
    return TransactionRepository(session_factory)


async def test_summary_for_unknown_user_is_zero(repo: TransactionRepository):
    total, count = await repo.user_summary("nobody")

    assert total == Decimal("0")
    assert count == 0


async def test_summary_sums_amount_usd(repo: TransactionRepository):
    await repo.insert_ignore_duplicate(
        _txn("a", amount_usd=Decimal("11.00"), timestamp=datetime(2026, 1, 1, tzinfo=UTC))
    )
    await repo.insert_ignore_duplicate(
        _txn("b", amount_usd=Decimal("22.00"), timestamp=datetime(2026, 1, 2, tzinfo=UTC))
    )

    total, count = await repo.user_summary("u1")

    assert total == Decimal("33.00")
    assert count == 2


async def test_range_is_half_open(repo: TransactionRepository):
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = datetime(2026, 1, 3, tzinfo=UTC)
    await repo.insert_ignore_duplicate(_txn("at_start", timestamp=start))
    await repo.insert_ignore_duplicate(_txn("middle", timestamp=datetime(2026, 1, 2, tzinfo=UTC)))
    await repo.insert_ignore_duplicate(_txn("at_end", timestamp=end))

    rows, total = await repo.list_user_transactions("u1", start=start, end=end, limit=10, offset=0)

    ids = {row.id for row in rows}
    assert ids == {"at_start", "middle"}
    assert total == 2


async def test_pagination_and_ordering_are_newest_first(repo: TransactionRepository):
    for i in range(3):
        await repo.insert_ignore_duplicate(
            _txn(f"t{i}", timestamp=datetime(2026, 1, i + 1, tzinfo=UTC))
        )

    page, total = await repo.list_user_transactions("u1", start=None, end=None, limit=1, offset=1)

    assert total == 3
    assert [row.id for row in page] == ["t1"]  # newest-first order is t2, t1, t0 — offset 1 is t1


async def test_same_timestamp_rows_have_a_stable_total_order(repo: TransactionRepository):
    """Regression: pagination needs a tiebreaker or same-second rows can be skipped or
    repeated across pages — timestamp alone isn't unique."""
    ts = datetime(2026, 1, 1, tzinfo=UTC)
    for id_ in ("a", "b", "c"):
        await repo.insert_ignore_duplicate(_txn(id_, timestamp=ts))

    page1, _ = await repo.list_user_transactions("u1", start=None, end=None, limit=2, offset=0)
    page2, _ = await repo.list_user_transactions("u1", start=None, end=None, limit=2, offset=2)

    seen = [row.id for row in page1] + [row.id for row in page2]
    assert sorted(seen) == ["a", "b", "c"]


async def test_timestamp_and_amount_round_trip_precisely(repo: TransactionRepository):
    """Numeric(18,4) and DateTime(timezone=True) are deliberate column choices — this is
    the test that actually checks the round trip keeps its promise instead of just assuming it."""
    ts = datetime(2026, 1, 1, 12, 30, tzinfo=UTC)
    await repo.insert_ignore_duplicate(
        _txn("t1", amount=Decimal("10.5"), amount_usd=Decimal("11.55"), timestamp=ts)
    )

    rows, _ = await repo.list_user_transactions("u1", start=None, end=None, limit=10, offset=0)
    row = rows[0]

    assert row.timestamp.tzinfo is not None
    assert row.timestamp == ts
    assert isinstance(row.amount, Decimal)
    assert row.amount == Decimal("10.50")
