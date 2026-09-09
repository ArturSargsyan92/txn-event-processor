"""Pins DatabaseUnavailable classification — see app/db/errors.py.

Nothing tested this before it was fixed for asyncpg, which is exactly how it slipped through
review the first time: `except (OSError, OperationalError, InterfaceError)` looked reasonable
and every existing test happened to only exercise the OSError arm (connection refused).
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from app.core.errors import DatabaseUnavailable
from app.db.engine import create_engine, create_session_factory
from app.db.models import ProcessedTransaction
from app.db.repository import TransactionRepository


def _txn(**overrides: object) -> ProcessedTransaction:
    fields = {
        "id": "t1",
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


async def test_connection_failure_is_classified_as_transient():
    """A closed port needs no live Postgres to prove — it's a pure connectivity failure, the
    exact case the whole retry/backoff story exists to absorb."""
    engine = create_engine("postgresql+asyncpg://postgres@127.0.0.1:59999/nowhere")
    repo = TransactionRepository(create_session_factory(engine))

    try:
        with pytest.raises(DatabaseUnavailable):
            await repo.insert_ignore_duplicate(_txn())
    finally:
        await engine.dispose()


async def test_data_error_is_not_classified_as_transient(session_factory):
    """currency is String(3); 'TOOLONG' violates that. This can never succeed on retry — it
    must not come back as DatabaseUnavailable, or it would be retried and eventually
    dead-lettered as 'exhausted', mislabeling a data bug as a transient outage."""
    repo = TransactionRepository(session_factory)

    with pytest.raises(Exception) as exc_info:
        await repo.insert_ignore_duplicate(_txn(currency="TOOLONG"))

    assert not isinstance(exc_info.value, DatabaseUnavailable)
