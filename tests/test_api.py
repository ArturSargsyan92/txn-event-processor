"""HTTP contract. Implemented in step 8, alongside app/main.py and app/api/routes.py.

Needs a real Postgres and Redis — see the `api_client`, `session_factory`, and `redis`
fixtures in conftest.py. No worker runs in these tests: the read-endpoint tests seed rows
directly via TransactionRepository (the same thing the worker itself would eventually write),
and the ingest-endpoint tests inspect the raw stream — this file is testing the HTTP contract,
not re-testing dedup or conversion, which already have their own dedicated test files.
"""

from datetime import UTC, datetime
from decimal import Decimal

import httpx
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import ProcessedTransaction
from app.db.repository import TransactionRepository
from app.schemas.events import StreamEnvelope


def _payload(id: str = "t1", **overrides: object) -> dict:
    fields = {
        "id": id,
        "user_id": "u1",
        "amount": "42.50",
        "currency": "EUR",
        "timestamp": "2026-01-01T00:00:00Z",
    }
    fields.update(overrides)
    return fields


async def _seed(
    session_factory: async_sessionmaker[AsyncSession], id: str, **overrides: object
) -> None:
    repo = TransactionRepository(session_factory)
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
    await repo.insert_ignore_duplicate(ProcessedTransaction(**fields))


async def test_post_event_returns_202_and_stream_id(api_client: httpx.AsyncClient):
    response = await api_client.post("/events", json=_payload("t1"))

    assert response.status_code == 202
    body = response.json()
    assert body["event_id"] == "t1"
    assert body["stream_id"]


async def test_post_event_publishes_envelope_to_stream(api_client: httpx.AsyncClient, redis: Redis):
    response = await api_client.post("/events", json=_payload("t1", amount="42.50", currency="EUR"))
    stream_id = response.json()["stream_id"]

    entries = await redis.xrange("transactions")

    assert len(entries) == 1
    entry_id, fields = entries[0]
    assert entry_id == stream_id
    event = StreamEnvelope.from_fields(fields).to_event()
    assert event.id == "t1"
    assert event.amount == Decimal("42.50")
    assert event.currency == "EUR"


async def test_post_event_rejects_invalid_payload(api_client: httpx.AsyncClient):
    response = await api_client.post("/events", json={"id": "t1"})  # missing required fields

    assert response.status_code == 422


async def test_post_event_does_not_touch_the_database(
    api_client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
):
    await api_client.post("/events", json=_payload("t1"))

    repo = TransactionRepository(session_factory)
    total, count = await repo.user_summary("u1")

    assert count == 0
    assert total == Decimal("0")


async def test_summary_totals_usd_and_counts(
    api_client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
):
    await _seed(
        session_factory,
        "s1",
        amount_usd=Decimal("11.00"),
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
    )
    await _seed(
        session_factory,
        "s2",
        amount_usd=Decimal("22.00"),
        timestamp=datetime(2026, 1, 2, tzinfo=UTC),
    )

    response = await api_client.get("/users/u1/summary")

    assert response.status_code == 200
    body = response.json()
    assert body["user_id"] == "u1"
    assert body["transaction_count"] == 2
    assert Decimal(body["total_usd"]) == Decimal("33.00")


async def test_summary_for_unknown_user_is_zero(api_client: httpx.AsyncClient):
    response = await api_client.get("/users/nobody/summary")

    assert response.status_code == 200
    body = response.json()
    assert body["transaction_count"] == 0
    assert Decimal(body["total_usd"]) == Decimal("0")


async def test_transactions_are_paginated(
    api_client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
):
    for i in range(3):
        await _seed(session_factory, f"p{i}", timestamp=datetime(2026, 1, i + 1, tzinfo=UTC))

    response = await api_client.get("/users/u1/transactions", params={"limit": 1, "offset": 1})

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert body["limit"] == 1
    assert body["offset"] == 1
    assert [item["id"] for item in body["items"]] == [
        "p1"
    ]  # newest-first: p2,p1,p0 -> offset 1 is p1


async def test_transactions_range_is_half_open(
    api_client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
):
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = datetime(2026, 1, 3, tzinfo=UTC)
    await _seed(session_factory, "at_start", timestamp=start)
    await _seed(session_factory, "middle", timestamp=datetime(2026, 1, 2, tzinfo=UTC))
    await _seed(session_factory, "at_end", timestamp=end)

    response = await api_client.get(
        "/users/u1/transactions", params={"from": start.isoformat(), "to": end.isoformat()}
    )

    assert response.status_code == 200
    ids = {item["id"] for item in response.json()["items"]}
    assert ids == {"at_start", "middle"}


async def test_transactions_ordered_newest_first(
    api_client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
):
    for i in range(3):
        await _seed(session_factory, f"o{i}", timestamp=datetime(2026, 1, i + 1, tzinfo=UTC))

    response = await api_client.get("/users/u1/transactions")

    ids = [item["id"] for item in response.json()["items"]]
    assert ids == ["o2", "o1", "o0"]
