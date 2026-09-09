"""HTTP contract. Implemented in step 8, alongside app/main.py and app/api/routes.py.

Needs a real Postgres and Redis — see the `api_client`, `session_factory`, and `redis`
fixtures in conftest.py. No worker runs in these tests: the read-endpoint tests seed rows
directly via TransactionRepository (the same thing the worker itself would eventually write),
and the ingest-endpoint tests inspect the raw stream — this file is testing the HTTP contract,
not re-testing dedup or conversion, which already have their own dedicated test files.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

import httpx
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.core.errors import DatabaseUnavailable
from app.db.models import ProcessedTransaction
from app.db.repository import TransactionRepository
from app.schemas.events import StreamEnvelope


@asynccontextmanager
async def _client_with_overrides(overrides: dict) -> AsyncIterator[httpx.AsyncClient]:
    """A client like the `api_client` fixture, but wired to fakes instead of real infra.

    The health/error-path tests below need Redis or Postgres to actually fail, which the
    `api_client` fixture's real connections can't do on demand — this builds a one-off client
    against `app.main.app` with only the given dependencies overridden.

    Restores the *prior* overrides on exit rather than clearing unconditionally: `app.main.app`
    is a module-global, so an unconditional `.clear()` would silently drop anything an outer
    `api_client` had already wired up if this were ever nested inside it — restoring is what
    makes this helper safe to use in isolation regardless of what else is touching the app.
    """
    from app.main import app

    previous = app.dependency_overrides.copy()
    app.dependency_overrides.update(overrides)
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield client
    finally:
        app.dependency_overrides = previous


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


async def test_post_event_publishes_envelope_to_stream(
    api_client: httpx.AsyncClient, redis: Redis, settings: Settings
):
    response = await api_client.post("/events", json=_payload("t1", amount="42.50", currency="EUR"))
    stream_id = response.json()["stream_id"]

    entries = await redis.xrange(settings.stream_name)

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


async def test_post_event_normalizes_lowercase_currency(
    api_client: httpx.AsyncClient, redis: Redis, settings: Settings
):
    response = await api_client.post("/events", json=_payload("t1", currency="eur"))
    stream_id = response.json()["stream_id"]

    entries = await redis.xrange(settings.stream_name)
    entry_id, fields = entries[0]
    assert entry_id == stream_id
    event = StreamEnvelope.from_fields(fields).to_event()
    assert event.currency == "EUR"


async def test_liveness_reports_alive(api_client: httpx.AsyncClient):
    response = await api_client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "alive"}


async def test_readiness_reports_ready_when_redis_and_db_are_up(api_client: httpx.AsyncClient):
    response = await api_client.get("/health/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ready"}


async def test_readiness_is_503_when_redis_is_unreachable():
    dead_redis = AsyncMock()
    dead_redis.ping.side_effect = RedisError("connection refused")

    from app.api.deps import get_redis, get_repository

    async with _client_with_overrides(
        {get_redis: lambda: dead_redis, get_repository: lambda: AsyncMock()}
    ) as client:
        response = await client.get("/health/ready")

    assert response.status_code == 503
    assert "redis unreachable" in response.json()["detail"]


async def test_readiness_is_503_when_database_is_unreachable():
    dead_repo = AsyncMock()
    dead_repo.user_summary.side_effect = DatabaseUnavailable("connection refused")

    from app.api.deps import get_redis, get_repository

    async with _client_with_overrides(
        {get_redis: lambda: AsyncMock(), get_repository: lambda: dead_repo}
    ) as client:
        response = await client.get("/health/ready")

    assert response.status_code == 503
    assert "database unreachable" in response.json()["detail"]


async def test_summary_is_503_when_database_is_unavailable():
    """Distinct from the /health/ready test above: this pins the global exception handler
    in app/main.py, which is what the two *read* endpoints actually rely on — not the
    readiness probe's own try/except, which has its own separate error text."""
    dead_repo = AsyncMock()
    dead_repo.user_summary.side_effect = DatabaseUnavailable("connection refused")

    from app.api.deps import get_repository

    async with _client_with_overrides({get_repository: lambda: dead_repo}) as client:
        response = await client.get("/users/u1/summary")

    assert response.status_code == 503
    assert "database unavailable" in response.json()["detail"]


async def test_metrics_endpoint_is_mounted_and_reachable(api_client: httpx.AsyncClient):
    # Starlette's Mount 307-redirects a bare "/metrics" to "/metrics/" — hit the slash form
    # directly to test the mount itself rather than the redirect.
    response = await api_client.get("/metrics/")

    assert response.status_code == 200
    assert "events_published_total" in response.text


def _events_published_total(metrics_text: str) -> float:
    for line in metrics_text.splitlines():
        if line.startswith("events_published_total "):
            return float(line.split()[1])
    raise AssertionError("events_published_total not found in /metrics output")


async def test_post_event_increments_events_published(api_client: httpx.AsyncClient):
    """The mount test above only proves the metric exists — this pins EVENTS_PUBLISHED
    actually incrementing on a successful publish, which is the part that was silently
    missing until the step-8 review-fix commit added `EVENTS_PUBLISHED.inc()`."""
    before = _events_published_total((await api_client.get("/metrics/")).text)

    await api_client.post("/events", json=_payload("metrics-check"))

    after = _events_published_total((await api_client.get("/metrics/")).text)
    assert after == before + 1


async def test_post_event_is_503_when_queue_is_unreachable():
    """No real Redis needed: the producer dependency is faked directly, so this runs even
    in the no-infra pytest mode — the queue-down 503 is the single most important new
    behavior in the step-8 review-fix commit and had no dedicated test until now."""
    dead_producer = AsyncMock()
    dead_producer.publish.side_effect = RedisError("connection refused")

    from app.api.deps import get_producer

    async with _client_with_overrides({get_producer: lambda: dead_producer}) as client:
        response = await client.post("/events", json=_payload("t-queue-down"))

    assert response.status_code == 503
    assert "queue unavailable" in response.json()["detail"]


async def test_transactions_naive_query_bounds_are_treated_as_utc():
    """Deterministic version of the from/to UTC-coercion fix: the repo is faked so the
    assertion doesn't depend on the test process's own timezone to reproduce the bug this
    guards against (a naive bound silently interpreted in the DB session's timezone)."""
    fake_repo = AsyncMock()
    fake_repo.list_user_transactions.return_value = ([], 0)

    from app.api.deps import get_repository

    async with _client_with_overrides({get_repository: lambda: fake_repo}) as client:
        response = await client.get(
            "/users/u1/transactions", params={"from": "2026-01-01T00:00:00"}
        )

    assert response.status_code == 200
    _, kwargs = fake_repo.list_user_transactions.call_args
    assert kwargs["start"] == datetime(2026, 1, 1, tzinfo=UTC)
