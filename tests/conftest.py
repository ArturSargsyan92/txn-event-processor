"""Shared fixtures.

Fixture bodies land alongside the code they support: each implementation step fills in the ones
it needs rather than everything being built up front against interfaces that may still move.
"""

import asyncio
import random
from collections.abc import AsyncIterator

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlmodel import SQLModel

from app.core.config import Settings
from app.core.errors import DatabaseUnavailable
from app.db.engine import create_engine, create_session_factory, init_models, wait_until_ready


@pytest.fixture
def settings() -> Settings:
    """Settings with test-safe values: tiny delays, low max_deliveries, short cache TTL.

    Bypasses `get_settings()`'s cache entirely — constructed directly so each test gets an
    isolated instance regardless of import order.
    """
    return Settings(
        retry_base_delay_s=0.01,
        retry_max_delay_s=0.08,
        rate_attempts=3,
        db_attempts=3,
        max_deliveries=2,
        rate_cache_ttl_s=0.05,
        reclaim_interval_s=0.05,
        min_idle_ms=50,
    )


@pytest.fixture
def no_sleep(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Patch asyncio.sleep to a no-op and record the delays it was asked for.

    Lets the retry tests assert the backoff schedule without actually waiting for it.
    """
    delays: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        delays.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    return delays


@pytest.fixture
def seeded_random(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin random.uniform so jittered delays are reproducible inside a test.

    Returns the upper bound rather than a fixed number, so the delay-growth and
    delay-cap tests can assert against `compute_delay`'s ceiling directly.
    """
    monkeypatch.setattr(random, "uniform", lambda low, high: high)


@pytest.fixture
async def session_factory(
    settings: Settings,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """Async session factory against the test database, with tables created and dropped.

    Needs a reachable Postgres: point APP_DATABASE_URL at one (docker-compose's `postgres`
    service, or a local instance) before running the DB-backed tests. Skips with a clear reason
    if none is reachable, so `uv run pytest` still runs everywhere — only the tests that need a
    real database are skipped.
    """
    engine = create_engine(settings.database_url)
    try:
        await wait_until_ready(engine, attempts=1, base_delay=0)
    except DatabaseUnavailable as exc:
        await engine.dispose()
        pytest.skip(f"no Postgres reachable at {settings.database_url!r}: {exc}")

    await init_models(engine)
    factory = create_session_factory(engine)
    try:
        yield factory
    finally:
        async with engine.begin() as conn:
            await conn.run_sync(SQLModel.metadata.drop_all)
        await engine.dispose()


@pytest.fixture
async def api_client():
    """httpx.AsyncClient bound to the app via ASGITransport, dependencies overridden."""
    ...
