"""Shared fixtures.

Fixture bodies land alongside the code they support: each implementation step fills in the ones
it needs rather than everything being built up front against interfaces that may still move.
"""

import asyncio
import os
import random
from collections.abc import AsyncIterator

import pytest
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlmodel import SQLModel

from app.core.config import Settings
from app.db.engine import create_engine, create_session_factory, init_models
from app.db.errors import DB_EXCEPTIONS
from app.queue.client import create_redis


@pytest.fixture
def settings() -> Settings:
    """Settings with test-safe values: tiny delays, low max_deliveries, short cache TTL.

    Bypasses `get_settings()`'s cache entirely — constructed directly so each test gets an
    isolated instance regardless of import order.

    `database_url`/`redis_url` fall back to plain localhost URLs rather than Settings' own
    defaults (docker-compose's `postgres`/`redis` services) — these fixtures drop every table
    and flush the whole database they touch, so those defaults must never be reachable by
    accident. Read directly from os.environ rather than left to Settings' own env parsing:
    passing either as a constructor kwarg at all, even conditionally, takes precedence over
    the matching APP_* env var in pydantic-settings' source order, so building the kwarg from
    the environment ourselves is what keeps the override actually overridable. `redis_url`
    defaults to db index 1, not 0, as one more margin against colliding with a Redis a
    developer happens to already be running locally for something else.
    """
    return Settings(
        database_url=os.environ.get(
            "APP_DATABASE_URL", "postgresql+asyncpg://postgres@localhost:5432/txn_events_test"
        ),
        redis_url=os.environ.get("APP_REDIS_URL", "redis://localhost:6379/1"),
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
async def postgres_engine(settings: Settings) -> AsyncIterator[AsyncEngine]:
    """A real, connected engine against a dedicated test database — no tables created.

    Needs a reachable Postgres: point APP_DATABASE_URL at a *dedicated test database* — this
    fixture drops every table after each test, so pointing it at docker-compose's real
    `postgres` service would wipe the app's own data. Skips with a clear reason if nothing is
    reachable, so `uv run pytest` still runs everywhere; only the tests that need a real
    database are skipped.

    Kept separate from `session_factory` (below) so a test can exercise `wait_until_ready`
    against a database that genuinely has no tables yet — the exact case its retry loop exists
    to wait out.
    """
    engine = create_engine(settings.database_url)
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except DB_EXCEPTIONS as exc:
        await engine.dispose()
        pytest.skip(f"no Postgres reachable at {settings.database_url!r}: {exc}")

    try:
        yield engine
    finally:
        async with engine.begin() as conn:
            await conn.run_sync(SQLModel.metadata.drop_all)
        await engine.dispose()


@pytest.fixture
async def session_factory(
    postgres_engine: AsyncEngine,
) -> async_sessionmaker[AsyncSession]:
    """Async session factory against the test database, with tables created before the test.

    Teardown (drop_all + dispose) happens once, in `postgres_engine` — this fixture only adds
    the schema on top of an already-verified-reachable engine.
    """
    await init_models(postgres_engine)
    return create_session_factory(postgres_engine)


@pytest.fixture
async def redis(settings: Settings) -> AsyncIterator[Redis]:
    """A real, connected Redis client — flushed (FLUSHDB) after each test.

    Needs a reachable Redis: point APP_REDIS_URL at a *dedicated test db index* — this
    fixture wipes the whole database it's pointed at. Skips with a clear reason if nothing is
    reachable, so `uv run pytest` still runs everywhere; only the tests that need a real queue
    are skipped.
    """
    client = create_redis(settings.redis_url)
    try:
        await client.ping()
    except (OSError, RedisError) as exc:
        await client.aclose()
        pytest.skip(f"no Redis reachable at {settings.redis_url!r}: {exc}")

    try:
        yield client
    finally:
        await client.flushdb()
        await client.aclose()


@pytest.fixture
async def api_client():
    """httpx.AsyncClient bound to the app via ASGITransport, dependencies overridden."""
    ...
