"""Async engine and session factory.

`init_models` is called from exactly one place — the API's lifespan. If both containers ran
create_all at boot they would race and can deadlock on the same CREATE TABLE; the worker instead
retry-connects until the tables exist, which it can already do with `retry_async`.
"""

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlmodel import SQLModel

from app.core.errors import DatabaseUnavailable
from app.core.retry import retry_async


def create_engine(database_url: str) -> AsyncEngine:
    """Build the asyncpg engine with a pool sized for this service's concurrency.

    A handful of connections is plenty at the assignment's ~100/sec target: the API never
    touches Postgres on its write path, and the worker processes one message at a time.
    """
    return create_async_engine(database_url, pool_size=5, max_overflow=5, pool_pre_ping=True)


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Session factory with expire_on_commit=False, so objects stay usable after commit."""
    return async_sessionmaker(engine, expire_on_commit=False)


async def init_models(engine: AsyncEngine) -> None:
    """Run SQLModel.metadata.create_all. API process only — see the module docstring."""
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)


async def wait_until_ready(engine: AsyncEngine, *, attempts: int, base_delay: float) -> None:
    """Block until a trivial SELECT succeeds, so the worker can start before Postgres is up.

    Runs once, at process startup — the cap is a fixed 5s rather than a Settings knob, since
    there's nothing here to tune for the fault-injection demo.
    """

    async def ping() -> None:
        try:
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
        except (OSError, SQLAlchemyError) as exc:
            raise DatabaseUnavailable(str(exc)) from exc

    await retry_async(
        ping,
        attempts=attempts,
        base_delay=base_delay,
        max_delay=5.0,
        retry_on=(DatabaseUnavailable,),
    )
