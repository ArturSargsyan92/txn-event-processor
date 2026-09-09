"""Async engine and session factory.

`init_models` is called from exactly one place — the API's lifespan. If both containers ran
create_all at boot they would race and can deadlock on the same CREATE TABLE; the worker instead
retry-connects until the tables exist, which it can already do with `retry_async`.
"""

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker


def create_engine(database_url: str) -> AsyncEngine:
    """Build the asyncpg engine with a pool sized for this service's concurrency."""
    ...


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker:
    """Session factory with expire_on_commit=False, so objects stay usable after commit."""
    ...


async def init_models(engine: AsyncEngine) -> None:
    """Run SQLModel.metadata.create_all. API process only — see the module docstring."""
    ...


async def wait_until_ready(engine: AsyncEngine, *, attempts: int, base_delay: float) -> None:
    """Block until a trivial SELECT succeeds, so the worker can start before Postgres is up."""
    ...
