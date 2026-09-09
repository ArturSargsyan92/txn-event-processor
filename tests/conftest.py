"""Shared fixtures.

Fixture bodies land alongside the code they support: each implementation step fills in the ones
it needs rather than everything being built up front against interfaces that may still move.
"""

import pytest


@pytest.fixture
def settings():
    """Settings with test-safe values: tiny delays, low max_deliveries, short cache TTL."""
    ...


@pytest.fixture
def no_sleep(monkeypatch):
    """Patch asyncio.sleep to a no-op and record the delays it was asked for.

    Lets the retry tests assert the backoff schedule without actually waiting for it.
    """
    ...


@pytest.fixture
def seeded_random(monkeypatch):
    """Pin random.uniform so jittered delays are reproducible inside a test."""
    ...


@pytest.fixture
async def session_factory():
    """Async session factory against the test database, with tables created and dropped."""
    ...


@pytest.fixture
async def api_client():
    """httpx.AsyncClient bound to the app via ASGITransport, dependencies overridden."""
    ...
