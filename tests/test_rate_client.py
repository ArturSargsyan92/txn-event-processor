"""RateClient against the real rate-service — the fault-injection test.

Runs rate_service.main.app on a real local port for each test, over real TCP, rather than
mocking httpx or using an in-process ASGI transport: httpx's client-level `timeout` is only
enforced by its real HTTP transport (ASGITransport has no timeout logic of its own at all —
confirmed by reading httpx/_transports/asgi.py), and the "timeout" fault mode is exactly the
behavior this file needs to prove actually works. This mirrors CLAUDE.md's own framing of
rate_service/ as a real network boundary to fault-inject against, not a mock.
"""

import asyncio
import socket
from collections.abc import AsyncIterator
from decimal import Decimal

import httpx
import pytest
import uvicorn

from app.core.errors import RateServiceUnavailable, UnknownCurrency
from app.services.rate_client import RateClient
from rate_service.main import app as rate_service_app


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


async def _set_fault(rate_service_url: str, mode: str, delay_s: float = 0.0) -> None:
    async with httpx.AsyncClient() as admin:
        await admin.post(f"{rate_service_url}/admin/fault", json={"mode": mode, "delay_s": delay_s})


@pytest.fixture
async def rate_service_url() -> AsyncIterator[str]:
    """The real rate-service FastAPI app, served on a real local port for one test.

    A new `uvicorn.Server` per test does *not* imply a fresh `_fault` — it's the same already-
    imported `rate_service.main` module, and its `_fault` global is process-wide, not
    per-server. Reset it explicitly before yielding, or whichever test last mutated it leaks
    its fault mode into the next one based on file order.
    """
    config = uvicorn.Config(
        rate_service_app, host="127.0.0.1", port=_free_port(), log_level="warning"
    )
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.01)

    port = server.servers[0].sockets[0].getsockname()[1]
    url = f"http://127.0.0.1:{port}"
    await _set_fault(url, mode="ok")
    try:
        yield url
    finally:
        server.should_exit = True
        await task


@pytest.fixture
async def http() -> AsyncIterator[httpx.AsyncClient]:
    # Generous timeout: only test_timeout_fault_raises_transient_error actually wants a tight
    # one. A 1.0s timeout shared by every test in this file means the cumulative overhead of
    # several real uvicorn servers starting and stopping back-to-back can make an unrelated
    # test's ordinary call spuriously exceed it.
    async with httpx.AsyncClient(timeout=10.0) as client:
        yield client


@pytest.fixture
def rate_client(http: httpx.AsyncClient, rate_service_url: str) -> RateClient:
    # Generous TTL: only test_cache_expires_after_ttl actually wants a short one. A tight TTL
    # shared across every test races against whatever real-world time an intervening call
    # (e.g. _set_fault's own HTTP round trip) happens to take.
    return RateClient(http, base_url=rate_service_url, cache_ttl_s=60.0)


async def test_returns_rate_for_known_currency(rate_client: RateClient):
    rate = await rate_client.get_rate("EUR")

    assert rate == Decimal("1.08")


async def test_unknown_currency_raises_permanent_error(rate_client: RateClient):
    with pytest.raises(UnknownCurrency):
        await rate_client.get_rate("XXX")


async def test_error_fault_raises_transient_error(rate_client: RateClient, rate_service_url: str):
    await _set_fault(rate_service_url, mode="error")

    with pytest.raises(RateServiceUnavailable):
        await rate_client.get_rate("EUR")


async def test_timeout_fault_raises_transient_error(rate_service_url: str):
    """A short-timeout client of its own, not the shared `http`/`rate_client` fixtures — this
    is the one test that actually cares about timing. The client's timeout is 0.5s; the server
    is told to sleep for 5s. The client must give up first, over a real socket, not just
    eventually get an answer."""
    await _set_fault(rate_service_url, mode="timeout", delay_s=5.0)

    async with httpx.AsyncClient(timeout=0.5) as tight_timeout_http:
        tight_timeout_client = RateClient(
            tight_timeout_http, base_url=rate_service_url, cache_ttl_s=60.0
        )
        with pytest.raises(RateServiceUnavailable):
            await tight_timeout_client.get_rate("EUR")


async def test_successful_lookup_is_cached(rate_client: RateClient, rate_service_url: str):
    first = await rate_client.get_rate("EUR")
    await _set_fault(rate_service_url, mode="error")  # would fail if this actually called out

    second = await rate_client.get_rate("EUR")

    assert first == second == Decimal("1.08")


async def test_cache_expires_after_ttl(http: httpx.AsyncClient, rate_service_url: str):
    """A short-TTL client of its own, not the shared `rate_client` fixture — this is the one
    test that actually cares about TTL timing, so it shouldn't race against every other test's
    unrelated network calls sharing the same tight window."""
    short_ttl_client = RateClient(http, base_url=rate_service_url, cache_ttl_s=0.1)
    await short_ttl_client.get_rate("EUR")
    await _set_fault(rate_service_url, mode="error")
    await asyncio.sleep(0.2)

    with pytest.raises(RateServiceUnavailable):
        await short_ttl_client.get_rate("EUR")


async def test_failures_are_not_negatively_cached(rate_client: RateClient, rate_service_url: str):
    await _set_fault(rate_service_url, mode="error")
    with pytest.raises(RateServiceUnavailable):
        await rate_client.get_rate("EUR")

    await _set_fault(rate_service_url, mode="ok")
    rate = await rate_client.get_rate("EUR")  # succeeds immediately — no TTL to wait out

    assert rate == Decimal("1.08")
