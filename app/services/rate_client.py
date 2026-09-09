"""Client for the rate-service, with a small in-process TTL cache.

This is the seam the failure demo uses: stop the `rate-service` container (or POST
/admin/fault on it) and, once the TTL lapses, every lookup starts raising TransientError.

The cache is a plain dict rather than Redis on purpose. At ~100 events/sec across a handful of
currencies an uncached client would issue ~100 HTTP calls/sec for the same few answers, but a
*shared* cache would buy that back at the cost of a distributed-staleness story. Per-worker
caching is the honest trade-off at this scale; N replicas doing N cold fetches is where Redis
would start to earn its place.
"""

import time
from decimal import Decimal

import httpx

from app.core.errors import RateServiceUnavailable, UnknownCurrency


class RateClient:
    """Fetches USD-per-unit rates, memoised for `cache_ttl_s`."""

    def __init__(
        self,
        http: httpx.AsyncClient,
        *,
        base_url: str,
        cache_ttl_s: float,
    ) -> None:
        self._http = http
        self._base_url = base_url
        self._cache_ttl_s = cache_ttl_s
        self._cache: dict[str, tuple[Decimal, float]] = {}
        """currency -> (rate, expires_at on the monotonic clock)."""

    async def get_rate(self, currency: str) -> Decimal:
        """Return the USD rate for `currency`, from cache when fresh.

        Raises:
            RateServiceUnavailable: timeout, connection error, or 5xx — retryable.
            UnknownCurrency: 404 — the event is unprocessable, no amount of retrying helps.

        Only successful lookups are cached. Failures are never negatively cached, so recovery is
        immediate rather than waiting out a TTL.
        """
        cached = self._cached(currency)
        if cached is not None:
            return cached

        try:
            response = await self._http.get(f"{self._base_url}/rates/{currency}")
        except httpx.RequestError as exc:
            # Covers every network/timeout failure httpx raises before a response arrives at
            # all (connection refused, DNS failure, read/connect timeout) — all one hierarchy.
            raise RateServiceUnavailable(f"rate-service unreachable: {exc}") from exc

        if response.status_code == 404:
            raise UnknownCurrency(f"no rate for currency {currency!r}")
        if response.status_code >= 500:
            raise RateServiceUnavailable(f"rate-service returned {response.status_code}")
        response.raise_for_status()  # anything else unexpected propagates unclassified

        rate = Decimal(response.json()["rate"])
        self._store(currency, rate)
        return rate

    def _cached(self, currency: str) -> Decimal | None:
        """Return the rate if present and unexpired, else None."""
        entry = self._cache.get(currency)
        if entry is None:
            return None
        rate, expires_at = entry
        if time.monotonic() >= expires_at:
            return None
        return rate

    def _store(self, currency: str, rate: Decimal) -> None:
        """Cache `rate` until now + cache_ttl_s."""
        self._cache[currency] = (rate, time.monotonic() + self._cache_ttl_s)
