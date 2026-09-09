"""Client for the rate-service, with a small in-process TTL cache.

This is the seam the failure demo uses: stop the `rate-service` container (or POST
/admin/fault on it) and, once the TTL lapses, every lookup starts raising TransientError.

The cache is a plain dict rather than Redis on purpose. At ~100 events/sec across a handful of
currencies an uncached client would issue ~100 HTTP calls/sec for the same few answers, but a
*shared* cache would buy that back at the cost of a distributed-staleness story. Per-worker
caching is the honest trade-off at this scale; N replicas doing N cold fetches is where Redis
would start to earn its place.
"""

from decimal import Decimal

import httpx


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
        ...

    def _cached(self, currency: str) -> Decimal | None:
        """Return the rate if present and unexpired, else None."""
        ...

    def _store(self, currency: str, rate: Decimal) -> None:
        """Cache `rate` until now + cache_ttl_s."""
        ...
