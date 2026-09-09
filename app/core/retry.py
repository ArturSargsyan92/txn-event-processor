"""Exponential backoff with full jitter.

Deliberately dependency-free: no metrics imports, no logging, no knowledge of what it is
retrying. The caller passes `on_retry` to record attempts, which keeps this unit-testable by
patching `asyncio.sleep` alone.

This is the *inner* of the service's two retry layers — it absorbs the sub-second blip inside a
single delivery. The outer layer is `worker.handle` leaving a message unacked so XAUTOCLAIM
redelivers it later; that one covers outages measured in minutes.
"""

import asyncio
import random
from collections.abc import Awaitable, Callable

from app.core.errors import TransientError

type OnRetry = Callable[[int, Exception, float], None]
"""Called as (attempt_number, error, delay_seconds) before each sleep."""


def compute_delay(attempt: int, base_delay: float, max_delay: float) -> float:
    """Full jitter: uniform(0, min(max_delay, base_delay * 2 ** (attempt - 1))).

    Full rather than equal jitter so that a fleet of workers retrying after a shared outage
    spreads out instead of stampeding the recovered downstream in lockstep.

    `attempt` is 1 for the delay after the first failure, 2 after the second, and so on —
    it counts failures, not the calls made.
    """
    ceiling = min(max_delay, base_delay * 2 ** (attempt - 1))
    return random.uniform(0, ceiling)


async def retry_async[T](
    fn: Callable[[], Awaitable[T]],
    *,
    attempts: int,
    base_delay: float,
    max_delay: float,
    retry_on: tuple[type[Exception], ...] = (TransientError,),
    on_retry: OnRetry | None = None,
) -> T:
    """Await `fn()`, retrying up to `attempts` times on `retry_on`.

    Anything outside `retry_on` (notably PermanentError) propagates immediately — retrying it
    would just burn the delivery budget. Once attempts run out the last error is re-raised, so
    the caller still sees a TransientError and can decide between "leave pending" and "DLQ".
    """
    for attempt in range(1, attempts + 1):
        try:
            return await fn()
        except retry_on as exc:
            if attempt >= attempts:
                raise
            delay = compute_delay(attempt, base_delay, max_delay)
            if on_retry is not None:
                on_retry(attempt, exc, delay)
            await asyncio.sleep(delay)
    # Unreachable: attempts >= 1 means the loop above always either returns or raises.
    raise AssertionError("retry_async requires attempts >= 1")
