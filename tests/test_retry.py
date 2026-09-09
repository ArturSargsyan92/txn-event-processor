"""Backoff behaviour. Implemented in step 2, alongside app/core/retry.py."""

import pytest

from app.core.errors import PermanentError, TransientError
from app.core.retry import compute_delay, retry_async


def _flaky(fail_times: int, result: str = "ok"):
    """Build an async fn that raises TransientError `fail_times` times, then returns `result`.

    Exposes `.calls` so a test can assert exactly how many times it was invoked.
    """
    calls = {"count": 0}

    async def fn() -> str:
        calls["count"] += 1
        if calls["count"] <= fail_times:
            raise TransientError(f"transient failure #{calls['count']}")
        return result

    fn.calls = calls
    return fn


async def test_returns_first_success_without_sleeping(no_sleep):
    async def fn() -> str:
        return "ok"

    result = await retry_async(fn, attempts=3, base_delay=0.01, max_delay=0.1)

    assert result == "ok"
    assert no_sleep == []


async def test_retries_transient_then_succeeds(no_sleep):
    fn = _flaky(fail_times=1)

    result = await retry_async(fn, attempts=3, base_delay=0.01, max_delay=0.1)

    assert result == "ok"
    assert fn.calls["count"] == 2
    assert len(no_sleep) == 1


async def test_reraises_after_attempts_exhausted(no_sleep):
    fn = _flaky(fail_times=99)

    with pytest.raises(TransientError):
        await retry_async(fn, attempts=3, base_delay=0.01, max_delay=0.1)

    assert fn.calls["count"] == 3
    assert len(no_sleep) == 2


async def test_permanent_error_is_not_retried(no_sleep):
    calls = {"count": 0}

    async def fn() -> str:
        calls["count"] += 1
        raise PermanentError("bad data")

    with pytest.raises(PermanentError):
        await retry_async(fn, attempts=5, base_delay=0.01, max_delay=0.1)

    assert calls["count"] == 1
    assert no_sleep == []


def test_delay_grows_exponentially(seeded_random):
    first = compute_delay(1, base_delay=0.1, max_delay=100.0)
    second = compute_delay(2, base_delay=0.1, max_delay=100.0)
    third = compute_delay(3, base_delay=0.1, max_delay=100.0)

    assert first == pytest.approx(0.1)
    assert second == pytest.approx(0.2)
    assert third == pytest.approx(0.4)


def test_delay_is_capped_at_max(seeded_random):
    delay = compute_delay(10, base_delay=0.1, max_delay=1.0)

    assert delay == pytest.approx(1.0)


def test_jitter_stays_within_bounds():
    for attempt in range(1, 6):
        ceiling = min(5.0, 0.1 * 2 ** (attempt - 1))
        for _ in range(50):
            delay = compute_delay(attempt, base_delay=0.1, max_delay=5.0)
            assert 0.0 <= delay <= ceiling


async def test_on_retry_receives_attempt_error_and_delay(no_sleep):
    fn = _flaky(fail_times=1)
    seen = []

    def on_retry(attempt: int, error: Exception, delay: float) -> None:
        seen.append((attempt, error, delay))

    await retry_async(fn, attempts=3, base_delay=0.01, max_delay=0.1, on_retry=on_retry)

    assert len(seen) == 1
    attempt, error, delay = seen[0]
    assert attempt == 1
    assert isinstance(error, TransientError)
    assert delay == no_sleep[0]
