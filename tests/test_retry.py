"""Backoff behaviour. Implemented in step 2, alongside app/core/retry.py."""

import pytest

pytestmark = pytest.mark.skip(reason="scaffold: implemented in step 2")


async def test_returns_first_success_without_sleeping() -> None:
    """The happy path costs nothing: one call, no delay."""


async def test_retries_transient_then_succeeds() -> None:
    """A transient failure followed by a success returns the value."""


async def test_reraises_after_attempts_exhausted() -> None:
    """The last error propagates so the caller can choose pending-vs-DLQ."""


async def test_permanent_error_is_not_retried() -> None:
    """Anything outside retry_on propagates on the first raise, unretried."""


def test_delay_grows_exponentially() -> None:
    """Uncapped, the ceiling doubles per attempt."""


def test_delay_is_capped_at_max() -> None:
    """The ceiling stops at max_delay however many attempts have passed."""


def test_jitter_stays_within_bounds() -> None:
    """Full jitter: every delay falls in [0, ceiling] across many samples."""


async def test_on_retry_receives_attempt_error_and_delay() -> None:
    """The callback gets what the metrics and log lines need."""
