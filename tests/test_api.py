"""HTTP contract. Implemented in step 8, alongside app/main.py and app/api/routes.py."""

import pytest

pytestmark = pytest.mark.skip(reason="scaffold: implemented in step 8")


async def test_post_event_returns_202_and_stream_id() -> None:
    """A valid event is accepted with 202 and its stream id echoed back."""


async def test_post_event_publishes_envelope_to_stream() -> None:
    """The payload actually lands on the stream and round-trips back to the same event."""


async def test_post_event_rejects_invalid_payload() -> None:
    """A missing or malformed field is a 422 before anything is queued."""


async def test_post_event_does_not_touch_the_database() -> None:
    """The write path is queue-only; that is what keeps the API flat under a burst."""


async def test_summary_totals_usd_and_counts() -> None:
    """Summary sums amount_usd and counts rows for the user."""


async def test_summary_for_unknown_user_is_zero() -> None:
    """No transactions yet is a normal state: (0, 0), not a 404."""


async def test_transactions_are_paginated() -> None:
    """limit/offset page cleanly and `total` reports the unpaginated count."""


async def test_transactions_range_is_half_open() -> None:
    """A row exactly on `to` is excluded and a row exactly on `from` is included."""


async def test_transactions_ordered_newest_first() -> None:
    """Ordering is by timestamp descending, stable across pages."""
