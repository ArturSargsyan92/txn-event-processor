"""Consumer-group mechanics. Implemented in step 5, alongside app/queue/consumer.py."""

import pytest

pytestmark = pytest.mark.skip(reason="scaffold: implemented in step 5")


async def test_ensure_group_is_idempotent() -> None:
    """Calling it twice is fine — BUSYGROUP is swallowed, as replicas all call it at boot."""


async def test_read_returns_published_events() -> None:
    """A published envelope comes back parsed, with its stream id."""


async def test_ack_removes_from_pending() -> None:
    """After ack, XPENDING no longer lists the message."""


async def test_unacked_message_stays_pending() -> None:
    """Not acking leaves the message pending — the precondition for reclaim."""


async def test_claim_stale_reclaims_idle_message() -> None:
    """XAUTOCLAIM returns a message left pending past min_idle_ms."""


async def test_claim_stale_reports_delivery_count() -> None:
    """Reclaimed messages carry an incremented delivery count for the DLQ cutoff."""


async def test_dead_letter_writes_to_dlq_and_acks() -> None:
    """The message lands on the DLQ stream with its reason and leaves the pending list."""


async def test_unparseable_payload_is_dead_lettered() -> None:
    """A malformed entry never reaches the processor."""


async def test_lag_reports_undelivered_entries() -> None:
    """Lag tracks entries published but not yet delivered to the group."""
