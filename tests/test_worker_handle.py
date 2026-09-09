"""The ack / dead-letter policy in `worker.handle`.

Implemented in step 7, in the same commit as the worker wiring.

`processor.process` is mocked to raise, and `StreamConsumer` is a mock, so `ack` and
`dead_letter` and their arguments are asserted directly. No Redis, no Postgres — this is a test
of the decision, not of the transport.
"""

import pytest

pytestmark = pytest.mark.skip(reason="scaffold: implemented in step 7")


async def test_success_acks() -> None:
    """A PROCESSED outcome acks the message and dead-letters nothing."""


async def test_duplicate_acks() -> None:
    """DUPLICATE is a success, not a failure: it acks too.

    Under at-least-once this is the normal path for a redelivered event, and treating it as an
    error would dead-letter perfectly good work.
    """


async def test_permanent_error_dead_letters_with_original_reason() -> None:
    """PermanentError -> dead_letter(msg, reason) carrying the original error text.

    The reason must survive to the DLQ entry; 'unknown currency: XYZ' is what makes a parked
    message diagnosable later. No ack — dead_letter does that itself.
    """


async def test_transient_error_under_limit_leaves_message_pending() -> None:
    """TransientError below max_deliveries -> neither ack nor dead_letter is called.

    The assertion is that *nothing happens*. Leaving the message in the pending-entries list is
    what hands it to `claim_stale` for redelivery, and it is invisible in the code — a refactor
    that "tidied up" by acking here would silently start dropping events under load.
    """


async def test_transient_error_at_limit_dead_letters() -> None:
    """delivery_count == max_deliveries -> dead_letter(msg, reason='max deliveries exceeded').

    Boundary case: at the limit, not merely past it.
    """


async def test_transient_error_over_limit_dead_letters() -> None:
    """delivery_count above max_deliveries dead-letters as well, in case a reclaim overshoots."""
