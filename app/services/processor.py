"""The per-event processing step: look up a rate, convert, store idempotently.

Knows nothing about Redis Streams. The worker owns delivery concerns (ack, reclaim, DLQ); this
owns what an event *means*. That split is what makes the processor testable without a broker.
"""

from enum import StrEnum

from app.core.config import Settings
from app.db.repository import TransactionRepository
from app.schemas.events import TransactionEvent
from app.services.rate_client import RateClient


class Outcome(StrEnum):
    """What became of an event. Both values are successes — neither should be retried."""

    PROCESSED = "processed"
    DUPLICATE = "duplicate"
    """The id was already stored. Expected under at-least-once delivery, not an error."""


class EventProcessor:
    """Wires the rate lookup and the repository together behind retry."""

    def __init__(
        self,
        rates: RateClient,
        repo: TransactionRepository,
        settings: Settings,
    ) -> None:
        self._rates = rates
        self._repo = repo
        self._settings = settings

    async def process(self, event: TransactionEvent) -> Outcome:
        """Look up the rate, convert to USD, and insert.

        The rate lookup is retried to exhaustion *before* the insert is attempted, so an event is
        never half-applied. A crash between the two steps is safe: the insert is idempotent, so
        redelivery simply redoes the lookup and lands the same row.

        Raises:
            TransientError: the downstream is still unavailable after all attempts. The caller
                decides whether to leave the message pending or dead-letter it.
            PermanentError: the event cannot be processed at all.
        """
        ...
