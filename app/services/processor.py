"""The per-event processing step: look up a rate, convert, store idempotently.

Knows nothing about Redis Streams. The worker owns delivery concerns (ack, reclaim, DLQ); this
owns what an event *means*. That split is what makes the processor testable without a broker.
"""

from collections.abc import Callable
from datetime import UTC, datetime
from enum import StrEnum

from app.core.config import Settings
from app.core.logging import get_logger
from app.core.metrics import PROCESSING_SECONDS, RETRIES
from app.core.retry import retry_async
from app.db.models import ProcessedTransaction
from app.db.repository import TransactionRepository
from app.schemas.events import TransactionEvent
from app.services.converter import convert_to_usd
from app.services.rate_client import RateClient

logger = get_logger(__name__)


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
        with PROCESSING_SECONDS.time():
            rate = await retry_async(
                lambda: self._rates.get_rate(event.currency),
                attempts=self._settings.rate_attempts,
                base_delay=self._settings.retry_base_delay_s,
                max_delay=self._settings.retry_max_delay_s,
                on_retry=self._on_retry(event.id, "rate_lookup"),
            )

            row = ProcessedTransaction(
                id=event.id,
                user_id=event.user_id,
                amount=event.amount,
                currency=event.currency,
                rate=rate,
                amount_usd=convert_to_usd(event.amount, rate),
                timestamp=event.timestamp,
                processed_at=datetime.now(UTC),
            )

            inserted = await retry_async(
                lambda: self._repo.insert_ignore_duplicate(row),
                attempts=self._settings.db_attempts,
                base_delay=self._settings.retry_base_delay_s,
                max_delay=self._settings.retry_max_delay_s,
                on_retry=self._on_retry(event.id, "db_insert"),
            )

        return Outcome.PROCESSED if inserted else Outcome.DUPLICATE

    @staticmethod
    def _on_retry(event_id: str, operation: str) -> Callable[[int, Exception, float], None]:
        """Build a retry_async callback bound to one event and one operation.

        Increments the retry metric and logs a greppable line — this is the only place either
        happens, since retry_async itself stays free of both by design.
        """

        def on_retry(attempt: int, error: Exception, delay: float) -> None:
            RETRIES.labels(operation=operation).inc()
            logger.warning(
                "retrying %s",
                operation,
                extra={
                    "event_id": event_id,
                    "operation": operation,
                    "attempt": attempt,
                    "delay_s": round(delay, 3),
                    "error": str(error),
                },
            )

        return on_retry
