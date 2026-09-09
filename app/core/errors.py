"""Exception taxonomy.

Every retry and dead-letter decision in the system branches on exactly one thing: whether an
error is a `TransientError` (worth retrying — the downstream may recover) or a `PermanentError`
(retrying can never help — the event itself is unprocessable). Keeping that split in one place
is what lets `retry_async` and `worker.handle` stay small.
"""


class AppError(Exception):
    """Base class for every error this service raises deliberately."""


class TransientError(AppError):
    """A failure that may succeed on a later attempt: timeouts, 5xx, dropped connections."""


class PermanentError(AppError):
    """A failure that will recur identically on every attempt: bad data, unknown currency."""


class RateServiceUnavailable(TransientError):
    """The rate-service did not answer, or answered 5xx."""


class UnknownCurrency(PermanentError):
    """The rate-service has no rate for this currency (404)."""


class DatabaseUnavailable(TransientError):
    """Postgres refused or dropped the connection."""
