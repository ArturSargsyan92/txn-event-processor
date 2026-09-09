"""Structured JSON logging.

The worker's logs are the primary evidence during the failure demo — backoff attempts, reclaims,
and dead-letters all need to be greppable by event id, so ids go in as fields rather than being
interpolated into the message.
"""

import json
import logging
import sys
from datetime import UTC, datetime

_RESERVED_RECORD_FIELDS = frozenset(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {
    "message"
}
"""Attributes every LogRecord carries. Anything else on a record came from `extra={...}`."""


class _JSONFormatter(logging.Formatter):
    """Renders one log record as one JSON line, `extra` fields included verbatim."""

    def format(self, record: logging.LogRecord) -> str:
        extras = {k: v for k, v in record.__dict__.items() if k not in _RESERVED_RECORD_FIELDS}

        # extras first, fixed keys last: a caller's extra={"level": ...} must never be able
        # to shadow the record's real level (or timestamp, logger, message).
        payload: dict[str, object] = {
            **extras,
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(
                timespec="milliseconds"
            ),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str)


def configure_logging(level: str) -> None:
    """Install a JSON formatter on the root logger and quiet the noisier libraries."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JSONFormatter())

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level.upper())

    # These log every request/connection at INFO and would otherwise drown out the
    # worker's own lines (retries, reclaims, dead-letters) during the failure demo.
    for noisy_logger in ("uvicorn.access", "httpx", "httpcore"):
        logging.getLogger(noisy_logger).setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Return a module logger. Bind per-event context with `logger.info(..., extra={...})`."""
    return logging.getLogger(name)
