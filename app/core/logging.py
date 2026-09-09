"""Structured JSON logging.

The worker's logs are the primary evidence during the failure demo — backoff attempts, reclaims,
and dead-letters all need to be greppable by event id, so ids go in as fields rather than being
interpolated into the message.
"""

import logging


def configure_logging(level: str) -> None:
    """Install a JSON formatter on the root logger and quiet the noisier libraries."""
    ...


def get_logger(name: str) -> logging.Logger:
    """Return a module logger. Bind per-event context with `logger.info(..., extra={...})`."""
    ...
