"""Runtime configuration, loaded from the environment by pydantic-settings.

One Settings object is shared by the API and the worker; docker-compose supplies the
differences. Every tunable that affects retry or delivery semantics lives here so the
behaviour can be reasoned about (and demoed) without touching code.

Defaults point at the docker-compose service names and are tuned for a live demo (short
TTLs, low retry/attempt counts) rather than production; override via APP_-prefixed
environment variables or a .env file for anything else.
"""

import socket
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Environment-driven settings. See `.env` / docker-compose for the deployed values."""

    # extra="forbid": a typo'd env var (APP_DATABSE_URL) fails at startup instead of being
    # silently dropped and surfacing later as an unexplained connection error.
    model_config = SettingsConfigDict(env_file=".env", env_prefix="APP_", extra="forbid")

    # --- Infrastructure -------------------------------------------------------------
    database_url: str = "postgresql+asyncpg://postgres:postgres@postgres:5432/txn_events"
    redis_url: str = "redis://redis:6379/0"
    rate_service_url: str = "http://rate-service:8000"

    # --- Stream / consumer group ----------------------------------------------------
    stream_name: str = "transactions"
    consumer_group: str = "transaction-processors"
    consumer_name: str = Field(default_factory=socket.gethostname)
    """Defaults to the container hostname, so `--scale worker=N` gives each replica a
    distinct consumer name for free — Docker assigns each container its own hostname."""

    batch_size: int = 10
    block_ms: int = 5000
    stream_maxlen: int | None = 100_000

    @property
    def dlq_stream_name(self) -> str:
        """Always `{stream_name}:dlq` — not an independent setting, so overriding
        stream_name alone can never leave the DLQ pointing at the wrong stream."""
        return f"{self.stream_name}:dlq"

    # --- Delivery semantics ---------------------------------------------------------
    max_deliveries: int = Field(default=5, ge=1)
    """Deliveries after which a still-failing message is dead-lettered instead of retried."""

    reclaim_interval_s: float = 5.0
    """How often the worker sweeps for messages abandoned by a dead or stuck consumer."""

    min_idle_ms: int = 15_000
    """How long a message must sit pending before XAUTOCLAIM will reclaim it.

    Kept low (~15s) so the demo doesn't have long dead air between a fault and the
    visible reclaim.
    """

    # --- Retry knobs ----------------------------------------------------------------
    rate_attempts: int = Field(default=4, ge=1)
    db_attempts: int = Field(default=4, ge=1)
    retry_base_delay_s: float = 0.2
    retry_max_delay_s: float = 5.0

    db_startup_attempts: int = Field(default=20, ge=1)
    db_startup_base_delay_s: float = 1.0
    """A separate, far more generous budget than db_attempts/retry_base_delay_s — those are
    tuned for one event's per-attempt retry inside a delivery; this is for wait_until_ready at
    process startup, waiting on an entirely different container to finish booting and run
    create_all. Worst case with these defaults is a couple of minutes, not ~1.4s."""

    # --- Rate lookup ----------------------------------------------------------------
    rate_cache_ttl_s: float = 5.0
    """In-process cache TTL. Short enough that stopping the rate-service shows up quickly."""

    rate_service_timeout_s: float = 2.0
    """HTTP timeout for a single call to the rate-service. Short: a hung rate-service should
    surface as a retryable failure quickly, not stall the worker waiting on one request."""

    # --- Observability --------------------------------------------------------------
    worker_metrics_port: int = 9100
    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    """Build (and cache) the Settings singleton.

    Cached with lru_cache rather than a module-level global so tests can bypass the
    cache entirely by constructing Settings(...) directly instead of touching it.
    """
    return Settings()
