"""Runtime configuration, loaded from the environment by pydantic-settings.

One Settings object is shared by the API and the worker; docker-compose supplies the
differences. Every tunable that affects retry or delivery semantics lives here so the
behaviour can be reasoned about (and demoed) without touching code.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Environment-driven settings. See `.env` / docker-compose for the deployed values."""

    model_config = SettingsConfigDict(env_file=".env", env_prefix="APP_", extra="ignore")

    # --- Infrastructure -------------------------------------------------------------
    database_url: str
    redis_url: str
    rate_service_url: str

    # --- Stream / consumer group ----------------------------------------------------
    stream_name: str
    dlq_stream_name: str
    consumer_group: str
    consumer_name: str
    batch_size: int
    block_ms: int
    stream_maxlen: int | None

    # --- Delivery semantics ---------------------------------------------------------
    max_deliveries: int
    """Deliveries after which a still-failing message is dead-lettered instead of retried."""

    reclaim_interval_s: float
    """How often the worker sweeps for messages abandoned by a dead or stuck consumer."""

    min_idle_ms: int
    """How long a message must sit pending before XAUTOCLAIM will reclaim it."""

    # --- Retry knobs ----------------------------------------------------------------
    rate_attempts: int
    db_attempts: int
    retry_base_delay_s: float
    retry_max_delay_s: float

    # --- Rate lookup ----------------------------------------------------------------
    rate_cache_ttl_s: float
    """In-process cache TTL. Short enough that stopping the rate-service shows up quickly."""

    # --- Observability --------------------------------------------------------------
    worker_metrics_port: int
    log_level: str


def get_settings() -> Settings:
    """Build (and cache) the Settings singleton."""
    ...
