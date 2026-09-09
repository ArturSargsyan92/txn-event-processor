"""FastAPI dependencies.

Long-lived objects (the Redis client, the engine) are created once in the lifespan and stashed
on `app.state`; these functions just hand them out, which is also what makes them trivial to
override in tests.
"""

from typing import Annotated

from fastapi import Depends, Request
from redis.asyncio import Redis

from app.core.config import Settings
from app.db.repository import TransactionRepository
from app.queue.producer import EventProducer


def get_settings(request: Request) -> Settings:
    """The Settings built at startup."""
    ...


def get_redis(request: Request) -> Redis:
    """The shared Redis client."""
    ...


def get_producer(request: Request) -> EventProducer:
    """The producer bound to the configured stream."""
    ...


def get_repository(request: Request) -> TransactionRepository:
    """Repository over the shared session factory."""
    ...


SettingsDep = Annotated[Settings, Depends(get_settings)]
RedisDep = Annotated[Redis, Depends(get_redis)]
ProducerDep = Annotated[EventProducer, Depends(get_producer)]
RepositoryDep = Annotated[TransactionRepository, Depends(get_repository)]
