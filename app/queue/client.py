"""Redis connection pool, shared by the API (producer) and the worker (consumer)."""

from redis.asyncio import Redis


def create_redis(redis_url: str) -> Redis:
    """Build a Redis client with decode_responses=True.

    Stream fields are text (a JSON payload and a version), so decoding at the client keeps
    bytes/str juggling out of the producer and consumer.
    """
    ...


async def close_redis(redis: Redis) -> None:
    """Close the client and its pool on shutdown."""
    ...
