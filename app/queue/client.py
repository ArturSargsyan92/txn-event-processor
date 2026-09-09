"""Redis connection pool, shared by the API (producer) and the worker (consumer)."""

from redis.asyncio import Redis


def create_redis(redis_url: str) -> Redis:
    """Build a Redis client with decode_responses=True.

    Stream fields are text (a JSON payload and a version), so decoding at the client keeps
    bytes/str juggling out of the producer and consumer.

    protocol=2 pins the classic RESP2 wire protocol explicitly, rather than leaving it to
    whatever the client library defaults to — the stream commands' response shapes (a list of
    (id, fields) tuples) are the well-documented ones this code is written against.
    """
    return Redis.from_url(redis_url, decode_responses=True, protocol=2)


async def close_redis(redis: Redis) -> None:
    """Close the client and its pool on shutdown."""
    await redis.aclose()
