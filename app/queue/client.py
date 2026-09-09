"""Redis connection pool, shared by the API (producer) and the worker (consumer)."""

from redis.asyncio import Redis


def create_redis(redis_url: str, *, socket_timeout: float = 5.0) -> Redis:
    """Build a Redis client with decode_responses=True.

    Stream fields are text (a JSON payload and a version), so decoding at the client keeps
    bytes/str juggling out of the producer and consumer.

    protocol=2 pins the classic RESP2 wire protocol explicitly, rather than leaving it to
    whatever the client library defaults to — the stream commands' response shapes (a list of
    (id, fields) tuples) are the well-documented ones this code is written against.

    socket_timeout is the client's own read timeout — independent of, and NOT automatically
    coordinated with, any command's server-side BLOCK duration. The 5.0 default here matches
    what redis-py itself falls back to and is fine for the producer's quick, non-blocking
    commands (XADD): a hung Redis fails the HTTP request within a bounded time instead of
    hanging it forever. A caller that issues blocking reads (XREADGROUP with BLOCK, as the
    consumer does) MUST override this to something comfortably longer than the longest BLOCK it
    uses, or the two independent clocks race: the client can time out first purely because the
    windows happen to be close in length, with no actual connection problem at all — confirmed
    live, 100% reproducible with zero signals or cancellation involved, simply by reading from
    an idle stream with BLOCK and socket_timeout left equal (both default to 5s).
    """
    return Redis.from_url(
        redis_url, decode_responses=True, protocol=2, socket_timeout=socket_timeout
    )


async def close_redis(redis: Redis) -> None:
    """Close the client and its pool on shutdown."""
    await redis.aclose()
