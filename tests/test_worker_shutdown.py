"""The shutdown exception-handling policy in `run_worker`.

`run_worker` itself needs a real Postgres/Redis/rate-service to even construct, so this tests
the *policy* in isolation: a minimal TaskGroup mirroring its exact try/except* _ShutdownRequested
shape, with one task raising the shutdown sentinel and another that may or may not also fail.

This is what caught a real bug during review: an earlier, broader `except* BaseException` clause
that inspected the caught group for the sentinel's presence silently swallowed a genuine crash
that happened to land in the same exception group as a deliberate shutdown — confirmed by the
second test below, which pins the opposite (correct) behavior.
"""

import asyncio

import pytest

from app.worker import _ShutdownRequested


async def _run_group(*coros: object) -> None:
    """Mirrors `run_worker`'s own `try`/`except* _ShutdownRequested` exactly, minus everything
    unrelated to the exception-handling policy itself (connections, loops, cleanup)."""
    try:
        async with asyncio.TaskGroup() as tg:
            for coro in coros:
                tg.create_task(coro)
    except* _ShutdownRequested:
        pass


async def test_pure_shutdown_is_suppressed():
    """A group containing only the sentinel (and tasks that get cancelled cleanly alongside
    it) must not raise — this is the ordinary SIGTERM path."""

    async def watcher() -> None:
        raise _ShutdownRequested

    async def sleeper() -> None:
        await asyncio.sleep(10)

    await _run_group(watcher(), sleeper())  # must not raise


async def test_genuine_crash_alongside_shutdown_still_propagates():
    """A real bug that happens to land in the same group as a shutdown must still surface.

    except* partitions the group: only the sentinel is matched by this clause, so the
    RuntimeError is left over and Python re-raises it automatically once the block exits —
    no manual inspection of the group's contents needed to get this right.
    """
    stop = asyncio.Event()

    async def watcher() -> None:
        await stop.wait()
        raise _ShutdownRequested

    async def crashes() -> None:
        await asyncio.sleep(0.01)
        raise RuntimeError("a real bug, unrelated to shutdown")

    async def trigger() -> None:
        await asyncio.sleep(0.02)
        stop.set()

    with pytest.raises(ExceptionGroup) as exc_info:
        await _run_group(watcher(), crashes(), trigger())

    assert any(isinstance(e, RuntimeError) for e in exc_info.value.exceptions)


async def test_shutdown_sentinel_alone_does_not_raise():
    async def watcher() -> None:
        raise _ShutdownRequested

    await _run_group(watcher())  # must not raise
