"""Route handlers: ingest, the two read endpoints, and health.

The ingest handler does no database work at all — validate, enqueue, 202. Everything expensive
happens in the worker, which is what lets the API stay flat under a burst.
"""

from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query
from redis.exceptions import RedisError

from app.api.deps import ProducerDep, RedisDep, RepositoryDep
from app.core.errors import DatabaseUnavailable
from app.core.logging import get_logger
from app.core.metrics import EVENTS_PUBLISHED
from app.db.errors import DB_EXCEPTIONS
from app.schemas.events import TransactionEvent, TransactionEventIn
from app.schemas.responses import AcceptedResponse, TransactionOut, TransactionPage, UserSummary

router = APIRouter()
logger = get_logger(__name__)


@router.post("/events", status_code=202, response_model=AcceptedResponse)
async def ingest_event(payload: TransactionEventIn, producer: ProducerDep) -> AcceptedResponse:
    """Accept a transaction event and queue it for processing.

    202, not 201: the event has been durably queued, but it has not been converted or stored
    yet. Claiming otherwise would be a lie the client could observe.
    """
    event = TransactionEvent(**payload.model_dump(), received_at=datetime.now(UTC))
    try:
        stream_id = await producer.publish(event)
    except (OSError, RedisError) as exc:
        # This is the one path the whole architecture is built around ("queue depth, not
        # Postgres, absorbs the burst") — a Redis blip here must read as "try again" (503),
        # the same signal /health/ready already gives for the identical failure, not an
        # opaque 500 with no retry hint for the client. Logged server-side too: the 503 alone
        # is visible to the client, but an operator watching a "stop redis" demo has nothing
        # else to look at — EVENTS_PUBLISHED only counts successes.
        logger.warning("event publish failed: %s", exc, extra={"event_id": event.id})
        raise HTTPException(status_code=503, detail=f"queue unavailable: {exc}") from exc

    EVENTS_PUBLISHED.inc()
    return AcceptedResponse(event_id=event.id, stream_id=stream_id)


@router.get("/users/{user_id}/summary", response_model=UserSummary)
async def user_summary(user_id: str, repo: RepositoryDep) -> UserSummary:
    """Total USD and transaction count for a user."""
    total, count = await repo.user_summary(user_id)
    return UserSummary(user_id=user_id, total_usd=total, transaction_count=count)


@router.get("/users/{user_id}/transactions", response_model=TransactionPage)
async def user_transactions(
    user_id: str,
    repo: RepositoryDep,
    from_: Annotated[datetime | None, Query(alias="from")] = None,
    to: Annotated[datetime | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> TransactionPage:
    """A user's transactions in [from, to), newest first, paginated.

    `from` is aliased because it is a Python keyword. The interval is half-open so callers can
    page through adjacent windows without seeing a boundary row twice.

    A `from`/`to` with no UTC offset is treated as UTC, not left ambiguous — otherwise it's
    silently interpreted in whatever timezone the *database session* happens to be in
    (confirmed live: with the session in Asia/Yerevan, a naive `13:00` matched a row stored
    at `09:00Z`), which breaks the half-open guarantee for any caller that omits the offset.
    """
    if from_ is not None and from_.tzinfo is None:
        from_ = from_.replace(tzinfo=UTC)
    if to is not None and to.tzinfo is None:
        to = to.replace(tzinfo=UTC)

    rows, total = await repo.list_user_transactions(
        user_id, start=from_, end=to, limit=limit, offset=offset
    )
    items = [
        TransactionOut(
            id=row.id,
            user_id=row.user_id,
            amount=row.amount,
            currency=row.currency,
            rate=row.rate,
            amount_usd=row.amount_usd,
            timestamp=row.timestamp,
        )
        for row in rows
    ]
    return TransactionPage(items=items, total=total, limit=limit, offset=offset)


@router.get("/health/live")
async def liveness() -> dict[str, str]:
    """Process is up. No dependency checks — this must not flap when Postgres blips."""
    return {"status": "alive"}


@router.get("/health/ready")
async def readiness(repo: RepositoryDep, redis: RedisDep) -> dict[str, str]:
    """Redis and Postgres are both reachable, so the API can actually serve traffic."""
    try:
        await redis.ping()
    except (OSError, RedisError) as exc:
        raise HTTPException(status_code=503, detail=f"redis unreachable: {exc}") from exc

    try:
        # No dedicated "ping the database" method — this is a real, cheap query through the
        # exact same repository code path a request would use, so it proves the whole stack
        # works, not just that a bare connection succeeds. An unknown user is a normal (0, 0)
        # result, not an error, so the user_id itself is a throwaway.
        await repo.user_summary("__healthcheck__")
    except (DatabaseUnavailable, *DB_EXCEPTIONS) as exc:
        # Deliberately broader than the read endpoints' own handling (which only maps
        # DatabaseUnavailable — a genuinely retryable failure — to 503, and lets anything
        # classify_db_error left unclassified propagate as a real error): a readiness probe's
        # only job is "can this process serve traffic right now," so a missing table (this
        # is the one process that runs create_all) means "not ready" too, not a stack trace.
        raise HTTPException(status_code=503, detail=f"database unreachable: {exc}") from exc

    return {"status": "ready"}
