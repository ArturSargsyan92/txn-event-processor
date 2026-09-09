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
from app.schemas.events import TransactionEvent, TransactionEventIn
from app.schemas.responses import AcceptedResponse, TransactionOut, TransactionPage, UserSummary

router = APIRouter()


@router.post("/events", status_code=202, response_model=AcceptedResponse)
async def ingest_event(payload: TransactionEventIn, producer: ProducerDep) -> AcceptedResponse:
    """Accept a transaction event and queue it for processing.

    202, not 201: the event has been durably queued, but it has not been converted or stored
    yet. Claiming otherwise would be a lie the client could observe.
    """
    event = TransactionEvent(**payload.model_dump(), received_at=datetime.now(UTC))
    stream_id = await producer.publish(event)
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
    """
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
    except DatabaseUnavailable as exc:
        raise HTTPException(status_code=503, detail=f"database unreachable: {exc}") from exc

    return {"status": "ready"}
