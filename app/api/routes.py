"""Route handlers: ingest, the two read endpoints, and health.

The ingest handler does no database work at all — validate, enqueue, 202. Everything expensive
happens in the worker, which is what lets the API stay flat under a burst.
"""

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Query

from app.api.deps import ProducerDep, RepositoryDep
from app.schemas.events import TransactionEventIn
from app.schemas.responses import AcceptedResponse, TransactionPage, UserSummary

router = APIRouter()


@router.post("/events", status_code=202, response_model=AcceptedResponse)
async def ingest_event(payload: TransactionEventIn, producer: ProducerDep) -> AcceptedResponse:
    """Accept a transaction event and queue it for processing.

    202, not 201: the event has been durably queued, but it has not been converted or stored
    yet. Claiming otherwise would be a lie the client could observe.
    """
    ...


@router.get("/users/{user_id}/summary", response_model=UserSummary)
async def user_summary(user_id: str, repo: RepositoryDep) -> UserSummary:
    """Total USD and transaction count for a user."""
    ...


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
    ...


@router.get("/health/live")
async def liveness() -> dict[str, str]:
    """Process is up. No dependency checks — this must not flap when Postgres blips."""
    ...


@router.get("/health/ready")
async def readiness() -> dict[str, str]:
    """Redis and Postgres are both reachable, so the API can actually serve traffic."""
    ...
