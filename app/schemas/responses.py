"""Response models for the read APIs."""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel


class AcceptedResponse(BaseModel):
    """202 body for POST /events. Returns the stream id so a client can correlate."""

    event_id: str
    stream_id: str


class UserSummary(BaseModel):
    """GET /users/{user_id}/summary."""

    user_id: str
    total_usd: Decimal
    transaction_count: int


class TransactionOut(BaseModel):
    """One stored, converted transaction."""

    id: str
    user_id: str
    amount: Decimal
    currency: str
    rate: Decimal
    amount_usd: Decimal
    timestamp: datetime


class TransactionPage(BaseModel):
    """GET /users/{user_id}/transactions — one page plus enough to drive the next request."""

    items: list[TransactionOut]
    total: int
    limit: int
    offset: int


class ErrorBody(BaseModel):
    """Uniform error shape."""

    detail: str
