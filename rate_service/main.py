"""The fake rate service: a static rate table plus a fault switch."""

from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="rate-service")

RATES: dict[str, str] = {}
"""Currency -> USD-per-unit, as strings so they parse straight into Decimal. Filled in step 6."""


class FaultConfig(BaseModel):
    """How the service should misbehave, for demonstrating the consumer's retry path."""

    mode: str
    """'ok' | 'error' (500s) | 'timeout' (hangs past the client's timeout)."""

    delay_s: float = 0.0


@app.get("/rates/{currency}")
async def get_rate(currency: str) -> dict[str, str]:
    """Return the USD rate for `currency`.

    404 for an unknown currency — the client maps that to a permanent error, because no amount
    of retrying will conjure a rate that does not exist. Faults injected via /admin/fault
    surface as 500 or a hang instead, which the client maps to transient.
    """
    ...


@app.post("/admin/fault")
async def set_fault(config: FaultConfig) -> FaultConfig:
    """Switch the service into a failure mode at runtime, without restarting the container."""
    ...


@app.get("/health")
async def health() -> dict[str, str]:
    """Unconditional OK, so compose can tell 'starting' from 'deliberately faulting'."""
    ...
