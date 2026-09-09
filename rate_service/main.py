"""The fake rate service: a static rate table plus a fault switch."""

import asyncio
from typing import Literal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

app = FastAPI(title="rate-service")

RATES: dict[str, str] = {
    "USD": "1",
    "EUR": "1.08",
    "GBP": "1.27",
    "JPY": "0.0067",
    "CAD": "0.73",
    "AUD": "0.66",
}
"""Currency -> USD-per-unit, as strings so they parse straight into Decimal. Illustrative, not
live rates — this is a demo fixture, not a real FX feed."""


class FaultConfig(BaseModel):
    """How the service should misbehave, for demonstrating the consumer's retry path."""

    mode: Literal["ok", "error", "timeout"] = "ok"
    delay_s: float = Field(default=0.0, ge=0)
    """Only used by mode="timeout": how long to sleep before responding. Set this longer than
    the client's own HTTP timeout so the client gives up first — the point is to demonstrate
    the client's timeout handling, not this service's ability to sleep."""


_fault = FaultConfig()
"""Current fault mode. Module-level and unlocked: this app runs single-process/single-worker,
so there's no concurrent-mutation risk to guard against."""


@app.get("/rates/{currency}")
async def get_rate(currency: str) -> dict[str, str]:
    """Return the USD rate for `currency`.

    404 for an unknown currency — the client maps that to a permanent error, because no amount
    of retrying will conjure a rate that does not exist. Faults injected via /admin/fault
    surface as 500 or a hang instead, which the client maps to transient.
    """
    if _fault.mode == "error":
        raise HTTPException(status_code=500, detail="fault injected: error")
    if _fault.mode == "timeout":
        await asyncio.sleep(_fault.delay_s)  # Field(ge=0) already rules out a negative value

    rate = RATES.get(currency.upper())
    if rate is None:
        raise HTTPException(status_code=404, detail=f"no rate for currency {currency!r}")
    return {"currency": currency.upper(), "rate": rate}


@app.post("/admin/fault")
async def set_fault(config: FaultConfig) -> FaultConfig:
    """Switch the service into a failure mode at runtime, without restarting the container."""
    global _fault
    _fault = config
    return _fault


@app.get("/health")
async def health() -> dict[str, str]:
    """Unconditional OK, so compose can tell 'starting' from 'deliberately faulting'.

    Deliberately ignores `_fault` — a fault demo must not make Docker think the container
    itself is unhealthy and restart it out from under the demo.
    """
    return {"status": "ok"}
