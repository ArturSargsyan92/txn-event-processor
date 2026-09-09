"""The API process: ingest and read endpoints, plus /metrics.

Runs as the `api` service in docker-compose (`uvicorn app.main:app`).
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Open the Redis client and the database engine, create tables, and tear both down.

    `init_models` runs here and only here. The worker deliberately does not create tables — two
    containers racing on create_all at boot can deadlock (see the plan, §3.6).
    """
    ...


def create_app() -> FastAPI:
    """Build the application: settings, lifespan, routes, and the Prometheus ASGI mount."""
    ...


app = create_app()
