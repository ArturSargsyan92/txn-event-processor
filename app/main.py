"""The API process: ingest and read endpoints, plus /metrics.

Runs as the `api` service in docker-compose (`uvicorn app.main:app`).
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from prometheus_client import make_asgi_app

from app.api.routes import router
from app.core.config import Settings
from app.core.errors import DatabaseUnavailable
from app.core.logging import configure_logging
from app.db.engine import create_engine, create_session_factory, init_models
from app.db.repository import TransactionRepository
from app.queue.client import close_redis, create_redis
from app.queue.producer import EventProducer
from app.schemas.responses import ErrorBody


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Open the Redis client and the database engine, create tables, and tear both down.

    `init_models` runs here and only here. The worker deliberately does not create tables — two
    containers racing on create_all at boot can deadlock (see the plan, §3.6). No wait/retry
    around Postgres or Redis becoming reachable: unlike the worker (which races the API's own
    create_all and needs a generous retry budget for that), the API's only dependency here is
    Postgres itself being up, which docker-compose's healthcheck-gated `depends_on` (step 9)
    is what actually sequences — this process failing fast and letting the orchestrator restart
    it is the simpler, equally correct answer to the same problem.
    """
    settings = app.state.settings  # set by create_app, before this ever runs

    engine = create_engine(settings.database_url)
    await init_models(engine)
    repository = TransactionRepository(create_session_factory(engine))

    redis = create_redis(settings.redis_url)
    producer = EventProducer(redis, settings.stream_name, settings.stream_maxlen)

    app.state.repository = repository
    app.state.producer = producer
    app.state.redis = redis

    try:
        yield
    finally:
        await close_redis(redis)
        await engine.dispose()


def create_app() -> FastAPI:
    """Build the application: settings, lifespan, routes, and the Prometheus ASGI mount.

    Settings() is constructed exactly once, here, and stashed on app.state before `lifespan`
    ever runs (that only happens once uvicorn starts serving) — `lifespan` reads it back
    rather than building its own second instance, so the whole process has one source of
    truth for its own config.
    """
    settings = Settings()
    configure_logging(settings.log_level)

    app = FastAPI(title="txn-event-processor", lifespan=lifespan)
    app.state.settings = settings
    app.include_router(router)
    app.mount("/metrics", make_asgi_app())

    @app.exception_handler(DatabaseUnavailable)
    async def database_unavailable_handler(
        request: Request, exc: DatabaseUnavailable
    ) -> JSONResponse:
        """The two read endpoints touch Postgres directly (the write path never does) — a
        transient outage there should read as "try again" (503), not an opaque 500."""
        return JSONResponse(
            status_code=503, content=ErrorBody(detail=f"database unavailable: {exc}").model_dump()
        )

    return app


app = create_app()
