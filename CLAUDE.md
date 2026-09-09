# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project state

This is a **take-home assignment**, currently at the scaffold stage. Every module under `app/` and
`rate_service/` exists with docstrings and typed signatures but `...` bodies. Implementation proceeds
one step at a time against the approved plan at
`~/.claude/plans/i-m-building-a-small-whimsical-flamingo.md` — read it before starting a step; it holds
the interface contracts and the reasoning behind each decision.

Two constraints shape everything here:

- **The follow-up interview is a line-by-line walkthrough.** The author must be able to explain and
  modify any line live. Prefer the simplest construction that works; a clever abstraction that saves
  five lines but costs an explanation is a net loss.
- **Target load is ~100 events/sec, bursting to ~1k/sec.** Not large. Do not add machinery for scale
  the assignment does not ask for — horizontal worker scaling is the documented answer.

All tests are currently `pytest.mark.skip` placeholders naming the step that implements them. When
implementing a step, delete that module's `pytestmark` line and fill in the bodies.

## Commands

```bash
uv sync                                    # install deps

uv run pytest -q                           # full suite
uv run pytest tests/test_retry.py -q       # one module
uv run pytest tests/test_retry.py::test_delay_is_capped_at_max   # one test
uv run pytest -k dedup                     # by name

uv run ruff check .                        # lint (E, F, I, UP, B; line-length 100)
uv run ruff check --fix .
uv run ruff format .
```

Process entrypoints — **these do not run yet**; `create_app()` and `run_worker()` land in steps 8 and 7:

```bash
uv run uvicorn app.main:app --reload       # API
uv run python -m app.worker                # consumer
```

## Architecture

**Two processes, one package.** `app.main` (FastAPI, run by uvicorn) and `app.worker` (the consumer,
run by `python -m`) share `app/` and are separate services in compose. The split is the point: the API
validates and enqueues, then returns 202. It never touches Postgres on the write path, which is what
keeps it flat under a 1k/sec burst — `XADD` is O(1), and queue depth rather than the database absorbs
the spike.

**Redis Streams with consumer groups** carry events between them. `EventProducer.publish` XADDs a
`StreamEnvelope` (a JSON payload plus a schema version, since stream fields are flat `str`→`str`);
`StreamConsumer` wraps XREADGROUP / XAUTOCLAIM / XACK for one consumer in one group. Consumer groups
are also why scaling out is free: `--scale worker=N` needs no code change.

**Delivery is at-least-once, not exactly-once.** A message is acked only after its side effect is
durable, so a crash means redelivery rather than loss. What makes redelivery harmless is the
idempotent write: `TransactionRepository.insert_ignore_duplicate` is a single
`INSERT ... ON CONFLICT (id) DO NOTHING RETURNING id`, keyed on the client-supplied event `id`. Dedup
lives there and nowhere else — no read-before-write, no Redis SETNX, nothing whose correctness depends
on how workers interleave.

**Two retry layers, deliberately.** `retry_async` (exponential backoff, full jitter) absorbs the
sub-second blip inside one delivery. Leaving a message *unacked* is the outer layer: it stays in the
pending-entries list and XAUTOCLAIM redelivers it, which covers minutes-long outages and crashed
consumers. Both exist because they solve different problems; if either looks redundant, re-read this
paragraph before deleting one.

**The DLQ is the exit condition.** Without a cutoff, a permanently-failing message is reclaimed forever
and the pending list grows unbounded. `worker.handle` dead-letters on `PermanentError` immediately, and
on `TransientError` once `delivery_count >= max_deliveries`. Dead-lettering means XADD to
`{stream}:dlq` **then** XACK the original — that ordering is what makes it impossible to lose an event.

`app/services/processor.py` holds what an event *means* (look up rate → convert → store) and knows
nothing about Redis Streams. `app/worker.py` holds delivery policy (ack, reclaim, DLQ) and knows nothing
about currencies. Keep that seam; it is why the processor is testable without a broker.

### rate_service/ is not part of the app

`rate_service/` is a stand-in for an external FX dependency, deliberately built to be broken: stopping
the container or POSTing to its `/admin/fault` is how the retry, backoff and dead-letter paths get
demonstrated against a real network boundary instead of a mock. It is a separate deployable that
happens to share the repository — **it must never import from `app.*`**, and `app` must never import
from it. Do not extend it beyond what a fault demo needs.

## Invariants worth knowing before editing

- **`worker.handle`'s middle branch does nothing on purpose.** `TransientError` under `max_deliveries`
  calls neither `ack` nor `dead_letter`, so the message stays pending for reclaim. It looks like a
  missing case; it is the mechanism. `tests/test_worker_handle.py` asserts that no call happens.
- **`SQLModel.metadata.create_all` runs in the API lifespan only.** Both containers doing it at boot
  can deadlock on the same CREATE TABLE. The worker retry-connects until the tables exist instead.
- **Money is `Decimal` end to end** — `Numeric(18, 4)` columns, `Decimal` in Pydantic, serialized as a
  JSON *string* across the queue. A float anywhere silently loses cents.
- **`ON CONFLICT DO NOTHING` is not reachable through SQLModel's ORM API.** `repository.py` drops to
  `sqlalchemy.dialects.postgresql.insert` for that one statement; `session.add()` is not an option on
  the write path.
- **The rate lookup retries to exhaustion before the insert is attempted**, so an event is never
  half-applied. A crash between the two steps is safe because the insert is idempotent.
- **The worker is not an ASGI app**, so it cannot serve a `/metrics` route. It runs
  `prometheus_client.start_http_server(worker_metrics_port)` and compose exposes a second port.
- **`RateClient`'s cache is a per-process dict, not Redis** — chosen to avoid a distributed-staleness
  story. Only successes are cached; failures are never negatively cached, so recovery is immediate.
  The short TTL is also what keeps the fault-injection demo honest.

## Demo commands

These are the checks the assignment is judged on. They need `docker-compose.yml`, which lands in step 9.

```bash
docker compose up

# dedup: same id twice, still one row
curl -X POST localhost:8000/events -H 'content-type: application/json' \
  -d '{"id":"t1","user_id":"u1","amount":"10.00","currency":"EUR","timestamp":"2026-01-01T00:00:00Z"}'

curl localhost:8000/users/u1/summary
curl 'localhost:8000/users/u1/transactions?from=2026-01-01T00:00:00Z&limit=50'

# retry/backoff: break the downstream, watch lag build, then recover
docker compose stop rate-service
#   ... wait out the ~5s rate cache TTL, post events, watch worker logs and
#   events_failed{kind="transient"} climb while consumer_lag rises
docker compose start rate-service          # pending messages reclaim and drain to zero

# crash recovery: no events lost, no duplicates
docker compose kill -s KILL worker         # mid-batch
docker compose start worker                # XAUTOCLAIM picks up the unacked messages

# dead letter: unknown currency parks after max_deliveries
docker compose exec redis redis-cli XRANGE transactions:dlq - +

docker compose up --scale worker=4         # consumer group needs no code change
```
