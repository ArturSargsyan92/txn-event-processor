# txn-event-processor

An async transaction-event processing service: ingest transaction events over HTTP, queue
them, consume asynchronously, dedupe by `id`, convert the amount to USD, and store the
result — surviving an unavailable database or rate service without losing events. Two read
APIs expose a per-user USD summary and a paginated, time-ranged transaction list.

## Contents

- [How to run](#how-to-run)
- [API](#api)
- [Architecture](#architecture)
- [Why Redis Streams](#why-redis-streams)
- [A trade-off worth naming: per-process rate caching](#a-trade-off-worth-naming-per-process-rate-caching)
- [What changes at 10x load](#what-changes-at-10x-load)
- [Demo walkthrough](#demo-walkthrough)
- [Tests](#tests)
- [Configuration](#configuration)

## How to run

Requires Docker and Docker Compose.

```bash
docker compose up --build
```

This starts five containers: `postgres`, `redis`, `rate-service` (a fault-injectable stand-in
for a real FX provider), `api` (FastAPI, port 8000), and `worker` (the consumer — not an ASGI
app, so it has no HTTP port of its own beyond its Prometheus exporter). `app/core/config.py`'s
defaults already match this compose file's service names, ports, and credentials, so no `.env`
or environment overrides are required to get a working stack.

```bash
curl -X POST localhost:8000/events -H 'content-type: application/json' \
  -d '{"id":"t1","user_id":"u1","amount":"10.00","currency":"EUR","timestamp":"2026-01-01T00:00:00Z"}'

curl localhost:8000/users/u1/summary
curl 'localhost:8000/users/u1/transactions?from=2026-01-01T00:00:00Z&limit=50'
```

Give the worker a couple of seconds to pick the event up before checking the read endpoints —
`POST /events` returns as soon as the event is durably queued, not once it's processed.

To run the API and worker outside Docker (for local iteration): `uv sync`, point
`APP_DATABASE_URL`/`APP_REDIS_URL`/`APP_RATE_SERVICE_URL` at reachable instances, then
`uv run uvicorn app.main:app --reload` and `uv run python -m app.worker` in separate
terminals.

## API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/events` | Accept `{id, user_id, amount, currency, timestamp}`, enqueue it, return `202` with `{event_id, stream_id}`. Never touches Postgres. |
| `GET` | `/users/{user_id}/summary` | `{user_id, total_usd, transaction_count}` — an unknown user is a normal `(0, 0)`, not a 404. |
| `GET` | `/users/{user_id}/transactions?from=&to=&limit=&offset=` | Paginated, newest-first, half-open `[from, to)` range. `limit` defaults to 50 (max 500). A `from`/`to` with no UTC offset is treated as UTC. |
| `GET` | `/health/live` | Process is up. No dependency checks. |
| `GET` | `/health/ready` | Redis and Postgres are both reachable. |
| `GET` | `/metrics/` | Prometheus exposition format (note the trailing slash — see [Demo walkthrough](#demo-walkthrough)). |

`amount` is a decimal string end to end (`"10.00"`, never a JSON float) — see
[Configuration](#configuration) for why. `currency` is case-insensitive on input and
normalized to upper case; a naive `timestamp` (no UTC offset) is treated as UTC rather than
whatever timezone the process happens to be running in.

## Architecture

```
        202                    XADD                XREADGROUP
client ─────▶ api ───────────────────▶ Redis Stream ───────────────▶ worker
                                        "transactions"                  │
                                                                         │ rate lookup, convert
                                                                         ▼
                                                                    rate-service
                                                                         │
                                                                         ▼
                                                              Postgres (idempotent insert)
```

**Two processes, one package.** `app.main` (FastAPI) and `app.worker` (the consumer) share
`app/` and run as separate containers. The API validates and enqueues, then returns 202 — it
never touches Postgres on the write path, which is what keeps it flat under a 1k/sec burst:
`XADD` is O(1), and queue depth, not the database, absorbs the spike.

**Redis Streams with consumer groups** carry events between the two. `EventProducer.publish`
XADDs a small JSON envelope; `StreamConsumer` wraps `XREADGROUP` / `XAUTOCLAIM` / `XACK` for
one consumer in one group. Consumer groups are also why horizontal scaling is free —
`docker compose up --scale worker=N` needs no code change (verified: see
[What changes at 10x load](#what-changes-at-10x-load)).

**Delivery is at-least-once, not exactly-once.** A message is acked only after its side effect
is durable, so a crash means redelivery, never silent loss. What makes redelivery harmless is
the idempotent write: `TransactionRepository.insert_ignore_duplicate` is a single
`INSERT ... ON CONFLICT (id) DO NOTHING RETURNING id`, keyed on the client-supplied event `id`.
Dedup lives there and nowhere else — no read-before-write, no separate dedup store, nothing
whose correctness depends on how concurrent workers interleave.

**Two retry layers, deliberately.** `retry_async` (exponential backoff with full jitter)
absorbs a sub-second blip inside one delivery attempt. Leaving a message *unacked* is the
outer layer: it stays in the consumer group's pending list, and `XAUTOCLAIM` redelivers it —
this is what covers a minutes-long outage or a crashed worker. Both exist because they solve
different problems at different timescales.

**The DLQ is the exit condition.** Without a cutoff, a permanently-failing message would be
reclaimed forever. `worker.handle` dead-letters immediately on a permanent error (e.g. an
unknown currency — no amount of retrying conjures a rate that doesn't exist), and on a
transient error once `delivery_count` reaches `max_deliveries`. Dead-lettering is `XADD` to
`transactions:dlq` **then** `XACK` the original, in that order — which is what makes it
impossible to lose the event in between.

## Why Redis Streams

The alternative most obviously on the table was a plain list-based queue (`RPUSH`/`BLPOP`) or
a pub/sub channel; a heavier broker like Kafka or RabbitMQ was the other end of the spectrum.
Streams won for a few concrete reasons that map directly onto this spec:

- **Consumer groups give at-least-once delivery with tracked, redeliverable state for free.**
  `XREADGROUP` records what each consumer has been handed but not yet acked; `XPENDING`/
  `XAUTOCLAIM` are exactly "what's stuck, reclaim it" — a plain list has none of this, so a
  crashed consumer's in-flight item is just gone. Rebuilding that on top of `RPOPLPUSH` is a
  known but genuinely fiddly pattern; Streams ship it.
- **It's the same Redis already needed for nothing else here**, so there's no second piece of
  infrastructure to run, and no separate durability story to reason about beyond "Redis is up
  or it isn't" (which `docker-compose`'s healthcheck-gated `depends_on` already handles).
- **It matches the stated load.** ~100/sec sustained, bursting to ~1k/sec, is comfortably
  inside what a single Redis instance handles; Kafka's partition/offset/consumer-rebalance
  machinery is real value at a scale this assignment doesn't ask for, and would cost more to
  explain in a line-by-line walkthrough than it buys here.
- **A DLQ is just another stream.** `XADD` to `{stream}:dlq` and `XRANGE` to inspect it needs
  no new concept, migration, or client.

The honest cost: Streams' delivery guarantee is at-least-once, and a Redis instance that loses
data (no persistence, no replica) *can* still lose messages between `XADD` and an fsync. In a
real deployment this means enabling AOF or clustering Redis; the compose setup here runs a
single ephemeral instance, which is the right level of infrastructure for a take-home demo but
not what governs a production durability guarantee — that would be a config change on the
Redis side, not an application change.

## A trade-off worth naming: per-process rate caching

`RateClient` caches successful currency lookups in a plain in-process `dict` with a short TTL
(5s by default), not in Redis. This was a deliberate choice, not an oversight, and it's the
trade-off I'd point to first if asked to defend one design decision in this codebase:

- **What it buys:** no distributed-cache-invalidation story to build or explain, and it stays
  a ~10-line, fully line-by-line-explainable piece of the processor. Only successes are
  cached — a failure is never negatively cached, so recovery after a `rate-service` blip is
  immediate, not gated on a TTL.
- **What it costs:** the cache doesn't share across workers. `--scale worker=4` means four
  independent cold caches, so scaling out worker count multiplies `rate-service` traffic by
  the replica count for the same event volume, rather than the traffic staying flat as it
  would with a shared Redis-backed cache.
- **Why it's still the right call here:** at the stated load (a handful of distinct
  currencies, ~100-1k events/sec), redundant per-replica cache misses are cheap, and this is
  exactly the kind of complexity the interview note ("prefer the simplest construction that
  works; a clever abstraction that saves five lines but costs an explanation is a net loss")
  argues against paying for before it's needed. If `rate-service` call volume ever became the
  bottleneck — which [10x load](#what-changes-at-10x-load) below says it would, first — moving
  this cache into Redis is a contained, one-file change, not a redesign.

## What changes at 10x load

~1k/sec sustained (10x the stated burst, not just the sustained rate) is where several of
today's deliberately-simple choices would need to change, roughly in the order they'd bite:

1. **Worker throughput first.** A single sequential worker processing one event per HTTP
   round-trip plus one insert tops out well under 1k/sec. The fix needs no code change:
   `docker compose up --scale worker=N` — consumer groups already split the stream's keys
   across however many replicas are running (verified live: `XINFO CONSUMERS` shows work
   actually distributing across 3 replicas with zero pending, zero lag). This is the design's
   deliberate answer, chosen ahead of adding concurrency inside one worker process — a
   semaphore/`TaskGroup`-per-batch is the next lever if scaling replica count alone isn't
   enough, but it's a strictly harder thing to explain line by line and isn't needed until
   horizontal scaling is exhausted.
2. **The rate cache becomes a real bottleneck, not just a documented trade-off.** At 10x the
   worker count needed for 10x load, `rate-service` sees 10x the redundant cold-cache traffic
   described above. This is the point where the in-process TTL cache should move to Redis —
   a distributed-staleness story finally earns its cost.
3. **Postgres connection and write pressure.** The current pool (`pool_size=5,
   max_overflow=5`) and single-row `ON CONFLICT DO NOTHING` inserts are sized for the stated
   load, not 10x it. Batching inserts (accumulate a small batch per worker tick, one
   multi-row `INSERT ... ON CONFLICT DO NOTHING` instead of N) would cut round-trips
   meaningfully; a bigger connection pool and read replicas for the two read endpoints would
   likely follow once summary/list queries start competing with the write path for
   connections.
4. **Redis itself.** A single Redis instance backing both the stream and the DLQ is fine at
   the stated load; at 10x, stream trimming (`MAXLEN ~`, already in place) matters more, and
   Redis's own durability (AOF, or a replica) stops being optional — see the honest cost noted
   under [Why Redis Streams](#why-redis-streams).
5. **Observability changes from "nice to have" to load-bearing.** The existing
   `consumer_lag` gauge and `events_failed{kind=...}` counters are exactly what an alert
   would key off at this scale — the metrics needed for 10x load already exist, just not yet
   wired to alerting.

## Demo walkthrough

All of this was run for real against `docker compose up` while building it — not just
asserted:

```bash
docker compose up --build

# dedup: same id twice, still one row
curl -X POST localhost:8000/events -H 'content-type: application/json' \
  -d '{"id":"t1","user_id":"u1","amount":"10.00","currency":"EUR","timestamp":"2026-01-01T00:00:00Z"}'
curl -X POST localhost:8000/events -H 'content-type: application/json' \
  -d '{"id":"t1","user_id":"u1","amount":"10.00","currency":"EUR","timestamp":"2026-01-01T00:00:00Z"}'

curl localhost:8000/users/u1/summary
curl 'localhost:8000/users/u1/transactions?from=2026-01-01T00:00:00Z&limit=50'

# retry/backoff: break the downstream, watch lag and failures build, then recover
docker compose stop rate-service
#   post an event; worker logs show full-jitter backoff across 3 attempts, then
#   "left pending after transient failure" — the message stays pending, un-acked
curl -X POST localhost:8000/events -H 'content-type: application/json' \
  -d '{"id":"t2","user_id":"u1","amount":"5.00","currency":"EUR","timestamp":"2026-01-01T00:00:00Z"}'
docker compose logs worker --tail 20
docker compose start rate-service
#   within min_idle_ms (~15s), XAUTOCLAIM reclaims the pending message and it processes

# crash recovery: no events lost, no duplicates
docker compose kill -s KILL worker      # mid-batch
docker compose start worker             # XAUTOCLAIM picks up whatever was left unacked

# dead letter: unknown currency parks immediately (permanent error, no retries)
curl -X POST localhost:8000/events -H 'content-type: application/json' \
  -d '{"id":"tbad","user_id":"u1","amount":"1.00","currency":"XYZ","timestamp":"2026-01-01T00:00:00Z"}'
docker compose exec redis redis-cli XRANGE transactions:dlq - +

# horizontal scaling: consumer group needs no code change
docker compose up -d --scale worker=4
docker compose exec redis redis-cli XINFO CONSUMERS transactions transaction-processors

# metrics
curl -L localhost:8000/metrics    # -L or a trailing slash — bare /metrics 307s (Starlette Mount)
```

## Tests

```bash
uv sync
uv run pytest -q          # full suite; tests needing Postgres/Redis skip cleanly if neither
                           # is reachable rather than failing the run
uv run ruff check .
uv run ruff format --check .
```

Dedup (`tests/test_dedup.py`) and currency conversion (`tests/test_conversion.py`) are the two
spec-required unit-test targets; `tests/test_worker_handle.py` pins the three-way ack/DLQ
branching in `worker.handle` (including the deliberately-silent "leave it pending" branch),
and `tests/test_api.py` covers the HTTP contract end to end, including the failure paths
(queue down, database down) against real Postgres/Redis.

## Configuration

All settings are `APP_`-prefixed environment variables, defined and documented in
`app/core/config.py` (defaults are tuned for this demo, not production — short retry/cache
TTLs so faults show up quickly). A few worth knowing about going in:

- `amount` is `Decimal` end to end — `Numeric(18,4)` in Postgres, `Decimal` in Pydantic,
  serialized as a JSON *string* across the queue. A `float` anywhere here would silently lose
  cents.
- `APP_MAX_DELIVERIES` (default 5) and `APP_MIN_IDLE_MS` (default 15000) control how
  aggressively a stuck message is reclaimed and how many attempts it gets before landing in
  the DLQ — tuned low here so a demo doesn't have long dead air waiting on them.
- `rate_service/` is a deliberately-breakable stand-in for a real FX provider, not part of the
  core app — it never imports from `app.*`. `POST /admin/fault` on it (body:
  `{"mode": "error" | "timeout" | "ok", "delay_s": <float>}`) switches its failure mode at
  runtime without a restart, which is what the retry/backoff demo above is exercising.
