# Notification Pipeline

A multi-tenant notification service: an HTTP producer accepts notifications and writes them to Kafka, and a consumer delivers them to a webhook with deduplication, per-tenant rate limiting, retries and a dead-letter topic. Every event's outcome is logged to PostgreSQL, and both services expose Prometheus metrics.

**Stack:** Node.js, TypeScript, Kafka (kafkajs), Redis, PostgreSQL, Prometheus, Docker Compose, GitHub Actions.

## Architecture

```
client ──POST /notify──▶ producer ──▶ Kafka topic "notifications" (6 partitions, key = tenantId)
                         (202 Accepted)            │
                                                   ▼
                                              consumer (batches, partitions in parallel)
                                                   │  for each event:
                                                   │   1. already delivered?      (Redis)  → duplicate
                                                   │   2. over tenant's limit?    (Redis)  → rate_limited
                                                   │   3. POST to webhook, retry with backoff
                                                   │        success → mark delivered (Redis)
                                                   │        still failing → "notifications.dlq"
                                                   ▼
                                     PostgreSQL: one bulk INSERT per batch (outcome, attempts, error)

Prometheus scrapes producer:3000/metrics and consumer:9100/metrics
```

## Design decisions

| Decision | Why |
|---|---|
| **One topic keyed by tenantId** (not a topic per tenant) | kafkajs matches a subscription pattern only at startup, so a new tenant's topic was never consumed until a restart. A key keeps each tenant on one partition and needs no new topics. |
| **`fromBeginning: true`** | With `false`, events published before the consumer group's first commit on a partition were skipped. Replays are safe because of deduplication. |
| **Ordering per user, up to 50 users in parallel** | One user's events are delivered in order; different users don't wait for each other. Ordering per tenant limited delivery to about one webhook call in flight per tenant. |
| **Mark delivered *after* delivery** | Marking first (the old `SET NX`) meant a failed delivery looked like a duplicate on retry and was lost. Check-then-mark is safe because a duplicate shares its messageId and user, so it's processed after the first copy finishes. |
| **Sliding-window rate limit in a Lua script** | The old `INCR` + `EXPIRE` fixed window let a tenant send 2× the limit across a window boundary. A sorted set of timestamps, updated atomically in Redis, enforces "at most N in any 60 s". |
| **Exponential backoff with full jitter, 4 attempts** | Retries network errors, 5xx and 429. Other 4xx fail fast. Jitter keeps consumers from retrying in lockstep. |
| **Dead-letter topic** | Events that still fail are kept with the error and attempt count instead of being dropped. |
| **Offsets committed after the batch is logged** | A crash replays the batch (at-least-once), and deduplication makes replays harmless. |
| **Client `messageId` (optional)** | Lets a client retry `POST /notify` safely: the same ID is delivered at most once. |
| **Producer reconnects** | A dropped Kafka connection used to fail every request until the service was restarted. |

## Measured results

Measured locally on a 10-core MacBook with Docker Desktop: single Kafka broker, everything on one machine, rate limit disabled for the test. Numbers will differ on other hardware. These show the relative effect of each change.

**Consumer capacity:** a backlog of 60,000 events is queued with the consumer stopped, then the drain is timed (`loadtest/drain.sh`).

| Version | Delivery | Consumer events/s |
|---|---|---|
| Original: one message at a time, a topic per tenant | console log | **1,463** (measured end-to-end; the consumer was the limit) |
| Batches + partitions in parallel + bulk insert | console log | **~22,800** |
| + webhook delivery, ordered per tenant | HTTP webhook | **2,606** |
| + ordered per user, 50 in parallel | HTTP webhook | **6,827** |

**End-to-end under load:** 20,000 events, 50 concurrent HTTP clients (`loadtest/run.mjs`).

| Version | Producer accepts/s | End-to-end p95 |
|---|---|---|
| Original | ~3,000 | ≤ 5 s (backlog building) |
| Final (webhook delivery) | ~3,500 | ≤ 1 s |

**Retries:** with the webhook failing 30% of requests on purpose: 2,000 events → 1,385 delivered first try, 597 after retries, 18 dead-lettered after 4 attempts (99.1% delivered; expected dead-letter rate 0.3⁴ = 0.81%).

## Run it

```bash
docker compose up -d --build
curl -X POST localhost:3000/notify -H 'content-type: application/json' \
  -d '{"tenantId":"acme","userId":"u1","message":"hello","channel":"email","messageId":"order-42"}'

curl localhost:8081/stats          # webhook sink: received / succeeded / failed
curl localhost:9100/metrics        # consumer metrics
open http://localhost:9090         # Prometheus
```

Options (environment variables for `docker compose up`):

| Variable | Default | Effect |
|---|---|---|
| `RATE_LIMIT` | 100 | events per tenant per 60 s |
| `FAILURE_RATE` | 0 | share of webhook calls the sink fails with 503 |
| `LATENCY_MS` | 0 | delay added by the sink |

## Load tests

```bash
RATE_LIMIT=1000000 docker compose up -d --build
node loadtest/run.mjs 20000 50 10     # events, concurrency, tenants
./loadtest/drain.sh 60000             # consumer capacity
```

## Metrics

| Metric | Meaning |
|---|---|
| `producer_notify_requests_total{status}` | requests by HTTP status (202, 400, 503) |
| `producer_publish_duration_seconds` | Kafka write latency |
| `consumer_events_processed_total{outcome}` | delivered, duplicate, rate_limited, dead_lettered |
| `consumer_end_to_end_latency_seconds` | accepted by producer → delivered (use for p95) |
| `consumer_delivery_attempts_total{result}` | success, retryable_error, permanent_error |
| `consumer_webhook_duration_seconds` | webhook call latency |
| `consumer_batch_duration_seconds` | time per Kafka batch |
| `consumer_lag_messages` | messages in Kafka not yet committed by the consumer group |

Example p95 query in Prometheus:
`histogram_quantile(0.95, rate(consumer_end_to_end_latency_seconds_bucket[1m]))`

## Tests

```bash
cd consumer && npm test    # unit tests + Redis integration tests (needs Redis on localhost:6379)
cd producer && npm test
```

CI (`.github/workflows/ci.yml`) runs both test suites with a Redis service, then starts the whole stack with Docker Compose and checks that an event is delivered once and its duplicate is skipped.

## Known limits

- A single Kafka broker with replication factor 1: fine locally, not durable.
- Retries happen inline, so a user with a failing webhook delays that user's later events by up to the retry time (backoff adds at most 0.7 s; with four 2 s timeouts the worst case is about 9 s). Larger systems use delayed retry topics.
- The dead-letter topic has no replay tool yet.
- Deduplication markers live for 24 hours.
