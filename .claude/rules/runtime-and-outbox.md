---
paths:
  - "internal/adapters/**/kafka/**"
  - "internal/adapters/outbound/events/**"
  - "apis/asyncapi*"
  - "internal/adapters/outbound/postgres/**"
  - "migrations/**"
---

# Runtime (Postgres + Kafka only), local run, and the transactional outbox

Moved out of `CLAUDE.md` to keep it under ~12KB; still authoritative.

## Runtime: real Postgres and real Kafka, nothing else (ADR 0037)

The shipped binaries (`cmd/pathmgmt`, `cmd/mcp`, `cmd/pathmgmt-projector`,
`cmd/pathmgmt-reports`) have no in-memory runtime path, no log sink and no
optional relay. A missing mandatory variable is a boot error that names the
variable, raised before any connection and before the listener binds:

| Binary | Mandatory | Rejected at boot |
|---|---|---|
| `cmd/pathmgmt` | `DATABASE_URL`, `KAFKA_BROKERS` | `EVENT_PUBLISHER=log` (and any value other than `kafka`) |
| `cmd/mcp` | `DATABASE_URL` | none (needs no Kafka) |
| `cmd/pathmgmt-projector` | `ANALYTICS_DATABASE_URL`, `KAFKA_BROKERS` | none |
| `cmd/pathmgmt-reports` | `ANALYTICS_DATABASE_URL` | none |

This context consumes no other context's topic, and the projector's consumer
group is a constant. `EVENT_PUBLISHER` is kept only because the chart and
warehouse-infra set it (unset or `kafka`).

`GET /readyz` = drain gate AND a Postgres ping AND a Kafka broker answering a
topic-less metadata request (reachability only, never topics; verdict cached
3s, bounded 2s). `/healthz` stays liveness only. The service never creates
topics: the cluster broker auto-creates them on first write.

The in-memory adapters (`internal/adapters/outbound/memory`) are TEST DOUBLES;
`TestRuntimeBinaryHasNoInMemoryAdapters` (with its `TestRuntimeBinarySensorBites`)
fails the build if any non-test file under `cmd/**` imports them. The wall
clock the binaries inject lives in `internal/adapters/outbound/system`.

## Transactional outbox (ADR 0003 — this repo is the fleet's REFERENCE implementation)

Every write use case (`DefinePath`, `RevisePath`, `DeactivatePath`) runs
`Repo.Save` and `EventPublisher.Publish` inside one `ports.UnitOfWork.Execute`
scope. `postgres.UnitOfWork` opens a `pgx.Tx`, binds it to the context, and
commits or rolls back around the callback — either both the aggregate row
and the `outbox_events` row land, or neither does. `postgres.OutboxRelay`
runs as a goroutine inside the `pathmgmt` process (not a separate binary),
claiming up to 100 pending rows with `SELECT … FOR UPDATE SKIP LOCKED ORDER BY id`
per pass, sending them to Kafka in order, marking `published_at`. This
pattern is the template four sibling services (wes-work-planning,
fulfillment-execution, workforce-management, labor-performance) are meant
to follow — see ADR 0003 for the full rationale, including the concrete
store/topic divergence bug (ADR 0002) that motivated it.

Every outbox row stores the already-encoded CloudEvents 1.0 bytes (ADR
0016): `event_id` is the CloudEvents `id` minted once by `OutboxPublisher`
(shared by the integration and analytics rows of one occurrence) and
`event_type` is the full CloudEvents `type`. The relay republishes the
stored bytes unchanged plus the `content-type:
application/cloudevents+json; charset=UTF-8` header, so a redelivery
carries the same `id`.

## Local run

Postgres and Kafka are both required:

```bash
docker compose up -d --wait postgres kafka   # Postgres 16 on localhost:5436, Kafka on localhost:9092
export DATABASE_URL='postgres://pathmgmt:pathmgmt@localhost:5436/pathmgmt?sslmode=disable'
export KAFKA_BROKERS=localhost:9092
go run ./cmd/pathmgmt                        # migrations run automatically at startup
```

`make seed-catalogue` applies `catalogue/paths.json` to the RUNNING service
over REST (a fresh database is empty). The Schemathesis `contract` job boots
the same binary against `postgres:16` and `confluent-local` service containers
(`scripts/contract-test.sh`, ADR 0037).

