# Runtime modes, local run, and the transactional outbox

Moved out of `CLAUDE.md` to keep it under ~12KB; still authoritative.

## Persistence and event delivery mode matrix

Composition root (`cmd/pathmgmt/main.go`) picks the mode at boot from
`DATABASE_URL` and `EVENT_PUBLISHER`:

| `DATABASE_URL` | `EVENT_PUBLISHER` | Publisher wired          | Outbox relay |
|----------------|--------------------|---------------------------|--------------|
| unset          | `log` (default)     | log                       | none         |
| unset          | `kafka`             | direct Kafka (no outbox)  | none         |
| set            | `log`                | log                       | none         |
| set            | `kafka`             | **transactional outbox**  | **yes**      |

The cluster runs the last row. With no `DATABASE_URL`, the service is
fully functional over REST on the in-memory adapter (`internal/adapters/outbound/memory`)
— no Postgres required for local dev.

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

Local run, no dependencies:

```bash
go run ./cmd/pathmgmt
# DATABASE_URL not set -> in-memory ProcessPathRepo; fully functional REST on :8080
```

With Postgres:

```bash
docker compose up -d postgres          # Postgres 16 on localhost:5436
export DATABASE_URL='postgres://pathmgmt:pathmgmt@localhost:5436/pathmgmt?sslmode=disable'
go run ./cmd/pathmgmt                  # migrations run automatically at startup
```

With Kafka publishing:

```bash
export EVENT_PUBLISHER=kafka
export KAFKA_BROKERS=localhost:9092
go run ./cmd/pathmgmt
```

