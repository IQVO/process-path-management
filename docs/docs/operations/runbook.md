---
id: runbook
title: Runbook
sidebar_label: Runbook
description: How process-path-management is deployed, migrated and scaled, which Kafka topics it produces and consumes, how the outbox relay, DLQ and housekeeping sweeper behave, the reports API, and routine procedures.
---

# Runbook

Everything on this page is derived from the code and the Helm chart in this
repository (`cmd/`, `internal/adapters/`, `charts/process-path-management`).
Where a procedure has not been exercised against a live cluster it says so.

## Deployment

All four binaries ship in one image (`Dockerfile`): `/app/pathmgmt` (the
image `ENTRYPOINT`), `/app/mcp`, `/app/pathmgmt-projector`,
`/app/pathmgmt-reports`, plus `/app/migrations` (OLTP and `analytics/`). The
image runs as uid 1000.

The Helm chart is `charts/process-path-management`. Each optional workload
is behind a flag and selects its binary with `command:`.

| Workload | Rendered when | Binary | Container port | Service | Probes (startup / liveness / readiness) |
| --- | --- | --- | --- | --- | --- |
| `<release>` Deployment, component `api` | always | `pathmgmt` (image entrypoint) | 8080 (`http`) | `<release>`, ClusterIP, port 80 → `http` | `/healthz` / `/healthz` / **`/readyz`** |
| `<release>-mcp`, component `mcp` | `mcp.enabled=true` | `/app/mcp` | 8090 | `<release>-mcp`, port 8090 | `/healthz` / `/healthz` / `/healthz` |
| `<release>-projector`, component `analytics-projector` | `analytics.enabled=true` | `/app/pathmgmt-projector` | 8091 (`admin`) | none | `/healthz` / `/healthz` / **`/readyz`** |
| `<release>-reports`, component `analytics-reports` | `analytics.enabled=true` | `/app/pathmgmt-reports` | 8092 | `<release>-reports`, port 80 → `http` | `/healthz` / `/healthz` / `/healthz` |
| `<release>-frontend` | `frontend.enabled=true` | nginx-unprivileged serving `web/` | 8080 | ClusterIP, port 80 | — |

Probe timings are the same for every workload: startup every 2 s with
`failureThreshold: 30` (a 60 s window), liveness every 10 s after 5 s,
readiness every 5 s after 3 s. `terminationGracePeriodSeconds` is 30 for the
api and projector.

Ingress (`ingress.enabled`) and Gateway API `HTTPRoute`
(`gatewayApi.enabled`, `stripPath: true`) are both off by default and only
route to the api Service. In the fleet's kind cluster the API is reached
through Kong at `/api/process-path-management` and the operator remote
through the Nginx web gateway at `/mfes/process-path-management/`; those
routes live in warehouse-infra, not here.

### What readiness waits for

Nothing at runtime. Every binary dials its database **before** it starts
listening (migrations, then `pool.Ping`, each wrapped in
`bootretry.Do`: 5 attempts, backoff 1/2/4/8 s). Once the listener is up,
`/readyz` answers `200 {"status":"ready"}` until shutdown begins.

On `SIGTERM`, `pathmgmt` (`cmd/pathmgmt/main.go`, `serveHTTP`):

1. flips `/readyz` to `503 {"status":"not_ready"}`;
2. calls `http.Server.Shutdown` with a 10 s budget;
3. cancels the outbox relay and waits (same budget) for its in-flight pass;
4. closes the Kafka writer and finally the pgx pool.

The projector does the same with its admin server and the analytics
consumer (waits for the in-flight message to commit or dead-letter). `mcp`
and `pathmgmt-reports` simply call `Shutdown` with a 10 s budget; they have
no `/readyz`.

`/readyz` does **not** track Postgres or Kafka health after boot. A database
outage surfaces as 500 responses and relay errors, not as an unready pod.

## Migrations

| Schema | Files | Run by | DSN |
| --- | --- | --- | --- |
| OLTP | `migrations/0001_init` … `0008_outbox_trace_context` (`*.up.sql`) | `pathmgmt` **and** `mcp`, at every boot, before the pool opens | `MIGRATIONS_DATABASE_URL`, else `DATABASE_URL` |
| Analytics | `migrations/analytics/0001_report.up.sql` | `pathmgmt-projector`, at every boot | `ANALYTICS_DATABASE_URL` |

Both use golang-migrate (`internal/adapters/outbound/postgres/migrate.go`),
which records its version in the default `schema_migrations` table and
serialises concurrent runners with a session-scoped `pg_advisory_lock`.
Because of that lock, a PgBouncer transaction-pool DSN breaks migrations
(replicas crash-loop with `unnamed prepared statement does not exist` or
statement timeouts); point `MIGRATIONS_DATABASE_URL` at a direct connection
([ADR 0015](../adr/0015-migrations-direct-postgres-connection.md)). The chart
reads it from the same secret as `DATABASE_URL`, key
`MIGRATIONS_DATABASE_URL`, marked `optional: true`.

There is no separate migration Job or `make migrate` target: a rollout *is*
the migration. A failed migration makes the pod exit non-zero
(`service exited with error`), so the old ReplicaSet keeps serving.

Keep the analytics database separate from the OLTP database: both schemas
use golang-migrate's default `schema_migrations` table, so pointing them at
the same database makes each runner see the other's version.

## Kafka

One fleet broker. Writers are kafka-go with the `Hash` balancer (FNV-1a
over the key), `RequiredAcks: RequireAll`, `BatchTimeout: 10ms` and
`AllowAutoTopicCreation: true`
(`internal/adapters/outbound/kafka/writer_config.go`, `publisher.go`). Every
value is a structured-mode CloudEvents 1.0 JSON document
(`internal/adapters/kafka/cloudevents`); W3C `traceparent`/`tracestate`
travel as Kafka headers ([ADR 0027](../adr/0027-w3c-trace-context-on-kafka-headers.md)).

| Topic | Direction | Producer / consumer | Key | Event types |
| --- | --- | --- | --- | --- |
| `warehouse.process-path-management.events` | produced | `pathmgmt` (outbox relay, or direct without Postgres) | `PathId`, or `SiteId` for CPT schedules | `com.warehouse.wes.process-path-management.processpath.ProcessPathCreated`, `...processpath.ProcessPathUpdated`, `...processpath.ProcessPathDeactivated`, `com.warehouse.wes.process-path-management.cptschedule.CPTScheduleChanged` |
| `warehouse.process-path-management.analytics` | produced and consumed | produced by `pathmgmt` (same event, same CloudEvents `id`, `dataschema` `urn:warehouse:process-path-management:analytics:<EventName>:v1`); consumed by `pathmgmt-projector`, group **`process-path-management-analytics`** | as above | same four types; the projector applies the three `ProcessPath*` types and commits `CPTScheduleChanged` as a no-op |
| `warehouse.process-path-management.analytics.dlq` | produced | `pathmgmt-projector` dead-letter writer | original key | the original, byte-identical message plus headers `x-dlq-source-topic`, `x-dlq-error`, `x-dlq-failed-at` |

This service consumes **no other context's topic**. Who consumes
`warehouse.process-path-management.events` is on
[Integration](../ecosystem/integration.md).

The projector's reader starts a brand-new group at the **earliest** offset,
so a first deployment projects the whole topic history.

## Outbox relay

Cluster mode (`DATABASE_URL` set, `EVENT_PUBLISHER=kafka`): each use case
inserts one `outbox_events` row per topic (two rows per domain event) inside
the transaction that changes the aggregate, `ON CONFLICT (event_id, topic)
DO NOTHING` (`outbox_publisher.go`). The relay (`outbox_relay.go`) runs in
every api replica:

- claims up to 100 rows `WHERE published_at IS NULL ORDER BY id ... FOR
  UPDATE SKIP LOCKED`, so replicas never send the same row concurrently;
- sends them in id order; on success sets `published_at = now()` and
  increments `attempts`;
- on the first send failure records `attempts + 1` and `last_error` on that
  row, commits the rows already sent, and stops the pass (a later event for
  the same aggregate never overtakes a failed one);
- loops immediately after a full batch, otherwise sleeps
  `OUTBOX_RELAY_INTERVAL` (1 s).

Delivery is at-least-once: a crash between the broker ack and the `UPDATE`
republishes the row on the next pass with the same CloudEvents `id`.
There is no maximum-attempts cut-off and no DLQ on the producer side: a
row that keeps failing blocks the rows behind it in the same pass and
retries forever. Watch `process_path_management.outbox.lag_seconds`
([Observability](./observability.md)).

## Analytics consumer and DLQ

`pathmgmt-projector` (`internal/adapters/inbound/kafka/analytics_consumer.go`)
handles each message as:

1. Decode as CloudEvents. Anything else (bad JSON, the retired flat
   envelope, missing attributes) goes straight to the DLQ and is committed.
2. Types other than the three `ProcessPath*` types are committed as a no-op.
3. `MarkProcessed(event id)` in `analytics_consumed_events`, retried up to 3
   attempts (backoff 100 ms → 2 s). Already seen → commit, nothing applied.
4. Apply the projection (one transaction: insert into
   `analytics_processed_events`, upsert the day row of
   `catalogue_growth_rollup`), retried up to 3 attempts.
5. If step 3 or 4 exhausts its retries, the message goes to the DLQ and the
   offset is committed anyway.

The DLQ writer auto-creates the `.dlq` topic and retries for up to
40 × 250 ms while the new topic elects a leader. If the DLQ write or an
offset commit fails, `Run` returns, the process logs `analytics consumer
stopped` and **keeps running** with `/readyz` still green: nothing consumes
until the pod is restarted. See
[Troubleshooting](./troubleshooting.md).

## Housekeeping sweeper

Runs inside every `pathmgmt` replica that has `DATABASE_URL`
(`internal/adapters/outbound/postgres/sweeper.go`): once at start, then every
`HOUSEKEEPING_INTERVAL` (1 h). Each pass deletes, in batches of 1000:

- `idempotency_keys` rows with `created_at` older than `IDEMPOTENCY_KEY_TTL`
  (24 h);
- `outbox_events` rows that are **published** and older than
  `OUTBOX_RETENTION` (168 h). Unpublished rows are never deleted.

It is safe across replicas (each `DELETE` targets ids chosen by a
subquery). A failed pass logs `housekeeping sweep failed` and retries on the
next tick; a pass that deleted something logs `housekeeping sweep` with
`idempotency_keys_deleted` and `outbox_events_deleted`. Setting the
variables to `0s` does not disable it (see
[Configuration](./configuration.md)).

## Scaling

| Workload | HPA block | Default | Notes |
| --- | --- | --- | --- |
| api | `autoscaling.api` (off) | min 1, max 4, 70 % CPU | Stateless; relay is `SKIP LOCKED`-safe at N replicas. |
| projector | `autoscaling.projector` (off) | min 1, max 2, 70 % CPU | Replicas share group `process-path-management-analytics`; partitions are split by Kafka. |
| reports | `autoscaling.reports` (off) | min 1, max 3, 70 % CPU | Read-only, stateless. |
| frontend | `autoscaling.frontend` (off) | min 1, max 3, 70 % CPU | Static assets. |
| mcp | none, `mcp.replicaCount` only | 1 | The Streamable HTTP handler keeps MCP sessions in process memory and the Service has no session affinity, so a second replica can receive a request for a session it never created. |

When an HPA is enabled the Deployment omits `replicas:`.

Connection budget per process
([ADR 0014](../adr/0014-horizontal-autoscaling-and-pgxpool-tuning.md)):
OLTP pool 10 connections with `statement_timeout=5s` (`pathmgmt` and
`mcp`, one pool each); analytics writer 5 / `10s`; analytics reader 5 /
`15s`, read-only. At the HPA maxima that is 4 × 10 + `mcp` replicas × 10
against the OLTP database and 2 × 5 + 3 × 5 against the analytical one.

## Reports API (`pathmgmt-reports`) {#reports-api-pathmgmt-reports}

These routes are served by `cmd/pathmgmt-reports`
(`internal/adapters/inbound/http/reports_handler.go`) and are **not** in
`apis/openapi.yaml`. They are unauthenticated and read the analytical
database through a read-only pool. There is no OTel middleware on this
router (only request id, request logger and panic recovery).

### `GET /reports/catalogue-growth`

| Query parameter | Required | Format | Meaning |
| --- | --- | --- | --- |
| `from` | yes | RFC 3339 | Window start, inclusive, compared with `day_bucket`. |
| `to` | yes | RFC 3339 | Window end, exclusive. |
| `granularity` | no | `day` | The only accepted value; anything else is 400. |

`200 application/json`:

```json
{
  "rows": [
    {"dayBucket": "2026-10-08T00:00:00Z", "pathsDefined": 3, "pathsRevised": 1, "pathsDeactivated": 0}
  ]
}
```

`dayBucket` is midnight UTC. Each counter is the number of
`ProcessPathCreated`, `ProcessPathUpdated` and `ProcessPathDeactivated`
events whose CloudEvents `time` fell in that UTC day. Days with no events
have no row. `rows` is `[]`, never `null`, when nothing matches.

Errors are `application/problem+json`: 400 type `.../invalid-report-query`
(missing or non-RFC 3339 `from`/`to`, bad `granularity`), 500 type
`.../report-store-error`.

### `GET /reports/catalogue-growth/freshness`

`200 {"lagSeconds": 1234.5}`: now minus the newest `occurred_at` in
`analytics_processed_events`, `0` when the read model is empty. Because the
catalogue changes rarely, this number grows steadily between operator edits
even when the projector is fully caught up; it measures "time since the last
projected change", not consumer lag.

### `GET /healthz`

`200 {"status":"ok"}`.

```bash
kubectl port-forward svc/<release>-reports 8092:80
curl -s 'localhost:8092/reports/catalogue-growth?from=2026-10-01T00:00:00Z&to=2026-11-01T00:00:00Z'
curl -s localhost:8092/reports/catalogue-growth/freshness
```

The same report is available to agents through the MCP tool
`get_catalogue_growth_report` ([MCP tools](../mcp/tools.md)).

## Routine procedures

These follow from the code paths above. They have not been rehearsed against
a live cluster as part of writing this page.

### Re-publish integration events

Consumers rebuild their caches from the topic, so re-publishing is a
supported recovery. To re-send recent events, mark their outbox rows
unpublished; the relay picks them up on its next pass with their original
CloudEvents `id`:

```sql
UPDATE outbox_events
SET published_at = NULL, last_error = NULL
WHERE topic = 'warehouse.process-path-management.events'
  AND aggregate_id = 'PICK';
```

This only works within `OUTBOX_RETENTION` (published rows older than that are
gone). For anything older, revise the path through the API (`PUT
/process-paths/{pathId}`), which raises a fresh `ProcessPathUpdated` if
something actually changed.

### Unstick the relay

If `outbox.lag_seconds` keeps rising, find the blocking row:

```sql
SELECT id, event_type, topic, attempts, last_error, created_at
FROM outbox_events
WHERE published_at IS NULL
ORDER BY id
LIMIT 5;
```

Fix the cause in `last_error` (broker unreachable, topic authorisation,
message too large). Do not delete the row unless you accept that consumers
will never see that event.

### Rebuild the analytics read model

The projection deduplicates on `analytics_consumed_events`, so replaying
the topic alone changes nothing. Scale the projector to 0, then in the
analytical database:

```sql
TRUNCATE analytics_consumed_events, analytics_processed_events, catalogue_growth_rollup;
```

reset the group to the earliest offset (for example
`kafka-consumer-groups.sh --bootstrap-server <broker> --group process-path-management-analytics --topic warehouse.process-path-management.analytics --reset-offsets --to-earliest --execute`)
and scale the projector back up. The rebuild is only as complete as the
analytics topic's retention.

### Inspect or replay the DLQ

There is no replay tool in this repository. DLQ messages keep the original
key and value, so after fixing the cause they can be copied back onto
`warehouse.process-path-management.analytics` with any Kafka producer.
Duplicates are harmless (dedupe on the CloudEvents `id`).

### Rotate database credentials

Credentials come only from the secrets (`DATABASE_URL`,
`MIGRATIONS_DATABASE_URL`, `ANALYTICS_DATABASE_URL`,
`ANALYTICS_READER_DATABASE_URL`). Pools read them once at boot, so update
the secret and restart the affected Deployments
(`kubectl rollout restart deploy/<release> deploy/<release>-mcp ...`). The
api pod template carries only a ConfigMap checksum, so a secret change does
not roll it by itself.
