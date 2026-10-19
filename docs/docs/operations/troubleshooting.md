---
id: troubleshooting
title: Troubleshooting
sidebar_label: Troubleshooting
description: Symptom, cause, check and fix for the failure modes visible in the process-path-management code - boot failures, readiness, outbox and consumer stalls, DLQ growth, and every RFC 7807 problem type the API returns.
---

# Troubleshooting

Every row below comes from a code path in this repository. Log messages are
quoted exactly; see [Observability](./observability.md) for their fields.

## Boot and readiness

| Symptom | Cause | Check | Fix |
| --- | --- | --- | --- |
| Pod restarts with `service exited with error` and `run migrations (after 5 attempts): ...` | Postgres unreachable, wrong credentials, or the first dial reset by a warming Istio sidecar for longer than the boot retry budget (5 attempts, ~15 s of backoff) | Preceding `retrying` lines (`op`, `err`); `kubectl get secret` for `DATABASE_URL` / `MIGRATIONS_DATABASE_URL` | Fix the DSN or network. A pure sidecar race succeeds on the next restart. |
| Migrations fail with `unnamed prepared statement does not exist` or statement timeouts; replicas crash-loop together | `MIGRATIONS_DATABASE_URL` unset, so golang-migrate runs through PgBouncer transaction pooling and its session advisory lock does not hold ([ADR 0015](../adr/0015-migrations-direct-postgres-connection.md)) | Is the `MIGRATIONS_DATABASE_URL` key present in the database secret? (the chart mounts it `optional: true`) | Add a direct, non-pooled DSN under that key and restart. |
| Migration reports a dirty version | A previous run died mid-migration; golang-migrate refuses to continue | `SELECT * FROM schema_migrations;` (`dirty = true`) | Repair the half-applied change by hand, then set `dirty = false` at the right version. There is no `make migrate` helper. |
| `pathmgmt-projector` or `pathmgmt-reports` exits immediately with `ANALYTICS_DATABASE_URL is required` | Env var missing | Pod env; analytics secret keys `ANALYTICS_DATABASE_URL` / `ANALYTICS_READER_DATABASE_URL` | Set `analytics.database.*` or `existingSecret`. |
| Projector migrations report a version that is not in `migrations/analytics` | The analytical DSN points at the OLTP database; both schemas share golang-migrate's `schema_migrations` table | `SELECT current_database();` on both DSNs | Use a separate database for analytics. |
| `/readyz` returns 503 `{"status":"not_ready"}` | The process received SIGTERM and is draining (this is the only thing that flips it) | Pod is `Terminating` | Expected. Readiness does not track Postgres or Kafka after boot. |
| In the kind cluster the api pod keeps receiving traffic during shutdown | warehouse-infra's `helm-values/process-path-management.yaml` overrides the readiness probe to `/healthz`, which never flips | `kubectl get deploy <release> -o yaml` → `readinessProbe.httpGet.path` | Point the override at `/readyz` (warehouse-infra change). |
| Startup probe fails before the app logs anything | Image or `command:` wrong; the MCP, projector and reports Deployments override the image `ENTRYPOINT` with `/app/<binary>` | `kubectl describe pod` | Fix the image tag or chart values. |

## Writes and the API

Every error body is `application/problem+json` with `type`
`https://errors.process-path-management.warehouse-systems.dev/<slug>`,
`title`, `status`, `detail` (the Go error text) and `instance` (the request
path). Mapping: `internal/adapters/inbound/http/errors.go`,
`idempotency.go`, `server.go`, `reports_handler.go`.

| Status | `type` slug | When | Fix |
| --- | --- | --- | --- |
| 400 | `malformed-request-body` | Body is not valid JSON (or could not be read by the idempotency middleware) | Send valid JSON. |
| 400 | `invalid-query-parameter` | `GET /process-paths?all=` is not exactly `true` or `false` | Use `true`/`false`. |
| 400 | `idempotency-key-required` | `POST /process-paths` without `Idempotency-Key`, only when `DATABASE_URL` is set | Send a unique key per logical create. |
| 400 | `invalid-report-query` | Reports API: missing or non-RFC 3339 `from`/`to`, or `granularity` other than `day` | Fix the query. |
| 404 | `path-not-found` | `GET`/`PUT`/`DELETE /process-paths/{pathId}` for an unknown id | Check the id (case-sensitive). |
| 404 | `cpt-schedule-not-found` | `GET /sites/{siteId}/cpt-schedule` before any `PUT` | Define the schedule first. |
| 404 | `route-not-found` | Any path that matches no route (including an empty path segment) | Check the URL; behind Kong the prefix is `/api/process-path-management`. |
| 409 | `path-already-exists` | `POST` with a `pathId` that exists, **active or deactivated**; also a concurrent create that lost the insert race | Pick another id. A deactivated id can never be reused. |
| 409 | `path-referenced-by-cpt-schedule` | `DELETE` of a path still listed in some site's `eligiblePathIds`; `detail` names the sites ([ADR 0026](../adr/0026-reject-deactivation-of-paths-in-cpt-schedules.md)) | `PUT` those schedules without the path, then retry. |
| 409 | `concurrent-modification` | The row's `version` changed between load and save ([ADR 0017](../adr/0017-optimistic-concurrency-version-column.md)) | Re-read and retry. |
| 422 | `idempotency-key-reused` | Same `Idempotency-Key` with a different body (SHA-256 of the raw bytes, so whitespace counts) | Use a new key for a different request. |
| 422 | `path-deactivated` | `PUT /process-paths/{pathId}` on a deactivated path | Deactivation is terminal; define a new path. |
| 422 | `empty-path-id`, `empty-match-prefix`, `match-prefix-not-lowercase`, `no-required-capabilities`, `invalid-cycle-time-p95`, `invalid-destination-location-role`, `invalid-intake` | Path invariants. `invalid-cycle-time-p95` also covers a missing or unparseable `cycleTimeP95` (it must be a positive Go duration such as `2h`). `destinationLocationRole` must be `Drop`, `WorkCenter`, `Shipping` or omitted. `intake` must hold distinct values of `CUSTOMER_ORDER`, `STOCK_TRANSFER`, `SLOT_RELOCATION`, `INVENTORY_CONTROL`, `REPLENISHMENT` ([ADR 0034](../adr/0034-process-path-intake.md), [ADR 0036](../adr/0036-replenish-process-path.md)) | Fix the body. |
| 422 | `ineligible-path-id` | A cutoff's `eligiblePathIds` names an unknown or deactivated path | Define or pick an Active path. |
| 422 | `empty-timezone`, `invalid-timezone`, `no-cutoffs`, `empty-cpt-id`, `duplicate-cpt-id`, `empty-local-time`, `invalid-local-time`, `no-days-of-week`, `invalid-day-of-week`, `empty-ship-method`, `no-eligible-path-ids` | CPT schedule invariants (IANA zone, `HH:MM` 24-hour, days `Mon`…`Sun`, unique `cptId` per site) | Fix the body. |
| 500 | `internal-error` | Any unmapped error: database down, `statement_timeout` (5 s) hit, idempotency bookkeeping failure | Logs around the `request_id`; Postgres health. |
| 500 | `report-store-error` | Reports API could not query the analytical database (15 s statement timeout, read-only pool) | Check the analytical database. |

There is no 412: the API has no `If-Match`/ETag precondition; concurrency is
reported as 409 `concurrent-modification`.

### Idempotency-key surprises

| Symptom | Cause | Fix |
| --- | --- | --- |
| A retried `POST` returns the same 422 or 409 as the first attempt even after fixing the data | Every non-panic response is cached under its key, including 4xx ([ADR 0011](../adr/0011-idempotency-key-middleware.md)) | Send a new `Idempotency-Key`. |
| An old key is accepted again as new | The sweeper deletes keys older than `IDEMPOTENCY_KEY_TTL` (24 h) | Expected; keys are only remembered for the TTL. |
| `POST` works locally without the header but fails in the cluster | The middleware is only wired when `DATABASE_URL` is set | Always send the header. |
| A retry hangs until the first request finishes | Concurrent requests with the same key serialise on the `idempotency_keys` primary key | Expected; the second request then replays the first's response. |

## Events not reaching consumers

| Symptom | Cause | Check | Fix |
| --- | --- | --- | --- |
| Pod exits at boot (`CrashLoopBackOff`) with `DATABASE_URL is required` / `KAFKA_BROKERS is required` | A mandatory variable is missing: there is no in-memory or log-only fallback ([ADR 0037](../adr/0037-postgres-and-kafka-are-the-only-runtime.md)). `EVENT_PUBLISHER` other than `kafka` also stops the boot | The error names the variable; `helm template` refuses to render without `database.url`/`database.existingSecret` and `kafka.brokers` | Set the value and redeploy. |
| Pod Running but not Ready; `/readyz` answers `503 dependency-unavailable` | Postgres or every Kafka broker is unreachable (verdict cached 3 s) | The problem detail names the dependency | Restore the dependency; writes already accepted are in the outbox and are relayed afterwards. |
| `process_path_management.outbox.lag_seconds` keeps rising; `outbox relay pass failed` every second | Broker unreachable, or one row keeps failing and blocks the rows behind it until it reaches `OUTBOX_MAX_ATTEMPTS` (default 20) | `SELECT id, event_type, attempts, last_error FROM outbox_events WHERE published_at IS NULL ORDER BY id LIMIT 5;` | Fix the cause in `last_error`; see [Runbook](./runbook.md#unstick-the-relay). |
| One event never arrives, later events of the same path do; `outbox relay: row reached the attempt cap and is no longer claimed` logged | The row failed `OUTBOX_MAX_ATTEMPTS` times and is parked ([ADR 0030](../adr/0030-outbox-relay-attempt-cap.md)); `outbox.lag_seconds` stays high while it is parked | The ERROR line (`outbox_id`, `topic`, `event_type`, `key`) and the row's `last_error` | Fix the cause, then `UPDATE outbox_events SET attempts = 0 WHERE id = ...` ([Runbook](./runbook.md#re-arm-an-outbox-row-that-reached-the-attempt-cap)). |
| Consumers see an event twice | At-least-once relay: crash between broker ack and the `UPDATE`, or a manual re-publish | Same CloudEvents `id` on both copies | Expected; consumers key by `PathId`/`SiteId` and the `id`. |
| A consumer sees events for one path out of order | Should not happen: rows drain in id order and the key (`PathId`) pins a partition via the `Hash` balancer ([ADR 0013](../adr/0013-kafka-writer-hash-balancer.md)). The one deliberate exception is a row parked at the attempt cap and later re-armed: it arrives after newer events of its path ([ADR 0030](../adr/0030-outbox-relay-attempt-cap.md)) | Did someone change the writer's balancer? Was a row re-armed? | Keep `kafkago.Hash`; after a re-arm, revise the path to re-publish its current state. |
| `PUT` returned 200 but no event was published | The revision changed nothing (same values), or `DELETE` hit an already-deactivated path; both are no-ops by design | Compare the body with the current resource | Expected. |

## Analytics

| Symptom | Cause | Check | Fix |
| --- | --- | --- | --- |
| Projector pod restarting, eventually `CrashLoopBackOff` | The consumer loop exited (an offset commit or a DLQ write failed, the broker is gone). Since gap fix D13 the projector then flips `/readyz` to 503 and exits 1 with `analytics consumer loop exited` instead of staying Ready while projecting nothing | Projector logs (previous container: `kubectl logs --previous`), broker health | Fix the broker or analytics database problem; the restarted pod resumes from its committed offsets. |
| `warehouse.process-path-management.analytics.dlq` growing | Non-CloudEvents messages on the analytics topic, or the analytical database failing 3 attempts in a row | `analytics: sending message to dead-letter topic` lines (`ce_type`, `error`); DLQ headers `x-dlq-error` | Fix the database or the producer; then replay (see [Runbook](./runbook.md#inspect-or-replay-the-dlq)). |
| Report empty after enabling analytics | The projector has not consumed yet, or the analytics database is a different one from the reports binary's | Topic offsets for `warehouse.process-path-management.analytics` and `analytics_consumed_events` | Check the projector's `KAFKA_BROKERS`/`ANALYTICS_DATABASE_URL` and its logs. |
| Replaying the topic does not change the report | Both `analytics_consumed_events` and `analytics_processed_events` deduplicate on the CloudEvents `id` | — | Truncate the three analytics tables before resetting offsets ([Runbook](./runbook.md#rebuild-the-analytics-read-model)). |
| `freshness` says `lagSeconds` is hours or days | It is "now minus the last projected event time", not consumer lag; it grows whenever nobody edits paths | Compare with the last `ProcessPath*` event time | Usually nothing to fix. Use broker-side group lag for real consumer lag. |
| `get_catalogue_growth_report` missing from MCP `tools/list` | `REPORTS_BASE_URL` unset on the MCP pod (the chart sets it only when `analytics.enabled` or `mcp.reportsBaseUrl`) | MCP pod env | Set it and restart. |
| MCP tool returns `reports client: unexpected status 400` | The tool forwards `from`/`to`/`granularity` as given; the reports API rejected them | Pass RFC 3339 timestamps and `day` | — |

## MCP

| Symptom | Cause | Fix |
| --- | --- | --- |
| MCP sees no paths that the REST API returns | `mcp` points at a different Postgres than `pathmgmt` (`DATABASE_URL` is mandatory for both; there is no in-memory store) | Point both binaries at the same Postgres. |
| `tools/call` fails with an unknown session after scaling MCP to 2+ replicas | Sessions live in process memory; the Service has no affinity | Keep `mcp.replicaCount: 1`. |
| `get_process_path` returns a tool error | Not found is returned as an error result, by design | Check the id with `list_process_paths` (`activeOnly: false`). |

## Housekeeping

| Symptom | Cause | Fix |
| --- | --- | --- |
| `outbox_events` or `idempotency_keys` grows without bound | `DATABASE_URL` unset on the replica (no sweeper), `HOUSEKEEPING_INTERVAL=0` (sweeper disabled), a zero `IDEMPOTENCY_KEY_TTL`/`OUTBOX_RETENTION` (that half disabled), or `housekeeping sweep failed` on every tick | Check the `housekeeping sweeper configured` startup line (`enabled`, TTLs) and the logs; the sweeper deletes in 1000-row batches and resumes on the next tick. |
| `IDEMPOTENCY_KEY_TTL=0s` keeps keys forever | Intended since gap fix D15: a zero duration disables that half of the sweeper (ADR 0018) | Set a positive Go duration (for example `24h`) to delete them again. |
