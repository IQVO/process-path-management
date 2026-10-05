---
id: 0015-migrations-direct-postgres-connection
slug: /adr/0015-migrations-direct-postgres-connection
title: "15. Run golang-migrate against a direct Postgres connection, not PgBouncer"
sidebar_label: "15. Migrations bypass PgBouncer"
sidebar_position: 15
description: "ADR 0015 — Phase 4 fleet-wide fix, ported from order-management's ADR-0029 (PR #115): golang-migrate's postgres driver takes a session-scoped pg_advisory_lock to serialize concurrent migration runs, which is incompatible with PgBouncer's transaction-pooling mode (warehouse-infra PR #43). Two or more process-path-management replicas starting concurrently (HPA scale-out, or an ordinary rolling deploy) would crash-loop until one won the advisory-lock race. Fix: a second env var, MIGRATIONS_DATABASE_URL (warehouse-infra PR #44), carries a direct (non-pooled) connection string used ONLY for the migration step; DATABASE_URL/the runtime pgxpool is untouched and keeps going through PgBouncer."
---

# 15. Run golang-migrate against a direct Postgres connection, not PgBouncer

## Status

Accepted — implemented in the same change that introduces this record.
This ports order-management's [ADR-0029](https://github.com/claudioed/order-management/blob/develop/docs/docs/adr/0029-migrations-direct-postgres-connection.md)
(PR #115), the fleet's reference implementation for this fix, to
process-path-management — the ninth of the fleet's 9 OLTP services to
adopt it. `warehouse-infra` PR #44 already provisions the
`MIGRATIONS_DATABASE_URL` secret key for all 9 OLTP services (this one
included), so no further `warehouse-infra` work is needed to land this
service's half of the fix.

## Context

`warehouse-infra`'s PgBouncer rollout (PR #43, Phase 3) repointed every
one of the fleet's 9 OLTP services' `DATABASE_URL` secret at PgBouncer
(`terraform/pgbouncer.tf`), in **transaction-pooling** mode
(`pool_mode = "transaction"`). That is the correct mode for this fleet's
steady-state traffic — application code never holds session state across
statements — but it does not cover **migrations**.

Both of this service's Postgres-backed composition roots —
`cmd/pathmgmt/main.go` (`buildPersistence`) and `cmd/mcp/main.go`
(`buildRepo`) — run golang-migrate's postgres driver
(`github.com/golang-migrate/migrate/v4/database/postgres`) against
`DATABASE_URL` at process startup, before serving any traffic.
golang-migrate's postgres driver calls `SELECT pg_advisory_lock($1)` to
serialize concurrent migration runs — by design, so that if two
processes start at once and both try to run the same migration,
whichever loses the lock blocks rather than races.

`pg_advisory_lock` is **session-scoped**: the lock is held by whichever
physical backend connection issued it. PgBouncer's transaction-pooling
mode does not preserve that mapping — each statement in a client's
logical session can be routed to a different physical backend
connection, because the client's backend connection is returned to the
pool the instant its transaction commits. Two pods dialing PgBouncer
concurrently can each get different, rotating backend connections mid-
"session" from the application's point of view, so the advisory lock
never behaves as a real mutex. Whichever pod's statements land on a
backend connection with unexpected transaction/prepared-statement state
gets errors like `pq: unnamed prepared statement does not exist` or
`pq: canceling statement due to statement timeout`, and crash-loops for
roughly 1-2 minutes until the race resolves.

This is a **latent, fleet-wide, production-blocking bug**: it fires on
any ordinary rolling ArgoCD deploy with more than 1 replica of an OLTP
service, and on every HPA scale-out event. It was found and reproduced
live against order-management during Phase 4 (k6/HPA load-test
validation) cleanup — see order-management's ADR-0029 for the full
incident record and live verification. It blocks safely enabling this
service's own `autoscaling.api.enabled` HPA
([ADR 0014](./0014-horizontal-autoscaling-and-pgxpool-tuning.md))
fleet-wide, the same way it blocked order-management's ADR-0026 HPAs.

`cmd/pathmgmt-projector` and `cmd/pathmgmt-reports` are **not** affected:
they run against `ANALYTICS_DATABASE_URL`, this service's own analytical
database, which — like every service's analytics DSN in this fleet —
was never routed through PgBouncer in the first place (low-QPS, single
long-lived consumer per service; `warehouse-infra`'s
`analytics_database_urls` local stays direct against Postgres). Only
`cmd/pathmgmt` and `cmd/mcp`, which share the OLTP `DATABASE_URL`, need
this fix.

## Decision

Give this service's OLTP `DATABASE_URL` a **second** connection string,
`MIGRATIONS_DATABASE_URL` — a direct (non-pooled, session-mode) Postgres
connection string, same user/password/dbname as `DATABASE_URL`, pointed
at Postgres itself rather than PgBouncer — used **only** for the
golang-migrate startup step in `cmd/pathmgmt/main.go` and
`cmd/mcp/main.go`. `DATABASE_URL` and the pgxpool built from it are
completely unchanged: every request either binary serves still goes
through PgBouncer in transaction-pooling mode, exactly as PR #43 set up.

This is architecturally identical to order-management's ADR-0029 and to
the analytics-DSN carve-out PR #43 already made for this service's own
`ANALYTICS_DATABASE_URL`: **migrations need a direct/session connection;
steady-state application traffic goes through the pooler.** PgBouncer's
`pool_mode` stays `transaction` — this fix routes one specific,
short-lived, startup-only operation around the pooler, nothing more.

`warehouse-infra` PR #44 already provisions `MIGRATIONS_DATABASE_URL` as
a new key alongside the existing `DATABASE_URL` key in each of the 9 OLTP
services' `kubernetes_secret.service_db`, this service's `process-path-
management-db` Secret included — no further `warehouse-infra` work is
needed. This repo's `cmd/pathmgmt/main.go` and `cmd/mcp/main.go` (both
run migrations) now read `MIGRATIONS_DATABASE_URL` for the migration
step only:

```go
migrationsDatabaseURL := getenv("MIGRATIONS_DATABASE_URL", databaseURL)
...
buildPersistence(ctx, databaseURL, migrationsDatabaseURL, migrationsPath, logger)
...
postgres.RunMigrations(migrationsDatabaseURL, migrationsPath)  // migrations only
// the pgxpool opened right after this still uses databaseURL, unchanged
```

The fallback to `databaseURL` when `MIGRATIONS_DATABASE_URL` is unset
keeps every environment that doesn't provision the split — local dev,
CI integration tests, or any cluster whose Terraform predates this fix —
working exactly as before, byte-for-byte. Nothing about local dev or CI
changes as a result of this PR.

The chart (`charts/process-path-management`) gains a new
`database.migrationsExistingSecretKey` value (default
`"MIGRATIONS_DATABASE_URL"`), rendering a `MIGRATIONS_DATABASE_URL` env
var — sourced from the same `existingSecret` `DATABASE_URL` already
uses, with `secretKeyRef.optional: true` — in both
`templates/deployment.yaml` (`cmd/pathmgmt`) and
`templates/mcp-deployment.yaml` (`cmd/mcp`). Without this env wiring the
secret key existing server-side does nothing: Kubernetes only injects
env vars a Deployment's pod spec explicitly asks for.
`templates/projector-deployment.yaml` and
`templates/reports-deployment.yaml` are untouched — they read
`ANALYTICS_DATABASE_URL`, not `DATABASE_URL`, and were never behind
PgBouncer.

### Why not just make PgBouncer's pool_mode session for this fleet?

Rejected, for the same reason order-management's ADR-0029 rejected it:
session pooling would fix the advisory-lock problem but throws away the
entire point of PgBouncer for this fleet — transaction pooling is what
lets many short-lived HTTP-request-scoped OLTP connections share a small
number of physical Postgres backends. Switching to session mode
fleet-wide to accommodate a ~1-2 second startup-time lock call is the
tail wagging the dog.

### Why not just remove the advisory lock / skip migrations on non-leader replicas?

Rejected, again mirroring order-management's ADR-0029: golang-migrate's
advisory lock is exactly the right mechanism *given a session-scoped
connection* — the bug is the mismatch between that mechanism and the
pooling mode migrations run through, not the mechanism itself. An
init-container Job that runs migrations exactly once before any replica
starts was considered but rejected: it is a bigger architectural change
(a new Kubernetes resource type per binary, coordination with each
Deployment's rollout strategy) for the same outcome this two-line
env-var fallback already achieves, and it would still need its own
direct-vs-pooled connection decision for the Job itself.

## Consequences

- **Fixes** the same fleet-wide crash-loop bug order-management's
  ADR-0029 fixed, for this service's two OLTP composition roots
  (`cmd/pathmgmt`, `cmd/mcp`). `cmd/pathmgmt-projector` and
  `cmd/pathmgmt-reports` needed no change — their `ANALYTICS_DATABASE_URL`
  was never behind PgBouncer.
- **No runtime behavior change**: `DATABASE_URL` is untouched, so
  request-serving connection pooling, `pool_mode`, and PgBouncer's own
  configuration are all unaffected by this PR.
- **No behavior change for environments without the split**: the
  `getenv("MIGRATIONS_DATABASE_URL", databaseURL)` fallback means local
  dev and CI integration tests keep using `DATABASE_URL` for everything,
  exactly as before.
- **Unblocks this service's own HPAs** ([ADR 0014](./0014-horizontal-autoscaling-and-pgxpool-tuning.md))
  being safely enabled: an HPA scale-out of `api` or `mcp` is exactly the
  "2+ replicas start concurrently" trigger for this bug.
- With this PR, all 9 of the fleet's OLTP services now consume the
  `MIGRATIONS_DATABASE_URL` secret key `warehouse-infra` PR #44
  provisioned for all of them — order-management was first (PR #115,
  the reference implementation), this is the ninth and last.
- One more secret key to keep in sync per service going forward;
  mechanically generated by Terraform from the same `local.services` map
  as `DATABASE_URL` already is (see `warehouse-infra` PR #44), so there
  is no new per-service manual step.

## Verification

`make check` (fmt-check, vet, build, lint, test) and `make check-all`
(check + 90% coverage gate on domain/application — measured 99.7% — +
arch-test + 30/30 BDD scenarios) both pass locally. `make integration`
(golang-migrate against a real local Postgres, `-tags=integration`)
passes, including the two new regression tests below.

Two regression tests were added, mirroring order-management's ADR-0029
tests, once for each affected binary:

- `cmd/pathmgmt/wiring_test.go`: `TestMigrationsDatabaseURLFallback`
  proves the `getenv` fallback in both directions (falls back to
  `DATABASE_URL` when unset; uses `MIGRATIONS_DATABASE_URL`, not
  `DATABASE_URL`, when set).
  `TestBuildPersistence_UsesMigrationsDatabaseURLNotDatabaseURLForMigrations`
  proves `buildPersistence` itself threads `migrationsDatabaseURL` into
  the migration step (not `databaseURL`), using a schemeless
  `MIGRATIONS_DATABASE_URL` to get a distinctive parse error that a
  dial/"connection refused" error against `databaseURL` could never
  produce.
- `cmd/mcp/wiring_test.go`: the same two tests
  (`TestMigrationsDatabaseURLFallback`,
  `TestBuildRepo_UsesMigrationsDatabaseURLNotDatabaseURLForMigrations`)
  against `cmd/mcp`'s own `buildRepo`.

`helm lint charts/process-path-management` passes. `helm template ... |
grep MIGRATIONS_DATABASE_URL` confirms the new env var renders, with
`optional: true`, in both `templates/deployment.yaml` and
`templates/mcp-deployment.yaml`, and does NOT render in
`templates/projector-deployment.yaml` / `templates/reports-deployment.yaml`
(those binaries do not take this parameter).

Live cluster rollout (deploying the `warehouse-infra` secret-key change
alongside this chart/code change, then forcing 2+ `api`/`mcp` replicas
to start concurrently) is the same class of verification order-
management's ADR-0029 already performed and recorded for this exact
fix pattern; this service is expected to behave identically since it
uses the same `getenv`/chart-wiring shape. A follow-up live-cluster
verification pass for this service specifically is tracked as part of
the same Phase 4 fleet-wide rollout that produced order-management's
verification record.
