---
id: 0014-horizontal-autoscaling-and-pgxpool-tuning
slug: /adr/0014-horizontal-autoscaling-and-pgxpool-tuning
title: "14. Per-workload HorizontalPodAutoscaler and pgxpool MaxConns/statement_timeout tuning"
sidebar_label: "14. HPA + pgxpool tuning"
sidebar_position: 14
description: "ADR 0014 — Phase 3 (scalability) for process-path-management, porting order-management's ADR 0026 / PR #110 reference: an autoscaling/v2 HorizontalPodAutoscaler per independently-assessed workload (api max 4, analytics-projector max 2, analytics-reports max 3, frontend max 3; mcp explicitly excluded for a real in-memory-session reason), all default-disabled via values.yaml so this PR changes nothing on merge; plus explicit pgxpool.Config MaxConns caps and per-pool statement_timeout values, sized against the shared Postgres instance's real max_connections=100 ceiling and PgBouncer (warehouse-infra PR #43) in front of the OLTP DSN."
---

# 14. Per-workload HorizontalPodAutoscaler and pgxpool MaxConns/statement_timeout tuning

## Status

Accepted — implemented in the same change that introduces this record.
This is Phase 3 (scalability) of the fleet production-readiness plan for
`process-path-management`, ported directly from `order-management`'s
ADR 0026 (PR #110) — the fleet's Phase 3 reference — adapted for this
repo's own binary shape and its stricter no-sibling-outbound-call rule.
Prior phases here: ADR 0011 (idempotency-key middleware), ADR 0012
(Kafka DLQ / graceful shutdown), ADR 0013 (Kafka writer Hash balancer).

## Context

Before this change, this chart had exactly one `HorizontalPodAutoscaler`
template, unconditionally targeting the `api` Deployment only, wired to a
flat `autoscaling.enabled/minReplicas/maxReplicas/
targetCPUUtilizationPercentage` block — untested against the other three
Deployments this chart renders (`mcp`, `frontend`, `analytics-projector`,
`analytics-reports`), and never assessed for whether scaling
`analytics-projector` past 1 replica was even safe given its Kafka
consumer-group membership.

Separately, no pool in the codebase set an explicit `pgxpool.Config.
MaxConns`, so every pool — the OLTP pool
(`internal/adapters/outbound/postgres/pool.go`, used by `cmd/pathmgmt`
and `cmd/mcp`) and the two analytics pools
(`internal/adapters/outbound/analyticsstore/pool.go`'s `NewPool` used by
`cmd/pathmgmt-projector`, and `NewReadOnlyPool` used by
`cmd/pathmgmt-reports`) — ran on pgx's library default, `max(4,
runtime.NumCPU())` connections per process. No pool set a
`statement_timeout` either, so a single runaway query could hold a
pooled connection indefinitely, with nothing to cancel it.

### This repo's binary shape (verified from code, not assumed)

`cmd/` has exactly four binaries plus a `web/` frontend, per AGENTS.md
and confirmed against the actual source tree:

- `cmd/pathmgmt` — REST API + in-process outbox relay (`:8080`). The
  `api` Deployment.
- `cmd/mcp` — MCP server, Streamable HTTP (`:8090`, ADR 0006). The `mcp`
  Deployment.
- `cmd/pathmgmt-projector` — analytics projector (admin `:8091`, ADR
  0007). The `analytics-projector` Deployment.
- `cmd/pathmgmt-reports` — read-only catalogue-growth report API
  (`:8092`, ADR 0007). The `analytics-reports` Deployment.
- `web/` — the `process_path_mfe` Module Federation remote, served by
  its own nginx-unprivileged pod. The `frontend` Deployment.

This is the same five-Deployment shape as `order-management`'s chart —
`api`, `mcp`, `analytics-projector`, `analytics-reports`, `frontend` —
so this PR ports order-management's exact HPA/pool topology rather than
inventing a different one. One repo-specific fact changes the *safety
reasoning*, not the *shape*: **this service has NO inbound Kafka
consumer on `cmd/pathmgmt` at all** — unlike `order-management`'s `api`,
whose `RepromiseConsumer` needed its own per-workload consumer-group
safety argument, `cmd/pathmgmt`'s only Kafka activity is outbound
publishing (it is the published-language SOURCE, never a consumer — see
AGENTS.md and `TestNoSiblingContextOutboundCalls` in
`internal/architecture/architecture_test.go`, which also fails the
build if any outbound adapter imports `net/http`, so no HTTP client to
a sibling can ever sneak in). This service's *only* inbound Kafka
consumer at all is `internal/adapters/inbound/kafka/analytics_consumer.go`,
run by `cmd/pathmgmt-projector`, reading this service's OWN analytics
topic (ADR 0007) — never a sibling's topic. That makes the `api`
Deployment's HPA reasoning simpler (and safer) than `order-management`'s:
no consumer-group question to answer at all, just outbox-relay
concurrency (see below).

### Finding: shared Postgres instance, PgBouncer in front of OLTP, real max_connections

Verified fleet-wide facts (not re-derived here): all 10 backend
services share ONE Postgres server instance, each with its own logical
database/role, `max_connections=100` unmodified default — the same
finding order-management's ADR 0026 made and confirmed live. Since that
PR, `warehouse-infra` PR #43 (merged) put PgBouncer in front of that
Postgres instance in transaction-pooling mode; every service's OLTP
`DATABASE_URL` Secret — including this service's — is **already
re-pointed at PgBouncer**, no chart or code change required here.
Analytics DSNs stay **direct** against Postgres per PR #43's own
reasoning (the analytics pools are small, low-cardinality, and do not
need PgBouncer's server-side multiplexing the way the fan-out-scalable
OLTP path does) — this PR mirrors that split exactly:
`postgres.NewPool` (OLTP, via PgBouncer) keeps a generous `MaxConns`
because PgBouncer absorbs the real server-side connection multiplexing;
`analyticsstore.NewPool`/`NewReadOnlyPool` (analytics, direct to
Postgres) keep the same small, flat ceilings order-management chose.

## Decision

### 1. HorizontalPodAutoscaler — one per independently-assessed workload

Assessed each of the five Deployments this chart renders on its own
merits, mirroring order-management's per-workload table:

| Deployment | HPA? | min | max | target CPU | Why |
|---|---|---|---|---|---|
| `api` (`cmd/pathmgmt`) | Yes | 1 | 4 | 70% | Stateless OLTP HTTP with **no inbound Kafka consumer at all** (this service's Kafka role is publish-only — see AGENTS.md / `TestNoSiblingContextOutboundCalls`). Its only background work is the in-process `postgres.OutboxRelay`, which claims rows via `SELECT ... FOR UPDATE SKIP LOCKED ORDER BY id` — `outbox_relay.go`'s own doc comment states this is safe for two or more relay instances running concurrently during a rolling deploy, so N steady-state replicas share the same safety property. Nothing here is unsafe at N>1. |
| `analytics-projector` (`cmd/pathmgmt-projector`) | Yes, capped at **2**, not api's 4 | 1 | 2 | 70% | Its Kafka consumer (`kafka.AnalyticsConsumerGroup` == `"process-path-management-analytics"`, ADR 0007/0012) is a **stable, shared** consumer group with no per-instance uniqueness (verified in `internal/adapters/inbound/kafka/analytics_consumer.go`) — the fleet's correctly-scalable pattern, N replicas share the topic's partitions via normal Kafka rebalancing. Capped at 2 rather than left at 4, mirroring order-management's reasoning exactly: (a) every write is already idempotent on `event_id` via `ProcessedEvents.MarkProcessed` before the projection apply (`handleFetchedMessage`'s own doc comment walks through why this two-transaction shape is safe under retry/redelivery), so correctness does not regress at 2 replicas; (b) it is a lightweight catalogue-growth projection workload with no throughput case yet that justifies wider fan-out. |
| `analytics-reports` (`cmd/pathmgmt-reports`) | Yes | 1 | 3 | 70% | Stateless read-only REST reader over its own read-only pgxpool (`analyticsstore.NewReadOnlyPool`) — no in-memory state, no Kafka consumption. Same treatment as `api`. |
| `frontend` (nginx-unprivileged serving the built SPA bundle) | Yes | 1 | 3 | 70% | Pure static-asset serving. No server-side session, no per-request state. The most trivially horizontally-scalable workload in this chart. |
| `mcp` (`cmd/mcp`) | **No — deliberately excluded, not just disabled** | — | — | — | The `github.com/modelcontextprotocol/go-sdk` `StreamableHTTPHandler` this adapter wraps (`internal/adapters/inbound/mcp/server.go`'s `Handler` function) keeps **per-process, in-memory session state** keyed by the MCP protocol's own `Mcp-Session-Id` header — a real multi-request session, not just a TCP/HTTP connection. `charts/process-path-management/templates/mcp-service.yaml` is a plain `ClusterIP` Service with no `sessionAffinity` configured, so under >1 replica a second request carrying the same `Mcp-Session-Id` (e.g. `tools/call` following an earlier `initialize`) could land on a different pod than the one that created the session, which has never heard of it. Fixing it for real needs either `sessionAffinity: ClientIP` (a partial mitigation only) or wiring the SDK's `StreamableHTTPOptions.EventStore` to a shared/external session store — a real code change, out of scope for this chart-and-pool-tuning PR. `mcp.replicaCount` stays a plain, manually-set value; no `autoscaling.mcp` block exists in `values.yaml` at all. Exact same reasoning and exclusion order-management's ADR 0026 documented for its own `mcp` Deployment. |

Every enabled block is namespaced under a single top-level `autoscaling:`
key in `values.yaml`
(`autoscaling.<api|projector|reports|frontend>.{enabled,minReplicas,
maxReplicas,targetCPUUtilizationPercentage}`), **every `enabled` value
defaults to `false`**. This PR makes per-workload HPA possible and
verified-correct; it deliberately does not turn any of it on.

**No replicas-vs-HPA fight.** Each Deployment template guards its
`spec.replicas` field with `{{- if not .Values.autoscaling.<x>.enabled
}}` — when a workload's HPA is enabled, its Deployment renders with NO
`replicas` field at all. Verified directly with `helm template`:

- Default values → 0 `HorizontalPodAutoscaler` resources render, every
  Deployment keeps its static `replicas:` field.
- All four `autoscaling.*.enabled=true` (plus `analytics.enabled=true`
  and `frontend.enabled=true`, required for the projector/reports/
  frontend HPAs to render at all) → exactly 4 `HorizontalPodAutoscaler`
  resources render (one per scalable workload, `mcp` has none by
  design), and none of those four Deployments has a `replicas:` field.
- Only `autoscaling.api.enabled=true` → exactly 1 HPA renders, only the
  `api` Deployment loses its `replicas:` field; the others keep theirs
  untouched. Mixed enablement is safe and independent per workload.

`helm lint` passes; `go vet`/`gofmt`/`make check`/`make arch-test` all
pass unchanged (no Go code path is affected by the chart changes).

### 2. pgxpool MaxConns

All three pools now set an explicit `pgxpool.Config.MaxConns` instead of
inheriting the CPU-derived library default, matching order-management's
numbers exactly (ADR 0026 / PR #110):

| Pool | Used by | `MaxConns` | Reasoning |
|---|---|---|---|
| OLTP (`postgres.NewPool`) | `cmd/pathmgmt` (`api`), `cmd/mcp` (`mcp`) | **10** | `api`'s HPA ceiling of 4 replicas × 10 = 40 connections against PgBouncer (this service's OLTP `DATABASE_URL` is already re-pointed at PgBouncer per warehouse-infra PR #43 — no chart change needed here), ~40% of the underlying instance's `max_connections=100` for this ONE of up to 10 fleet services' OLTP path alone if PgBouncer's own pooling were bypassed entirely — the same conservative budget order-management chose, deliberately matched rather than re-derived so the fleet's connection-budget accounting stays comparable service-to-service. |
| Analytics writer (`analyticsstore.NewPool`) | `cmd/pathmgmt-projector` | **5** | The projector's HPA ceiling is capped at 2 (see the table above) and does single-row upserts against the catalogue-growth projection; a small, flat pool is enough at either 1 or 2 replicas. Analytics DSNs stay direct against Postgres (not through PgBouncer, per PR #43's own split), so this ceiling is the real per-process cap against the shared instance. |
| Analytics reader (`analyticsstore.NewReadOnlyPool`) | `cmd/pathmgmt-reports` | **5** (`ReportsMaxConns`) | `reports` IS HPA-scalable (max 3); at that ceiling, 3 × 5 = 15 connections against the analytical database — comfortably inside the shared ceiling alongside the OLTP path's 40 (mediated by PgBouncer) and the projector's 5–10. |

> **Note (2026-10-04):** `cmd/mcp` briefly opened TWO OLTP pools (one per
> repo: `buildRepo` + `buildCPTScheduleRepo`), doubling each pod's real
> ceiling to 20 connections against this table's 10. Both repos now
> share ONE pool — `buildRepo` opens it, pings it, and hands it to
> `buildCPTScheduleRepo` — the same single-pool shape
> `cmd/pathmgmt`'s `buildPersistence` already used for the same two
> repos, so this table's per-pod arithmetic holds as written.
> Pinned by `TestBuildCPTScheduleRepo_SharesBuildRepoPool` and the
> existing `TestNewPool_AppliesMaxConns` (still asserts 10).

Worst case across every workload simultaneously at its proposed HPA
maximum, accounting for PgBouncer sitting in front of the OLTP path
(so the OLTP `MaxConns × replicas` figure is a PgBouncer-side client
count, not a 1:1 server-side connection count — PgBouncer's own
transaction-pooling multiplexing is what actually bounds real
server-side connections for that path): `projector` capped at 2 × 5 =
10, `reports` 3 × 5 = 15 direct-to-Postgres connections — **25** of the
shared instance's 100 connections for this service's *direct*
(non-PgBouncer) analytics paths at max scale, comfortably inside
budget on its own. The OLTP path's 40 PgBouncer-side client
connections do not map 1:1 onto server-side Postgres connections the
way order-management's pre-PgBouncer accounting had to assume — that
is precisely the problem PgBouncer (warehouse-infra PR #43) was
introduced to solve fleet-wide, and this PR relies on that solution
rather than re-solving it. `mcp` has no HPA (assume 2 manually-set
replicas × 10 = 20 PgBouncer-side clients, same caveat).

If/when the fleet turns on multiple workloads' HPA simultaneously and
the *direct* analytics budget gets tight in practice (the OLTP path is
PgBouncer's problem to absorb), the next lever is `max_connections`
itself — an unexamined Bitnami default — which is a `warehouse-infra`
Terraform change and a separate, cross-service decision, out of scope
here.

### 3. statement_timeout

All three pools set `statement_timeout` via `pgxpool.Config.
AfterConnect`, running `SET statement_timeout = '<value>'` on every new
physical connection as it's established (not per-query, so it survives
connection reuse across pooled acquisitions):

| Pool | `statement_timeout` | Reasoning |
|---|---|---|
| OLTP (`postgres.StatementTimeout`) | **5s** | Every OLTP query (`DefinePath`, `RevisePath`, `DeactivatePath`, `GetPath`, `ListPaths`, `DefineCPTSchedule`, `GetCPTSchedule`) is a single-aggregate read/write keyed by id, normally low-single-digit milliseconds. 5s is roughly 1000x that — generous headroom for real transient contention without ever being a normal-path concern, matching order-management's OLTP value exactly. |
| Analytics writer (`analyticsstore.StatementTimeout`) | **10s** | A Kafka consumer replaying a backlog after a redeploy issues upserts in a tight loop; a transient lock wait here doesn't need to be as tight as an interactive OLTP request, but must still be bounded — an unbounded query here could stall the entire analytics pipeline at 1 replica, or degrade throughput materially at 2. Matches order-management's analytics writer value. |
| Analytics reader (`analyticsstore.ReportsStatementTimeout`) | **15s** | The catalogue-growth report aggregates rows across a caller-chosen time range — wider than the OLTP side's always-single-aggregate-by-id shape — so it gets more headroom, but still a hard ceiling. Matches order-management's analytics reader value. |

Verified with a real Postgres via testcontainers
(`internal/adapters/outbound/postgres/pool_limits_integration_test.go`,
`-tags=integration`), not a mock and not just reading `pg_settings` —
the exact same two tests order-management's ADR 0026 introduced,
ported unchanged in shape:

- `TestNewPool_AppliesStatementTimeoutToNewConnections` — opens a pool
  against a real `postgres:16-alpine` container with a short test-only
  timeout (200ms, via the shared `NewPoolWithLimits` the production
  `NewPool` wraps), confirms `SHOW statement_timeout` reads back
  `200ms` on a freshly acquired connection, then runs `SELECT
  pg_sleep(2)` and asserts Postgres itself cancels it (SQLSTATE 57014)
  rather than letting it run the full 2s, and finally confirms the pool
  is still usable afterward.
- `TestNewPool_AppliesMaxConns` — acquires exactly `maxConns`
  connections from a pool configured with `MaxConns=2`, then asserts a
  further `Acquire` blocks until `context.DeadlineExceeded`, proving
  `MaxConns` is the pool's real, enforced ceiling rather than advisory.

Both tests pass locally against a real Postgres container (verified via
`go test -tags=integration ./internal/adapters/outbound/postgres/...
-run TestNewPool -race -count=1 -v`).

## Consequences

- HPA is now possible, correct, and independently verified per workload
  for four of this chart's five Deployments — but **off by default
  everywhere**. Merging this PR changes nothing about production
  replica counts; `MaxConns`/`statement_timeout` are the only behavior
  change that takes effect on deploy, and both are conservative
  relative to today's unbounded defaults.
- `mcp` remains explicitly un-autoscaled, with the exact reason
  (in-memory session state, no sticky routing) recorded here and in
  `values.yaml`'s comments, so a future contributor doesn't
  mechanically copy `api`'s HPA block onto it without re-solving the
  session-affinity problem first.
- This service's connection-budget accounting leans on PgBouncer
  (warehouse-infra PR #43) for the OLTP path in a way order-management's
  original ADR 0026 could not, since that PR landed after ADR 0026 was
  written — this ADR's "80/100 worst case" framing does not directly
  apply here; only the *direct* analytics paths (25/100 at max scale)
  are a real, unmediated draw on the shared instance. That is a
  narrower, better residual risk than order-management's own, but is
  still not a solved fleet-wide accounting — a proper fleet-wide sum
  across all 10 services' direct-Postgres analytics paths remains a
  follow-up, out of scope here.
- A read replica for the analytics/reports read path is explicitly OUT
  OF SCOPE for this PR — not evaluated, not designed, not decided.
- `max_connections=100` itself is an unexamined Bitnami chart default,
  not a value anyone has deliberately sized for this fleet's real
  demand. This ADR treats it as a hard external constraint to work
  within, not something in scope to change.

## References

- `order-management` ADR 0026 (PR #110) — the fleet's Phase 3
  scalability reference this ADR ports.
- `warehouse-infra` PR #43 — PgBouncer in front of the shared Postgres
  instance in transaction-pooling mode; re-points every service's OLTP
  `DATABASE_URL` at PgBouncer, analytics DSNs stay direct.
- ADR 0007 — analytical data product (the projector/reports split this
  ADR's pool tuning applies to).
- ADR 0012 — Kafka DLQ and graceful shutdown (the analytics consumer
  group's stability this ADR's projector HPA reasoning depends on).
