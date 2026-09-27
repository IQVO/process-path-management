---
id: 0012-kafka-dlq-and-graceful-shutdown
slug: /adr/0012-kafka-dlq-and-graceful-shutdown
title: "12. Analytics-consumer dead-letter queue and graceful shutdown hardening"
sidebar_label: "12. DLQ and graceful shutdown"
sidebar_position: 12
description: "ADR 0012 — Phase 2 resilience for process-path-management: a dead-letter topic for the analytics projector's Kafka consumer so one poison message cannot block the partition, and a readiness-flip-first graceful shutdown sequence for both the OLTP API and the analytics projector, mirroring order-management's ADR-0025 for these two pieces only (this service has no sync outbound HTTP calls to a sibling context, so no circuit breaker work applies here)."
---

# 12. Analytics-consumer dead-letter queue and graceful shutdown hardening

## Status

Accepted — implemented in the same change that introduced this record.
This is process-path-management's slice of Phase 2 (resilience) of the
fleet production-readiness plan, copying order-management's ADR-0025
(PR #107) for its DLQ and graceful-shutdown pieces only. ADR-0025's
circuit-breaker work does NOT apply here: this service has no sync
outbound HTTP calls to any sibling bounded context (verified — every
`internal/adapters/outbound/*` package here targets Postgres or Kafka,
never another service's REST API; this is one of the two repos in the
fleet with that stricter cross-context isolation rule), so there is
nothing to wrap in a breaker.

## Context

process-path-management is the SOURCE of the process-path published
language — it has no inbound Kafka consumer reacting to another
context's events (see this repo's own `AGENTS.md` and `cmd/pathmgmt`'s
package doc comment). Its ONLY inbound Kafka consumer
(`internal/adapters/inbound/kafka/analytics_consumer.go`, run by
`cmd/pathmgmt-projector`) reads this service's OWN analytics topic
(`warehouse.process-path-management.analytics`, ADR 0007), replaying
`ProcessPathCreated`/`ProcessPathUpdated`/`ProcessPathDeactivated` into
the catalogue-growth read model.

Before this change, that consumer had no dead-letter handling: a message
whose `ProcessedEvents.MarkProcessed` call or projection-apply call
always failed (a permanently broken analytics database connection, a
malformed message from a future producer bug) would be logged and
SKIPPED — `Run`'s pre-existing `HandleMessage` error path only logged at
ERROR level and moved on, with `ReadMessage`/auto-commit already having
advanced past the message. That is actually a silent-drop failure mode,
not a retry-forever one — worse than order-management's pre-existing
gap, since a redelivery was already impossible (the offset had already
auto-committed by the time the error was logged).

Both composition roots' (`cmd/pathmgmt`, `cmd/pathmgmt-projector`)
graceful shutdown had the same gaps order-management's ADR-0025
identified: no readiness-flip step distinct from the liveness probe, and
(for the projector specifically) no bounded wait for the Kafka
consumer's in-flight message to actually finish before the process
exited — `cancelConsumer()` was called and the function returned
immediately after `srv.Shutdown`, racing the consumer goroutine rather
than waiting for it.

## Decision

### 1. Dead-letter queue for the analytics consumer, adapted for its two-separate-transaction shape

Unlike order-management's `RepromiseConsumer` (whose dedupe check and
mutation are ONE atomic use case wrapped in a single `UnitOfWork`
transaction, so the WHOLE handler can be retried safely), this
consumer's idempotency gate (`ProcessedEvents.MarkProcessed`, its own
Postgres transaction against `analytics_consumed_events`) and its
projection apply (`PostgresProjection`, a SEPARATE transaction against
the rollup tables) are two independent stores. Naively retrying the
whole of the pre-existing `HandleMessage` would be unsafe: a first
attempt that marks an event processed and then fails to apply would
make a second attempt's `MarkProcessed` report `isNew=false` and
silently skip the apply forever, masking a real failure as a permanent
success.

`handleFetchedMessage` (new, used by `Run`; the pre-existing
`HandleMessage` is kept, unchanged, as a single-shot no-DLQ path the
existing unit tests still exercise directly) fixes this by retrying each
step SEPARATELY, `cenkalti/backoff/v4`, jittered, up to
`maxAnalyticsHandlerAttempts` (3) attempts each:

1. Decode. A malformed message is logged and committed (nothing a retry
   fixes).
2. Filter by event type. A non-projecting event is committed as a no-op
   (unchanged from `HandleMessage`).
3. `markProcessedWithRetry`: retries `MarkProcessed` itself. Safe to
   retry because `MarkProcessed`'s own contract is idempotent
   (`INSERT ... ON CONFLICT DO NOTHING`, reporting `isNew=false` on a
   repeat) and no apply has happened yet at this point.
4. `applyProjectionWithRetry`: retries ONLY the projection apply call.
   Safe to retry because each attempt is one atomic Postgres transaction
   (`PostgresProjection`'s own `inTx` wrapping) — a failed attempt rolls
   back cleanly, so a retry can never double-count the same event.
5. After either retry loop exhausts its budget, `deadLetterAndCommit`
   publishes the raw, unmodified message plus error context
   (`x-dlq-source-topic`/`x-dlq-error`/`x-dlq-failed-at` headers,
   mirroring `RepromiseConsumer`'s header shape exactly) to
   `<source-topic>.dlq` via a `*kafkago.Writer` the consumer now owns
   (`AnalyticsConsumer.dlqWriter`, closed alongside the reader in
   `Close`), and the offset is committed anyway: one poison message must
   never permanently block every other event behind it on the same
   partition.

`NewAnalyticsConsumer` derives the DLQ topic as
`<its own source topic>+".dlq"` (never a fixed constant), exactly
mirroring `RepromiseConsumer.NewRepromiseConsumerForTopic`'s convention
— an isolated integration-test topic gets its own isolated DLQ topic for
free.

`Run` was changed from `ReadMessage` (auto-commit) to
`FetchMessage`+explicit `CommitMessages` — required for the DLQ path to
control exactly when an offset advances (previously impossible: with
auto-commit, the offset had already advanced by the time a handling
error was even logged, which is why the pre-existing gap was a silent
drop rather than a safe-redelivery retry loop).

Proven end to end with a real testcontainers Kafka
(`analytics_consumer_dlq_integration_test.go`,
`TestAnalyticsConsumer_PoisonMessage_GoesToDeadLetterTopicWithoutBlockingPartition`):
a message whose `MarkProcessed` call is made to always fail (via a
`ProcessedEvents` wrapper that unconditionally errors for one specific
`event_id`) lands on the `.dlq` topic after exactly 3 attempts, with the
raw original JSON payload and the error-context headers intact, and — a
well-formed message published right after the poison message on the
SAME topic — is projected without delay, proving the partition was
never blocked.

### 2. No inbound consumer exists on `cmd/pathmgmt` (the OLTP API) — no DLQ work there

`cmd/pathmgmt` (the REST API + outbox relay) is confirmed to have no
inbound Kafka consumer of any kind — it only PUBLISHES onto
`warehouse.process-path-management.events` and
`warehouse.process-path-management.analytics` via the transactional
outbox (ADR 0003, ADR 0007). There is nothing to add a DLQ to here; this
is a documented finding, not a gap.

### 3. Graceful shutdown hardening — both composition roots

Mirroring order-management's ADR-0025 §graceful shutdown sequence
exactly:

**`cmd/pathmgmt`** (the OLTP API): a new `inboundhttp.Readiness` gate
backs `GET /readyz` (distinct from the pre-existing `GET /healthz`
liveness probe, which stays a pure "is the process alive" signal never
flipped by shutdown). The shutdown sequence is now:

1. `readiness.SetNotReady()` — FIRST, before anything else stops.
2. `httpServer.Shutdown(shutdownCtx)` — stop accepting new connections,
   drain in-flight requests.
3. Stop the outbox relay cleanly: cancel its context, wait (bounded by
   the SAME `shutdownCtx`) for its goroutine to finish its in-flight
   pass — unchanged from before this ADR, already correct.
4. The deferred `persistence.close()`/`closePublisher()` calls (LIFO
   order) close the pgx pool LAST, after the relay has already stopped
   touching it.

`Readiness`'s zero value (and a `nil *Readiness`) is always ready —
every existing test and any caller that predates this type behaves
exactly as before.

**`cmd/pathmgmt-projector`** (the analytics writer): a new
`readinessGate` (a small, package-private type mirroring
`inboundhttp.Readiness`'s shape — this binary serves a bare `net/http`
mux, not the chi API router, so it does not import the HTTP inbound
adapter package for this) backs a new `GET /readyz` on the admin port,
alongside the pre-existing `GET /healthz`. The shutdown sequence changed
from a fire-and-forget `cancelConsumer()` immediately followed by
`return srv.Shutdown(ctx)` to:

1. `readiness.setNotReady()` — FIRST.
2. `srv.Shutdown(shutdownCtx)` — stop the admin server.
3. `cancelConsumer()`, then WAIT (bounded by the SAME `shutdownCtx`) on
   a new `consumerDone` channel for the analytics consumer's `Run`
   goroutine to actually return — including having committed (or
   dead-lettered and committed) whatever message it was mid-handling —
   rather than racing it. This is the same "final offset commit"
   guarantee order-management's ADR-0025 established for
   `RepromiseConsumer`.
4. `consumer.Close()` (closes the Kafka reader and the new DLQ writer),
   THEN `pool.Close()` — the pgx pool is now closed explicitly at the
   END of the function (no longer `defer`red immediately after
   `analyticsstore.NewPool`), guaranteeing it closes LAST, after both the
   admin server and the consumer have stopped touching it.

### 4. Helm chart: `readinessProbe` moved to `/readyz`, `terminationGracePeriodSeconds` added

`charts/process-path-management/values.yaml`'s top-level
`readinessProbe` (the OLTP API deployment) and
`analytics.projector`-scoped readiness probe (the projector deployment)
now point at `/readyz` (was `/healthz` for both).
`startupProbe`/`livenessProbe` are UNCHANGED for both — still
`/healthz` — because liveness must never be flipped by a graceful drain,
or Kubernetes would SIGKILL the pod mid-drain instead of letting it
finish. `terminationGracePeriodSeconds: 30` was added at the top level
(consumed by `deployment.yaml`, the OLTP API) and under
`analytics.projector` (consumed by `projector-deployment.yaml`) —
previously unset for both, a confirmed gap: the 10s HTTP/consumer-stop
budget plus margin for the `readinessProbe`'s `periodSeconds: 5` to have
observed the not-ready flip before traffic fully stops arriving.
`cmd/mcp` and `cmd/pathmgmt-reports` are read-only, stateless
deployables with no inbound Kafka consumer, no outbox relay and no
mutating work to drain — their existing `/healthz`-only probes and
default termination grace period are left unchanged; scoping this
hardening to them was not part of the plan.

### 5. `cenkalti/backoff/v4` promoted from indirect to direct

Already present transitively (`v4.3.0`, matching the version
order-management's ADR-0025 uses); `go mod tidy` after adding the import
promoted it to a direct dependency. `v5` remains present transitively
but is NOT adopted, matching order-management's decision.

## Consequences

- The analytics projector can no longer silently drop a message it
  fails to process — every failure either heals within 3 retries or
  lands on `<topic>.dlq` with full replay context, and every OTHER
  message on the partition keeps flowing. The `.dlq` topic is a new
  operational surface needing monitoring/alerting (out of scope here —
  the ERROR-level log line is the interim signal, same as
  order-management) and a manual replay tool (also out of scope).
- `GET /readyz` is a new, distinct endpoint on both the OLTP API and the
  analytics projector; fleet operators/SRE tooling should point
  `readinessProbe`s at it going forward for any process-path-management
  workload that participates in a graceful drain.
- The projector's shutdown now genuinely waits for its consumer to
  finish in-flight work (bounded, 10s) instead of racing it — a SIGTERM
  during message handling no longer risks the process exiting between a
  successful `MarkProcessed`/apply and its own return, which previously
  had no guard at all.
- No circuit breaker, no outbound retry, and no bulkhead work was done
  here — there is no sync outbound HTTP call in this service to protect.
  A future context that DOES gain one (there is none today) would need
  its own ADR for that piece, following order-management's ADR-0025
  circuit-breaker design at that time.

## Alternatives considered

- **Retrying the whole pre-existing `HandleMessage` as one unit,
  mirroring `RepromiseConsumer`'s `handleWithRetry` literally:**
  rejected — `RepromiseConsumer`'s retry-the-whole-handler safety
  depends entirely on `RepromiseOrder.Execute`'s dedupe-and-mutate being
  ONE atomic transaction. This consumer's dedupe and apply are two
  separate transactions; retrying the whole handler would let a
  redelivered "already marked, apply failed" message wrongly report
  `isNew=false` and skip the apply forever. Retrying the two steps
  separately (§1 above) is the design this service's actual transaction
  boundaries require.
- **Adding a circuit breaker somewhere in this service "for
  completeness" with the rest of the fleet's Phase 2 rollout:**
  rejected — there is no sync outbound HTTP dependency in this service
  to wrap. Inventing one would contradict this service's own
  architecture (Open Host Service / Published Language source, zero
  inbound dependency, Kafka-only cross-context propagation).
- **Switching the projector's readiness type to reuse
  `internal/adapters/inbound/http.Readiness` directly:** rejected — that
  package is the OLTP API's chi-router adapter; the projector serves a
  bare `net/http.ServeMux` on an admin port and has no reason to import
  the whole HTTP inbound adapter package for one atomic gate. A small,
  duplicated, package-private `readinessGate` (identical semantics, ~10
  lines) keeps the two composition roots' dependencies as narrow as they
  already were.
- **Leaving the projector's `pool.Close()` as an immediate `defer`
  (its pre-existing shape):** rejected once the consumer-wait step was
  added — a deferred close runs at function-return time by Go's normal
  defer semantics, which combined with the ALREADY-EXISTING unbounded
  race between `cancelConsumer()` and the consumer's actual goroutine
  exit, meant the pool could close while the consumer was still
  mid-transaction. Moving the close to an explicit call after the new
  bounded wait removes that race by construction.

## References

- order-management PR #107 / ADR-0025 — the reference this record copies
  for the DLQ and graceful-shutdown pieces only (its circuit-breaker
  sections do not apply here).
- ADR 0007 — the analytics data product (topic, projector, reports) this
  record's DLQ work extends, not re-derives.
- ADR 0003 — the transactional outbox `cmd/pathmgmt` already runs
  alongside its graceful shutdown sequence (unchanged by this record).
- This repo's `AGENTS.md` — "This service never subscribes to another
  context's topic... zero inbound dependency" — the fact this record's
  §2 finding (no DLQ work needed on `cmd/pathmgmt`) and its "no circuit
  breaker applies here" framing both rest on.
