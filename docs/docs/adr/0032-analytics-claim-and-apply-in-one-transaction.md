---
id: 0032-analytics-claim-and-apply-in-one-transaction
slug: /adr/0032-analytics-claim-and-apply-in-one-transaction
title: "0032. The analytics consumer's dedupe claim and projection write commit in one transaction"
sidebar_label: "0032. Analytics claim + apply atomic"
description: "ADR 0032 — AnalyticsConsumer runs ProcessedEvents.MarkProcessed and the projection apply inside one analytics-database transaction (analyticsstore.UnitOfWork), so a process stop between them can no longer lose a projection update; the claim+apply step is retried as one unit. Amends ADR 0012."
---

# 0032. The analytics consumer's dedupe claim and projection write commit in one transaction

## Status

**Accepted** (2026-10-13). Amends [ADR 0012](./0012-kafka-dlq-and-graceful-shutdown.md)
(its split of "mark once, then retry only the apply"). No migration, no
contract change.

## Context

`kafka.AnalyticsConsumer` (`cmd/pathmgmt-projector`, ADR 0007) dedupes
every CloudEvent on its `id` in `analytics_consumed_events`
(`ConsumedEventsRepo.MarkProcessed`). It then applies the projection
(`PostgresProjection.Apply*`), which claims the id again in
`analytics_processed_events` and upserts `catalogue_growth_rollup` in its own
transaction. `handleFetchedMessage` ran these as two separate transactions:
`markProcessedWithRetry`, then `applyProjectionWithRetry`.

ADR 0012 saw that retrying the whole handler after a successful mark and a
failed apply would see `isNew=false` and skip the apply. Its fix was to mark
once and retry only the apply. That fix did not cover the process stopping
(SIGTERM, eviction, OOM, crash) after the mark committed and before the
apply committed. The offset is not committed in that case, so Kafka
redelivers the message. Since the clean-shutdown fix (PR #153), `Run` also
treats the cancellation as a clean stop. But the redelivery's `MarkProcessed`
returns `isNew=false` and the apply is skipped. The projection update is
lost, with no error and no dead letter.

The fleet already avoids this. labor-performance's `RecordTaskPerformance`
runs `MarkProcessed` and its writes in one `UnitOfWork`, and
order-management's `RepromiseConsumer`, which ADR 0012 mirrors, dedupes
inside its use case's single `UnitOfWork`. This repository's OLTP side has
the same tool: `postgres.UnitOfWork` carries a `pgx.Tx` in the context.

## Decision

1. **One analytics transaction.** A new `analyticsstore.UnitOfWork`
   (`internal/adapters/outbound/analyticsstore/unit_of_work.go`) does on the
   analytical pool what `postgres.UnitOfWork` does on the OLTP pool.
   `Execute(ctx, fn)` begins a transaction and carries it in the context
   under a key private to `analyticsstore`, so an OLTP transaction can never
   be joined by mistake. It deliberately does not use `internal/pgtx`: that
   key binds the OLTP database's transaction, and the analytical database is
   a separate database with its own pool. It commits only when `fn` returns nil, and a nested
   `Execute` joins the outer scope. Rollback runs on a context without
   cancellation, so a shutdown that interrupted `fn` still releases the
   transaction.
2. **The stores join it.** `ConsumedEventsRepo.MarkProcessed` issues its
   insert through the context's transaction when one is present.
   `PostgresProjection.inTx` uses the context's transaction instead of
   beginning its own, and the UnitOfWork owns commit and rollback. Outside a
   UnitOfWork both behave as before.
3. **The consumer claims and applies as one unit.**
   `AnalyticsConsumer.claimAndApply` runs `MarkProcessed` and, for a new id,
   the projection apply inside `Atomic.Execute`. `handleFetchedMessage`
   retries that whole step (jittered backoff 100 ms to 2 s,
   `maxAnalyticsHandlerAttempts` = 3). Retrying is now safe because every
   failed attempt rolled its claim back. This replaces ADR 0012's "mark
   once, retry only the apply" split, and the dead-letter cause is now
   prefixed `claim and apply:`. `NewAnalyticsConsumer` and
   `NewAnalyticsConsumerInGroup` take the UnitOfWork, and
   `cmd/pathmgmt-projector` passes `analyticsstore.NewUnitOfWork(pool)`. A
   consumer built without one runs the step directly. That case covers the
   in-memory store and the test fakes, which have no transaction to share.
4. **DLQ and shutdown rules are unchanged.** A message that is not a valid
   CloudEvent is dead-lettered at once. An exhausted retry budget is
   dead-lettered and committed. Cancellation is never dead-lettered: the
   offset stays uncommitted, `Run` returns nil, and the redelivery now
   applies the event because the interrupted transaction left no claim.

## Consequences

- A stop at any point between the claim and the projection write leaves
  either both rows or neither. The redelivery applies the event exactly
  once.
- The two idempotency layers (`analytics_consumed_events`,
  `analytics_processed_events`) now commit together. Both are kept: the
  projection's own claim still protects a direct `Apply*` caller.
- One transaction per message instead of two. No new table, env var, topic
  or metric.

## Verification

- `internal/adapters/inbound/kafka/analytics_atomic_claim_integration_test.go`,
  `TestAnalyticsConsumer_CrashBetweenClaimAndApply_RedeliveryAppliesExactlyOnce`
  (testcontainers Kafka and Postgres). The first consumer's projection
  cancels the run context on its first apply, which simulates the process
  stopping after the claim. The test asserts that no
  `analytics_consumed_events` row is left. A second consumer in the same
  group then gets the redelivered message, and the test asserts
  `paths_defined = 1` plus exactly one row in each claim table. Before this
  change it failed on both assertions: the claim row was left behind and the
  rollup stayed at 0.
- `internal/adapters/outbound/analyticsstore/unit_of_work_integration_test.go`
  checks three cases. Rollback removes the claim, the projection claim and
  the rollup row together. Commit keeps all three. A nested `Execute` joins
  the outer transaction.
- `internal/adapters/inbound/kafka/analytics_consumer_atomic_test.go` checks
  that the claim and the apply run in the same UnitOfWork scope and that an
  apply or claim error fails the UnitOfWork.
