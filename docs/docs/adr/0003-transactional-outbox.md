---
id: 0003-transactional-outbox
slug: /adr/0003-transactional-outbox
title: 0003. Transactional outbox for the process-path Published Language
sidebar_label: 0003. Transactional outbox
description: ADR 0003 — why this service stopped publishing to Kafka from inside its use cases and now commits every event to an outbox table in the same transaction as the aggregate, with an in-process relay draining it to the topic.
---

# 0003. Transactional outbox for the process-path Published Language

## Status

Accepted — implemented in the same change that introduced this record.
The outbox mechanics stand; the encoded message format it stores (the
flat `event_id`/`event_type`/`occurred_at` envelope) is **Superseded by
ADR-0016** — rows now hold the structured-mode CloudEvents 1.0 bytes, with
`event_id` = the CloudEvents `id` and `event_type` = the full CloudEvents
`type`.

## Context

ADR 0002 records that this service's Postgres store and its Kafka topic
were once found **diverged in both directions at once**: paths that
existed in the store but had never reached the topic, and events on the
topic for paths the store no longer agreed with. The cutover succeeded
anyway because the seed script was idempotent and re-runnable, but the
underlying cause was left in place:

```go
if err := uc.Repo.Save(ctx, p); err != nil { return err }
if err := uc.Publisher.Publish(ctx, evt); err != nil { return err }
```

Two independent writes to two independent systems, no compensation. A
crash, a broker timeout, or a pod eviction between them leaves a path in
the store that fulfillment-execution, wes-work-planning and
workforce-management will **never learn about**, since all three build
their catalogue exclusively from the topic (ADR 0001). The reverse
failure — Publish succeeds, then the HTTP response is lost and the
operator retries — is already covered by the use cases' idempotency, but
the first one had no answer at all.

This is not a theoretical concern for this particular service. It is the
**source of truth** for the fleet's Published Language; every other
context in the WES tier is a Conformist to what this topic says. If any
service in the fleet must guarantee that its store and its topic agree,
it is this one. The same Save-then-Publish shape exists in four sibling
services (wes-work-planning, fulfillment-execution, workforce-management,
labor-performance); this ADR is the template they will follow, which is
why it is written at this level of detail.

## Decision

Adopt the **transactional outbox** pattern:

1. **`outbox_events` table** (migration `0002_outbox`). Each row is one
   already-encoded Kafka message: `event_id` (UUID, the envelope's id),
   `event_type`, `aggregate_id` (the PathId — the partition key),
   `payload` (the JSON envelope, byte-for-byte what the topic will
   carry), `occurred_at`, plus `published_at`, `attempts`, `last_error`
   for the relay. A partial index over `published_at IS NULL` keeps the
   relay's scan tiny.

2. **`ports.UnitOfWork`** — a new driven port:
   `Execute(ctx, fn func(ctx) error) error`. The application layer
   wraps `Repo.Save` + `Publisher.Publish` in one call; adapters that
   receive the inner `ctx` join the same scope. The port is optional
   (`nil` = "run them back to back"), which is exactly the in-memory /
   log-publisher dev configuration. The domain and application layers
   gain no knowledge of transactions, Postgres, or Kafka — the arch-go
   fitness tests are unchanged and still pass.

3. **`postgres.UnitOfWork`** opens a `pgx.Tx`, binds it to the context,
   and commits or rolls back around `fn`. `ProcessPathRepo` and the new
   `OutboxPublisher` resolve their querier from the context: the pool
   when standalone, the bound transaction when inside a unit of work.
   Nested `Execute` calls join the outer transaction rather than opening
   a second one.

4. **`postgres.OutboxPublisher`** implements `ports.EventPublisher` by
   `INSERT`ing into `outbox_events`. It never touches the broker. The
   envelope is produced by the same `kafka.Encode` the direct publisher
   uses, so the outbox and the direct path can never disagree on wire
   format.

5. **`postgres.OutboxRelay`** runs as a goroutine inside the `pathmgmt`
   process, next to the HTTP server. Each pass claims up to 100 pending
   rows with `SELECT … FOR UPDATE SKIP LOCKED ORDER BY id`, sends them
   to the Kafka `Publisher` in order, and marks each `published_at`.
   On a send failure it stops the pass at that row (so a later event for
   the same path can never overtake a failed earlier one), records the
   error on the row, commits what was already sent, and retries on the
   next tick. Sleep between empty passes is `OUTBOX_RELAY_INTERVAL`
   (default `1s`); a full batch is followed immediately by another pass.

6. **Composition root** (`cmd/pathmgmt/main.go`) picks the mode:

   | `DATABASE_URL` | `EVENT_PUBLISHER` | Publisher wired          | Relay |
   |----------------|-------------------|--------------------------|-------|
   | unset          | `log` (default)   | log                      | none  |
   | unset          | `kafka`           | direct Kafka (no outbox) | none  |
   | set            | `log`             | log                      | none  |
   | set            | `kafka`           | **outbox**               | **yes** |

   The cluster runs the last row. Graceful shutdown stops the HTTP server
   first, then cancels the relay and waits for its in-flight pass, so an
   event committed by a request that completed a moment before SIGTERM is
   not stranded until the next pod boots.

### Delivery semantics (what consumers may now rely on)

- **Atomicity**: an aggregate change and its event are committed
  together or not at all. Verified by an integration test that forces the
  outbox insert to fail and asserts the `process_paths` row is absent.
- **At-least-once**: a crash between a successful `Send` and the row's
  `UPDATE` republishes that row on the next pass. Consumers already
  tolerate this — they replay the whole topic from offset zero on every
  boot and key their cache by PathId, so a duplicate is an idempotent
  overwrite. The republished message carries the **same `event_id`**,
  so a consumer that ever wants exact-once can dedupe on it.
- **Per-path ordering**: preserved. Rows are drained in insertion order
  within one relay, keyed by PathId onto one partition, and a failed row
  blocks everything behind it rather than being skipped.
- **Latency**: events reach the topic within one relay interval (≤1s in
  the cluster) of the HTTP response, versus "before the response" under
  the old direct publish. For a catalogue of process paths edited by
  operators this is invisible; the live-propagation probe still observed
  a new path in wes-work-planning within 2s of `POST /process-paths`.

## Consequences

**Positive**
- The divergence documented in ADR 0002 cannot recur: there is no code
  path that persists a path without also persisting its event.
- No broker dependency on the request path. `POST /process-paths` now
  succeeds when Kafka is down; the event is published once it returns.
  Previously the request failed with a 500 after the row was already
  committed — the worst of both worlds.
- Use cases are simpler to reason about: one atomic scope, one error.
- The pattern is reusable verbatim by the four sibling services that
  still Save-then-Publish; only the `Encode` function is service-specific.

**Negative / accepted**
- One more table, one more goroutine, one more failure mode (the relay)
  to observe. The relay logs every failed pass at ERROR with the row id
  and the broker error; `outbox_events.attempts`/`last_error` are
  queryable for operators. Metrics for outbox lag are a follow-up.
- Events are no longer synchronous with the HTTP response. Documented
  above; acceptable for this domain.
- With two pods overlapping during a rolling deploy, `SKIP LOCKED`
  guarantees no double-claim within one pass, but does **not** guarantee
  global order across the two relays for different paths. Per-path order
  is what consumers depend on, and that is preserved because all events
  for one path are inserted from one request and drained by whichever
  relay claims that contiguous range first.
- `EVENT_PUBLISHER=kafka` without `DATABASE_URL` still publishes
  directly. That mode exists only for in-memory local runs and is logged
  as such at startup (`direct, no outbox`).

## Alternatives considered

- **Keep Save-then-Publish and add retry around Publish.** Does not fix
  a crash between the two writes, and retries on the request path make
  the broker's latency the operator's latency. Rejected.
- **Publish-then-Save.** Inverts the failure: an event on the topic for a
  path that never persisted, which is strictly worse for Conformist
  consumers. Rejected.
- **Change-data-capture (Debezium) on `process_paths`.** Correct, but
  adds a Kafka Connect deployment to a kind cluster that already runs
  Istio, Kong, Kafka, Postgres and the observability stack, and moves
  envelope encoding out of the service's own code (where ADR 0001 wants
  the Published Language to live). Deferred until more than one service
  needs it.
- **A separate relay binary** (like the analytics projectors in sibling
  services). Cleaner isolation, but this service has no other reason to
  run a second pod and the relay's work is a single `SELECT`/`UPDATE`
  loop. In-process, with graceful-shutdown handling, is proportionate.
  If the relay ever needs independent scaling it can be lifted into
  `cmd/pathmgmt-relay` without touching the adapters.

## Verification

- Unit: `internal/application/usecases/unit_of_work_test.go` — every use
  case runs Save and Publish inside exactly one scope, a publish failure
  rolls the scope back, no-op revise/idempotent deactivate open no scope,
  nil UnitOfWork still works.
- Integration (`-tags=integration`, testcontainers Postgres — the test
  owns its own database, never an external `DATABASE_URL`):
  `outbox_integration_test.go` — commit-together, rollback-on-publish-
  failure, relay ordering + marking, relay stops at a failed row and
  recovers.
- Cluster: after deploy, `POST /process-paths` then
  `SELECT event_type, published_at FROM outbox_events ORDER BY id DESC
  LIMIT 1` shows the row published within one interval, and
  wes-work-planning accepts a `POST /paths/<prefix>-zone-1/charge` for
  the new prefix without restart.
