---
id: 0013-kafka-writer-hash-balancer
slug: /adr/0013-kafka-writer-hash-balancer
title: "13. Hash balancer for the outbound Kafka writers, closing a latent partition-affinity gap"
sidebar_label: "13. Kafka writer Hash balancer"
sidebar_position: 13
description: "ADR 0013 — this service's outbound Kafka writers (Publisher, AnalyticsPublisher) already keyed every message by PathId/SiteId, but kafka-go's LeastBytes balancer silently ignores Message.Key when routing to a partition, so the per-aggregate ordering guarantee was never actually enforced once warehouse-infra PR #42 scaled every business topic from 1 to 8 partitions. Fix: switch both writers' Balancer to kafkago.Hash (FNV-1a over Key), verified with a real-broker Testcontainers test against an 8-partition topic. Mirrors order-management's companion ADR 0027 (PR #111)."
---

# 13. Hash balancer for the outbound Kafka writers, closing a latent partition-affinity gap

## Status

Accepted — implemented in the same change that introduces this record.
Companion to order-management's ADR-0027 (PR #111), which found and fixed
the identical bug in that service's own outbound Kafka writers; this ADR
is process-path-management's slice of the same fleet-wide audit.

## Context

`warehouse-infra` PR #42 (already merged into `develop`) took every
business topic — including both of this service's own topics,
`warehouse.process-path-management.events` (the integration topic) and
`warehouse.process-path-management.analytics` — from 1 partition to 8, to
raise consumer-side throughput headroom fleet-wide.

Unlike order-management's version of this bug, this service's message
`Key` was never missing: `internal/adapters/outbound/kafka/publisher.go`'s
`Encode` function has always set `kafkago.Message.Key` to the event's
`PathId` (or `SiteId` for `CPTScheduleChanged`), and
`analytics_publisher.go`'s `AnalyticsEncoder.Encode` has always reused
that same key for the analytics topic. By inspection, both publishers
looked correct: every message written carries a non-nil, aggregate-scoped
key, which is exactly the standard idiom for "route by key" in Kafka.

What was wrong is a level below the `Key` field: `kafka-go`'s
`Writer.Balancer` — not the mere presence of a `Message.Key` — decides
partition placement. Both `NewPublisher` and
`NewAnalyticsDirectPublisher` constructed their `*kafkago.Writer` with
`Balancer: &kafkago.LeastBytes{}`. `LeastBytes.Balance` picks whichever
partition has received the fewest cumulative bytes so far and reads
`Message.Key`/`Value` only to add their lengths to that running total —
it never hashes or otherwise routes on the key's *content*. At 1
partition this was invisible (there was only one partition to route to,
so every message landed on it regardless of balancer). At 8 partitions,
`LeastBytes` freely spreads same-key messages across whichever partition
currently has the least cumulative bytes, so `ProcessPathCreated`,
`ProcessPathUpdated`, and `ProcessPathDeactivated` for the SAME `PathId`
can land on different partitions, consumed by different consumer
instances with no ordering relationship between them — the exact
per-aggregate ordering guarantee this service's own `Encode` doc comment
promises ("every event for the same path lands on the same partition and
a replaying consumer sees a given path's Created/Updated/Deactivated in
publish order") was not actually being kept against a real broker.

### Verifying the fix required a real broker, not a fake Writer

This service's existing unit tests (`publisher_test.go`,
`analytics_publisher_test.go`) assert against a `fakeWriter` that simply
appends every `kafkago.Message` it receives to a slice — they prove `Key`
is populated and correct, but a fake writer has no concept of
partitions or a `Balancer` at all, so it could not have caught this
class of bug and still cannot regress-test the fix. Following
order-management PR #111's approach, the fix here is proven instead by a
new Testcontainers-backed integration test
(`publisher_balancer_integration_test.go`) that starts a real Kafka
broker, creates an 8-partition topic, publishes 3 events for one
`PathId` and 1 for another via the real `Encode`/`Send` path, and reads
every partition back to assert all 3 same-`PathId` messages land on one
partition while the other `PathId`'s message is free to land elsewhere.
Run against the pre-fix `&kafkago.LeastBytes{}` balancer, this test
fails (only 1 of the 3 same-key messages landed on the majority
partition in the run that motivated this change); it passes only after
switching to `&kafkago.Hash{}`.

## Decision

1. **`Publisher`'s writer (`NewPublisher`,
   `internal/adapters/outbound/kafka/publisher.go`) switches its
   `Balancer` from `&kafkago.LeastBytes{}` to `&kafkago.Hash{}`.** This
   writer is shared by the direct-publish path (`EVENT_PUBLISHER=kafka`,
   no Postgres) and the transactional outbox relay's `Sink`
   (`postgres.OutboxRelay`, ADR 0003) — both call `Publisher.Send`, so
   both paths get the same partition-affinity guarantee from one change.
2. **`AnalyticsPublisher`'s writer (`NewAnalyticsDirectPublisher`,
   `internal/adapters/outbound/kafka/analytics_publisher.go`) switches
   its `Balancer` the same way.** This is the direct-publish path for
   the analytics topic when running with Kafka but no Postgres; the
   outbox path reuses `Publisher`'s already-fixed writer via
   `AnalyticsEncoder.Encode` + `Publisher.Send`.
3. **No `Key`-setting logic changes anywhere.** `Encode` and
   `AnalyticsEncoder.Encode` already derive the correct partition key
   (`PathId`, or `SiteId` for `CPTScheduleChanged`) for every event type
   this service publishes — the audit that produced this ADR confirmed
   that both publish call sites (`Publisher.Send`,
   `AnalyticsPublisher.Publish`) already use `enc.Key`, never a
   freshly-derived or fixed key. This ADR only changes which `Balancer`
   the writer uses to route on that key.
4. **New test:
   `internal/adapters/outbound/kafka/publisher_balancer_integration_test.go`**
   (`//go:build integration`) starts a real Kafka broker via
   Testcontainers (satisfying this repo's own
   `TestKafkaIntegrationTestsUseTestcontainers` fitness test), creates an
   8-partition topic — mirroring the exact partition count
   warehouse-infra PR #42 set in the cluster — and proves same-`PathId`
   events land on the same partition while a different `PathId`'s event
   does not have to. This is a regression test for the balancer choice
   specifically: a fake-writer unit test cannot exercise
   `kafka-go`'s `Balancer` at all.

No custom `Balancer` implementation, no producer-side partition pinning,
and no consumer-side reordering buffer were introduced — `kafka-go`'s
stock `Hash` balancer (FNV-1a over `Message.Key`) plus the `Key` this
service already sets is Kafka's standard idiom for "route by key," and it
is sufficient here: this service does not need a specific partition
number, only that the SAME path's (or site's) events always land on the
SAME partition as each other, regardless of partition count.

## Consequences

- Every event this service publishes for the same `PathId` — across
  `ProcessPathCreated`, `ProcessPathUpdated`, and `ProcessPathDeactivated`
  — now deterministically lands on the same partition of
  `warehouse.process-path-management.events`, and likewise for
  `CPTScheduleChanged` events sharing a `SiteId`. A consumer reading
  either topic with multiple consumer-group instances observes a
  correct relative order for any single path's (or site's) own event
  history. Ordering ACROSS different paths/sites is still not
  guaranteed and was never a requirement.
- The same guarantee now genuinely holds for the analytics topic
  (`warehouse.process-path-management.analytics`), which looked correct
  by inspection (it already set `Key`) but was, like the integration
  topic, silently defeated by `LeastBytes`.
- No wire-format change: `Message.Key` is Kafka metadata, not part of the
  JSON envelope/payload a consumer decodes. Neither
  `fulfillment-execution`, `wes-work-planning`, nor
  `workforce-management` (this service's downstream integration-topic
  consumers) need any change.
- A downstream consumer that happened to rely on either topic's total
  publish-time order across *different* paths/sites (nothing in this
  fleet's documented consumer contracts does) would need to re-evaluate
  that assumption — but that guarantee was already gone the moment
  warehouse-infra's PR #42 changed the partition count; this ADR does
  not introduce it, it only stops silently pretending per-aggregate
  ordering still held.
- This ADR's finding — that setting `Message.Key` alone is not
  sufficient with `kafka-go`, and that `Writer.Balancer` must also be a
  key-aware balancer (`Hash`, not `LeastBytes`) — mirrors
  order-management's ADR-0027 exactly and was found via the same
  fleet-wide audit that produced fixes in order-management (PR #111),
  inventory-storage (PR #102), and workforce-management (PR #106). A
  future new writer added to this package should default to `Hash`
  unless there is a documented reason not to key by the aggregate id.
