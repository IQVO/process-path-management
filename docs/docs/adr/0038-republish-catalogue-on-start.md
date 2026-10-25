---
id: 0038-republish-catalogue-on-start
slug: /adr/0038-republish-catalogue-on-start
title: "0038. Republish the whole current catalogue at startup"
sidebar_label: "0038. Republish catalogue on start"
description: "ADR 0038 — consumers rebuild their path catalogue by replaying warehouse.process-path-management.events from the earliest offset, and the broker's 7-day retention had expired the creation events of older paths, so freshly started consumers lacked PICK/PACK/REBIN/RELOCATE/SLAM and order-management answered 422 line-ineligible-for-resolved-path to every plain order. process-path-management now re-enqueues one ProcessPathUpdated per ACTIVE path and one CPTScheduleChanged per schedule through the transactional outbox once at every start, sorted and bounded, integration topic only, never fatal and never silent. No wire change, no new environment variable. Alternatives rejected: topic compaction, longer retention."
---

# 0038. Republish the whole current catalogue at startup

## Status

**Accepted** (2026-10-25). Extends [ADR 0003](./0003-transactional-outbox.md)
(the republish uses the same outbox) and [ADR 0010](./0010-fulfillment-capability-contract.md)
(the self-sufficient `ProcessPathUpdated` / `CPTScheduleChanged` snapshots).
No change to any event type, payload, `dataschema`, topic or environment
variable.

## Context

Five services (order-management, fulfillment-execution, wes-work-planning,
workforce-management, network-fulfillment) hold the path catalogue, and
order-management and network-fulfillment also the CPT schedules, in memory.
None of them calls this service (no REST, MCP or gRPC between contexts; this repo's AGENTS.md). Each rebuilds its copy on every
start by replaying `warehouse.process-path-management.events` **from the
earliest offset** under a fresh, process-unique consumer group, and reports
ready once it has read up to the end offsets it saw at start.

That design assumes the topic still holds the history. It does not have to:
the cluster broker's default retention is 7 days and topics are auto-created
with that default. A path's `ProcessPathCreated` is written once, when the
path is defined, and every later event for it is a revision that may never
happen. After a week the creation events of the paths seeded at bootstrap
expire. Seen live on 2026-10-25: the topic held 7 events and none for `PICK`,
`PACK`, `REBIN`, `RELOCATE` or `SLAM`. After the Phase-5 paths `FULL_CASE` and
`CROSS_DOCK` (which require product attributes, ADR 0034) were added,
order-management's replayed catalogue had `CUSTOMER_ORDER` candidates but none
eligible for a plain line, so **every plain `POST /orders` answered 422
`line-ineligible-for-resolved-path`** until `PICK` was re-published by hand (a
revision of `cycleTimeP95` and back). A no-op `RevisePath` deliberately does
not republish (the `changed` guard), so there was no supported way to do it.

The failure is silent and delayed: a service works for as long as the
consumers that were running when the events were written keep their in-memory
copy, and breaks at the first restart after retention expired them. It is the
same class of defect as a cache that cannot be rebuilt from its source of
truth.

## Decision

At every start, `cmd/pathmgmt` runs `usecases.RepublishCatalogue` once, after
the migrations and the connection ping, before the listener binds and before
the outbox relay starts. It enqueues, through the outbox:

1. one `ProcessPathUpdated` per **ACTIVE** path, in ascending `pathId`
   order. The payload is built by the same function `RevisePath` uses
   (`processPathUpdated`): the path's full current state (`match_prefix`,
   `direct`, `required_capabilities`, `destination_location_role`,
   `cycle_time_p95`, `eligibility`, `intake`);
2. then one `CPTScheduleChanged` per site, in ascending `siteId` order, with
   the schedule's current timezone and cutoffs (`cptschedule.ToSnapshot`).

Properties:

- **Same outbox, one transaction.** The whole republish is one unit of work
  and the events go through `postgres.OutboxPublisher`, so they are stored
  as already-encoded CloudEvents with a stable `id`, relayed in order by the
  existing `OutboxRelay`, and survive a Kafka outage like any other event. It
  does not write to the broker directly.
- **Deactivated paths are not republished.** A cache that never saw the path
  cannot hold it, and one that did processed the `ProcessPathDeactivated`.
  Republishing a deactivated path as `Updated` would resurrect it.
- **No stale snapshot can overtake a newer event.** Between reading a path and
  committing its outbox row, a concurrent `RevisePath` / `DefineCPTSchedule`
  could commit a newer state; the outbox orders rows by id, so the older
  snapshot would then be relayed last and consumers (last write wins) would
  end on stale state. The republish therefore takes `FOR SHARE` on every path
  it publishes (`LockByIDsForShare`, ADR 0028, ascending) and on every
  schedule (`CPTScheduleRepo.LockAllForShare`, ascending) and publishes the
  state it reads under the lock. A concurrent write either committed first
  (and is what gets republished) or waits and enqueues its event after. A
  path deactivated after the unlocked listing is judged on its committed state
  and skipped. Lock order (paths, then schedules, each ascending) matches
  `DefineCPTSchedule`, so no cycle.
- **Deterministic and bounded.** Sorted by id; at most
  `MaxRepublishedPerKind` (1000) paths and 1000 schedules per start. The
  catalogue is operator-defined and tens of entries; the cap only protects the
  startup transaction. What exceeds it is counted in `RepublishResult.Omitted`
  and logged at ERROR, never dropped silently.
- **Integration topic only.** The republish uses an `OutboxPublisher` with the
  integration encoder alone. The analytics projector counts every
  `ProcessPathUpdated` as a path revision (`paths_revised`, ADR 0007) and
  claims by `eventId`; mirrored onto the analytics topic, every restart and
  every replica would inflate the revised-paths report with changes that
  never happened. A republish is not a catalogue change, so it is not an
  analytics occurrence. The wire contract of both topics is unchanged (no
  CloudEvents extension attribute, no payload change).
- **Never fatal, never silent.** Neither the API nor the relay depends on it,
  and crash-looping a healthy pod over a retention safety net would be an
  availability regression. A failure (or an over-cap truncation) is logged
  at `ERROR` and counted:
  `process_path_management.catalogue.republished{kind=path|schedule,outcome=accepted}`
  counts enqueued events; a failed run adds
  `{outcome=failed}`. It is bounded by a 30 s timeout. A failed run is
  all-or-nothing (one transaction), so a consumer never sees half a
  catalogue from it. The next start tries again.
- **No new configuration.** There is no switch: the republish is always on.

## Consumers: re-applying an `Updated` is an idempotent upsert

Read on `origin/develop` of each repository on 2026-10-25:

| Consumer | Behaviour on `ProcessPathUpdated` / `CPTScheduleChanged` |
| --- | --- |
| order-management `internal/adapters/outbound/kafkacatalog/consumer.go` | `handle` (l.448-473) treats `Created` and `Updated` identically and calls `applyUpsert` (l.475-490), which overwrites `c.paths[upper(pathId)]` with a freshly built definition. The doc comment (l.446-447) states: "last-write-wins snapshot per path, so no id dedupe is needed: re-applying the same event is idempotent". |
| order-management `.../kafkacptschedule/consumer.go` | `handle` (l.321-347): "each `CPTScheduleChanged` is a full per-site snapshot, so re-applying the same event is idempotent"; it assigns `c.schedules[siteId]`. |
| fulfillment-execution `.../kafkacatalog/consumer.go` | `handle` (l.373-398) routes `Created`/`Updated` to `applyUpsert` (l.400-410), a map assignment keyed by upper-cased `pathId`. |
| wes-work-planning `.../kafkacatalog/consumer.go` | `handle` (l.343-367) routes both to `applyUpsert` (l.370-380), a map assignment. |
| workforce-management `.../kafkacatalog/consumer.go` | `handle` (l.356-381): "a redelivered id is an idempotent overwrite and needs no id-based dedupe"; `applyUpsert` (l.383-392) is a map assignment. |
| network-fulfillment `.../processpathcache/consumer.go` | `handle` (l.423-454) routes `Created`/`Updated` to `applyPath` (l.456-465, assignment to `c.paths[pathId]`) and `CPTScheduleChanged` to `applySchedule` (l.473-477, assignment to `c.schedules[siteId]`). |
| labor-performance | Does not consume this topic. |

All of them keep only the last state per key, do not dedupe on the CloudEvents
`id`, and ignore unknown types. A republished event for a known path is
therefore a write of the same state; for an unknown path it is the missing
creation. Because the key is `pathId` / `siteId` (same partition) and the
republish happens before any later write, per-key order is preserved.

The one consumer that is **not** idempotent is this service's own analytics
projector (`cmd/pathmgmt-projector`): `ApplyProcessPathUpdated` is idempotent
on `eventId`, but each republished event has a new id and would add one to
`paths_revised`. That is why the republish stays off the analytics topic (see
above) rather than being tagged: the projector never sees it.

## Alternatives considered

- **Topic compaction (`cleanup.policy=compact`).** The structural cure for a
  keyed "latest state" topic: the broker keeps the last event per `path_id` /
  `site_id` and a replay needs no history. Not chosen here because it is not
  this service's to decide: topics are auto-created by the cluster broker with
  default configuration and this service never creates or configures topics
  (ADR 0037), so it is a warehouse-infra change (topic provisioning with
  per-topic configs) that must also settle how a `ProcessPathDeactivated`
  record is eventually dropped. It also repairs nothing already expired: a
  topic that lost its events would still need a one-off republish. It is
  compatible with this decision and can be added later; the republish then
  becomes belt and braces.
- **Longer (or infinite) retention.** Moves the cliff, does not remove it: a
  path created a year ago and never revised still expires. It also keeps every
  revision forever, so replay time grows with history rather than with the
  size of the catalogue. Rejected.
- **A REST endpoint or MCP tool to republish on demand.** Needs an operator
  to remember, and the failure it fixes is a cold start nobody is watching.
  It would also have to be secured (ADR 0005 removed REST auth) and
  rate-limited. Rejected; the start-up run is the default and needs no action.
- **Make the consumers read the catalogue from this service's REST API.**
  Forbidden: no context calls another (AGENTS.md), and a live lookup couples
  availability.
- **Republish periodically.** Unneeded load, a standing source of events with
  new ids, and no benefit over start-up: a consumer only needs the state when
  it starts, and the topic keeps an event until retention expires. Can be
  revisited if consumers ever start without this service having restarted
  within the retention window.
- **Tag the republish with a CloudEvents extension attribute** so the
  projector can skip it. Rejected in favour of keeping it off the analytics
  topic: no wire change at all.

## Consequences

**Easier**

- A consumer started at any time reads the current state of every ACTIVE path
  and every schedule within the retention window of the most recent start of
  this service, with no manual step and no dependence on when a path was
  created.
- The fix for the incident is permanent, not an operator's hand-revision.

**Harder, and what to know**

- **Every start adds ~(paths + schedules) events to the topic** (tens, a few
  KB) and every replica does it, so a rolling deploy of N replicas adds N
  copies. Consumers upsert, so this is harmless; replay time grows by a
  negligible constant per deploy until retention drops the old copies.
- **The topic is no longer a pure log of changes.** A `ProcessPathUpdated` on
  it can be a republish of an unchanged path. Do not count them as revisions;
  the analytics stream, which is the source for the revised-paths report,
  does not carry them.
- **The guarantee is "as of the last start of this service".** A consumer that
  starts after retention expired the last republish while this service has
  not restarted since (a quiet week) will still miss paths; restarting the
  service republishes. Compaction (above) is the structural cure.
- A failed run is visible only in the ERROR log and the
  `outcome=failed` counter; alert on it.
- Writes that race with the start-up transaction wait for it (a few
  milliseconds on a catalogue of this size) on the rows it share-locks.

## Out of scope

- Topic configuration (compaction, retention): warehouse-infra.
- Changing any consumer. None needs to change.
