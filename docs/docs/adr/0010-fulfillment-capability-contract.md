---
id: 0010-fulfillment-capability-contract
slug: /adr/0010-fulfillment-capability-contract
title: 0010. Process paths publish a fulfillment capability contract (cycle time, eligibility, CPT schedule)
sidebar_label: 0010. Fulfillment capability contract
description: ADR 0010 — a ProcessPath carries the static capability facts a promise engine needs (p95 cycle time, eligibility rules) and this service owns a site-scoped CPT schedule, so order-management can derive a delivery promise from path feasibility instead of an environment-variable lead time.
---

# 0010. Process paths publish a fulfillment capability contract (cycle time, eligibility, CPT schedule)

## Status

Accepted. Companion to order-management ADR 0014 (promise derived from
fulfillment capability), accepted together.

## Context

In a real fulfillment centre the customer-facing delivery promise and the
building's process paths are one mechanism seen from two sides. The
promise engine offers a date only if some path inside the building can
carry the order to a truck that leaves before that date; the process
path selection then constrains routing so the committed orders actually
fit. The coupling point is the Critical Pull Time (CPT): the last moment
a package can be manifested and still make a given departure. A major
e-commerce retailer's
process-path model publishes, per building, which paths exist, how long
each takes end to end, what each may carry, and which CPTs each can hit;
the promise is derived from that, never guessed independently of it.

This fleet has the skeleton of that coupling but not the substance:

- `order-management` computes `promiseDate = now + leadTime`
  (`order.LeadTimePolicy`), where `leadTime` comes from the environment
  variable `PROMISE_PATH_LEAD_TIMES` (e.g. `pick=24h,singles=6h`) with a
  48h default. The promise is a configuration guess made outside every
  bounded context that knows anything about the floor.
- `wes-work-planning` consumes `OrderAllocated.promise_date` and uses it
  literally as the work unit's CPT (`shared.NewCPT(data.PromiseDate)`).
  Because the promise is a continuous timestamp, two orders placed one
  minute apart get two different "CPTs" instead of the same truck, and
  the per-CPT `ChargeForecast` buckets WES already models cannot line up
  with the CPTs real work carries.
- This service's `ProcessPath` aggregate holds identity, `matchPrefix`,
  `direct`, `requiredCapabilities`, and an optional
  `destinationLocationRole` (ADR 0009). It says nothing about how long a
  path takes, what it may carry, or which departures it can feed. There
  is no bounded context anywhere in the fleet that owns a CPT schedule;
  the word "CPT" exists only as a value object in WES and
  fulfillment-execution, both of which receive it and neither of which
  defines it.
- `order-management`'s `PathSelectionPolicy` (its ADR 0013) resolves
  every line to `PICK` because, as that ADR states, "process-path-
  management's `RequiredCapabilities` field has no real fleet data to
  route against today". Routing needs eligibility facts a path declares
  about the work it accepts, not only the capabilities a worker must
  hold.

The consequence is exactly the failure mode the reference model exists to
prevent: the building absorbs the promise error as missed CPTs, and
nothing measures it. Fixing it requires one context to own and publish
the static half of path capability. This service is the fleet's Open
Host Service for the process-path Published Language (ADR 0001, 0002,
0003) with four Conformist consumers already replaying its topic, so it
is the natural owner. The alternative owners were considered and
rejected below.

### What is static capability and what is not

Two distinct things are easy to conflate:

- **Capability** — facts an operator declares and that change on the
  order of days or weeks: how long a path takes at p95, what a path may
  carry, which CPTs a path can feed. Reference configuration.
- **Capacity** — how many more units a path can absorb before a given
  CPT right now. Changes every second with every enqueue, release and
  completion. Operational state.

This ADR covers capability only. Capacity is operational state that
`wes-work-planning` already tracks (`WorkPool` WIP and rates,
`ChargeForecast` per CPT) and will publish under its own ADR; this
service must not become a mirror of WES state.

## Decision

We will make `process-path-management` the fleet's source of truth for
process-path **capability**, published on the existing
`warehouse.process-path-management.events` topic through the existing
transactional outbox (ADR 0003), so every current consumer receives it
with no new subscription.

### 1. `ProcessPath` gains two revisable capability facts

- `cycleTimeP95` (duration, required, positive). The end-to-end time
  from release into the path to manifest, at the 95th percentile, as
  the operator declares it. It is a declared standard, not a measured
  value: `labor-performance` measures, this service declares, and the
  gap between the two is a finding for `labor-performance`'s analytics,
  not a reason to couple the two contexts. Used by the promise engine to
  decide whether an order released now can make a CPT.
- `eligibility` (value object, required, may be permissive). Rules a
  unit of work must satisfy to be routed to this path, expressed over
  attributes the fleet already carries on the wire:
  `maxUnitsPerLine` (nil = unbounded; `1` is how a singles path is
  declared), `requiredProductAttributes` and
  `excludedProductAttributes` (sets over the existing product
  classification vocabulary — `hazmat`, `fragile` — plus `giftWrap`),
  and `nonSortable` (bool). A permissive eligibility (`{}`) is valid and
  is what `PICK` gets on migration, so today's behaviour is preserved
  exactly until an operator narrows it.

Both are revisable via `Revise` while Active, raise `ProcessPathUpdated`
when they change, and are frozen on deactivation, exactly like
`matchPrefix` and `requiredCapabilities`. Both are additive fields on
the existing `ProcessPathCreated`/`ProcessPathUpdated` payloads; the
migration backfills `cycleTimeP95` with a flat 2-hour default (no
single value was universal in the retired YAML's implied
`PROMISE_PATH_LEAD_TIMES`, so operators revise per path after the
migration) so no existing row is invalid.

### 2. A new `CPTSchedule` aggregate, site-scoped

A CPT is a property of a departure, not of a path: several paths feed
the same truck, and a slow path simply cannot make the later ones. So
the schedule is modelled once per site rather than duplicated on every
path:

```
CPTSchedule (aggregate root, id = siteId)
  timezone            IANA zone, e.g. America/Sao_Paulo
  cutoffs[]           CPTCutoff (entity)
    cptId             stable id within the site, e.g. "sp1-1500"
    localTime         "15:00" — recurring daily cutoff
    daysOfWeek        subset of Mon..Sun
    shipMethod        free-form label ("ground", "same-day"); no
                      carrier integration exists in this fleet
    eligiblePathIds   the path families that can make this cutoff
```

Invariants: at least one cutoff; cptIds unique within a site; every
`eligiblePathIds` entry names an Active `ProcessPath` in this service's
own store (checked at write time — this is the one cross-aggregate rule,
and it is inside one bounded context, so it is enforced in the use
case, not by a foreign key). Revising a schedule raises a new event,
`CPTScheduleChanged`, carrying the full schedule (a snapshot, like the
existing path events, so a fresh consumer replaying from `FirstOffset`
needs no prior state). The next concrete occurrence of a cutoff is
computed by consumers from `(localTime, daysOfWeek, timezone)`; this
service never publishes absolute timestamps for a recurring rule.

`siteId` uses `facility-layout`'s site identifier vocabulary but is not
validated against it: this service has zero inbound dependencies (ADR
0001) and that posture is kept. A schedule for an unknown site is an
operator error that shows up as an unroutable order in
order-management, not a coupling here.

### 3. REST and MCP surfaces

- `PUT /sites/{siteId}/cpt-schedule`, `GET /sites/{siteId}/cpt-schedule`
  on the existing REST adapter, RFC 7807 errors as today.
- `cycleTimeP95` and `eligibility` on the existing path create/revise
  DTOs and on `process-path-mfe`.
- Two read-only MCP tools (`get_cpt_schedule`, `list_process_paths`
  widened), consistent with ADR 0006's curated intent-level tool
  posture. No write tools.

### 4. What is explicitly out of scope here

- Publishing remaining capacity per CPT. That is `wes-work-planning`'s
  operational state and will be its own ADR there (working title
  `PathCapacityChanged`).
- Carrier or ship-method integration. `shipMethod` is a label.
- Rewriting how `wes-work-planning` maps promise to CPT. That changes
  when order-management ADR 0014 starts sending a `cptId` instead of a
  bare timestamp; WES's change is recorded in that ADR's rollout.

### Alternatives considered

- **A new `transportation` / `outbound-scheduling` bounded context owning
  the CPT schedule.** This is where a CPT schedule lives in a full
  production estate, next to carrier lanes and trailer plans, and it is
  the answer if this fleet ever gets a real transportation context.
  Rejected for now because a ninth-plus service whose only content is
  one aggregate with one event, consumed by one service, is a bounded
  context with no domain to bound; the placement here is recorded as
  provisional, and the aggregate is designed to lift out unchanged.
- **CPT cutoffs as a list on each `ProcessPath`.** Simplest, but it
  denormalises one departure onto N paths and makes "the 15:00 truck"
  a coincidence of N operator edits agreeing. Rejected.
- **Keeping the promise as `PROMISE_PATH_LEAD_TIMES` and just widening
  the durations.** Leaves the promise a guess and leaves nothing to
  measure it against. Rejected; this is the status quo.
- **Letting `order-management` own capability facts about paths.** It
  is the consumer of that information, not its author, and it does not
  own the path catalogue. Rejected as a boundary inversion.

## Consequences

**Easier**

- A delivery promise can be derived from declared path feasibility and
  a real departure schedule, which is the precondition for every
  promise KPI (on-time-to-CPT, re-promise rate) meaning anything.
- `order-management`'s `PathSelectionPolicy` finally has declared
  eligibility data to route against, unblocking the attribute-driven
  routing its ADR 0013 deferred.
- `wes-work-planning`'s `ChargeForecast` CPT buckets and the CPTs on
  real work units can share identifiers, making charge planning
  per-CPT rather than per-order-timestamp.
- Existing consumers need no new topic, consumer group or readiness
  gate: the same replay-from-`FirstOffset` cache pattern (per-instance
  consumer group, offset-target readiness) picks up the new payload
  fields and the new event type.

**Harder**

- Two more things operators must configure correctly before the promise
  is better than the lead-time fallback. A wrong `cycleTimeP95` produces
  confidently wrong promises; that is the same trade a real building
  makes, and the KPIs are what expose it.
- A provisional placement: `CPTSchedule` sits in a Generic Subdomain
  service because there is no Transportation context. If one appears,
  this aggregate and its event move, and `order-management` changes its
  subscription. That cost is accepted knowingly.
- Every consumer's `kafkacatalog` decoder must ignore the unknown event
  type and the new fields until it opts in. Verifying this across
  `order-management`, `wes-work-planning`, `fulfillment-execution` and
  `workforce-management` is part of the rollout, not an afterthought:
  a strict decoder that fails on an unknown `event_type` would stall its
  readiness gate on the first `CPTScheduleChanged` message.
- `eligibility` references the product classification vocabulary owned
  elsewhere in the fleet (`hazmat`, `fragile`). Adding a new attribute
  means agreeing on the name in two places. The vocabulary is small and
  changes rarely; a shared enum package was rejected as cross-repo
  coupling for the same reason the fleet already rejected shared code
  for `OrderRef`.

## Rollout (Phase 1 of the promise plan)

1. Migration `0005_capability.up.sql`: `cycle_time_p95` (interval, NOT
   NULL, backfilled with a flat `'2 hours'` default — no YAML-derived
   per-path backfill shipped; no single value was universal across the
   retired catalogue's path families, so the conservative flat default
   this service's own validation accepts was chosen and operators revise
   per path via PUT), `eligibility` (jsonb, NOT NULL, default `{}`), new
   `cpt_schedules` table.
2. Domain + use cases + tests (90% gate, gremlins baseline held).
3. `apis/openapi.yaml`, `apis/asyncapi.yaml` (new message, widened
   payloads), regenerate docs.
4. Decoder tolerance test in each of the four consumers, then this
   service's PR merges first.
5. Seed the real site schedule and per-path cycle times through the
   REST API against the cluster with `EVENT_PUBLISHER=kafka` live, so
   the outbox carries them; this also closes the still-open seeding gap
   from ADR 0002's cutover.
