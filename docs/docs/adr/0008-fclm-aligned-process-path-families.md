---
id: 0008-fclm-aligned-process-path-families
slug: /adr/0008-fclm-aligned-process-path-families
title: 0008. Seven new process-path families aligned to real FC labor-tracking vocabulary
sidebar_label: 0008. FCLM-aligned path families
description: ADR 0008 — PREP, PROBLEM_SOLVE, WATER_SPIDER, AMNESTY, RETURNS, TRANSFER, and DAMAGE join PICK/PACK/REBIN/SLAM as real, capability-gated process-path catalogue entries, deliberately grounded in a major e-commerce retailer's FCLM actual floor vocabulary rather than an invented taxonomy.
---

# 0008. Seven new process-path families aligned to real FC labor-tracking vocabulary

## Status

Accepted.

## Context

This service's catalogue held exactly four production-intent path
families — `PICK`, `PACK`, `REBIN`, `SLAM` — inherited unchanged from the
static YAML this service replaced (ADR 0001, ADR 0002). `STAGE` and `WRAP`
also exist in the live store, but as artifacts of the store/topic
divergence testing ADR 0002 documents, not as deliberately chosen
production families.

Four paths cover the platform's inbound-fulfillment/outbound-dispatch
core, but they leave real, common warehouse floor work with nowhere to
be dispatched as labor: an associate reworking a damaged carton, one
running a live problem-solve at a jammed conveyor, one covering a
water-spider replenishment lap, none of that work has ever had a
process-path home in this fleet. It either goes unrepresented, or gets
awkwardly folded into an existing path it doesn't actually belong to.

A real vocabulary — floor terminology and a major e-commerce retailer's FCLM (Fulfillment
Center Labor Management) hierarchy naming — was supplied as the basis
for closing this gap, explicitly caveated as "common vocabulary, not a
canonical spec" since naming drifts across building generations. Cross-
referencing it against this platform's own reference research
(`fulfillment-reference-ddd.md`) and this session's prior investigation
(see the Domain Vision page's "real e-commerce fulfillment flow, mapped"
section, `warehouse-docs` PR #9) surfaced a real tension: several of the
supplied families collide with architectural decisions this fleet
already made deliberately.

### What was excluded, and why

- **Dock, Receive, Decant, Stow, HRV Stow.** `inventory-storage` already
  owns Receive and Stow as OLTP writes against its `Stock` aggregate
  (`receiveStock`, `stowStock`) — real, e-commerce-accurate chaotic stow, not
  a simplification. A receiving/stowing associate's unit of work is
  "record this stock fact," not "claim the next task off a queue." These
  were never process-path candidates; adding them here would duplicate
  an aggregate this fleet already has, in the wrong bounded context.
- **Ship Sort, Sorter Induct, Chute Clear, Ship Dock/OB Dock, Fluid
  Load, Trailer Load, Pallet Wrap.** Equipment-tier (WCS) or physical
  dock/trailer logistics — exactly the boundary `fulfillment-execution`
  ADR-0015 draws as a structural anti-corruption-layer seam
  (`EquipmentCommandPort`, unimplemented by design, "buy don't build").
  Adding these as claimable process paths would put equipment vocabulary
  directly into the one place this fleet has deliberately kept it out
  of.
- **ICQA (Cycle Count, Bin Audit, etc.).** Genuinely overlaps
  `inventory-storage`'s existing `runCycleCount` REST endpoint. Whether
  cycle-counting should become a dispatched process-path labor queue
  that *calls* that endpoint, stay purely an `inventory-storage` action,
  or both, is an open question deliberately left unresolved rather than
  guessed at here — flagged, not decided, in this ADR.

None of the above is a gap this ADR closes; each is a decision this
fleet already made, being respected rather than quietly reversed by
adding a superficially-plausible new catalogue row.

### What genuinely fits

Seven families have no existing owner anywhere in the fleet, no
equipment-vocabulary conflict, and are structurally identical to
`PICK`/`PACK`/`REBIN`/`SLAM` — pull-dispatched, claimable, completable
labor, not an OLTP write and not equipment control:

`PREP`, `PROBLEM_SOLVE` (Inbound and Outbound problem-solve unified into
one family — the work itself is the same shape regardless of which side
of the building it happens on, and this fleet has no IB/OB split
anywhere else in the catalogue), `WATER_SPIDER`, `AMNESTY`, `RETURNS`,
`TRANSFER`, `DAMAGE`.

Each covers several named floor variants (e.g. `PREP` covers Prep
Bagging, Prep Bubble, Prep Labeling, Prep Kitting, Hazmat Prep) through
`matchPrefix`, exactly the way `PICK` already covers `pick-zone-a` or
`pick-soak` — one row per family, not one row per named variant, since
the granularity that already exists for the original four is the
established convention, not a new pattern being invented here.

### A real defect found while seeding these

Every attempt to `POST /process-paths` for these new families initially
failed with a `500` and `null value in column "topic" of relation
"outbox_events"` — not a hypothetical, an actual error against the live
cluster. Root cause: the running pod's image was built 2026-09-07, four
days before ADR 0007 (the analytics-topic outbox extension, merged
2026-09-11) landed. The Postgres migration behind ADR 0007 had already
run against the shared database — `outbox_events.topic` was already
`NOT NULL` — but the stale binary's `OutboxPublisher` predated the code
that populates it. The transactional outbox itself worked exactly as
designed: every failed attempt rolled back completely, with zero
partial/corrupt writes — verified directly against the store before and
after the fix. Rebuilding and rolling the deployment (`terraform taint`
+ `apply` + `kubectl rollout restart`) resolved it; all seven paths were
then created successfully and independently verified present on
`warehouse.process-path-management.events` via a direct Kafka consumer
read, confirming the fix was real and not merely a green HTTP response.
This is an infrastructure/deployment-currency defect, not a code defect
in `develop` — flagged here because it was discovered in the course of
this decision, not because this ADR changes anything about the outbox
implementation itself.

## Decision

**Define seven new Active process paths — `PREP`, `PROBLEM_SOLVE`,
`WATER_SPIDER`, `AMNESTY`, `RETURNS`, `TRANSFER`, `DAMAGE` — each with a
lower-case `matchPrefix` equal to its own kebab-case name and a single
`requiredCapabilities` entry matching that same name**, via this
service's existing `POST /process-paths` API — no schema change, no new
use case, no new domain concept. `PICK`/`PACK`/`REBIN`/`SLAM` already
proved this exact shape (one path, one capability, `matchPrefix` as the
family key); these seven are additional data under the identical model,
not a new pattern.

`IB Problem Solve` and `OB Problem Solve` are deliberately unified under
one `PROBLEM_SOLVE` path rather than split, since this fleet's catalogue
has no existing IB/OB distinction for any other family and the work
itself — diagnose and resolve a stuck task/jam/exception — is the same
labor shape regardless of which side of the building it occurs on. If a
real operational need to distinguish IB from OB problem-solve staffing
ever emerges, that is a `matchPrefix` convention (`problem-solve-ib`,
`problem-solve-ob`) each consumer's existing prefix rule already
supports without a schema change — not a reason to split the path today
on a need that does not yet exist.

## Consequences

### Easier

- **Real, common warehouse-floor work now has a place to be dispatched
  as labor**, closing a gap that existed simply because the original
  four families were carried over unchanged from a YAML file that only
  ever described this platform's inbound/outbound fulfillment core, not
  the full floor.
- **The vocabulary is grounded in how a major e-commerce retailer's FCLM hierarchy and real
  floor terminology actually name this work**, not an invented taxonomy
  — traceable to a real source rather than guessed at, the same
  discipline this session's Domain Vision research applied to the
  Pick/Pack/SLAM stages already in the catalogue.
- **A real, previously-latent outbox defect was caught and fixed** as a
  direct result of exercising a write path (`POST /process-paths`) that
  hadn't been exercised against this specific pod since ADR 0007
  shipped — this ADR's data change had a genuine, useful side effect on
  fleet health independent of the taxonomy decision itself.

### Harder

- **Seven new capability strings (`prep`, `problem-solve`,
  `water-spider`, `amnesty`, `returns`, `transfer`, `damage`) now exist
  in the published vocabulary with no consumer wired to react to them
  yet.** Per ADR 0001's own honestly-documented state, none of
  `fulfillment-execution`, `wes-work-planning`, or
  `workforce-management` has a consumer wired to this topic at all as
  of this ADR — these seven paths are exactly as unconsumed as the
  original four were at ADR 0001's writing, not a regression this ADR
  introduces.
- **ICQA remains a genuinely open question**, deliberately not decided
  here. Whether it becomes an eighth process-path family, stays inside
  `inventory-storage`, or becomes a path that calls into
  `inventory-storage`'s existing endpoint is real, unresolved scope —
  flagged rather than guessed at.
- **`PROBLEM_SOLVE`'s IB/OB unification is a judgment call that could be
  wrong.** If real operational staffing needs eventually require
  distinguishing IB from OB problem-solve headcount specifically (not
  just via `matchPrefix` at the reporting layer), this decision would
  need revisiting — noted honestly rather than assumed settled forever.
