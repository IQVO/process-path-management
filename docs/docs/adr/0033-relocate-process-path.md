---
id: 0033-relocate-process-path
slug: /adr/0033-relocate-process-path
title: "0033. A RELOCATE process path carries slot-plan moves as floor work"
sidebar_label: "0033. RELOCATE process path"
description: "ADR 0033 — the catalogue gains a RELOCATE path (matchPrefix relocate, direct, required capability relocate) so wes-work-planning can release approved slot-plan moves to fulfillment-execution as ordinary floor work. Its eligibility requires the slot-relocation attribute, so order-management never routes a customer order line onto it. The entry is declared in catalogue/paths.json and applied through POST /process-paths by scripts/seed_catalogue.py. Companion to slotting-optimization ADR 0010."
---

# 0033. A RELOCATE process path carries slot-plan moves as floor work

## Status

**Accepted** (2026-10-13). Companion to slotting-optimization
[ADR 0010 "Approved slot plans are executed as WES relocation work"](https://github.com/IQVO/slotting-optimization/tree/develop/docs/docs/adr)
(Option A of the 2026-10-13 decision log, which supersedes slotting ADR 0007).
No schema change, no new event, no migration: the path is catalogue data
under the existing `ProcessPath` model (ADR 0008 set the same precedent).

## Context

slotting-optimization produces approved slot plans (`SlotPlanApproved`: one
`Relocate`, `Vacate` or `Assign` move per SKU). A forward-slot move is
physical work on the floor. Slotting ADR 0010 decides that the fleet's
existing floor-work chain executes it:

- **wes-work-planning** plans one RELOCATE work unit per move, sequences
  the moves inside a plan, and releases them through its normal release
  flow.
- **process-path-management** (this service) routes the work: a released
  unit names a path id, and every consumer resolves it against this
  catalogue (`warehouse.process-path-management.events`, Kafka catalogue
  mode, prefix match `id == prefix` or `id` starts with `prefix + "-"`,
  longest prefix wins).
- **fulfillment-execution** turns the released unit into a task of type
  `RELOCATE` that an operator holding the required capability claims and
  completes. Its `TaskCompleted` then drives inventory-storage's
  `RelocateStock`.

The catalogue had no path for this work. wes-work-planning validates the
`path_id` of every released unit against its catalogue copy and rejects an
unknown one, and fulfillment-execution builds `Task.RequiredCapabilities`
from the path. Without a catalogue entry, relocation work could not be
released or staffed.

One more consumer behaviour constrains the entry. order-management's
`PathSelectionPolicy` (its ADR-0021) routes each customer order line to the
**eligible active path with the shortest known `cycleTimeP95`** and breaks
ties on the lower path id. It considers every active path in the catalogue.
A RELOCATE path with permissive eligibility (`{}`) could therefore win a
customer order line, for example if an operator later gives it a shorter
cycle time than `PICK`. Slot moves are not order fulfilment, so that must
not happen.

## Decision

Define one Active process path, through the same `DefinePath` use case,
outbox and `ProcessPathCreated` CloudEvent as every other path:

| Field | Value | Why |
| --- | --- | --- |
| `pathId` | `RELOCATE` | The canonical id. It is also the fulfillment-execution task type and the wes-work-planning work kind that slotting ADR 0010 names. |
| `matchPrefix` | `relocate` | The family key. wes-work-planning may release `relocate` or a qualified form such as `relocate-zone-a`. No existing prefix (`pick`, `pack`, `rebin`, `slam`, ADR 0008's families, `transfer-dispatch`) is a `-`-separated prefix of it, and it is not one of theirs, so the longest-prefix rule cannot confuse it. |
| `direct` | `true` | A released relocation unit becomes one execution task, the same shape as the other paths. |
| `requiredCapabilities` | `["relocate"]` | One capability named after the family, the convention PICK/PACK/REBIN/SLAM and ADR 0008 use. Only stations and associates certified for slot moves can claim the task, and workforce-management plans headcount against it. |
| `cycleTimeP95` | `2h` | The fleet's flat default (ADR 0010 of this service). It feeds the promise engine only, and RELOCATE is never a promise candidate (next row). Operators revise it with `PUT` when they measure it. |
| `eligibility` | `{"requiredProductAttributes": ["slot-relocation"]}` | A customer order line never carries the `slot-relocation` attribute. order-management's `lineEligible` therefore rejects every line for RELOCATE, whatever its cycle time, and the path is never chosen for customer orders. wes-work-planning plans relocation units directly on the path and does not evaluate eligibility, so the rule does not block the work it exists for. |
| `destinationLocationRole` | unset | A move ends in a forward slot or a reserve bin, not in a Drop, WorkCenter or Shipping location. |

The declaration lives in the repository as data, in the request-body shape of
`POST /process-paths`: `catalogue/paths.json`. `scripts/seed_catalogue.py`
applies it idempotently through the public REST API. Paths still exist only
as data in this service's store. The file is not a second source of truth,
and nothing at boot reads it. It is the reviewed, versioned record of what
an environment must define, and its seed writes through the same use case,
outbox and topic as an operator would:

| Store state vs declaration | Action |
| --- | --- |
| absent | `POST /process-paths`, which publishes `ProcessPathCreated` |
| present and identical | nothing (no spurious `ProcessPathUpdated`) |
| present, a revisable field differs | `PUT /process-paths/{pathId}`, which publishes `ProcessPathUpdated` |
| present, `direct` or `destinationLocationRole` differs | reported as an error; those fields are immutable (ADR 0009), so a new `pathId` is needed |
| present and `DEACTIVATED` | reported and skipped, never resurrected (a deactivated id is permanent) |

Ordering: the RELOCATE entry must be in the topic before wes-work-planning
releases the first relocation unit. Run the seed when this change is
deployed, before slotting-optimization approves a plan in that environment.
The consumers apply the event live, so no restart is needed. A CPT schedule
never lists RELOCATE: relocation work has no truck to make.

## Consequences

**Easier**

- Slot moves reach the floor through the existing planner, router and
  executor. Labour planning, capability gating, stuck-task diagnostics and
  telemetry apply to them with no new machinery.
- A new environment (kind cluster, e2e harness) gets the path from one
  idempotent command instead of an undocumented `curl`.
- No consumer changes for the catalogue itself. All five consumers already
  decode `ProcessPathCreated`, and order-management already evaluates
  `requiredProductAttributes`.

**Harder**

- `slot-relocation` is a convention in the product-attribute vocabulary:
  nothing may ever classify a product with it. It is documented here and in
  the ubiquitous language. If order-management ever gains an explicit
  "not routable for orders" flag, this entry should move to it.
- The path is data. An environment that skips the seed has no RELOCATE path,
  and wes-work-planning then rejects relocation units as an unknown path id
  (fail-closed: the work is refused, not silently routed elsewhere).
- ADR 0008's families were defined with permissive eligibility. They are
  equally visible to order-management's path selection. This ADR does not
  change them; it is noted as a follow-up for the catalogue owners.
