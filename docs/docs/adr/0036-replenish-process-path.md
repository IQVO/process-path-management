---
id: 0036-replenish-process-path
slug: /adr/0036-replenish-process-path
title: "0036. A REPLENISH process path and a REPLENISHMENT demand source carry pick-face top-ups"
sidebar_label: "0036. REPLENISH process path"
description: "ADR 0036 — the closed intake vocabulary of ADR 0034 gains a fifth demand source, REPLENISHMENT, and the catalogue gains a REPLENISH path (matchPrefix replenish, direct, required capability replenish, intake REPLENISHMENT) so pick-face top-ups planned by wes-work-planning can become fulfillment-execution REPLENISH tasks. REPLENISHMENT is the only source that routes onto REPLENISH, and REPLENISH carries no other source. Declared in catalogue/paths.json and applied by scripts/seed_catalogue.py. Additive on REST, MCP and ProcessPathCreated/Updated; no migration, no new event. Companion to wes-work-planning ADR 0042 and fulfillment-execution ADR 0050."
---

# 0036. A REPLENISH process path and a REPLENISHMENT demand source carry pick-face top-ups

## Status

**Accepted** (2026-10-17). Companion to wes-work-planning ADR 0042 (a
`REPLENISH` work unit is released on the path id `REPLENISH`) and to
[fulfillment-execution ADR 0050](https://github.com/IQVO/fulfillment-execution/blob/develop/docs/docs/adr/0050-replenish-tasks-execute-pick-face-replenishment.md)
(`REPLENISH` is a sibling of `RELOCATE`, capability `replenish`). Follows
[ADR 0033](./0033-relocate-process-path.md) and
[ADR 0035](./0035-dispatch-process-path.md) (the path is catalogue data under
the existing `ProcessPath` model) and extends the closed vocabulary of
[ADR 0034](./0034-process-path-intake.md) by one value. No schema change, no
migration, no new event type.

## Context

Pick-face replenishment executes through the relocation chain:
pick-face-replenishment requests a top-up, wes-work-planning plans one
`REPLENISH` work unit and releases it on the `REPLENISH` path,
fulfillment-execution builds a `REPLENISH` task from the catalogue, and
inventory-storage records the move from the task's completion.

The catalogue is owned here. It is a published language: wes-work-planning,
fulfillment-execution and workforce-management replay
`warehouse.process-path-management.events` and resolve a released path id
against their copy. The catalogue declared PICK, PACK, REBIN, SLAM, RELOCATE
and DISPATCH only, so a released `REPLENISH` work unit matched nothing and
could not be accepted.

Two things are missing, and they are separate decisions:

1. **A path.** fulfillment-execution takes the task type from the path
   definition's id (`task.Type(pathDef.Id)`, see ADR 0035), so the id has to
   be `REPLENISH`, and the capability has to be one stations can hold.
2. **A demand source.** ADR 0034 made *who may route work onto a path* an
   explicit, closed field of the path. None of the four sources fits a
   pick-face top-up: it is not a customer order line, a stock-transfer leg, an
   approved slot-plan move or internal inventory control. Reusing
   `SLOT_RELOCATION` would let a slot-plan consumer route onto REPLENISH and
   would make the two flows indistinguishable in the catalogue. Declaring
   REPLENISH with an empty intake would hide the one source that is allowed to
   use it.

## Decision

### The demand source

`REPLENISHMENT` joins `CUSTOMER_ORDER`, `STOCK_TRANSFER`, `SLOT_RELOCATION`
and `INVENTORY_CONTROL` in the intake vocabulary:

| Demand source | Who routes work with it |
| --- | --- |
| `REPLENISHMENT` | pick-face top-ups from pick-face-replenishment, planned through wes-work-planning |

It is added exactly the way ADR 0034 prescribes for a closed, service-owned
vocabulary: one constant and one entry in the validation in `shared`
(`ParseIntake` rejects anything else with `invalid-intake`, 422 on REST),
the enum in `apis/openapi.yaml` (`DemandSource`) and in `apis/asyncapi.yaml`
(`intake` items), the error and MCP tool texts that list the vocabulary, and
this ADR. The set stays canonical (sorted, distinct), so `REPLENISHMENT` sorts
between `INVENTORY_CONTROL` and `SLOT_RELOCATION`. The enum is
additive: no existing value, field, event or golden changes.

### The path

One Active process path, defined through the same `DefinePath` use case,
outbox and `ProcessPathCreated` CloudEvent as every other path:

| Field | Value | Why |
| --- | --- | --- |
| `pathId` | `REPLENISH` | The canonical id, and therefore the fulfillment-execution task type and the wes-work-planning work kind. |
| `matchPrefix` | `replenish` | The family key. No existing prefix (`pick`, `pack`, `rebin`, `slam`, `relocate`, `dispatch`, ADR 0008's families) is a prefix of it or has it as a prefix, so the longest-prefix rule cannot confuse it. |
| `direct` | `true` | A released replenishment becomes one execution task, the same shape as RELOCATE. |
| `requiredCapabilities` | `["replenish"]` | One capability named after the family. Only stations and associates holding `replenish` can claim the task, and workforce-management plans headcount against it. |
| `cycleTimeP95` | `2h` | Copied from RELOCATE, the fleet's flat default (ADR 0010). It is not measured; see below. |
| `eligibility` | `{}` (permissive) | Eligibility describes the unit of work a path can take, not who may route onto it (ADR 0034). The intake keeps order lines off the path. |
| `intake` | `["REPLENISHMENT"]` | The only source that may route work onto REPLENISH. |
| `destinationLocationRole` | unset | Mirrors RELOCATE. |

Both halves of the routing rule are pinned by tests: `REPLENISH` carries no
other source, and no other declared path accepts `REPLENISHMENT`.
order-management considers only paths whose intake contains `CUSTOMER_ORDER`,
so REPLENISH is never chosen for a customer order line, whatever its cycle
time.

The entry is declared in `catalogue/paths.json` in the request-body shape of
`POST /process-paths`, and `scripts/seed_catalogue.py` converges it with the
table of ADR 0033: `POST` when absent, nothing when identical, `PUT` when a
revisable field differs, an error for an immutable difference, never a
resurrection of a deactivated path. Re-running the seed against an environment
that already has REPLENISH changes nothing. Nothing at boot reads the file.

### Why a closed enum

Which demand may enter a path is routing policy, and routing policy belongs to
the catalogue owner. A consumer that could extend the set by publishing a new
string would be able to route work onto a path without this service, the
reviewed `paths.json` or the declared-catalogue tests ever seeing it. A closed
enum makes a new source a code change plus an ADR here, which is the intended
cost for a routing-policy enumeration (ADR 0034, Consequences). Consumers that
decode `intake` as free strings (wes-work-planning's catalogue consumer does)
need no change; a consumer that validates against a fixed list must accept the
new value.

## What is not done

- **No eligibility rules.** REPLENISH is permissive like RELOCATE. Nothing
  narrows which units of work it can carry.
- **No cycle-time tuning.** `cycleTimeP95` is the flat 2h copied from
  RELOCATE. It is not a measured value; operators revise it with `PUT` once
  there is data.
- **No new event type, REST route, MCP tool or table.** `intake` already
  exists on every `ProcessPath` response and on `ProcessPathCreated`/`Updated`.
- **The frozen file-based catalogue in warehouse-infra
  (`config/process-paths/sortable-fc.yaml`, rollback-only) is not touched.**
- **The seed is not applied to any running cluster by this change.** That is a
  separate, operator-held step.

## Consequences

**Easier**

- wes-work-planning can release `REPLENISH` work, fulfillment-execution can
  accept it, and workforce-management can plan headcount against the
  `replenish` capability, all from the same live replay of the topic.
- A new environment gets the path from the same idempotent command as RELOCATE
  and DISPATCH (`make seed-catalogue`).
- The wire shape is pinned by a golden test of the `ProcessPathCreated` event
  on both streams.

**Harder, and who must set what**

- **The seed must run before the first replenishment is released.** An
  environment that skips it has no REPLENISH path, and the work is refused
  (fail-closed: refused, not routed elsewhere). Consumers apply the event
  live, so no restart is needed.
- **Stations that execute the task need the capability `replenish`.** Without
  it nobody can claim a REPLENISH task.
- **The vocabulary is still closed.** A sixth demand source needs a code
  change and an ADR here.
- **wes-work-planning does not yet filter on `REPLENISHMENT`.** It plans on the
  explicit path id `REPLENISH` and carries the declared intake verbatim, so a
  catalogue event with the new value decodes without a change there. Its tests
  that stand in a placeholder intake for REPLENISH can adopt the real value
  later.
- The path id `REPLENISH` is effectively permanent: a deactivated id is never
  resurrected (ADR 0033).
